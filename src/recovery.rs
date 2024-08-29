// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    batch::PartitionKey,
    file::{
        FLUSH_MARKER, FLUSH_PARTITIONS_LIST, JOURNALS_FOLDER, LSM_MANIFEST_FILE, PARTITIONS_FOLDER,
        PARTITION_DELETED_MARKER,
    },
    journal::Journal,
    partition::options::Options as PartitionOptions,
    HashMap, Keyspace, PartitionHandle,
};
use lsm_tree::{AbstractTree, AnyTree};

/// Recovers partitions
pub fn recover_partitions(keyspace: &Keyspace) -> crate::Result<()> {
    let partitions_folder = keyspace.config.path.join(PARTITIONS_FOLDER);

    #[allow(clippy::significant_drop_tightening)]
    let mut partitions_lock = keyspace.partitions.write().expect("lock is poisoned");

    for dirent in std::fs::read_dir(&partitions_folder)? {
        let dirent = dirent?;
        let partition_name = dirent.file_name();
        let partition_path = dirent.path();

        assert!(dirent.file_type()?.is_dir());

        log::trace!("Recovering partition {:?}", partition_name);

        // NOTE: Check deletion marker
        if partition_path.join(PARTITION_DELETED_MARKER).try_exists()? {
            log::debug!("Deleting deleted partition {:?}", partition_name);

            // IMPORTANT: First, delete the manifest,
            // once that is deleted, the partition is treated as uninitialized
            // even if the .deleted marker is removed
            //
            // This is important, because if somehow `remove_dir_all` ends up
            // deleting the `.deleted` marker first, we would end up resurrecting
            // the partition
            let manifest_file = partition_path.join(LSM_MANIFEST_FILE);
            if manifest_file.try_exists()? {
                std::fs::remove_file(manifest_file)?;
            }

            std::fs::remove_dir_all(partition_path)?;

            continue;
        }

        // NOTE: Check for marker, maybe the partition is not fully initialized
        if !partition_path.join(LSM_MANIFEST_FILE).try_exists()? {
            log::debug!("Deleting uninitialized partition {:?}", partition_name);

            std::fs::remove_dir_all(partition_path)?;

            continue;
        }

        let partition_name = partition_name
            .to_str()
            .expect("should be valid partition name");

        let path = partitions_folder.join(partition_name);

        let mut base_config = lsm_tree::Config::new(path)
            .descriptor_table(keyspace.config.descriptor_table.clone())
            .block_cache(keyspace.config.block_cache.clone())
            .blob_cache(keyspace.config.blob_cache.clone());

        // TODO: 2.0.0 recover
        let recovered_config = PartitionOptions::default();
        base_config.bloom_bits_per_key = recovered_config.bloom_bits_per_key;
        base_config.data_block_size = recovered_config.data_block_size;
        base_config.index_block_size = recovered_config.index_block_size;
        base_config.bloom_bits_per_key = recovered_config.bloom_bits_per_key;
        base_config.compression = recovered_config.compression;
        base_config.blob_compression = recovered_config.blob_compression;
        base_config.blob_file_target_size = recovered_config.blob_file_target_size;
        base_config.blob_file_separation_threshold =
            recovered_config.blob_file_separation_threshold;

        let is_blob_tree = partition_path
            .join(lsm_tree::file::BLOBS_FOLDER)
            .try_exists()?;

        let tree = if is_blob_tree {
            AnyTree::Blob(base_config.open_as_blob_tree()?)
        } else {
            AnyTree::Standard(base_config.open()?)
        };

        let partition =
            PartitionHandle::from_keyspace(keyspace, tree, partition_name.into(), recovered_config);

        // Add partition to dictionary
        partitions_lock.insert(partition_name.into(), partition.clone());

        log::trace!("Recovered partition {:?}", partition_name);
    }

    Ok(())
}

#[allow(clippy::too_many_lines)]
pub fn recover_sealed_memtables(keyspace: &Keyspace) -> crate::Result<()> {
    use crate::journal::partition_manifest::PartitionManifest;

    #[allow(clippy::significant_drop_tightening)]
    let mut flush_manager_lock = keyspace.flush_manager.write().expect("lock is poisoned");

    #[allow(clippy::significant_drop_tightening)]
    let mut journal_manager_lock = keyspace.journal_manager.write().expect("lock is poisoned");

    #[allow(clippy::significant_drop_tightening)]
    let partitions_lock = keyspace.partitions.read().expect("lock is poisoned");

    let journals_folder = keyspace.config.path.join(JOURNALS_FOLDER);
    let mut dirents = std::fs::read_dir(journals_folder)?.collect::<std::io::Result<Vec<_>>>()?;
    dirents.sort_by_key(std::fs::DirEntry::file_name);

    log::trace!(
        "looking for sealed journals in potentially {} found journals",
        dirents.len()
    );

    for dirent in dirents {
        let journal_path = dirent.path();

        assert!(dirent.file_type()?.is_dir(), "journal should be directory");

        // IMPORTANT: Check if journal is sealed
        if !dirent.path().join(FLUSH_MARKER).try_exists()? {
            continue;
        }

        log::debug!("Recovering sealed journal: {journal_path:?}");

        let journal_size = fs_extra::dir::get_size(&journal_path)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e.kind)))?;

        log::trace!("Reading sealed journal at {journal_path:?}");

        // Only consider partitions that are registered in the journal
        let file_content = std::fs::read_to_string(journal_path.join(FLUSH_PARTITIONS_LIST))?;
        let partitions_to_consider = PartitionManifest::from_str(&file_content)?;

        log::trace!(
            "Journal contains data of {} partitions",
            partitions_to_consider.len()
        );

        let mut partition_seqno_map: HashMap<PartitionKey, _> = HashMap::default();

        // NOTE: Only get the partitions that have a lower seqno than the journal
        // which means there's still some unflushed data in this sealed journal
        for entry in partitions_to_consider {
            let Some(partition) = partitions_lock.get(entry.partition_name) else {
                // Partition was probably deleted
                log::trace!("Partition {} does not exist", entry.partition_name);
                continue;
            };

            let partition_lsn = partition.tree.get_highest_persisted_seqno();
            let has_lower_lsn =
                partition_lsn.map_or(true, |partition_lsn| entry.seqno > partition_lsn);

            if has_lower_lsn {
                partition_seqno_map.insert(
                    entry.partition_name.into(),
                    crate::journal::manager::PartitionSeqNo {
                        lsn: entry.seqno,
                        partition: partition.clone(),
                    },
                );
            } else {
                log::trace!(
                    "Partition {} has higher seqno ({partition_lsn:?}), skipping",
                    entry.partition_name
                );
            }
        }

        log::trace!(
            "Recovering sealed memtables for partitions: {:#?}",
            partition_seqno_map.keys()
        );
        let reader = Journal::get_reader(&journal_path)?;

        for batch in reader {
            let batch = batch?;

            for item in batch.items {
                if let Some(handle) = partition_seqno_map.get(&item.partition) {
                    let tree = &handle.partition.tree;

                    match item.value_type {
                        lsm_tree::ValueType::Value => {
                            tree.insert(item.key, item.value, batch.seqno);
                        }
                        lsm_tree::ValueType::Tombstone => {
                            tree.remove(item.key, batch.seqno);
                        }
                        lsm_tree::ValueType::WeakTombstone => {
                            tree.remove_weak(item.key, batch.seqno);
                        }
                    }
                }
            }
        }

        log::trace!("Sealing recovered memtables");
        let mut recovered_count = 0;

        for handle in partition_seqno_map.values() {
            let tree = &handle.partition.tree;
            let memtable_id = tree.get_next_segment_id();

            if let Some((_, sealed_memtable)) = tree.rotate_memtable() {
                // Maybe the memtable has a higher seqno, so try to set to maximum
                let maybe_next_seqno = tree.get_highest_seqno().map(|x| x + 1).unwrap_or_default();

                keyspace
                    .seqno
                    .fetch_max(maybe_next_seqno, std::sync::atomic::Ordering::AcqRel);

                log::debug!("Keyspace seqno is now {}", keyspace.seqno.get());

                // IMPORTANT: Add sealed memtable size to current write buffer size
                keyspace
                    .write_buffer_manager
                    .allocate(sealed_memtable.size().into());

                // TODO: unit test write buffer size after recovery

                // IMPORTANT: Add sealed memtable to flush manager, so it can be flushed
                flush_manager_lock.enqueue_task(
                    handle.partition.name.clone(),
                    crate::flush::manager::Task {
                        id: memtable_id,
                        sealed_memtable,
                        partition: handle.partition.clone(),
                    },
                );

                recovered_count += 1;
            };
        }

        log::trace!("Recovered {recovered_count} sealed memtables");

        // IMPORTANT: Add sealed journal to journal manager
        journal_manager_lock.enqueue(crate::journal::manager::Item {
            partition_seqnos: partition_seqno_map.into_values().collect(),
            path: journal_path.clone(),
            size_in_bytes: journal_size,
        });

        log::trace!("Requeued sealed journal at {:?}", journal_path);
    }

    Ok(())
}
