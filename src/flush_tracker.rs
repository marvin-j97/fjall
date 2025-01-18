// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    batch::PartitionKey,
    flush::manager::{FlushTaskQueue, Task},
    journal::{
        batch_reader::JournalBatchReader,
        manager::{EvictionWatermark, Item, JournalItemQueue},
        reader::JournalReader,
        writer::Writer,
    },
    keyspace::Partitions,
};
use lsm_tree::{AbstractTree, SequenceNumberCounter};
use std::{
    path::PathBuf,
    sync::{Arc, MutexGuard, RwLock},
};

/// Keeps track of and flushes sealed journals and memtables
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct FlushTracker {
    /// It caps the write buffer size by flushing memtables to disk segments.
    tasks: FlushTaskQueue,
    /// It checks on-disk journal size and flushes memtables if needed, to
    /// garbage collect sealed journals.
    items: JournalItemQueue,
}

impl Drop for FlushTracker {
    fn drop(&mut self) {
        log::trace!("Dropping flush tracker");

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::decrement_drop_counter();
    }
}

impl FlushTracker {
    pub fn new(active_path: PathBuf) -> FlushTracker {
        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Self {
            tasks: FlushTaskQueue::new(),
            items: JournalItemQueue::from_active(active_path),
        }
    }

    #[allow(clippy::too_many_lines)]
    pub fn recover_sealed_memtables(
        &self,
        partitions: &RwLock<Partitions>,
        seqno: &SequenceNumberCounter,
        sealed_journal_paths: impl Iterator<Item = PathBuf>,
    ) -> crate::Result<()> {
        #[allow(clippy::significant_drop_tightening)]
        let partitions_lock = partitions.read().expect("lock is poisoned");

        for journal_path in sealed_journal_paths {
            log::debug!("Recovering sealed journal: {journal_path:?}");

            let journal_size = journal_path.metadata()?.len();

            log::debug!("Reading sealed journal at {journal_path:?}");

            let raw_reader = JournalReader::new(&journal_path)?;
            let reader = JournalBatchReader::new(raw_reader);

            let mut watermarks: Vec<EvictionWatermark> = Vec::new();

            for batch in reader {
                let batch = batch?;

                for item in batch.items {
                    if let Some(handle) = partitions_lock.get(&item.partition) {
                        let tree = &handle.tree;

                        match watermarks.binary_search_by(|watermark| {
                            watermark.partition.name.cmp(&item.partition)
                        }) {
                            Ok(index) => {
                                if let Some(prev) = watermarks.get_mut(index) {
                                    prev.lsn = prev.lsn.max(batch.seqno);
                                }
                            }
                            Err(index) => {
                                watermarks.insert(
                                    index,
                                    EvictionWatermark {
                                        partition: handle.clone(),
                                        lsn: batch.seqno,
                                    },
                                );
                            }
                        };

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

            log::debug!("Sealing recovered memtables");
            let mut recovered_count = 0;

            for handle in &watermarks {
                let tree = &handle.partition.tree;

                let partition_lsn = tree.get_highest_persisted_seqno();

                // IMPORTANT: Only apply sealed memtables to partitions
                // that have a lower seqno to avoid double flushing
                let should_skip_sealed_memtable =
                    partition_lsn.map_or(false, |partition_lsn| partition_lsn >= handle.lsn);

                if should_skip_sealed_memtable {
                    handle.partition.tree.lock_active_memtable().clear();

                    log::trace!(
                        "Partition {} has higher seqno ({partition_lsn:?}), skipping",
                        handle.partition.name
                    );
                    continue;
                }

                if let Some((memtable_id, sealed_memtable)) = tree.rotate_memtable() {
                    assert_eq!(
                        Some(handle.lsn),
                        sealed_memtable.get_highest_seqno(),
                        "memtable lsn does not match what was recovered - this is a bug"
                    );

                    log::trace!(
                        "sealed memtable of {} has {} items",
                        handle.partition.name,
                        sealed_memtable.len(),
                    );

                    // Maybe the memtable has a higher seqno, so try to set to maximum
                    let maybe_next_seqno =
                        tree.get_highest_seqno().map(|x| x + 1).unwrap_or_default();

                    seqno.fetch_max(maybe_next_seqno, std::sync::atomic::Ordering::AcqRel);

                    log::debug!("Keyspace seqno is now {}", seqno.get());

                    // IMPORTANT: Add sealed memtable size to current write buffer size
                    self.tasks
                        .buffer_size()
                        .increment(sealed_memtable.size().into());

                    // TODO: unit test write buffer size after recovery

                    // IMPORTANT: Add sealed memtable to flush manager, so it can be flushed
                    self.tasks.enqueue(Task {
                        id: memtable_id,
                        sealed_memtable,
                        partition: handle.partition.clone(),
                    });

                    recovered_count += 1;
                };
            }

            log::debug!("Recovered {recovered_count} sealed memtables");

            // IMPORTANT: Add sealed journal to journal manager
            self.items.enqueue(Item {
                watermarks,
                path: journal_path.clone(),
                size_in_bytes: journal_size,
            });

            log::debug!("Requeued sealed journal at {:?}", journal_path);
        }

        Ok(())
    }
}

/// Flush Task Queues
impl FlushTracker {
    pub fn clear_queues(&self) {
        self.tasks.clear();
    }

    /// Gets the names of partitions that have queued tasks.
    pub fn get_partitions_with_tasks(&self) -> Vec<PartitionKey> {
        self.tasks.get_partitions_with_tasks()
    }

    /// Returns the amount of queues.
    pub fn queue_count(&self) -> usize {
        self.tasks.queue_count()
    }

    /// Returns the amount of bytes queued.
    pub fn queued_size(&self) -> u64 {
        self.tasks.queued_size()
    }

    // NOTE: is actually used in tests
    #[allow(dead_code)]
    /// Returns the amount of tasks that are queued to be flushed.
    pub fn task_count(&self) -> usize {
        self.tasks.task_count()
    }

    // NOTE: is actually used in tests
    #[allow(dead_code)]
    #[must_use]
    pub fn is_task_queue_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub fn remove_partition(&self, name: &str) {
        self.tasks.remove_partition(name);
    }

    pub fn enqueue_task(&self, task: Task) {
        self.tasks.enqueue(task);
    }

    /// Returns a list of tasks per partition.
    pub fn collect_tasks(&self, limit: usize) -> Vec<Vec<Arc<Task>>> {
        self.tasks.collect_tasks(limit)
    }

    pub fn dequeue_tasks(&self, partition_name: &PartitionKey, cnt: usize) {
        self.tasks.dequeue(partition_name, cnt);
    }
}

/// Journal Item Queue
impl FlushTracker {
    pub fn clear_items(&self) {
        self.items.clear();
    }

    /// Returns the amount of journals
    pub fn journal_count(&self) -> usize {
        self.items.journal_count()
    }

    // NOTE: is actually used in tests
    #[allow(dead_code)]
    /// Returns the amount of sealed journals
    pub fn sealed_journal_count(&self) -> usize {
        self.items.sealed_journal_count()
    }

    /// Returns the amount of bytes used on disk by journals
    pub fn disk_space_used(&self) -> u64 {
        self.items.disk_space().get()
    }

    /// Performs maintenance, maybe deleting some old journals
    pub fn maintenance(&self) -> crate::Result<()> {
        self.items.maintenance()
    }

    pub fn rotate_journal(
        &self,
        journal_writer: &mut MutexGuard<Writer>,
        watermarks: Vec<EvictionWatermark>,
    ) -> crate::Result<()> {
        self.items.rotate_journal(journal_writer, watermarks)
    }
}

/// Write Buffer
impl FlushTracker {
    pub fn buffer_size(&self) -> u64 {
        self.tasks.buffer_size().get()
    }

    pub fn increment_buffer_size(&self, n: u64) -> u64 {
        self.tasks.buffer_size().increment(n)
    }

    pub fn decrement_buffer_size(&self, n: u64) -> u64 {
        self.tasks.buffer_size().decrement(n)
    }
}
