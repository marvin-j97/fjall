// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    config::Config as KeyspaceConfig, flush_tracker::FlushTracker, keyspace::Partitions,
    snapshot_tracker::SnapshotTracker, Keyspace,
};
use lsm_tree::{AbstractTree, SequenceNumberCounter};
use std::sync::{atomic::AtomicBool, Arc, RwLock};

/// Monitors write buffer size & journal size
pub struct Monitor {
    keyspace_config: KeyspaceConfig,
    flush_tracker: Arc<FlushTracker>,
    partitions: Arc<RwLock<Partitions>>,
    seqno: SequenceNumberCounter,
    snapshot_tracker: Arc<SnapshotTracker>,
    keyspace_poison: Arc<AtomicBool>,
}

impl Drop for Monitor {
    fn drop(&mut self) {
        log::trace!("Dropping monitor");

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::decrement_drop_counter();
    }
}

impl Monitor {
    pub fn new(keyspace: &Keyspace) -> Self {
        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Self {
            keyspace_config: keyspace.config.clone(),
            flush_tracker: keyspace.flush_tracker.clone(),
            partitions: keyspace.partitions.clone(),
            seqno: keyspace.seqno.clone(),
            snapshot_tracker: keyspace.snapshot_tracker.clone(),
            keyspace_poison: keyspace.is_poisoned.clone(),
        }
    }

    fn try_reduce_journal_size(&self) {
        log::debug!(
            "monitor: try flushing affected partitions because journals have passed 50% of threshold"
        );

        // TODO: this may not scale well for many partitions
        let lowest_persisted_partition = self
            .partitions
            .read()
            .expect("lock is poisoned")
            .values()
            .filter(|x| x.tree.active_memtable_size() > 0)
            .min_by(|a, b| {
                a.tree
                    .get_highest_persisted_seqno()
                    .cmp(&b.tree.get_highest_persisted_seqno())
            })
            .cloned();

        if let Some(lowest_persisted_partition) = lowest_persisted_partition {
            let partitions_names_with_queued_tasks = self.flush_tracker.get_partitions_with_tasks();

            if partitions_names_with_queued_tasks.contains(&lowest_persisted_partition.name) {
                return;
            }

            match lowest_persisted_partition.rotate_memtable() {
                Ok(_) => {}
                Err(e) => {
                    log::error!(
                        "monitor: memtable rotation failed for {:?}: {e:?}",
                        lowest_persisted_partition.name
                    );

                    self.keyspace_poison
                        .store(true, std::sync::atomic::Ordering::Release);
                }
            };
        }
    }

    fn try_reduce_write_buffer_size(&self) {
        log::trace!(
            "monitor: flush inactive partition because write buffer has passed 50% of threshold"
        );
        let mut partitions_with_tasks = self.flush_tracker.get_partitions_with_tasks();
        partitions_with_tasks.sort();

        let mut partitions = self
            .partitions
            .read()
            .expect("lock is poisoned")
            .values()
            .filter(|x| partitions_with_tasks.binary_search(&x.name).is_err())
            .cloned()
            .collect::<Vec<_>>();

        partitions.sort_by(|a, b| {
            b.tree
                .active_memtable_size()
                .cmp(&a.tree.active_memtable_size())
        });

        for partition in partitions {
            log::debug!("monitor: WB rotating {:?}", partition.name);

            match partition.rotate_memtable() {
                Ok(rotated) => {
                    if rotated {
                        break;
                    }
                }
                Err(e) => {
                    log::error!(
                        "monitor: memtable rotation failed for {:?}: {e:?}",
                        partition.name
                    );

                    self.keyspace_poison
                        .store(true, std::sync::atomic::Ordering::Release);
                }
            };
        }
    }

    pub fn run(&self) -> bool {
        let mut idle = true;

        // TODO: don't do this too often
        let current_seqno = self.seqno.get();
        let gc_seqno_watermark = self.snapshot_tracker.get_seqno_safe_to_gc();

        // NOTE: If the difference between watermark if too large, and
        // we never opened a snapshot, we need to pull the watermark up
        //
        // https://github.com/fjall-rs/fjall/discussions/85
        if (current_seqno - gc_seqno_watermark) > 100 && self.snapshot_tracker.data.is_empty() {
            *self
                .snapshot_tracker
                .lowest_freed_instant
                .write()
                .expect("lock is poisoned") = current_seqno.saturating_sub(100);
        }

        let jm_size = self.flush_tracker.disk_space_used();

        let max_journal_size = self.keyspace_config.max_journaling_size_in_bytes;

        if jm_size as f64 > (max_journal_size as f64 * 0.5) {
            self.try_reduce_journal_size();
            idle = false;
        }

        let write_buffer_size = self.flush_tracker.buffer_size();

        let queued_size = self.flush_tracker.queued_size();

        // TODO: This should never ever overflow
        // TODO: because that is definitely a logic error
        // TODO: need to make sure it's impossible this can happen
        #[cfg(debug_assertions)]
        {
            // NOTE: Cannot use panic because we are in a thread that should not
            // crash
            if queued_size > write_buffer_size {
                log::error!(
                    "Queued size should not be able to be greater than entire write buffer size"
                );
                return idle;
            }
        }

        // NOTE: We cannot flush more stuff if the journal is already too large
        if jm_size < max_journal_size {
            let max_write_buffer_size = self.keyspace_config.max_write_buffer_size_in_bytes;

            // NOTE: Take the queued size of unflushed memtables into account
            // so the system isn't performing a flush storm once the threshold is reached
            //
            // Also, As a fail safe, use saturating_sub so it doesn't overflow
            let buffer_size_without_queued_size = write_buffer_size.saturating_sub(queued_size);

            if buffer_size_without_queued_size as f64 > (max_write_buffer_size as f64 * 0.5) {
                self.try_reduce_write_buffer_size();
                idle = false;
            }
        } else {
            log::debug!("cannot rotate memtable to free write buffer - journal too large");
        }

        idle
    }
}
