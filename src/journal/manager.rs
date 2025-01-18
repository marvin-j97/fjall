// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::writer::Writer;
use crate::{space_tracker::SpaceTracker, PartitionHandle};
use lsm_tree::{AbstractTree, SeqNo};
use std::{
    path::PathBuf,
    sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

/// Stores the highest seqno of a partition found in a journal.
#[derive(Clone)]
pub struct EvictionWatermark {
    pub partition: PartitionHandle,
    pub lsn: SeqNo,
}

impl std::fmt::Debug for EvictionWatermark {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.partition.name, self.lsn)
    }
}

pub struct Item {
    pub path: PathBuf,
    pub size_in_bytes: u64,
    pub watermarks: Vec<EvictionWatermark>,
}

impl std::fmt::Debug for Item {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JournalItem {:?} => {:#?}", self.path, self.watermarks)
    }
}

/// The [`JournalItemQueue`] keeps track of sealed journals that are being flushed.
///
/// Each journal may contain items of different partitions.
#[derive(Debug)]
pub struct JournalItemQueue {
    active_path: Mutex<PathBuf>, // TODO: remove?
    items: RwLock<Vec<Item>>,

    // TODO: should be taking into account active journal, which is preallocated...
    disk_space: SpaceTracker,
}

impl JournalItemQueue {
    pub fn from_active(path: PathBuf) -> Self {
        Self {
            active_path: Mutex::new(path),
            items: RwLock::new(Vec::with_capacity(10)),
            disk_space: SpaceTracker::new(),
        }
    }

    #[track_caller]
    fn items_read_lock(&self) -> RwLockReadGuard<'_, Vec<Item>> {
        self.items.read().expect("lock is poisoned")
    }

    #[track_caller]
    fn items_write_lock(&self) -> RwLockWriteGuard<'_, Vec<Item>> {
        self.items.write().expect("lock is poisoned")
    }

    #[track_caller]
    pub fn clear(&self) {
        self.items_write_lock().clear();
    }

    #[track_caller]
    pub fn enqueue(&self, item: Item) {
        self.disk_space.increment(item.size_in_bytes);
        self.items_write_lock().push(item);
    }

    /// Returns the amount of journals
    #[track_caller]
    pub fn journal_count(&self) -> usize {
        // NOTE: + 1 = active journal
        self.sealed_journal_count() + 1
    }

    /// Returns the amount of sealed journals
    #[track_caller]
    pub fn sealed_journal_count(&self) -> usize {
        self.items_read_lock().len()
    }

    /// Returns the amount of bytes used on disk by journals
    pub fn disk_space(&self) -> &SpaceTracker {
        &self.disk_space
    }

    /// Performs maintenance, maybe deleting some old journals
    #[track_caller]
    pub fn maintenance(&self) -> crate::Result<()> {
        let mut items = self.items_write_lock();

        while let Some(item) = items.first() {
            // TODO: unit test: check deleted partition does not prevent journal eviction
            for item in &item.watermarks {
                // Only check partition seqno if not deleted
                if !item
                    .partition
                    .is_deleted
                    .load(std::sync::atomic::Ordering::Acquire)
                    && item.partition.tree.get_highest_persisted_seqno() < Some(item.lsn)
                {
                    return Ok(());
                }
            }

            // NOTE: Once the LSN of *every* partition's segments [1] is higher than the journal's stored partition seqno,
            // it can be deleted from disk, as we know the entire journal has been flushed to segments [2].
            //
            // [1] We cannot use the partition's max seqno, because the memtable will get writes, which increase the seqno.
            // We *need* to check the disk segments specifically, they are the source of truth for flushed data.
            //
            // [2] Checking the seqno is safe because the queues inside the flush manager are FIFO.
            //
            // IMPORTANT: On recovery, the journals need to be flushed from oldest to newest.
            log::trace!("Removing fully flushed journal at {:?}", item.path);
            std::fs::remove_file(&item.path)?;

            self.disk_space.decrement(item.size_in_bytes);

            items.remove(0);
        }

        Ok(())
    }

    #[track_caller]
    pub fn rotate_journal(
        &self,
        journal_writer: &mut MutexGuard<Writer>,
        watermarks: Vec<EvictionWatermark>,
    ) -> crate::Result<()> {
        let journal_size = journal_writer.len()?;

        let (sealed_path, next_journal_path) = journal_writer.rotate()?;
        *self.active_path.lock().expect("lock is poisoned") = next_journal_path;

        self.enqueue(Item {
            path: sealed_path,
            watermarks,
            size_in_bytes: journal_size,
        });

        Ok(())
    }
}
