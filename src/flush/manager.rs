// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::queue::FlushQueue;
use crate::{batch::PartitionKey, space_tracker::SpaceTracker, HashMap, PartitionHandle};
use lsm_tree::{Memtable, SegmentId};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct Task {
    /// ID of memtable
    pub id: SegmentId,

    /// Memtable to flush
    pub sealed_memtable: Arc<Memtable>,

    /// Partition
    pub partition: PartitionHandle,
}

impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FlushTask {}:{}", self.partition.name, self.id)
    }
}

/// The [`FlushTaskQueue`] stores a dictionary of queues, each queue
/// containing some flush tasks.
///
/// Each flush task references a sealed memtable and the given partition.
#[derive(Debug)]
pub struct FlushTaskQueue {
    queues: RwLock<HashMap<PartitionKey, FlushQueue>>,
    /// Keeps track of write buffer size
    buffer_size: SpaceTracker,
}

impl FlushTaskQueue {
    pub fn new() -> Self {
        Self {
            queues: RwLock::default(),
            buffer_size: SpaceTracker::new(),
        }
    }

    #[track_caller]
    fn queues_read_lock(&self) -> RwLockReadGuard<'_, HashMap<Arc<str>, FlushQueue>> {
        self.queues.read().expect("lock is poisoned")
    }

    #[track_caller]
    fn queues_write_lock(&self) -> RwLockWriteGuard<'_, HashMap<Arc<str>, FlushQueue>> {
        self.queues.write().expect("lock is poisoned")
    }

    pub fn buffer_size(&self) -> &SpaceTracker {
        &self.buffer_size
    }

    #[track_caller]
    pub fn clear(&self) {
        self.queues_write_lock().clear();
    }

    /// Gets the names of partitions that have queued tasks.
    #[track_caller]
    pub fn get_partitions_with_tasks(&self) -> Vec<PartitionKey> {
        self.queues_read_lock()
            .iter()
            .filter(|(_, v)| !v.is_empty())
            .map(|(k, _)| k)
            .cloned()
            .collect()
    }

    /// Returns the amount of queues.
    #[track_caller]
    pub fn queue_count(&self) -> usize {
        self.queues_read_lock().len()
    }

    /// Returns the amount of bytes queued.
    #[track_caller]
    pub fn queued_size(&self) -> u64 {
        self.queues_read_lock()
            .values()
            .map(FlushQueue::size)
            .sum::<u64>()
    }

    // NOTE: is actually used in tests
    #[allow(dead_code)]
    /// Returns the amount of tasks that are queued to be flushed.
    #[track_caller]
    pub fn task_count(&self) -> usize {
        self.queues_read_lock()
            .values()
            .map(FlushQueue::len)
            .sum::<usize>()
    }

    // NOTE: is actually used in tests
    #[allow(dead_code)]
    #[must_use]
    #[track_caller]
    pub fn is_empty(&self) -> bool {
        self.queues_read_lock().values().all(FlushQueue::is_empty)
    }

    #[track_caller]
    pub fn remove_partition(&self, name: &str) {
        self.queues_write_lock().remove(name);
    }

    #[track_caller]
    pub fn enqueue(&self, task: Task) {
        let partition_name = task.partition.name.clone();
        log::debug!(
            "Enqueuing {partition_name}:{} for flushing ({} B)",
            task.id,
            task.sealed_memtable.size()
        );

        self.queues_write_lock()
            .entry(partition_name)
            .or_default()
            .enqueue(Arc::new(task));
    }

    /// Returns a list of tasks per partition.
    #[track_caller]
    pub fn collect_tasks(&self, limit: usize) -> Vec<Vec<Arc<Task>>> {
        let mut collected = Vec::new();

        // NOTE: Returning multiple tasks per partition is fine and will
        // help with flushing very active partitions.
        //
        // Because we are flushing them atomically inside one batch,
        // we will never cover up a lower seqno of some other segment.
        // For this to work, all tasks need to be successful and atomically
        // applied (all-or-nothing).
        for (partition_name, item) in self
            .queues_read_lock()
            .iter()
            .flat_map(|(partition_name, queue)| {
                queue.iter().map(move |item| (partition_name, item))
            })
            .take(limit)
        {
            match collected.last_mut() {
                None => collected.push(vec![item.clone()]),
                Some(items) => {
                    if items
                        .last()
                        .map_or(true, |item| &item.partition.name == partition_name)
                    {
                        items.push(item.clone());
                    } else {
                        collected.push(vec![item.clone()]);
                    }
                }
            }
        }

        collected
    }

    #[track_caller]
    pub fn dequeue(&self, partition_name: &PartitionKey, cnt: usize) {
        if let Some(queue) = self.queues_write_lock().get_mut(partition_name) {
            queue.dequeue(cnt)
        }
    }
}
