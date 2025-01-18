// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::manager::Task;
use crate::{
    compaction::manager::CompactionManager, flush_tracker::FlushTracker,
    snapshot_tracker::SnapshotTracker, PartitionHandle,
};
use lsm_tree::{AbstractTree, Segment, SeqNo};
use std::sync::Arc;

/// Flushes a single segment.
fn run_flush_worker(task: &Arc<Task>, eviction_threshold: SeqNo) -> crate::Result<Option<Segment>> {
    #[rustfmt::skip]
    let segment = task.partition.tree.flush_memtable(
        // IMPORTANT: Segment has to get the task ID
        // otherwise segment ID and memtable ID will not line up
        task.id,
        &task.sealed_memtable,
        eviction_threshold,
    );

    // TODO: test this after a failed flush
    if segment.is_err() {
        // IMPORTANT: Need to decrement pending segments counter
        if let crate::AnyTree::Blob(tree) = &task.partition.tree {
            tree.pending_segments
                .fetch_sub(1, std::sync::atomic::Ordering::Release);
        }
    }

    Ok(segment?)
}

struct MultiFlushResultItem {
    partition: PartitionHandle,
    created_segments: Vec<Segment>,

    /// Size sum of sealed memtables that have been flushed
    size: u64,
}

type MultiFlushResults = Vec<crate::Result<MultiFlushResultItem>>;

/// Distributes tasks of multiple partitions over multiple worker threads.
///
/// Each thread is responsible for the tasks of one partition.
fn run_multi_flush(
    partitioned_tasks: Vec<Vec<Arc<Task>>>,
    eviction_threshold: SeqNo,
) -> MultiFlushResults {
    log::debug!("spawning {} worker threads", partitioned_tasks.len());

    // NOTE: Don't trust clippy
    #[allow(clippy::needless_collect)]
    let threads = partitioned_tasks
        .into_iter()
        .map(|tasks| {
            std::thread::spawn(move || {
                let partition = tasks
                    .first()
                    .expect("should always have at least one task")
                    .partition
                    .clone();

                log::trace!(
                    "flushing {} memtables for partition {:?}",
                    tasks.len(),
                    partition.name
                );

                let memtables_size: u64 = tasks
                    .iter()
                    .map(|t| u64::from(t.sealed_memtable.size()))
                    .sum();

                // NOTE: Don't trust clippy
                #[allow(clippy::needless_collect)]
                let flush_workers = tasks
                    .into_iter()
                    .map(|task| {
                        std::thread::spawn(move || run_flush_worker(&task, eviction_threshold))
                    })
                    .collect::<Vec<_>>();

                let mut created_segments = Vec::with_capacity(flush_workers.len());

                for t in flush_workers {
                    if let Some(segment) = t.join().expect("should join")? {
                        created_segments.push(segment);
                    }
                }

                Ok(MultiFlushResultItem {
                    partition,
                    created_segments,
                    size: memtables_size,
                })
            })
        })
        .collect::<Vec<_>>();

    threads
        .into_iter()
        .map(|t| t.join().expect("should join"))
        .collect::<Vec<_>>()
}

/// Runs flush logic.
#[allow(clippy::too_many_lines)]
pub fn run(
    flush_tracker: &FlushTracker,
    compaction_manager: &CompactionManager,
    snapshot_tracker: &SnapshotTracker,
    parallelism: usize,
) {
    log::debug!("read locking flush manager");
    let partitioned_tasks = flush_tracker.collect_tasks(parallelism);

    if partitioned_tasks.is_empty() {
        log::debug!("No tasks collected");
        return;
    }

    for result in run_multi_flush(partitioned_tasks, snapshot_tracker.get_seqno_safe_to_gc()) {
        match result {
            Ok(MultiFlushResultItem {
                partition,
                created_segments,
                size: memtables_size,
            }) => {
                // IMPORTANT: Flushed segments need to be applied *atomically* into the tree
                // otherwise we could cover up an unwritten journal, which will result in data loss
                if let Err(e) = partition.tree.register_segments(&created_segments) {
                    log::error!("Failed to register segments: {e:?}");
                } else {
                    log::debug!("write locking flush manager to submit results");
                    log::debug!(
                        "Dequeuing flush tasks: {} => {}",
                        partition.name,
                        created_segments.len()
                    );

                    flush_tracker.dequeue_tasks(&partition.name, created_segments.len());
                    flush_tracker.decrement_buffer_size(memtables_size);

                    for _ in 0..parallelism {
                        compaction_manager.notify(partition.clone());
                    }
                }
            }
            Err(e) => {
                log::error!("Flush error: {e:?}");
            }
        }
    }

    log::debug!("write locking journal manager to maybe do maintenance");
    if let Err(e) = flush_tracker.maintenance() {
        log::error!("journal GC failed: {e:?}");
    }

    log::debug!("fully done");
}
