// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::manager::CompactionManager;
use crate::snapshot_tracker::SnapshotTracker;
use lsm_tree::AbstractTree;
use std::sync::atomic::AtomicUsize;

/// Runs a single run of compaction.
pub fn run(
    compaction_manager: &CompactionManager,
    snapshot_tracker: &SnapshotTracker,
    compaction_counter: &AtomicUsize,
) {
    let Some(item) = compaction_manager.pop() else {
        return;
    };

    log::trace!(
        "compactor: calling compaction strategy for partition {:?}",
        item.0.name
    );

    let strategy = &item.config.compaction_strategy;

    // TODO: loop if there's more work to do

    compaction_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if let Err(e) = item
        .tree
        .compact(strategy.inner(), snapshot_tracker.get_seqno_safe_to_gc())
    {
        log::error!("Compaction failed: {e:?}");
    };
    compaction_counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
}
