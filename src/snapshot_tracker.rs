// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::Instant;
use dashmap::DashMap;
use std::sync::{atomic::AtomicU64, RwLock};

/// Keeps track of open snapshots
#[allow(clippy::module_name_repetitions)]
pub struct SnapshotTracker {
    // TODO: maybe use rustc_hash or ahash
    pub(crate) data: DashMap<Instant, usize, xxhash_rust::xxh3::Xxh3Builder>,

    freed_count: AtomicU64,
    safety_gap: u64,

    pub(crate) lowest_freed_instant: RwLock<Instant>,
}

impl Default for SnapshotTracker {
    fn default() -> Self {
        Self {
            data: DashMap::default(),
            safety_gap: 50,
            freed_count: AtomicU64::default(),
            lowest_freed_instant: RwLock::default(),
        }
    }
}

impl SnapshotTracker {
    pub fn open(&self, seqno: Instant) {
        log::trace!("open snapshot {seqno}");

        self.data
            .entry(seqno)
            .and_modify(|x| {
                *x += 1;
            })
            .or_insert(1);
    }

    pub fn close(&self, seqno: Instant) {
        log::trace!("close snapshot {seqno}");

        self.data.alter(&seqno, |_, v| v.saturating_sub(1));

        let freed = self
            .freed_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;

        if (freed % self.safety_gap) == 0 {
            self.gc(seqno);
        }
    }

    pub fn get_seqno_safe_to_gc(&self) -> Instant {
        *self.lowest_freed_instant.read().expect("lock is poisoned")
    }

    fn gc(&self, watermark: Instant) {
        log::trace!("snapshot gc, watermark={watermark}");

        let mut lock = self.lowest_freed_instant.write().expect("lock is poisoned");

        let seqno_threshold = watermark.saturating_sub(self.safety_gap);

        let mut lowest_retained = 0;

        self.data.retain(|&k, v| {
            let should_be_retained = *v > 0 || k > seqno_threshold;

            if should_be_retained {
                lowest_retained = match lowest_retained {
                    0 => k,
                    lo => lo.min(k),
                };
            }

            should_be_retained
        });

        log::trace!("lowest retained snapshot={lowest_retained}");

        *lock = match *lock {
            0 => lowest_retained.saturating_sub(1),
            lo => lo.max(lowest_retained.saturating_sub(1)),
        };

        log::trace!("gc threshold now {}", *lock);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn seqno_tracker_one_shot() {
        let mut map = SnapshotTracker::default();
        map.safety_gap = 5;

        map.open(1);
        map.close(1);

        assert_eq!(map.get_seqno_safe_to_gc(), 0);
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn seqno_tracker_reverse_order() {
        let mut map = SnapshotTracker::default();
        map.safety_gap = 5;

        map.open(1);
        map.open(2);
        map.open(3);
        map.open(4);
        map.open(5);
        map.open(6);
        map.open(7);
        map.open(8);
        map.open(9);
        map.open(10);

        map.close(10);
        map.close(9);
        map.close(8);
        map.close(7);
        map.close(6);
        map.close(5);
        map.close(4);
        map.close(3);
        map.close(2);
        map.close(1);

        map.open(11);
        map.close(11);
        map.gc(11);

        assert_eq!(map.get_seqno_safe_to_gc(), 6);
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn seqno_tracker_simple_2() {
        let mut map = SnapshotTracker::default();
        map.safety_gap = 5;

        map.open(1);
        assert_eq!(map.get_seqno_safe_to_gc(), 0);

        map.open(2);
        assert_eq!(map.get_seqno_safe_to_gc(), 0);

        map.open(3);
        assert_eq!(map.get_seqno_safe_to_gc(), 0);

        map.open(4);
        assert_eq!(map.get_seqno_safe_to_gc(), 0);

        map.open(5);
        assert_eq!(map.get_seqno_safe_to_gc(), 0);

        map.open(6);
        assert_eq!(map.get_seqno_safe_to_gc(), 0);

        map.close(1);
        assert_eq!(map.get_seqno_safe_to_gc(), 0);

        map.close(2);
        assert_eq!(map.get_seqno_safe_to_gc(), 0);

        map.close(3);
        assert_eq!(map.get_seqno_safe_to_gc(), 0);

        map.close(4);
        assert_eq!(map.get_seqno_safe_to_gc(), 0);

        map.close(5);
        assert_eq!(map.get_seqno_safe_to_gc(), 0);

        map.close(6);
        map.gc(6);
        assert_eq!(map.get_seqno_safe_to_gc(), 1);

        map.open(7);
        map.close(7);
        map.gc(7);
        assert_eq!(map.get_seqno_safe_to_gc(), 2);

        map.open(8);
        map.open(9);
        map.close(9);
        map.gc(9);
        assert_eq!(map.get_seqno_safe_to_gc(), 4);

        map.close(8);
        map.gc(8);
        assert_eq!(map.get_seqno_safe_to_gc(), 4);
    }
}
