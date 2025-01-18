// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::sync::atomic::{AtomicU64, Ordering};

/// Keeps track of disk space or buffer size in bytes
#[derive(Debug)]
pub struct SpaceTracker(AtomicU64);

impl SpaceTracker {
    pub fn new() -> SpaceTracker {
        Self(AtomicU64::new(0))
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }

    // Adds some bytes to the space counter.
    //
    // Returns the counter *after* incrementing.
    pub fn increment(&self, n: u64) -> u64 {
        self.0.fetch_add(n, Ordering::AcqRel) + n
    }

    // Frees some bytes from the space counter.
    //
    // Returns the counter *after* decrementing.
    pub fn decrement(&self, n: u64) -> u64 {
        use Ordering::{Acquire, SeqCst};

        loop {
            let now = self.0.load(Acquire);
            let subbed = now.saturating_sub(n);

            if self.0.compare_exchange(now, subbed, SeqCst, SeqCst).is_ok() {
                return subbed;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn space_tracker_increment() {
        let m = SpaceTracker::new();
        m.increment(5);
        assert_eq!(m.get(), 5);

        m.increment(15);
        assert_eq!(m.get(), 20);
    }

    #[test]
    fn space_tracker_decrement() {
        let m = SpaceTracker::new();
        m.increment(20);
        assert_eq!(m.get(), 20);

        m.decrement(5);
        assert_eq!(m.get(), 15);

        m.decrement(20);
        assert_eq!(m.get(), 0);
    }
}
