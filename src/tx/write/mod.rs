// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#[cfg(feature = "single_writer_tx")]
pub mod single_writer;

#[cfg(feature = "ssi_tx")]
pub mod ssi;

use crate::{
    batch::{item::Item, PartitionKey},
    snapshot_nonce::SnapshotNonce,
    Batch, HashMap, PersistMode, TxKeyspace, TxPartitionHandle,
};
use lsm_tree::{AbstractTree, InternalValue, KvPair, Memtable, SeqNo, UserKey, UserValue};
use std::{ops::RangeBounds, sync::Arc};

fn ignore_tombstone_value(item: InternalValue) -> Option<InternalValue> {
    if item.is_tombstone() {
        None
    } else {
        Some(item)
    }
}

/// A single-writer (serialized) cross-partition transaction
///
/// Use [`WriteTransaction::commit`] to commit changes to the keyspace.
///
/// Drop the transaction to rollback changes.
pub(super) struct BaseTransaction {
    durability: Option<PersistMode>,
    pub(super) keyspace: TxKeyspace,
    memtables: HashMap<PartitionKey, Arc<Memtable>>,

    nonce: SnapshotNonce,
}

impl BaseTransaction {
    pub(crate) fn new(keyspace: TxKeyspace, nonce: SnapshotNonce) -> Self {
        Self {
            keyspace,
            memtables: HashMap::default(),
            nonce,
            durability: None,
        }
    }

    /// Sets the durability level.
    #[must_use]
    pub(super) fn durability(mut self, mode: Option<PersistMode>) -> Self {
        self.durability = mode;
        self
    }

    /// Removes an item and returns its value if it existed.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn take<K: Into<UserKey>>(
        &mut self,
        partition: &TxPartitionHandle,
        key: K,
    ) -> crate::Result<Option<UserValue>> {
        self.fetch_update(partition, key, |_| None)
    }

    /// Atomically updates an item and returns the new value.
    ///
    /// Returning `None` removes the item if it existed before.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn update_fetch<
        K: Into<UserKey>,
        F: FnMut(Option<&UserValue>) -> Option<UserValue>,
    >(
        &mut self,
        partition: &TxPartitionHandle,
        key: K,
        mut f: F,
    ) -> crate::Result<Option<UserValue>> {
        let key = key.into();
        let prev = self.get(partition, &key)?;
        let updated = f(prev.as_ref());

        if let Some(value) = updated.clone() {
            // NOTE: Skip insert if the value hasn't changed
            if prev.as_ref() != Some(&value) {
                self.insert(partition, key, value);
            }
        } else if prev.is_some() {
            self.remove(partition, key);
        }

        Ok(updated)
    }

    /// Atomically updates an item and returns the previous value.
    ///
    /// Returning `None` removes the item if it existed before.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn fetch_update<
        K: Into<UserKey>,
        F: FnMut(Option<&UserValue>) -> Option<UserValue>,
    >(
        &mut self,
        partition: &TxPartitionHandle,
        key: K,
        mut f: F,
    ) -> crate::Result<Option<UserValue>> {
        let key = key.into();
        let prev = self.get(partition, &key)?;
        let updated = f(prev.as_ref());

        if let Some(value) = updated {
            // NOTE: Skip insert if the value hasn't changed
            if prev.as_ref() != Some(&value) {
                self.insert(partition, key, value);
            }
        } else if prev.is_some() {
            self.remove(partition, key);
        }

        Ok(prev)
    }

    /// Retrieves an item from the transaction's state.
    ///
    /// The transaction allows reading your own writes (RYOW).
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn get<K: AsRef<[u8]>>(
        &self,
        partition: &TxPartitionHandle,
        key: K,
    ) -> crate::Result<Option<UserValue>> {
        if let Some(memtable) = self.memtables.get(&partition.inner.name) {
            if let Some(item) = memtable.get(&key, None) {
                return Ok(ignore_tombstone_value(item).map(|x| x.value));
            }
        }

        let res = partition
            .inner
            .snapshot_at(self.nonce.instant)
            .get(key.as_ref())?;

        Ok(res)
    }

    /// Retrieves the size of an item from the transaction's state.
    ///
    /// The transaction allows reading your own writes (RYOW).
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn size_of<K: AsRef<[u8]>>(
        &self,
        partition: &TxPartitionHandle,
        key: K,
    ) -> crate::Result<Option<u32>> {
        if let Some(memtable) = self.memtables.get(&partition.inner.name) {
            if let Some(item) = memtable.get(&key, None) {
                return Ok(ignore_tombstone_value(item).map(|x| x.value.len() as u32));
            }
        }

        let res = partition
            .inner
            .snapshot_at(self.nonce.instant)
            .size_of(key.as_ref())?;

        Ok(res)
    }

    /// Returns `true` if the transaction's state contains the specified key.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn contains_key<K: AsRef<[u8]>>(
        &self,
        partition: &TxPartitionHandle,
        key: K,
    ) -> crate::Result<bool> {
        if let Some(memtable) = self.memtables.get(&partition.inner.name) {
            if let Some(item) = memtable.get(&key, None) {
                return Ok(!item.key.is_tombstone());
            }
        }

        let contains = partition
            .inner
            .snapshot_at(self.nonce.instant)
            .contains_key(key.as_ref())?;

        Ok(contains)
    }

    /// Returns the first key-value pair in the transaction's state.
    /// The key in this pair is the minimum key in the transaction's state.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn first_key_value(
        &self,
        partition: &TxPartitionHandle,
    ) -> crate::Result<Option<KvPair>> {
        // TODO: calling .iter will mark the partition as fully read, is that what we want?
        self.iter(partition).next().transpose()
    }

    /// Returns the last key-value pair in the transaction's state.
    /// The key in this pair is the maximum key in the transaction's state.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn last_key_value(
        &self,
        partition: &TxPartitionHandle,
    ) -> crate::Result<Option<KvPair>> {
        // TODO: calling .iter will mark the partition as fully read, is that what we want?
        self.iter(partition).next_back().transpose()
    }

    /// Scans the entire partition, returning the amount of items.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn len(&self, partition: &TxPartitionHandle) -> crate::Result<usize> {
        let mut count = 0;

        // TODO: calling .iter will mark the partition as fully read, is that what we want?
        let iter = self.iter(partition);

        for kv in iter {
            let _ = kv?;
            count += 1;
        }

        Ok(count)
    }

    /// Iterates over the transaction's state.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    #[must_use]
    pub(super) fn iter(
        &self,
        partition: &TxPartitionHandle,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        partition
            .inner
            .tree
            .iter(
                Some(self.nonce.instant),
                self.memtables.get(&partition.inner.name).cloned(),
            )
            .map(|item| item.map_err(Into::into))
    }

    /// Iterates over the transaction's state, returning keys only.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    #[must_use]
    pub(super) fn keys(
        &self,
        partition: &TxPartitionHandle,
    ) -> impl DoubleEndedIterator<Item = crate::Result<UserKey>> + 'static {
        partition
            .inner
            .tree
            .keys(Some(self.nonce.instant), None)
            .map(|item| item.map_err(Into::into))
    }

    /// Iterates over the transaction's state, returning values only.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    #[must_use]
    pub(super) fn values(
        &self,
        partition: &TxPartitionHandle,
    ) -> impl DoubleEndedIterator<Item = crate::Result<UserValue>> + 'static {
        partition
            .inner
            .tree
            .values(Some(self.nonce.instant), None)
            .map(|item| item.map_err(Into::into))
    }

    /// Iterates over a range of the transaction's state.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    #[must_use]
    pub(super) fn range<'b, K: AsRef<[u8]> + 'b, R: RangeBounds<K> + 'b>(
        &'b self,
        partition: &'b TxPartitionHandle,
        range: R,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        partition
            .inner
            .tree
            .range(
                range,
                Some(self.nonce.instant),
                self.memtables.get(&partition.inner.name).cloned(),
            )
            .map(|item| item.map_err(Into::into))
    }

    /// Iterates over a prefixed set of the transaction's state.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    #[must_use]
    pub(super) fn prefix<'b, K: AsRef<[u8]> + 'b>(
        &'b self,
        partition: &'b TxPartitionHandle,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        partition
            .inner
            .tree
            .prefix(
                prefix,
                Some(self.nonce.instant),
                self.memtables.get(&partition.inner.name).cloned(),
            )
            .map(|item| item.map_err(Into::into))
    }

    /// Inserts a key-value pair into the partition.
    ///
    /// Keys may be up to 65536 bytes long, values up to 2^32 bytes.
    /// Shorter keys and values result in better performance.
    ///
    /// If the key already exists, the item will be overwritten.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &mut self,
        partition: &TxPartitionHandle,
        key: K,
        value: V,
    ) {
        // TODO: PERF: slow??
        self.memtables
            .entry(partition.inner.name.clone())
            .or_default()
            .insert(lsm_tree::InternalValue::from_components(
                key,
                value,
                // NOTE: Just take the max seqno, which should never be reached
                // that way, the write is definitely always the newest
                SeqNo::MAX,
                lsm_tree::ValueType::Value,
            ));
    }

    /// Removes an item from the partition.
    ///
    /// The key may be up to 65536 bytes long.
    /// Shorter keys result in better performance.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn remove<K: Into<UserKey>>(&mut self, partition: &TxPartitionHandle, key: K) {
        // TODO: PERF: slow??
        self.memtables
            .entry(partition.inner.name.clone())
            .or_default()
            .insert(lsm_tree::InternalValue::new_tombstone(
                key,
                // NOTE: Just take the max seqno, which should never be reached
                // that way, the write is definitely always the newest
                SeqNo::MAX,
            ));
    }

    /// Removes an item from the partition, leaving behind a weak tombstone.
    ///
    /// When a weak tombstone is matched with a single write in a compaction,
    /// the tombstone will be removed along with the value. If the key was
    /// overwritten the result of a `remove_weak` is undefined.
    ///
    /// Only use this remove if it is known that the key has only been written
    /// to once since its creation or last `remove_weak`.
    ///
    /// The key may be up to 65536 bytes long.
    /// Shorter keys result in better performance.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn remove_weak<K: Into<UserKey>>(&mut self, partition: &TxPartitionHandle, key: K) {
        // TODO: PERF: slow??
        self.memtables
            .entry(partition.inner.name.clone())
            .or_default()
            .insert(lsm_tree::InternalValue::new_weak_tombstone(
                key,
                // NOTE: Just take the max seqno, which should never be reached
                // that way, the write is definitely always the newest
                SeqNo::MAX,
            ));
    }

    /// Commits the transaction.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn commit(self) -> crate::Result<()> {
        // skip all the logic if no keys were written to
        if self.memtables.is_empty() {
            return Ok(());
        }

        let mut batch = Batch::new(self.keyspace.inner).durability(self.durability);

        for (partition_key, memtable) in self.memtables {
            for item in memtable.iter() {
                batch.data.push(Item::new(
                    partition_key.clone(),
                    item.key.user_key.clone(),
                    item.value.clone(),
                    item.key.value_type,
                ));
            }
        }

        // TODO: instead of using batch, write batch::commit as a generic function that takes
        // a impl Iterator<BatchItem>
        // that way, we don't have to move the memtable(s) into the batch first to commit
        batch.commit()?;

        Ok(())
    }

    /// More explicit alternative to dropping the transaction
    /// to roll it back.
    #[allow(clippy::unused_self)]
    pub(super) fn rollback(self) {}
}

#[cfg(test)]
mod tests {
    use crate::{
        snapshot_nonce::SnapshotNonce, Config, PartitionCreateOptions,
        TransactionalPartitionHandle, TxKeyspace,
    };
    use tempfile::TempDir;

    struct TestEnv {
        ks: TxKeyspace,
        part: TransactionalPartitionHandle,

        #[allow(unused)]
        tmpdir: TempDir,
    }

    fn setup() -> Result<TestEnv, Box<dyn std::error::Error>> {
        let tmpdir = tempfile::tempdir()?;
        let ks = Config::new(tmpdir.path()).open_transactional()?;

        let part = ks.open_partition("foo", PartitionCreateOptions::default())?;

        Ok(TestEnv { ks, part, tmpdir })
    }

    #[test]
    fn update_fetch() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        env.part.insert([2u8], [20u8])?;

        let mut tx = super::BaseTransaction::new(
            env.ks.clone(),
            SnapshotNonce::new(
                env.ks.inner.instant(),
                env.ks.inner.snapshot_tracker.clone(),
            ),
        );

        let new = tx.update_fetch(&env.part, [2u8], |v| {
            v.and_then(|v| v.first().copied()).map(|v| [v + 5].into())
        })?;
        assert_eq!(new, Some([25u8].into()));
        tx.commit()?;

        Ok(())
    }
}
