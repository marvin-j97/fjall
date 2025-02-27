// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{gc::GarbageCollection, PartitionHandle, TxKeyspace};
use lsm_tree::{gc::Report as GcReport, KvPair, UserKey, UserValue};
use std::path::PathBuf;

/// Access to a partition of a transactional keyspace
#[derive(Clone)]
pub struct TransactionalPartitionHandle {
    pub(crate) inner: PartitionHandle,
    pub(crate) keyspace: TxKeyspace,
}

impl GarbageCollection for TransactionalPartitionHandle {
    fn gc_scan(&self) -> crate::Result<GcReport> {
        crate::gc::GarbageCollector::scan(self.inner())
    }

    fn gc_with_space_amp_target(&self, factor: f32) -> crate::Result<u64> {
        crate::gc::GarbageCollector::with_space_amp_target(self.inner(), factor)
    }

    fn gc_with_staleness_threshold(&self, threshold: f32) -> crate::Result<u64> {
        crate::gc::GarbageCollector::with_staleness_threshold(self.inner(), threshold)
    }

    fn gc_drop_stale_segments(&self) -> crate::Result<u64> {
        crate::gc::GarbageCollector::drop_stale_segments(self.inner())
    }
}

impl TransactionalPartitionHandle {
    /// Returns the underlying LSM-tree's path
    #[must_use]
    pub fn path(&self) -> PathBuf {
        self.inner.path().into()
    }

    /// Removes an item and returns its value if it existed.
    ///
    /// The operation will run wrapped in a transaction.
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    ///
    /// let taken = partition.take("a")?.unwrap();
    /// assert_eq!(b"abc", &*taken);
    ///
    /// let item = partition.get("a")?;
    /// assert!(item.is_none());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn take<K: Into<UserKey>>(&self, key: K) -> crate::Result<Option<UserValue>> {
        self.fetch_update(key, |_| None)
    }

    /// Atomically updates an item and returns the previous value.
    ///
    /// Returning `None` removes the item if it existed before.
    ///
    /// The operation will run wrapped in a transaction.
    ///
    /// # Note
    ///
    /// The provided closure can be called multiple times as this function
    /// automatically retries on conflict. Since this is an `FnMut`, make sure
    /// it is idempotent and will not cause side-effects.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, Slice, PartitionCreateOptions};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    ///
    /// let prev = partition.fetch_update("a", |_| Some(Slice::from(*b"def")))?.unwrap();
    /// assert_eq!(b"abc", &*prev);
    ///
    /// let item = partition.get("a")?;
    /// assert_eq!(Some("def".as_bytes().into()), item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    ///
    /// let prev = partition.fetch_update("a", |_| None)?.unwrap();
    /// assert_eq!(b"abc", &*prev);
    ///
    /// let item = partition.get("a")?;
    /// assert!(item.is_none());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[allow(unused_mut)]
    pub fn fetch_update<K: Into<UserKey>, F: FnMut(Option<&UserValue>) -> Option<UserValue>>(
        &self,
        key: K,
        mut f: F,
    ) -> crate::Result<Option<UserValue>> {
        let key: UserKey = key.into();

        #[cfg(feature = "single_writer_tx")]
        {
            let mut tx = self.keyspace.write_tx();

            let prev = tx.fetch_update(self, key, f)?;
            tx.commit()?;

            Ok(prev)
        }

        #[cfg(feature = "ssi_tx")]
        loop {
            let mut tx = self.keyspace.write_tx()?;
            let prev = tx.fetch_update(self, key.clone(), &mut f)?;
            if tx.commit()?.is_ok() {
                return Ok(prev);
            }
        }
    }

    /// Atomically updates an item and returns the new value.
    ///
    /// Returning `None` removes the item if it existed before.
    ///
    /// The operation will run wrapped in a transaction.
    ///
    /// # Note
    ///
    /// The provided closure can be called multiple times as this function
    /// automatically retries on conflict. Since this is an `FnMut`, make sure
    /// it is idempotent and will not cause side-effects.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, Slice, PartitionCreateOptions};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    ///
    /// let updated = partition.update_fetch("a", |_| Some(Slice::from(*b"def")))?.unwrap();
    /// assert_eq!(b"def", &*updated);
    ///
    /// let item = partition.get("a")?;
    /// assert_eq!(Some("def".as_bytes().into()), item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    ///
    /// let updated = partition.update_fetch("a", |_| None)?;
    /// assert!(updated.is_none());
    ///
    /// let item = partition.get("a")?;
    /// assert!(item.is_none());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[allow(unused_mut)]
    pub fn update_fetch<K: Into<UserKey>, F: FnMut(Option<&UserValue>) -> Option<UserValue>>(
        &self,
        key: K,
        mut f: F,
    ) -> crate::Result<Option<UserValue>> {
        let key = key.into();

        #[cfg(feature = "single_writer_tx")]
        {
            let mut tx = self.keyspace.write_tx();
            let updated = tx.update_fetch(self, key, f)?;
            tx.commit()?;

            Ok(updated)
        }

        #[cfg(feature = "ssi_tx")]
        loop {
            let mut tx = self.keyspace.write_tx()?;
            let updated = tx.update_fetch(self, key.clone(), &mut f)?;
            if tx.commit()?.is_ok() {
                return Ok(updated);
            }
        }
    }

    /// Inserts a key-value pair into the partition.
    ///
    /// Keys may be up to 65536 bytes long, values up to 2^32 bytes.
    /// Shorter keys and values result in better performance.
    ///
    /// If the key already exists, the item will be overwritten.
    ///
    /// The operation will run wrapped in a transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    ///
    /// assert!(!keyspace.read_tx().is_empty(&partition)?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        key: K,
        value: V,
    ) -> crate::Result<()> {
        #[cfg(feature = "single_writer_tx")]
        {
            let mut tx = self.keyspace.write_tx();
            tx.insert(self, key, value);
            tx.commit()?;
            Ok(())
        }

        #[cfg(feature = "ssi_tx")]
        {
            let mut tx = self.keyspace.write_tx()?;
            tx.insert(self, key, value);
            tx.commit()?.expect("blind insert should not conflict ever");
            Ok(())
        }
    }

    /// Removes an item from the partition.
    ///
    /// The key may be up to 65536 bytes long.
    /// Shorter keys result in better performance.
    ///
    /// The operation will run wrapped in a transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    /// assert!(!keyspace.read_tx().is_empty(&partition)?);
    ///
    /// partition.remove("a")?;
    /// assert!(keyspace.read_tx().is_empty(&partition)?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn remove<K: Into<UserKey>>(&self, key: K) -> crate::Result<()> {
        #[cfg(feature = "single_writer_tx")]
        {
            let mut tx = self.keyspace.write_tx();
            tx.remove(self, key);
            tx.commit()?;
            Ok(())
        }

        #[cfg(feature = "ssi_tx")]
        {
            let mut tx = self.keyspace.write_tx()?;
            tx.remove(self, key);
            tx.commit()?.expect("blind remove should not conflict ever");
            Ok(())
        }
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
    /// The operation will run wrapped in a transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    /// assert!(!keyspace.read_tx().is_empty(&partition)?);
    ///
    /// partition.remove_weak("a")?;
    /// assert!(keyspace.read_tx().is_empty(&partition)?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn remove_weak<K: Into<UserKey>>(&self, key: K) -> crate::Result<()> {
        #[cfg(feature = "single_writer_tx")]
        {
            let mut tx = self.keyspace.write_tx();
            tx.remove_weak(self, key);
            tx.commit()?;
            Ok(())
        }

        #[cfg(feature = "ssi_tx")]
        {
            let mut tx = self.keyspace.write_tx()?;
            tx.remove_weak(self, key);
            tx.commit()?.expect("blind remove should not conflict ever");
            Ok(())
        }
    }

    /// Retrieves an item from the partition.
    ///
    /// The operation will run wrapped in a read snapshot.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "my_value")?;
    ///
    /// let item = partition.get("a")?;
    /// assert_eq!(Some("my_value".as_bytes().into()), item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<lsm_tree::UserValue>> {
        self.inner.get(key)
    }

    /// Retrieves the size of an item from the partition.
    ///
    /// The operation will run wrapped in a read snapshot.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "my_value")?;
    ///
    /// let len = partition.size_of("a")?.unwrap_or_default();
    /// assert_eq!("my_value".len() as u32, len);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn size_of<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<u32>> {
        self.inner.size_of(key)
    }

    /// Returns the first key-value pair in the partition.
    /// The key in this pair is the minimum key in the partition.
    ///
    /// The operation will run wrapped in a read snapshot.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "my_value")?;
    /// partition.insert("b", "my_value")?;
    ///
    /// assert_eq!(b"a", &*partition.first_key_value()?.unwrap().0);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn first_key_value(&self) -> crate::Result<Option<KvPair>> {
        let read_tx = self.keyspace.read_tx();
        read_tx.first_key_value(self)
    }

    /// Returns the last key-value pair in the partition.
    /// The key in this pair is the maximum key in the partition.
    ///
    /// The operation will run wrapped in a read snapshot.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "my_value")?;
    /// partition.insert("b", "my_value")?;
    ///
    /// assert_eq!(b"b", &*partition.last_key_value()?.unwrap().0);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn last_key_value(&self) -> crate::Result<Option<KvPair>> {
        let read_tx = self.keyspace.read_tx();
        read_tx.last_key_value(self)
    }

    /// Returns `true` if the partition contains the specified key.
    ///
    /// The operation will run wrapped in a read snapshot.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "my_value")?;
    ///
    /// assert!(partition.contains_key("a")?);
    /// assert!(!partition.contains_key("b")?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<bool> {
        self.inner.contains_key(key)
    }

    /// Allows access to the inner partition handle, allowing to
    /// escape from the transactional context.
    #[doc(hidden)]
    #[must_use]
    pub fn inner(&self) -> &PartitionHandle {
        &self.inner
    }
}
