use super::BaseTransaction;
use crate::{
    snapshot_nonce::SnapshotNonce,
    tx::{conflict_manager::ConflictManager, oracle::CommitOutcome},
    PersistMode, TxKeyspace, TxPartitionHandle,
};
use lsm_tree::{KvPair, Slice, UserKey, UserValue};
use std::{
    fmt,
    ops::{Bound, RangeBounds, RangeFull},
};

#[derive(Debug)]
pub struct Conflict;

impl std::error::Error for Conflict {}

impl fmt::Display for Conflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        "transaction conflict".fmt(f)
    }
}

/// A SSI (Serializable Snapshot Isolation) cross-partition transaction
///
/// Use [`WriteTransaction::commit`] to commit changes to the partition(s).
///
/// Drop the transaction to rollback changes.
pub struct WriteTransaction {
    inner: BaseTransaction,
    cm: ConflictManager,
}

impl WriteTransaction {
    pub(crate) fn new(keyspace: TxKeyspace, nonce: SnapshotNonce) -> Self {
        Self {
            inner: BaseTransaction::new(keyspace, nonce),
            cm: ConflictManager::default(),
        }
    }

    /// Sets the durability level.
    #[must_use]
    pub fn durability(mut self, mode: Option<PersistMode>) -> Self {
        self.inner = self.inner.durability(mode);
        self
    }

    /// Removes an item and returns its value if it existed.
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
    /// let mut tx = keyspace.write_tx()?;
    ///
    /// let taken = tx.take(&partition, "a")?.unwrap();
    /// assert_eq!(b"abc", &*taken);
    /// tx.commit()?;
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
    pub fn take<K: Into<UserKey>>(
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
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions, Slice};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    ///
    /// let mut tx = keyspace.write_tx()?;
    ///
    /// let updated = tx.update_fetch(&partition, "a", |_| Some(Slice::from(*b"def")))?.unwrap();
    /// assert_eq!(b"def", &*updated);
    /// tx.commit()?;
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
    /// let mut tx = keyspace.write_tx()?;
    ///
    /// let updated = tx.update_fetch(&partition, "a", |_| None)?;
    /// assert!(updated.is_none());
    /// tx.commit()?;
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
    pub fn update_fetch<K: Into<UserKey>, F: FnMut(Option<&UserValue>) -> Option<UserValue>>(
        &mut self,
        partition: &TxPartitionHandle,
        key: K,
        f: F,
    ) -> crate::Result<Option<UserValue>> {
        let key: UserKey = key.into();

        let updated = self.inner.update_fetch(partition, key.clone(), f)?;

        self.cm.mark_read(&partition.inner.name, key.clone());
        self.cm.mark_conflict(&partition.inner.name, key);

        Ok(updated)
    }

    /// Atomically updates an item and returns the previous value.
    ///
    /// Returning `None` removes the item if it existed before.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions, Slice};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    ///
    /// let mut tx = keyspace.write_tx()?;
    ///
    /// let prev = tx.fetch_update(&partition, "a", |_| Some(Slice::from(*b"def")))?.unwrap();
    /// assert_eq!(b"abc", &*prev);
    /// tx.commit()?;
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
    /// let mut tx = keyspace.write_tx()?;
    ///
    /// let prev = tx.fetch_update(&partition, "a", |_| None)?.unwrap();
    /// assert_eq!(b"abc", &*prev);
    /// tx.commit()?;
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
    pub fn fetch_update<K: Into<UserKey>, F: FnMut(Option<&UserValue>) -> Option<UserValue>>(
        &mut self,
        partition: &TxPartitionHandle,
        key: K,
        f: F,
    ) -> crate::Result<Option<UserValue>> {
        let key = key.into();

        let prev = self.inner.fetch_update(partition, key.clone(), f)?;

        self.cm.mark_read(&partition.inner.name, key.clone());
        self.cm.mark_conflict(&partition.inner.name, key);

        Ok(prev)
    }

    /// Retrieves an item from the transaction's state.
    ///
    /// The transaction allows reading your own writes (RYOW).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "previous_value")?;
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    ///
    /// let mut tx = keyspace.write_tx()?;
    /// tx.insert(&partition, "a", "new_value");
    ///
    /// // Read-your-own-write
    /// let item = tx.get(&partition, "a")?;
    /// assert_eq!(Some("new_value".as_bytes().into()), item);
    ///
    /// drop(tx);
    ///
    /// // Write was not committed
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get<K: AsRef<[u8]>>(
        &mut self,
        partition: &TxPartitionHandle,
        key: K,
    ) -> crate::Result<Option<UserValue>> {
        let res = self.inner.get(partition, key.as_ref())?;

        self.cm
            .mark_read(&partition.inner.name, key.as_ref().into());

        Ok(res)
    }

    /// Retrieves the size of an item from the transaction's state.
    ///
    /// The transaction allows reading your own writes (RYOW).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "previous_value")?;
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    ///
    /// let mut tx = keyspace.write_tx()?;
    /// tx.insert(&partition, "a", "new_value");
    ///
    /// // Read-your-own-write
    /// let len = tx.size_of(&partition, "a")?.unwrap_or_default();
    /// assert_eq!("new_value".len() as u32, len);
    ///
    /// drop(tx);
    ///
    /// // Write was not committed
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Ersr` if an IO error occurs.
    pub fn size_of<K: AsRef<[u8]>>(
        &self,
        partition: &TxPartitionHandle,
        key: K,
    ) -> crate::Result<Option<u32>> {
        self.inner.size_of(partition, key)
    }

    /// Returns `true` if the transaction's state contains the specified key.
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
    /// assert!(keyspace.read_tx().contains_key(&partition, "a")?);
    ///
    /// let mut tx = keyspace.write_tx()?;
    /// assert!(tx.contains_key(&partition, "a")?);
    ///
    /// tx.insert(&partition, "b", "my_value2");
    /// assert!(tx.contains_key(&partition, "b")?);
    ///
    /// // Transaction not committed yet
    /// assert!(!keyspace.read_tx().contains_key(&partition, "b")?);
    ///
    /// tx.commit()?;
    /// assert!(keyspace.read_tx().contains_key(&partition, "b")?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn contains_key<K: AsRef<[u8]>>(
        &mut self,
        partition: &TxPartitionHandle,
        key: K,
    ) -> crate::Result<bool> {
        let contains = self.inner.contains_key(partition, key.as_ref())?;

        self.cm
            .mark_read(&partition.inner.name, key.as_ref().into());

        Ok(contains)
    }

    /// Returns the first key-value pair in the transaction's state.
    /// The key in this pair is the minimum key in the transaction's state.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// #
    /// let mut tx = keyspace.write_tx()?;
    /// tx.insert(&partition, "1", "abc");
    /// tx.insert(&partition, "3", "abc");
    /// tx.insert(&partition, "5", "abc");
    ///
    /// let (key, _) = tx.first_key_value(&partition)?.expect("item should exist");
    /// assert_eq!(&*key, "1".as_bytes());
    ///
    /// assert!(keyspace.read_tx().first_key_value(&partition)?.is_none());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn first_key_value(
        &mut self,
        partition: &TxPartitionHandle,
    ) -> crate::Result<Option<KvPair>> {
        self.iter(partition).next().transpose()
    }

    /// Returns the last key-value pair in the transaction's state.
    /// The key in this pair is the maximum key in the transaction's state.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// #
    /// let mut tx = keyspace.write_tx()?;
    /// tx.insert(&partition, "1", "abc");
    /// tx.insert(&partition, "3", "abc");
    /// tx.insert(&partition, "5", "abc");
    ///
    /// let (key, _) = tx.last_key_value(&partition)?.expect("item should exist");
    /// assert_eq!(&*key, "5".as_bytes());
    ///
    /// assert!(keyspace.read_tx().last_key_value(&partition)?.is_none());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn last_key_value(
        &mut self,
        partition: &TxPartitionHandle,
    ) -> crate::Result<Option<KvPair>> {
        self.iter(partition).next_back().transpose()
    }

    /// Scans the entire partition, returning the amount of items.
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
    /// partition.insert("b", "my_value2")?;
    ///
    /// let mut tx = keyspace.write_tx()?;
    /// assert_eq!(2, tx.len(&partition)?);
    ///
    /// tx.insert(&partition, "c", "my_value3");
    ///
    /// // read-your-own write
    /// assert_eq!(3, tx.len(&partition)?);
    ///
    /// // Transaction is not committed yet
    /// assert_eq!(2, keyspace.read_tx().len(&partition)?);
    ///
    /// tx.commit()?;
    /// assert_eq!(3, keyspace.read_tx().len(&partition)?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn len(&mut self, partition: &TxPartitionHandle) -> crate::Result<usize> {
        let mut count = 0;

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
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// #
    /// let mut tx = keyspace.write_tx()?;
    /// tx.insert(&partition, "a", "abc");
    /// tx.insert(&partition, "f", "abc");
    /// tx.insert(&partition, "g", "abc");
    ///
    /// assert_eq!(3, tx.iter(&partition).count());
    /// assert_eq!(0, keyspace.read_tx().iter(&partition).count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn iter<'a>(
        &'a mut self,
        partition: &TxPartitionHandle,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'a {
        self.cm.mark_range(&partition.inner.name, RangeFull);

        self.inner.iter(partition)
    }

    /// Iterates over the transaction's state, returning keys only.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    #[must_use]
    pub fn keys<'a>(
        &'a mut self,
        partition: &TxPartitionHandle,
    ) -> impl DoubleEndedIterator<Item = crate::Result<UserKey>> + 'a {
        self.cm.mark_range(&partition.inner.name, RangeFull);

        self.inner.keys(partition)
    }

    /// Iterates over the transaction's state, returning values only.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    #[must_use]
    pub fn values<'a>(
        &'a mut self,
        partition: &TxPartitionHandle,
    ) -> impl DoubleEndedIterator<Item = crate::Result<UserValue>> + 'a {
        self.cm.mark_range(&partition.inner.name, RangeFull);

        self.inner.values(partition)
    }

    /// Iterates over a range of the transaction's state.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// #
    /// let mut tx = keyspace.write_tx()?;
    /// tx.insert(&partition, "a", "abc");
    /// tx.insert(&partition, "f", "abc");
    /// tx.insert(&partition, "g", "abc");
    ///
    /// assert_eq!(2, tx.range(&partition, "a"..="f").count());
    /// assert_eq!(0, keyspace.read_tx().range(&partition, "a"..="f").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn range<'b, K: AsRef<[u8]> + 'b, R: RangeBounds<K> + 'b>(
        &'b mut self,
        partition: &'b TxPartitionHandle,
        range: R,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'b {
        // TODO: Bound::map 1.77
        let start: Bound<Slice> = match range.start_bound() {
            Bound::Included(k) => Bound::Included(k.as_ref().into()),
            Bound::Excluded(k) => Bound::Excluded(k.as_ref().into()),
            Bound::Unbounded => Bound::Unbounded,
        };
        // TODO: Bound::map 1.77
        let end: Bound<Slice> = match range.end_bound() {
            Bound::Included(k) => Bound::Included(k.as_ref().into()),
            Bound::Excluded(k) => Bound::Excluded(k.as_ref().into()),
            Bound::Unbounded => Bound::Unbounded,
        };
        self.cm.mark_range(&partition.inner.name, (start, end));

        self.inner.range(partition, range)
    }

    /// Iterates over a prefixed set of the transaction's state.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// #
    /// let mut tx = keyspace.write_tx()?;
    /// tx.insert(&partition, "a", "abc");
    /// tx.insert(&partition, "ab", "abc");
    /// tx.insert(&partition, "abc", "abc");
    ///
    /// assert_eq!(2, tx.prefix(&partition, "ab").count());
    /// assert_eq!(0, keyspace.read_tx().prefix(&partition, "ab").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn prefix<'b, K: AsRef<[u8]> + 'b>(
        &'b mut self,
        partition: &'b TxPartitionHandle,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'b {
        self.range(partition, lsm_tree::range::prefix_to_range(prefix.as_ref()))
    }

    /// Inserts a key-value pair into the partition.
    ///
    /// Keys may be up to 65536 bytes long, values up to 2^32 bytes.
    /// Shorter keys and values result in better performance.
    ///
    /// If the key already exists, the item will be overwritten.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "previous_value")?;
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    ///
    /// let mut tx = keyspace.write_tx()?;
    /// tx.insert(&partition, "a", "new_value");
    ///
    /// drop(tx);
    ///
    /// // Write was not committed
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &mut self,
        partition: &TxPartitionHandle,
        key: K,
        value: V,
    ) {
        let key: UserKey = key.into();

        self.inner.insert(partition, key.clone(), value);
        self.cm.mark_conflict(&partition.inner.name, key);
    }

    /// Removes an item from the partition.
    ///
    /// The key may be up to 65536 bytes long.
    /// Shorter keys result in better performance.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "previous_value")?;
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    ///
    /// let mut tx = keyspace.write_tx()?;
    /// tx.remove(&partition, "a");
    ///
    /// // Read-your-own-write
    /// let item = tx.get(&partition, "a")?;
    /// assert_eq!(None, item);
    ///
    /// drop(tx);
    ///
    /// // Deletion was not committed
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn remove<K: Into<UserKey>>(&mut self, partition: &TxPartitionHandle, key: K) {
        let key: UserKey = key.into();

        self.inner.remove(partition, key.clone());
        self.cm.mark_conflict(&partition.inner.name, key);
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
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "previous_value")?;
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    ///
    /// let mut tx = keyspace.write_tx()?;
    /// tx.remove_weak(&partition, "a");
    ///
    /// // Read-your-own-write
    /// let item = tx.get(&partition, "a")?;
    /// assert_eq!(None, item);
    ///
    /// drop(tx);
    ///
    /// // Deletion was not committed
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn remove_weak<K: Into<UserKey>>(&mut self, partition: &TxPartitionHandle, key: K) {
        let key: UserKey = key.into();

        self.inner.remove_weak(partition, key.clone());
        self.cm.mark_conflict(&partition.inner.name, key);
    }

    /// Commits the transaction.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn commit(self) -> crate::Result<Result<(), Conflict>> {
        // NOTE: We have no write set, so we are basically
        // a read-only transaction, so nothing to do here
        if self.inner.memtables.is_empty() {
            return Ok(Ok(()));
        }

        let oracle = self.inner.keyspace.oracle.clone();

        match oracle.with_commit(self.inner.nonce.instant, self.cm, move || {
            self.inner.commit()
        })? {
            CommitOutcome::Ok => Ok(Ok(())),
            CommitOutcome::Aborted(e) => Err(e),
            CommitOutcome::Conflicted => Ok(Err(Conflict)),
        }
    }

    /// More explicit alternative to dropping the transaction
    /// to roll it back.
    pub fn rollback(self) {
        self.inner.rollback();
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        tx::write::ssi::Conflict, Config, GarbageCollection, KvSeparationOptions,
        PartitionCreateOptions, TransactionalPartitionHandle, TxKeyspace,
    };
    use tempfile::TempDir;
    use test_log::test;

    struct TestEnv {
        ks: TxKeyspace,
        part: TransactionalPartitionHandle,

        #[allow(unused)]
        tmpdir: TempDir,
    }

    impl TestEnv {
        fn seed_hermitage_data(&self) -> crate::Result<()> {
            self.part.insert([1u8], [10u8])?;
            self.part.insert([2u8], [20u8])?;
            Ok(())
        }
    }

    fn setup() -> Result<TestEnv, Box<dyn std::error::Error>> {
        let tmpdir = tempfile::tempdir()?;
        let ks = Config::new(tmpdir.path()).open_transactional()?;

        let part = ks.open_partition("foo", PartitionCreateOptions::default())?;

        Ok(TestEnv { ks, part, tmpdir })
    }

    // Adapted from https://github.com/al8n/skipdb/issues/10
    #[test]
    #[allow(clippy::unwrap_used)]
    fn tx_ssi_arthur_1() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        let mut tx = env.ks.write_tx()?;
        tx.insert(&env.part, "a1", 10u64.to_be_bytes());
        tx.insert(&env.part, "b1", 100u64.to_be_bytes());
        tx.insert(&env.part, "b2", 200u64.to_be_bytes());
        tx.commit()??;

        let mut tx1 = env.ks.write_tx()?;
        let val = tx1
            .range(&env.part, "a".."b")
            .map(|kv| {
                let (_, v) = kv.unwrap();

                let mut buf = [0u8; 8];
                buf.copy_from_slice(&v);
                u64::from_be_bytes(buf)
            })
            .sum::<u64>();
        tx1.insert(&env.part, "b3", 10u64.to_be_bytes());
        assert_eq!(10, val);

        let mut tx2 = env.ks.write_tx()?;
        let val = tx2
            .range(&env.part, "b".."c")
            .map(|kv| {
                let (_, v) = kv.unwrap();

                let mut buf = [0u8; 8];
                buf.copy_from_slice(&v);
                u64::from_be_bytes(buf)
            })
            .sum::<u64>();
        tx2.insert(&env.part, "a3", 300u64.to_be_bytes());
        assert_eq!(300, val);
        tx2.commit()??;
        assert!(matches!(tx1.commit()?, Err(Conflict)));

        let mut tx3 = env.ks.write_tx()?;
        let val = tx3
            .iter(&env.part)
            .filter_map(|kv| {
                let (k, v) = kv.unwrap();

                if k.starts_with(b"a") {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&v);
                    Some(u64::from_be_bytes(buf))
                } else {
                    None
                }
            })
            .sum::<u64>();
        assert_eq!(310, val);

        Ok(())
    }

    // Adapted from https://github.com/al8n/skipdb/issues/10
    #[test]
    #[allow(clippy::unwrap_used)]
    fn tx_ssi_arthur_2() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        let mut tx = env.ks.write_tx()?;
        tx.insert(&env.part, "b1", 100u64.to_be_bytes());
        tx.insert(&env.part, "b2", 200u64.to_be_bytes());
        tx.commit()??;

        let mut tx1 = env.ks.write_tx()?;
        let val = tx1
            .range(&env.part, "a".."b")
            .map(|kv| {
                let (_, v) = kv.unwrap();

                let mut buf = [0u8; 8];
                buf.copy_from_slice(&v);
                u64::from_be_bytes(buf)
            })
            .sum::<u64>();
        tx1.insert(&env.part, "b3", 0u64.to_be_bytes());
        assert_eq!(0, val);

        let mut tx2 = env.ks.write_tx()?;
        let val = tx2
            .range(&env.part, "b".."c")
            .map(|kv| {
                let (_, v) = kv.unwrap();

                let mut buf = [0u8; 8];
                buf.copy_from_slice(&v);
                u64::from_be_bytes(buf)
            })
            .sum::<u64>();
        tx2.insert(&env.part, "a3", 300u64.to_be_bytes());
        assert_eq!(300, val);
        tx2.commit()??;
        assert!(matches!(tx1.commit()?, Err(Conflict)));

        let mut tx3 = env.ks.write_tx()?;
        let val = tx3
            .iter(&env.part)
            .filter_map(|kv| {
                let (k, v) = kv.unwrap();

                if k.starts_with(b"a") {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&v);
                    Some(u64::from_be_bytes(buf))
                } else {
                    None
                }
            })
            .sum::<u64>();
        assert_eq!(300, val);

        Ok(())
    }

    #[test]
    fn tx_ssi_basic() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        let mut tx1 = env.ks.write_tx()?;
        let mut tx2 = env.ks.write_tx()?;

        tx1.insert(&env.part, "hello", "world");

        tx1.commit()??;
        assert!(env.part.contains_key("hello")?);

        assert_eq!(tx2.get(&env.part, "hello")?, None);

        tx2.insert(&env.part, "hello", "world2");
        assert!(matches!(tx2.commit()?, Err(Conflict)));

        let mut tx1 = env.ks.write_tx()?;
        let mut tx2 = env.ks.write_tx()?;

        tx1.iter(&env.part).next();
        tx2.insert(&env.part, "hello", "world2");

        tx1.insert(&env.part, "hello2", "world1");
        tx1.commit()??;

        tx2.commit()??;

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn tx_ssi_ww() -> Result<(), Box<dyn std::error::Error>> {
        // https://en.wikipedia.org/wiki/Write%E2%80%93write_conflict
        let env = setup()?;

        let mut tx1 = env.ks.write_tx()?;
        let mut tx2 = env.ks.write_tx()?;

        tx1.insert(&env.part, "a", "a");
        tx2.insert(&env.part, "b", "c");
        tx1.insert(&env.part, "b", "b");
        tx1.commit()??;

        tx2.insert(&env.part, "a", "c");

        tx2.commit()??;
        assert_eq!(b"c", &*env.part.get("a")?.unwrap());
        assert_eq!(b"c", &*env.part.get("b")?.unwrap());

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn tx_ssi_swap() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        env.part.insert("x", "x")?;
        env.part.insert("y", "y")?;

        let mut tx1 = env.ks.write_tx()?;
        let mut tx2 = env.ks.write_tx()?;

        {
            let x = tx1.get(&env.part, "x")?.unwrap();
            tx1.insert(&env.part, "y", x);
        }

        {
            let y = tx2.get(&env.part, "y")?.unwrap();
            tx2.insert(&env.part, "x", y);
        }

        tx1.commit()??;
        assert!(matches!(tx2.commit()?, Err(Conflict)));

        Ok(())
    }

    #[test]
    fn tx_ssi_write_cycles() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;
        env.seed_hermitage_data()?;

        let mut t1 = env.ks.write_tx()?;
        let mut t2 = env.ks.write_tx()?;

        t1.insert(&env.part, [1u8], [11u8]);
        t2.insert(&env.part, [1u8], [12u8]);
        t1.insert(&env.part, [2u8], [21u8]);
        t1.commit()??;

        assert_eq!(env.part.get([1u8])?, Some([11u8].into()));

        t2.insert(&env.part, [2u8], [22u8]);
        t2.commit()??;

        assert_eq!(env.part.get([1u8])?, Some([12u8].into()));
        assert_eq!(env.part.get([2u8])?, Some([22u8].into()));

        Ok(())
    }

    #[test]
    fn tx_ssi_aborted_reads() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;
        env.seed_hermitage_data()?;

        let mut t1 = env.ks.write_tx()?;
        let mut t2 = env.ks.write_tx()?;

        t1.insert(&env.part, [1u8], [101u8]);

        assert_eq!(t2.get(&env.part, [1u8])?, Some([10u8].into()));

        t1.rollback();

        assert_eq!(t2.get(&env.part, [1u8])?, Some([10u8].into()));

        t2.commit()??;

        Ok(())
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn tx_ssi_anti_dependency_cycles() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;
        env.seed_hermitage_data()?;

        let mut t1 = env.ks.write_tx()?;
        {
            let mut iter = t1.iter(&env.part);
            assert_eq!(iter.next().unwrap()?, ([1u8].into(), [10u8].into()));
            assert_eq!(iter.next().unwrap()?, ([2u8].into(), [20u8].into()));
            assert!(iter.next().is_none());
        }

        let mut t2 = env.ks.write_tx()?;
        let new = t2.update_fetch(&env.part, [2u8], |v| {
            v.and_then(|v| v.first().copied()).map(|v| [v + 5].into())
        })?;
        assert_eq!(new, Some([25u8].into()));
        t2.commit()??;

        let mut t3 = env.ks.write_tx()?;
        {
            let mut iter = t3.iter(&env.part);
            assert_eq!(iter.next().unwrap()?, ([1u8].into(), [10u8].into()));
            assert_eq!(iter.next().unwrap()?, ([2u8].into(), [25u8].into())); // changed here
            assert!(iter.next().is_none());
        }

        t3.commit()??;

        t1.insert(&env.part, [1u8], [0u8]);

        assert!(matches!(t1.commit()?, Err(Conflict)));

        Ok(())
    }

    #[test]
    fn tx_ssi_update_fetch_update() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        let mut t1 = env.ks.write_tx()?;
        let mut t2 = env.ks.write_tx()?;

        let new = t1.update_fetch(&env.part, "hello", |_| Some("world".into()))?;
        assert_eq!(new, Some("world".into()));
        let old = t2.fetch_update(&env.part, "hello", |_| Some("world2".into()))?;
        assert_eq!(old, None);

        t1.commit()??;
        assert!(matches!(t2.commit()?, Err(Conflict)));

        assert_eq!(env.part.get("hello")?, Some("world".into()));

        let mut t1 = env.ks.write_tx()?;
        let mut t2 = env.ks.write_tx()?;

        let old = t1.fetch_update(&env.part, "hello", |_| Some("world3".into()))?;
        assert_eq!(old, Some("world".into()));
        let new = t2.update_fetch(&env.part, "hello2", |_| Some("world2".into()))?;
        assert_eq!(new, Some("world2".into()));

        t1.commit()??;
        t2.commit()??;

        assert_eq!(env.part.get("hello")?, Some("world3".into()));
        assert_eq!(env.part.get("hello2")?, Some("world2".into()));

        Ok(())
    }

    #[test]
    fn tx_ssi_range() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        let mut t1 = env.ks.write_tx()?;
        let mut t2 = env.ks.write_tx()?;

        _ = t1.range(&env.part, "h"..="hello");
        t1.insert(&env.part, "foo", "bar");

        // insert a key INSIDE the range read by t1
        t2.insert(&env.part, "hello", "world");

        t2.commit()??;
        assert!(matches!(t1.commit()?, Err(Conflict)));

        let mut t1 = env.ks.write_tx()?;
        let mut t2 = env.ks.write_tx()?;

        _ = t1.range(&env.part, "h"..="hello");
        t1.insert(&env.part, "foo", "bar");

        // insert a key OUTSIDE the range read by t1
        t2.insert(&env.part, "hello2", "world");

        t2.commit()??;
        t1.commit()??;

        Ok(())
    }

    #[test]
    fn tx_ssi_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        let mut t1 = env.ks.write_tx()?;
        let mut t2 = env.ks.write_tx()?;

        _ = t1.prefix(&env.part, "hello");
        t1.insert(&env.part, "foo", "bar");

        // insert a key MATCHING the prefix read by t1
        t2.insert(&env.part, "hello", "world");

        t2.commit()??;
        assert!(matches!(t1.commit()?, Err(Conflict)));

        let mut t1 = env.ks.write_tx()?;
        let mut t2 = env.ks.write_tx()?;

        _ = t1.prefix(&env.part, "hello");
        t1.insert(&env.part, "foo", "bar");

        // insert a key NOT MATCHING the range read by t1
        t2.insert(&env.part, "foobar", "world");

        t2.commit()??;
        t1.commit()??;

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn tx_ssi_gc_shadowing() -> Result<(), Box<dyn std::error::Error>> {
        let tmpdir = tempfile::tempdir()?;
        let ks = Config::new(tmpdir.path()).open_transactional()?;

        let part = ks.open_partition(
            "foo",
            PartitionCreateOptions::default().with_kv_separation(
                KvSeparationOptions::default()
                    .separation_threshold(/* IMPORTANT: always separate */ 1),
            ),
        )?;

        part.insert("a", "a")?;
        part.inner().rotate_memtable_and_wait()?; // blob file #0

        part.insert("a", "b")?;
        part.inner().rotate_memtable_and_wait()?; // blob file #1

        // NOTE: a->a is now stale

        let mut tx = ks.write_tx()?;
        tx.insert(&part, "a", "tx");

        log::info!("running GC");
        part.gc_scan()?;
        part.gc_with_staleness_threshold(0.0)?;
        // NOTE: The GC has now added a new value handle to the memtable
        // because a->b was written into blob file #2

        log::info!("committing tx");
        tx.commit()??;

        // NOTE: We should see the transaction's write
        assert_eq!(b"tx", &*part.get("a")?.unwrap());

        // NOTE: We should still see the transaction's write
        part.inner().rotate_memtable_and_wait()?;
        assert_eq!(b"tx", &*part.get("a")?.unwrap());

        Ok(())
    }
}
