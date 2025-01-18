// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod name;
pub mod options;
mod write_delay;

use crate::{
    batch::PartitionKey,
    compaction::manager::CompactionManager,
    config::Config as KeyspaceConfig,
    file::{LSM_MANIFEST_FILE, PARTITIONS_FOLDER, PARTITION_CONFIG_FILE, PARTITION_DELETED_MARKER},
    flush::manager::Task as FlushTask,
    flush_tracker::FlushTracker,
    gc::GarbageCollection,
    journal::{manager::EvictionWatermark, Journal},
    keyspace::Partitions,
    snapshot_nonce::SnapshotNonce,
    snapshot_tracker::SnapshotTracker,
    Error, Keyspace,
};
use lsm_tree::{
    gc::Report as GcReport, AbstractTree, AnyTree, KvPair, SequenceNumberCounter, UserKey,
    UserValue,
};
use options::CreateOptions;
use std::{
    fs::File,
    ops::RangeBounds,
    path::Path,
    sync::{atomic::AtomicBool, Arc, RwLock},
    time::Duration,
};
use std_semaphore::Semaphore;
use write_delay::get_write_delay;

#[allow(clippy::module_name_repetitions)]
pub struct PartitionHandleInner {
    // Internal
    //
    /// Partition name
    pub name: PartitionKey,

    // Partition configuration
    #[doc(hidden)]
    pub config: CreateOptions,

    /// If `true`, the partition is marked as deleted
    pub(crate) is_deleted: AtomicBool,

    /// If `true`, fsync failed during persisting, see `Error::Poisoned`
    pub(crate) is_poisoned: Arc<AtomicBool>,

    /// LSM-tree wrapper
    #[doc(hidden)]
    pub tree: AnyTree,

    // Keyspace stuff
    //
    /// Config of keyspace
    pub(crate) keyspace_config: KeyspaceConfig,

    /// Flush tracker of keyspace
    pub(crate) flush_tracker: Arc<FlushTracker>,

    /// Compaction manager of keyspace
    pub(crate) compaction_manager: Arc<CompactionManager>,

    // TODO: notifying flush worker should probably become a method in FlushManager
    /// Flush semaphore of keyspace
    pub(crate) flush_semaphore: Arc<Semaphore>,

    /// Journal of keyspace
    pub(crate) journal: Arc<Journal>,

    /// Partition map of keyspace
    pub(crate) partitions: Arc<RwLock<Partitions>>,

    /// Sequence number generator of keyspace
    #[doc(hidden)]
    pub seqno: SequenceNumberCounter,

    /// Visible sequence number of keyspace
    #[doc(hidden)]
    pub visible_seqno: SequenceNumberCounter,

    /// Snapshot tracker
    pub(crate) snapshot_tracker: Arc<SnapshotTracker>,
}

impl Drop for PartitionHandleInner {
    fn drop(&mut self) {
        log::trace!("Dropping partition inner: {:?}", self.name);

        if self.is_deleted.load(std::sync::atomic::Ordering::Acquire) {
            let path = &self.tree.tree_config().path;

            // IMPORTANT: First, delete the manifest,
            // once that is deleted, the partition is treated as uninitialized
            // even if the .deleted marker is removed
            //
            // This is important, because if somehow `remove_dir_all` ends up
            // deleting the `.deleted` marker first, we would end up resurrecting
            // the partition
            let manifest_file = path.join(LSM_MANIFEST_FILE);

            // TODO: use https://github.com/rust-lang/rust/issues/31436 if stable
            #[allow(clippy::collapsible_else_if)]
            match manifest_file.try_exists() {
                Ok(exists) => {
                    if exists {
                        if let Err(e) = std::fs::remove_file(manifest_file) {
                            log::error!("Failed to cleanup partition manifest at {path:?}: {e}");
                        } else {
                            if let Err(e) = std::fs::remove_dir_all(path) {
                                log::error!(
                                    "Failed to cleanup deleted partition's folder at {path:?}: {e}"
                                );
                            };
                        }
                    }
                }
                Err(e) => {
                    log::error!("Failed to cleanup partition manifest at {path:?}: {e}");
                }
            };
        }

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::decrement_drop_counter();
    }
}

/// Access to a keyspace partition
///
/// Each partition is backed by an LSM-tree to provide a
/// disk-backed search tree, and can be configured individually.
///
/// A partition generally only takes a little bit of memory and disk space,
/// but does not spawn its own background threads.
#[derive(Clone)]
#[allow(clippy::module_name_repetitions)]
#[doc(alias = "column family")]
#[doc(alias = "locality group")]
#[doc(alias = "table")]
pub struct PartitionHandle(pub(crate) Arc<PartitionHandleInner>);

impl std::ops::Deref for PartitionHandle {
    type Target = PartitionHandleInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq for PartitionHandle {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for PartitionHandle {}

impl std::hash::Hash for PartitionHandle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write(self.name.as_bytes());
    }
}

impl GarbageCollection for PartitionHandle {
    fn gc_scan(&self) -> crate::Result<GcReport> {
        let _nonce = SnapshotNonce::new(self.seqno.get(), self.snapshot_tracker.clone());
        crate::gc::GarbageCollector::scan(self)
    }

    fn gc_with_space_amp_target(&self, factor: f32) -> crate::Result<u64> {
        crate::gc::GarbageCollector::with_space_amp_target(self, factor)
    }

    fn gc_with_staleness_threshold(&self, threshold: f32) -> crate::Result<u64> {
        crate::gc::GarbageCollector::with_staleness_threshold(self, threshold)
    }

    fn gc_drop_stale_segments(&self) -> crate::Result<u64> {
        crate::gc::GarbageCollector::drop_stale_segments(self)
    }
}

impl PartitionHandle {
    pub(crate) fn from_keyspace(
        keyspace: &Keyspace,
        tree: AnyTree,
        name: PartitionKey,
        config: CreateOptions,
    ) -> Self {
        Self(Arc::new(PartitionHandleInner {
            name,
            tree,
            partitions: keyspace.partitions.clone(),
            keyspace_config: keyspace.config.clone(),
            flush_tracker: keyspace.flush_tracker.clone(),
            flush_semaphore: keyspace.flush_semaphore.clone(),
            journal: keyspace.journal.clone(),
            compaction_manager: keyspace.compaction_manager.clone(),
            seqno: keyspace.seqno.clone(),
            visible_seqno: keyspace.visible_seqno.clone(),
            is_deleted: AtomicBool::default(),
            is_poisoned: keyspace.is_poisoned.clone(),
            snapshot_tracker: keyspace.snapshot_tracker.clone(),
            config,
        }))
    }

    /// Creates a new partition.
    pub(crate) fn create_new(
        keyspace: &Keyspace,
        name: PartitionKey,
        config: CreateOptions,
    ) -> crate::Result<Self> {
        use lsm_tree::coding::Encode;

        log::debug!("Creating partition {name:?}");

        let base_folder = keyspace.config.path.join(PARTITIONS_FOLDER).join(&*name);

        if base_folder.join(PARTITION_DELETED_MARKER).try_exists()? {
            log::error!("Failed to open partition, partition is deleted.");
            return Err(Error::PartitionDeleted);
        }

        std::fs::create_dir_all(&base_folder)?;

        // Write config
        let mut file = File::create(base_folder.join(PARTITION_CONFIG_FILE))?;
        config.encode_into(&mut file)?;
        file.sync_all()?;

        let mut base_config = lsm_tree::Config::new(base_folder)
            .descriptor_table(keyspace.config.descriptor_table.clone())
            .block_cache(keyspace.config.block_cache.clone())
            .blob_cache(keyspace.config.blob_cache.clone())
            .data_block_size(config.data_block_size)
            .index_block_size(config.index_block_size)
            .level_count(config.level_count)
            .compression(config.compression);

        if let Some(kv_opts) = &config.kv_separation {
            base_config = base_config
                .blob_compression(kv_opts.compression)
                .blob_file_separation_threshold(kv_opts.separation_threshold)
                .blob_file_target_size(kv_opts.file_target_size);
        }

        #[cfg(feature = "bloom")]
        {
            base_config = base_config.bloom_bits_per_key(config.bloom_bits_per_key);
        }

        let tree = match config.tree_type {
            lsm_tree::TreeType::Standard => AnyTree::Standard(base_config.open()?),
            lsm_tree::TreeType::Blob => AnyTree::Blob(base_config.open_as_blob_tree()?),
        };

        Ok(Self(Arc::new(PartitionHandleInner {
            name,
            config,
            partitions: keyspace.partitions.clone(),
            keyspace_config: keyspace.config.clone(),
            flush_tracker: keyspace.flush_tracker.clone(),
            flush_semaphore: keyspace.flush_semaphore.clone(),
            journal: keyspace.journal.clone(),
            compaction_manager: keyspace.compaction_manager.clone(),
            seqno: keyspace.seqno.clone(),
            visible_seqno: keyspace.visible_seqno.clone(),
            tree,
            is_deleted: AtomicBool::default(),
            is_poisoned: keyspace.is_poisoned.clone(),
            snapshot_tracker: keyspace.snapshot_tracker.clone(),
        })))
    }

    /// Returns the underlying LSM-tree's path.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.tree.tree_config().path.as_path()
    }

    /// Returns the disk space usage of this partition.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// assert_eq!(0, partition.disk_space());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn disk_space(&self) -> u64 {
        self.tree.disk_space()
    }

    /// Returns an iterator that scans through the entire partition.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    /// partition.insert("f", "abc")?;
    /// partition.insert("g", "abc")?;
    /// assert_eq!(3, partition.iter().count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        self.tree.iter().map(|item| item.map_err(Into::into))
    }

    /// Returns an iterator that scans through the entire partition, returning only keys.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    #[must_use]
    pub fn keys(&self) -> impl DoubleEndedIterator<Item = crate::Result<UserKey>> + 'static {
        self.tree.keys().map(|item| item.map_err(Into::into))
    }

    /// Returns an iterator that scans through the entire partition, returning only values.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    #[must_use]
    pub fn values(&self) -> impl DoubleEndedIterator<Item = crate::Result<UserValue>> + 'static {
        self.tree.values().map(|item| item.map_err(Into::into))
    }

    /// Returns an iterator over a range of items.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    /// partition.insert("f", "abc")?;
    /// partition.insert("g", "abc")?;
    /// assert_eq!(2, partition.range("a"..="f").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    pub fn range<'a, K: AsRef<[u8]> + 'a, R: RangeBounds<K> + 'a>(
        &'a self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        self.tree.range(range).map(|item| item.map_err(Into::into))
    }

    /// Returns an iterator over a prefixed set of items.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    /// partition.insert("ab", "abc")?;
    /// partition.insert("abc", "abc")?;
    /// assert_eq!(2, partition.prefix("ab").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    pub fn prefix<'a, K: AsRef<[u8]> + 'a>(
        &'a self,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        self.tree
            .prefix(prefix)
            .map(|item| item.map_err(Into::into))
    }

    /// Approximates the amount of items in the partition.
    ///
    /// For update -or delete-heavy workloads, this value will
    /// diverge from the real value, but is a O(1) operation.
    ///
    /// For insert-only workloads (e.g. logs, time series)
    /// this value is reliable.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// assert_eq!(partition.len()?, 0);
    ///
    /// partition.insert("1", "abc")?;
    /// assert_eq!(partition.approximate_len(), 1);
    ///
    /// partition.remove("1")?;
    /// // Oops! approximate_len will not be reliable here
    /// assert_eq!(partition.approximate_len(), 2);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn approximate_len(&self) -> usize {
        self.tree.approximate_len()
    }

    /// Scans the entire partition, returning the amount of items.
    ///
    /// ###### Caution
    ///
    /// This operation scans the entire partition: O(n) complexity!
    ///
    /// Never, under any circumstances, use .`len()` == 0 to check
    /// if the partition is empty, use [`PartitionHandle::is_empty`] instead.
    ///
    /// If you want an estimate, use [`PartitionHandle::approximate_len`] instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// assert_eq!(partition.len()?, 0);
    ///
    /// partition.insert("1", "abc")?;
    /// partition.insert("3", "abc")?;
    /// partition.insert("5", "abc")?;
    /// assert_eq!(partition.len()?, 3);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn len(&self) -> crate::Result<usize> {
        let mut count = 0;

        for kv in self.iter() {
            let _ = kv?;
            count += 1;
        }

        Ok(count)
    }

    /// Returns `true` if the partition is empty.
    ///
    /// This operation has O(1) complexity.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// assert!(partition.is_empty()?);
    ///
    /// partition.insert("a", "abc")?;
    /// assert!(!partition.is_empty()?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn is_empty(&self) -> crate::Result<bool> {
        self.first_key_value().map(|x| x.is_none())
    }

    /// Returns `true` if the partition contains the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// assert!(!partition.contains_key("a")?);
    ///
    /// partition.insert("a", "abc")?;
    /// assert!(partition.contains_key("a")?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<bool> {
        self.tree.contains_key(key).map_err(Into::into)
    }

    /// Retrieves an item from the partition.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
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
        Ok(self.tree.get(key)?)
    }

    /// Retrieves the size of an item from the partition.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
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
        Ok(self.tree.size_of(key)?)
    }

    /// Returns the first key-value pair in the partition.
    /// The key in this pair is the minimum key in the partition.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("1", "abc")?;
    /// partition.insert("3", "abc")?;
    /// partition.insert("5", "abc")?;
    ///
    /// let (key, _) = partition.first_key_value()?.expect("item should exist");
    /// assert_eq!(&*key, "1".as_bytes());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn first_key_value(&self) -> crate::Result<Option<KvPair>> {
        Ok(self.tree.first_key_value()?)
    }

    /// Returns the last key-value pair in the partition.
    /// The key in this pair is the maximum key in the partition.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("1", "abc")?;
    /// partition.insert("3", "abc")?;
    /// partition.insert("5", "abc")?;
    ///
    /// let (key, _) = partition.last_key_value()?.expect("item should exist");
    /// assert_eq!(&*key, "5".as_bytes());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn last_key_value(&self) -> crate::Result<Option<KvPair>> {
        Ok(self.tree.last_key_value()?)
    }

    // NOTE: Used in tests
    #[doc(hidden)]
    pub fn rotate_memtable_and_wait(&self) -> crate::Result<()> {
        if self.rotate_memtable()? {
            while !self.flush_tracker.is_task_queue_empty() {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
        Ok(())
    }

    /// Returns `true` if the memtable was indeed rotated.
    #[doc(hidden)]
    pub fn rotate_memtable(&self) -> crate::Result<bool> {
        log::debug!("Rotating memtable {:?}", self.name);

        log::trace!("partition: acquiring journal lock");
        let mut journal = self.journal.get_writer();

        // Rotate memtable
        let Some((yanked_id, yanked_memtable)) = self.tree.rotate_memtable() else {
            log::debug!("Got no sealed memtable, someone beat us to it");
            return Ok(false);
        };

        let seqno_map = {
            let partitions = self.partitions.write().expect("lock is poisoned");

            let mut seqnos = Vec::with_capacity(partitions.len());

            for partition in partitions.values() {
                if let Some(lsn) = partition.tree.get_highest_memtable_seqno() {
                    seqnos.push(EvictionWatermark {
                        lsn,
                        partition: partition.clone(),
                    });
                }
            }

            seqnos
        };

        log::trace!("partition: acquiring journal manager lock");
        self.flush_tracker.rotate_journal(&mut journal, seqno_map)?;

        log::trace!("partition: acquiring flush manager lock");
        self.flush_tracker.enqueue_task(FlushTask {
            id: yanked_id,
            partition: self.clone(),
            sealed_memtable: yanked_memtable,
        });

        drop(journal);

        // Notify flush worker that new work has arrived
        self.flush_semaphore.release();

        Ok(true)
    }

    fn check_journal_size(&self) {
        loop {
            let bytes = self.flush_tracker.disk_space_used();

            if bytes <= self.keyspace_config.max_journaling_size_in_bytes {
                if bytes as f64 > self.keyspace_config.max_journaling_size_in_bytes as f64 * 0.9 {
                    log::info!(
                        "partition: write stall because 90% journal threshold has been reached"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(500));
                }

                break;
            }

            log::info!("partition: write halt because of too many journals");
            std::thread::sleep(std::time::Duration::from_millis(100)); // TODO: maybe exponential backoff
        }
    }

    fn check_write_stall(&self) {
        let seg_count = self.tree.first_level_segment_count();

        if seg_count >= 20 {
            if self.tree.is_first_level_disjoint() {
                // NOTE: If the first level is disjoint, we are probably dealing with a monotonic series
                // so nothing to do
                return;
            }

            let sleep_us = get_write_delay(seg_count);

            if sleep_us > 0 {
                log::info!("Stalling writes by {sleep_us}µs, many segments in L0...");
                self.compaction_manager.notify(self.clone());
                std::thread::sleep(Duration::from_micros(sleep_us));
            }
        }
    }

    fn check_write_halt(&self) {
        while self.tree.first_level_segment_count() >= 32 {
            if self.tree.is_first_level_disjoint() {
                // NOTE: If the first level is disjoint, we are probably dealing with a monotonic series
                // so nothing to do
                return;
            }

            log::info!("Halting writes until L0 is cleared up...");
            self.compaction_manager.notify(self.clone());
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    pub(crate) fn check_memtable_overflow(&self, size: u32) -> crate::Result<()> {
        if size > self.config.max_memtable_size {
            self.rotate_memtable().map_err(|e| {
                self.is_poisoned
                    .store(true, std::sync::atomic::Ordering::Relaxed);

                e
            })?;

            self.check_journal_size();
            self.check_write_halt();
        }

        self.check_write_stall();

        Ok(())
    }

    pub(crate) fn check_write_buffer_size(&self, initial_size: u64) {
        let limit = self.keyspace_config.max_write_buffer_size_in_bytes;

        if initial_size > limit {
            let p90_limit = (limit as f64) * 0.9;

            loop {
                let bytes = self.flush_tracker.buffer_size();

                if bytes < limit {
                    if bytes as f64 > p90_limit {
                        log::info!(
                            "partition: write stall because 90% write buffer threshold has been reached"
                        );
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                    break;
                }

                log::info!("partition: write halt because of write buffer saturation");
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.tree.segment_count()
    }

    /// Opens a snapshot of this partition.
    #[must_use]
    pub fn snapshot(&self) -> crate::Snapshot {
        self.snapshot_at(self.seqno.get())
    }

    /// Opens a snapshot of this partition with a given sequence number.
    #[must_use]
    pub fn snapshot_at(&self, seqno: crate::Instant) -> crate::Snapshot {
        crate::Snapshot::new(
            self.tree.snapshot(seqno),
            SnapshotNonce::new(seqno, self.snapshot_tracker.clone()),
        )
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
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    ///
    /// assert!(!partition.is_empty()?);
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
        use std::sync::atomic::Ordering;

        if self.is_deleted.load(Ordering::Relaxed) {
            return Err(crate::Error::PartitionDeleted);
        }

        let key = key.into();
        let value = value.into();

        let mut journal_writer = self.journal.get_writer();

        let seqno = self.seqno.next();

        // IMPORTANT: Check the poisoned flag after getting journal mutex, otherwise TOCTOU
        if self.is_poisoned.load(Ordering::Relaxed) {
            return Err(crate::Error::Poisoned);
        }

        journal_writer.write_raw(&self.name, &key, &value, lsm_tree::ValueType::Value, seqno)?;

        if !self.config.manual_journal_persist {
            journal_writer
                .persist(crate::PersistMode::Buffer)
                .map_err(|e| {
                    log::error!(
                    "persist failed, which is a FATAL, and possibly hardware-related, failure: {e:?}"
                );
                    self.is_poisoned.store(true, Ordering::Relaxed);
                    e
                })?;
        }

        let (item_size, memtable_size) = self.tree.insert(key, value, seqno);

        self.visible_seqno.fetch_max(seqno + 1, Ordering::AcqRel);

        drop(journal_writer);

        let write_buffer_size = self
            .flush_tracker
            .increment_buffer_size(u64::from(item_size));

        self.check_memtable_overflow(memtable_size)?;

        self.check_write_buffer_size(write_buffer_size);

        Ok(())
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
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    ///
    /// let item = partition.get("a")?.expect("should have item");
    /// assert_eq!("abc".as_bytes(), &*item);
    ///
    /// partition.remove("a")?;
    ///
    /// let item = partition.get("a")?;
    /// assert_eq!(None, item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn remove<K: Into<UserKey>>(&self, key: K) -> crate::Result<()> {
        use std::sync::atomic::Ordering;

        if self.is_deleted.load(Ordering::Relaxed) {
            return Err(crate::Error::PartitionDeleted);
        }

        let key = key.into();

        let mut journal_writer = self.journal.get_writer();

        let seqno = self.seqno.next();

        // IMPORTANT: Check the poisoned flag after getting journal mutex, otherwise TOCTOU
        if self.is_poisoned.load(Ordering::Relaxed) {
            return Err(crate::Error::Poisoned);
        }

        journal_writer.write_raw(&self.name, &key, &[], lsm_tree::ValueType::Tombstone, seqno)?;

        if !self.config.manual_journal_persist {
            journal_writer
                .persist(crate::PersistMode::Buffer)
                .map_err(|e| {
                    log::error!(
                        "persist failed, which is a FATAL, and possibly hardware-related, failure: {e:?}"
                    );
                    self.is_poisoned.store(true, Ordering::Relaxed);
                    e
                })?;
        }

        let (item_size, memtable_size) = self.tree.remove(key, seqno);

        self.visible_seqno.fetch_max(seqno + 1, Ordering::AcqRel);

        drop(journal_writer);

        let write_buffer_size = self
            .flush_tracker
            .increment_buffer_size(u64::from(item_size));

        self.check_memtable_overflow(memtable_size)?;
        self.check_write_buffer_size(write_buffer_size);

        Ok(())
    }
}
