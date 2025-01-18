// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    batch::{Batch, PartitionKey},
    compaction::manager::CompactionManager,
    config::Config,
    file::{
        fsync_directory, FJALL_MARKER, JOURNALS_FOLDER, LSM_MANIFEST_FILE, PARTITIONS_FOLDER,
        PARTITION_CONFIG_FILE, PARTITION_DELETED_MARKER,
    },
    flush_tracker::FlushTracker,
    journal::{writer::PersistMode, Journal},
    monitor::Monitor,
    partition::name::is_valid_partition_name,
    snapshot_tracker::SnapshotTracker,
    version::Version,
    HashMap, PartitionCreateOptions, PartitionHandle,
};
use lsm_tree::{AbstractTree, AnyTree, SequenceNumberCounter};
use std::{
    fs::{remove_dir_all, File},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc, RwLock,
    },
};
use std_semaphore::Semaphore;

pub type Partitions = HashMap<PartitionKey, PartitionHandle>;

#[allow(clippy::module_name_repetitions)]
pub struct KeyspaceInner {
    /// Dictionary of all partitions
    #[doc(hidden)]
    pub partitions: Arc<RwLock<Partitions>>,

    /// Journal (write-ahead-log/WAL)
    pub(crate) journal: Arc<Journal>,

    /// Keyspace configuration
    #[doc(hidden)]
    pub config: Config,

    /// Current sequence number
    pub(crate) seqno: SequenceNumberCounter,

    /// Current visible sequence number
    pub(crate) visible_seqno: SequenceNumberCounter,

    pub(crate) flush_tracker: Arc<FlushTracker>,

    /// Notifies flush threads
    pub(crate) flush_semaphore: Arc<Semaphore>,

    /// Keeps track of which partitions are most likely to be
    /// candidates for compaction
    pub(crate) compaction_manager: Arc<CompactionManager>,

    /// Stop signal when keyspace is dropped to stop background threads
    stop_signal: lsm_tree::stop_signal::StopSignal,

    /// Counter of background threads
    active_background_threads: Arc<AtomicUsize>,

    /// True if fsync failed
    pub(crate) is_poisoned: Arc<AtomicBool>,

    /// Active compaction conter
    pub(crate) active_compaction_count: Arc<AtomicUsize>,

    #[doc(hidden)]
    pub snapshot_tracker: Arc<SnapshotTracker>,
}

impl Drop for KeyspaceInner {
    fn drop(&mut self) {
        log::trace!("Dropping Keyspace");

        self.stop_signal.send();

        while self
            .active_background_threads
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
        {
            std::thread::sleep(std::time::Duration::from_micros(100));

            // NOTE: Trick threads into waking up
            self.flush_semaphore.release();
            self.compaction_manager.notify_empty();
        }

        self.config.descriptor_table.clear();

        if self.config.clean_path_on_drop {
            log::info!(
                "Deleting keyspace because temporary=true: {:?}",
                self.config.path
            );

            if let Err(err) = remove_dir_all(&self.config.path) {
                eprintln!("Failed to clean up path: {:?} - {err}", self.config.path);
            }
        }

        // IMPORTANT: Break cyclic Arcs
        self.flush_tracker.clear_queues();
        self.compaction_manager.clear();
        self.partitions.write().expect("lock is poisoned").clear();
        self.flush_tracker.clear_items();

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::decrement_drop_counter();
    }
}

/// A keyspace is a single logical database
/// which can house multiple partitions
///
/// In your application, you should create a single keyspace
/// and keep it around for as long as needed
/// (as long as you are using its partitions).
#[derive(Clone)]
#[doc(alias = "database")]
#[doc(alias = "collection")]
pub struct Keyspace(pub(crate) Arc<KeyspaceInner>);

impl std::ops::Deref for Keyspace {
    type Target = KeyspaceInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Keyspace {
    /// Initializes a new atomic write batch.
    ///
    /// Items may be written to multiple partitions, which
    /// will be be updated atomically when the batch is committed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// let mut batch = keyspace.batch();
    ///
    /// assert_eq!(partition.len()?, 0);
    /// batch.insert(&partition, "1", "abc");
    /// batch.insert(&partition, "3", "abc");
    /// batch.insert(&partition, "5", "abc");
    ///
    /// assert_eq!(partition.len()?, 0);
    ///
    /// batch.commit()?;
    /// assert_eq!(partition.len()?, 3);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn batch(&self) -> Batch {
        let mut batch = Batch::new(self.clone());

        if !self.config.manual_journal_persist {
            batch = batch.durability(Some(PersistMode::Buffer));
        }

        batch
    }

    /// Returns the current write buffer size (active + sealed memtables).
    #[must_use]
    pub fn write_buffer_size(&self) -> u64 {
        self.flush_tracker.buffer_size()
    }

    /// Returns the number of active compactions currently running.
    #[doc(hidden)]
    #[must_use]
    pub fn active_compactions(&self) -> usize {
        self.active_compaction_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns the amount of journals on disk.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// assert_eq!(1, keyspace.journal_count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn journal_count(&self) -> usize {
        self.flush_tracker.journal_count()
    }

    /// Returns the disk space usage of the journal.
    #[doc(hidden)]
    #[must_use]
    pub fn journal_disk_space(&self) -> u64 {
        // TODO: 3.0.0 error is not handled because that would break the API...
        self.journal.get_writer().len().unwrap_or_default() + self.flush_tracker.disk_space_used()
    }

    /// Returns the disk space usage of the entire keyspace.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// assert!(keyspace.disk_space() > 0);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    pub fn disk_space(&self) -> u64 {
        let partitions_size = self
            .partitions
            .read()
            .expect("lock is poisoned")
            .values()
            .map(PartitionHandle::disk_space)
            .sum::<u64>();

        self.journal_disk_space() + partitions_size
    }

    /// Flushes the active journal. The durability depends on the [`PersistMode`]
    /// used.
    ///
    /// Persisting only affects durability, NOT consistency! Even without flushing
    /// data is crash-safe.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, PersistMode, Keyspace, PartitionCreateOptions};
    /// # let folder = tempfile::tempdir()?;
    /// let keyspace = Config::new(folder).open()?;
    /// let items = keyspace.open_partition("my_items", PartitionCreateOptions::default())?;
    ///
    /// items.insert("a", "hello")?;
    ///
    /// keyspace.persist(PersistMode::SyncAll)?;
    /// #
    /// # Ok::<_, fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub fn persist(&self, mode: PersistMode) -> crate::Result<()> {
        if self.is_poisoned.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(crate::Error::Poisoned);
        }

        if let Err(e) = self.journal.persist(mode) {
            self.is_poisoned
                .store(true, std::sync::atomic::Ordering::Release);

            log::error!(
                "flush failed, which is a FATAL, and possibly hardware-related, failure: {e:?}"
            );

            return Err(crate::Error::Poisoned);
        };

        Ok(())
    }

    /// Opens a keyspace in the given directory.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub fn open(config: Config) -> crate::Result<Self> {
        log::debug!(
            "block cache capacity={}MiB",
            config.block_cache.capacity() / 1_024 / 1_024,
        );
        log::debug!(
            "blob cache capacity={}MiB",
            config.blob_cache.capacity() / 1_024 / 1_024,
        );

        let keyspace = Self::create_or_recover(config)?;
        keyspace.start_background_threads()?;

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Ok(keyspace)
    }

    /// Same as [`Keyspace::open`], but does not start background threads.
    ///
    /// Needed to open a keyspace without threads for testing.
    ///
    /// Should not be user-facing.
    #[doc(hidden)]
    pub fn create_or_recover(config: Config) -> crate::Result<Self> {
        log::info!("Opening keyspace at {:?}", config.path);

        if config.path.join(FJALL_MARKER).try_exists()? {
            Self::recover(config)
        } else {
            Self::create_new(config)
        }
    }

    /// Starts background threads that maintain the keyspace.
    ///
    /// Should not be called, unless in [`Keyspace::open`]
    /// and should definitely not be user-facing.
    pub(crate) fn start_background_threads(&self) -> crate::Result<()> {
        if self.config.flush_workers_count > 0 {
            self.spawn_flush_worker()?;

            for _ in 0..self.flush_tracker.queue_count() {
                self.flush_semaphore.release();
            }
        }

        log::debug!(
            "Spawning {} compaction threads",
            self.config.compaction_workers_count
        );

        for _ in 0..self.config.compaction_workers_count {
            self.spawn_compaction_worker()?;
        }

        if let Some(ms) = self.config.fsync_ms {
            self.spawn_fsync_thread(ms.into())?;
        }

        self.spawn_monitor_thread()
    }

    /// Destroys the partition, removing all data associated with it.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn delete_partition(&self, handle: PartitionHandle) -> crate::Result<()> {
        let partition_path = handle.path();

        let file = File::create(partition_path.join(PARTITION_DELETED_MARKER))?;
        file.sync_all()?;

        // IMPORTANT: fsync folder on Unix
        fsync_directory(partition_path)?;

        handle
            .is_deleted
            .store(true, std::sync::atomic::Ordering::Release);

        // IMPORTANT: Care, locks partitions map
        self.compaction_manager.remove_partition(&handle.name);

        self.flush_tracker.remove_partition(&handle.name);

        self.partitions
            .write()
            .expect("lock is poisoned")
            .remove(&handle.name);

        Ok(())
    }

    /// Creates or opens a keyspace partition.
    ///
    /// Partition names can be up to 255 characters long, can not be empty and
    /// can only contain alphanumerics, underscore (`_`), dash (`-`), hash tag (`#`) and dollar (`$`).
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    ///
    /// # Panics
    ///
    /// Panics if the partition name is invalid.
    pub fn open_partition(
        &self,
        name: &str,
        create_options: PartitionCreateOptions,
    ) -> crate::Result<PartitionHandle> {
        assert!(is_valid_partition_name(name));

        let mut partitions = self.partitions.write().expect("lock is poisoned");

        Ok(if let Some(partition) = partitions.get(name) {
            partition.clone()
        } else {
            let name: PartitionKey = name.into();

            let handle = PartitionHandle::create_new(self, name.clone(), create_options)?;
            partitions.insert(name, handle.clone());

            #[cfg(feature = "__internal_whitebox")]
            crate::drop::increment_drop_counter();

            handle
        })
    }

    /// Returns the amount of partitions
    #[must_use]
    pub fn partition_count(&self) -> usize {
        self.partitions.read().expect("lock is poisoned").len()
    }

    /// Gets a list of all partition names in the keyspace
    #[must_use]
    pub fn list_partitions(&self) -> Vec<PartitionKey> {
        self.partitions
            .read()
            .expect("lock is poisoned")
            .keys()
            .cloned()
            .collect()
    }

    /// Returns `true` if the partition with the given name exists.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// assert!(!keyspace.partition_exists("default"));
    /// keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// assert!(keyspace.partition_exists("default"));
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn partition_exists(&self, name: &str) -> bool {
        self.partitions
            .read()
            .expect("lock is poisoned")
            .contains_key(name)
    }

    /// Gets the current sequence number.
    ///
    /// Can be used to start a cross-partition snapshot, using [`PartitionHandle::snapshot_at`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// let partition1 = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// let partition2 = keyspace.open_partition("another", PartitionCreateOptions::default())?;
    ///
    /// partition1.insert("abc1", "abc")?;
    /// partition2.insert("abc2", "abc")?;
    ///
    /// let instant = keyspace.instant();
    /// let snapshot1 = partition1.snapshot_at(instant);
    /// let snapshot2 = partition2.snapshot_at(instant);
    ///
    /// assert!(partition1.contains_key("abc1")?);
    /// assert!(partition2.contains_key("abc2")?);
    ///
    /// assert!(snapshot1.contains_key("abc1")?);
    /// assert!(snapshot2.contains_key("abc2")?);
    ///
    /// partition1.insert("def1", "def")?;
    /// partition2.insert("def2", "def")?;
    ///
    /// assert!(!snapshot1.contains_key("def1")?);
    /// assert!(!snapshot2.contains_key("def2")?);
    ///
    /// assert!(partition1.contains_key("def1")?);
    /// assert!(partition2.contains_key("def2")?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn instant(&self) -> crate::Instant {
        self.visible_seqno.get()
    }

    fn check_version<P: AsRef<Path>>(path: P) -> crate::Result<()> {
        let bytes = std::fs::read(path.as_ref().join(FJALL_MARKER))?;

        if let Some(version) = Version::parse_file_header(&bytes) {
            if version != Version::V2 {
                return Err(crate::Error::InvalidVersion(Some(version)));
            }
        } else {
            return Err(crate::Error::InvalidVersion(None));
        }

        Ok(())
    }

    /// Recovers existing keyspace from directory.
    #[allow(clippy::too_many_lines)]
    #[doc(hidden)]
    pub fn recover(config: Config) -> crate::Result<Self> {
        log::info!("Recovering keyspace at {:?}", config.path);

        // TODO:
        // let recovery_mode = config.journal_recovery_mode;

        // Check version
        Self::check_version(&config.path)?;

        // Reload active journal
        let journals_folder = config.path.join(JOURNALS_FOLDER);
        let journal_recovery = Journal::recover(journals_folder)?;
        log::debug!("journal recovery result: {journal_recovery:#?}");

        let active_journal = Arc::new(journal_recovery.active);
        let sealed_journals = journal_recovery.sealed;

        let active_path = active_journal.path();

        // Construct (empty) keyspace, then fill back with partition data
        let inner = KeyspaceInner {
            config,
            journal: active_journal,
            partitions: Arc::new(RwLock::new(Partitions::with_capacity_and_hasher(
                10,
                xxhash_rust::xxh3::Xxh3Builder::new(),
            ))),
            seqno: SequenceNumberCounter::default(),
            visible_seqno: SequenceNumberCounter::default(),
            flush_tracker: Arc::new(FlushTracker::new(active_path)),
            flush_semaphore: Arc::new(Semaphore::new(0)),
            compaction_manager: Arc::default(),
            stop_signal: lsm_tree::stop_signal::StopSignal::default(),
            active_background_threads: Arc::default(),
            is_poisoned: Arc::default(),
            snapshot_tracker: Arc::default(),
            active_compaction_count: Arc::default(),
        };

        let keyspace = Self(Arc::new(inner));

        // Recover partitions
        keyspace.recover_partitions()?;

        // Recover sealed memtables by walking through old journals
        keyspace.flush_tracker.recover_sealed_memtables(
            &keyspace.partitions,
            &keyspace.seqno,
            sealed_journals.into_iter().map(|(_, x)| x),
        )?;

        {
            let partitions = keyspace.partitions.read().expect("lock is poisoned");

            #[cfg(debug_assertions)]
            for partition in partitions.values() {
                // NOTE: If this triggers, the last sealed memtable
                // was not correctly rotated
                debug_assert!(
                    partition.tree.lock_active_memtable().is_empty(),
                    "active memtable is not empty - this is a bug"
                );
            }

            // NOTE: We only need to recover the active journal, if it actually existed before
            // nothing to recover, if we just created it
            if !journal_recovery.was_active_created {
                log::trace!("Recovering active memtables from active journal");

                let reader = keyspace.journal.get_reader()?;

                for batch in reader {
                    let batch = batch?;

                    for item in batch.items {
                        if let Some(partition) = partitions.get(&item.partition) {
                            let tree = &partition.tree;

                            match item.value_type {
                                lsm_tree::ValueType::Value => {
                                    tree.insert(item.key, item.value, batch.seqno);
                                }
                                lsm_tree::ValueType::Tombstone => {
                                    tree.remove(item.key, batch.seqno);
                                }
                                lsm_tree::ValueType::WeakTombstone => {
                                    tree.remove_weak(item.key, batch.seqno);
                                }
                            }
                        }
                    }
                }

                for partition in partitions.values() {
                    let size = partition.tree.active_memtable_size().into();

                    log::trace!(
                        "Recovered active memtable of size {size}B for partition {:?}",
                        partition.name
                    );

                    // IMPORTANT: Add active memtable size to current write buffer size
                    keyspace.flush_tracker.increment_buffer_size(size);

                    // Recover seqno
                    let maybe_next_seqno = partition
                        .tree
                        .get_highest_seqno()
                        .map(|x| x + 1)
                        .unwrap_or_default();

                    keyspace
                        .seqno
                        .fetch_max(maybe_next_seqno, std::sync::atomic::Ordering::AcqRel);

                    log::debug!("Keyspace seqno is now {}", keyspace.seqno.get());
                }
            }
        }

        keyspace.visible_seqno.store(
            keyspace.seqno.load(std::sync::atomic::Ordering::Acquire),
            std::sync::atomic::Ordering::Release,
        );

        Ok(keyspace)
    }

    fn recover_partitions(&self) -> crate::Result<()> {
        use lsm_tree::coding::Decode;

        let partitions_folder = self.config.path.join(PARTITIONS_FOLDER);

        #[allow(clippy::significant_drop_tightening)]
        let mut partitions_lock = self.partitions.write().expect("lock is poisoned");

        for dirent in std::fs::read_dir(&partitions_folder)? {
            let dirent = dirent?;
            let partition_name = dirent.file_name();
            let partition_path = dirent.path();

            assert!(
                dirent.file_type()?.is_dir(),
                "Found stray file in partitions folder",
            );

            log::trace!("Recovering partition {:?}", partition_name);

            // NOTE: Check deletion marker
            if partition_path.join(PARTITION_DELETED_MARKER).try_exists()? {
                log::debug!("Deleting deleted partition {:?}", partition_name);

                // IMPORTANT: First, delete the manifest,
                // once that is deleted, the partition is treated as uninitialized
                // even if the .deleted marker is removed
                //
                // This is important, because if somehow `remove_dir_all` ends up
                // deleting the `.deleted` marker first, we would end up resurrecting
                // the partition
                let manifest_file = partition_path.join(LSM_MANIFEST_FILE);
                if manifest_file.try_exists()? {
                    std::fs::remove_file(manifest_file)?;
                }

                std::fs::remove_dir_all(partition_path)?;
                continue;
            }

            // NOTE: Check for marker, maybe the partition is not fully initialized
            if !partition_path.join(LSM_MANIFEST_FILE).try_exists()? {
                log::debug!("Deleting uninitialized partition {:?}", partition_name);
                std::fs::remove_dir_all(partition_path)?;
                continue;
            }

            let partition_name = partition_name
                .to_str()
                .expect("should be valid partition name");

            let path = partitions_folder.join(partition_name);

            let mut config_file = File::open(partition_path.join(PARTITION_CONFIG_FILE))?;
            let recovered_config = PartitionCreateOptions::decode_from(&mut config_file)?;

            let mut base_config = lsm_tree::Config::new(path)
                .descriptor_table(self.config.descriptor_table.clone())
                .block_cache(self.config.block_cache.clone())
                .blob_cache(self.config.blob_cache.clone());

            base_config.bloom_bits_per_key = recovered_config.bloom_bits_per_key;
            base_config.data_block_size = recovered_config.data_block_size;
            base_config.index_block_size = recovered_config.index_block_size;
            base_config.bloom_bits_per_key = recovered_config.bloom_bits_per_key;
            base_config.compression = recovered_config.compression;

            if let Some(kv_opts) = &recovered_config.kv_separation {
                base_config = base_config
                    .blob_compression(kv_opts.compression)
                    .blob_file_separation_threshold(kv_opts.separation_threshold)
                    .blob_file_target_size(kv_opts.file_target_size);
            }

            let is_blob_tree = partition_path
                .join(lsm_tree::file::BLOBS_FOLDER)
                .try_exists()?;

            let tree = if is_blob_tree {
                AnyTree::Blob(base_config.open_as_blob_tree()?)
            } else {
                AnyTree::Standard(base_config.open()?)
            };

            let partition =
                PartitionHandle::from_keyspace(self, tree, partition_name.into(), recovered_config);

            // Add partition to dictionary
            partitions_lock.insert(partition_name.into(), partition.clone());

            log::trace!("Recovered partition {:?}", partition_name);
        }

        Ok(())
    }

    #[doc(hidden)]
    pub fn create_new(config: Config) -> crate::Result<Self> {
        let path = config.path.clone();
        log::info!("Creating keyspace at {path:?}");

        std::fs::create_dir_all(&path)?;

        let marker_path = path.join(FJALL_MARKER);
        assert!(!marker_path.try_exists()?);

        let journal_folder_path = path.join(JOURNALS_FOLDER);
        let partition_folder_path = path.join(PARTITIONS_FOLDER);

        std::fs::create_dir_all(&journal_folder_path)?;
        std::fs::create_dir_all(&partition_folder_path)?;

        let active_journal_path = journal_folder_path.join("0");
        let journal = Journal::create_new(&active_journal_path)?;
        let journal = Arc::new(journal);

        let inner = KeyspaceInner {
            config,
            journal,
            partitions: Arc::new(RwLock::new(Partitions::with_capacity_and_hasher(
                10,
                xxhash_rust::xxh3::Xxh3Builder::new(),
            ))),
            seqno: SequenceNumberCounter::default(),
            visible_seqno: SequenceNumberCounter::default(),
            flush_tracker: Arc::new(FlushTracker::new(active_journal_path)),
            flush_semaphore: Arc::new(Semaphore::new(0)),
            compaction_manager: Arc::default(),
            stop_signal: lsm_tree::stop_signal::StopSignal::default(),
            active_background_threads: Arc::default(),
            is_poisoned: Arc::default(),
            snapshot_tracker: Arc::default(),
            active_compaction_count: Arc::default(),
        };

        // NOTE: Lastly, fsync .fjall marker, which contains the version
        // -> the keyspace is fully initialized
        let mut file = std::fs::File::create(marker_path)?;
        Version::V2.write_file_header(&mut file)?;
        file.sync_all()?;

        // IMPORTANT: fsync folders on Unix
        fsync_directory(&journal_folder_path)?;
        fsync_directory(&partition_folder_path)?;
        fsync_directory(&path)?;

        Ok(Self(Arc::new(inner)))
    }

    fn spawn_monitor_thread(&self) -> crate::Result<()> {
        let monitor = Monitor::new(self);
        let stop_signal = self.stop_signal.clone();
        let thread_counter = self.active_background_threads.clone();

        thread_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        std::thread::Builder::new()
            .name("monitor".into())
            .spawn(move || {
                while !stop_signal.is_stopped() {
                    let idle = monitor.run();

                    if idle {
                        std::thread::sleep(std::time::Duration::from_millis(250));
                    }
                }

                log::trace!("monitor: exiting because keyspace is dropping");
                thread_counter.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            })
            .map(|_| ())
            .map_err(Into::into)
    }

    fn spawn_fsync_thread(&self, ms: usize) -> crate::Result<()> {
        let journal = self.journal.clone();
        let stop_signal = self.stop_signal.clone();
        let is_poisoned = self.is_poisoned.clone();
        let thread_counter = self.active_background_threads.clone();

        thread_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        std::thread::Builder::new()
            .name("syncer".into())
            .spawn(move || {
                while !stop_signal.is_stopped() {
                    log::trace!("fsync thread: sleeping {ms}ms");
                    std::thread::sleep(std::time::Duration::from_millis(ms as u64));

                    log::trace!("fsync thread: fsyncing journal");
                    if let Err(e) = journal.persist(PersistMode::SyncAll) {
                        is_poisoned.store(true, std::sync::atomic::Ordering::Release);
                        log::error!(
                            "flush failed, which is a FATAL, and possibly hardware-related, failure: {e:?}"
                        );
                        return;
                    }
                }

                log::trace!("fsync thread: exiting because keyspace is dropping");

                thread_counter.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            })
            .map(|_| ())
            .map_err(Into::into)
    }

    fn spawn_compaction_worker(&self) -> crate::Result<()> {
        let compaction_manager = self.compaction_manager.clone();
        let stop_signal = self.stop_signal.clone();
        let thread_counter = self.active_background_threads.clone();
        let snapshot_tracker = self.snapshot_tracker.clone();
        let compaction_counter = self.active_compaction_count.clone();

        thread_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        std::thread::Builder::new()
            .name("compaction".into())
            .spawn(move || {
                while !stop_signal.is_stopped() {
                    log::trace!("compaction: waiting for work");
                    compaction_manager.wait_for();

                    crate::compaction::worker::run(
                        &compaction_manager,
                        &snapshot_tracker,
                        &compaction_counter,
                    );
                }

                log::trace!("compaction thread: exiting because keyspace is dropping");
                thread_counter.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            })
            .map(|_| ())
            .map_err(Into::into)
    }

    /// Only used for internal testing.
    ///
    /// Should NOT be called when there is a flush worker active already!!!
    #[doc(hidden)]
    pub fn force_flush(&self) {
        let parallelism = self.config.flush_workers_count;

        crate::flush::worker::run(
            &self.flush_tracker,
            &self.compaction_manager,
            &self.snapshot_tracker,
            parallelism,
        );
    }

    fn spawn_flush_worker(&self) -> crate::Result<()> {
        let flush_tracker = self.flush_tracker.clone();
        let compaction_manager = self.compaction_manager.clone();
        let flush_semaphore = self.flush_semaphore.clone();
        let snapshot_tracker = self.snapshot_tracker.clone();

        let thread_counter = self.active_background_threads.clone();
        let stop_signal = self.stop_signal.clone();

        let parallelism = self.config.flush_workers_count;

        thread_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        std::thread::Builder::new()
            .name("flusher".into())
            .spawn(move || {
                while !stop_signal.is_stopped() {
                    log::trace!("flush worker: acquiring flush semaphore");
                    flush_semaphore.acquire();

                    crate::flush::worker::run(
                        &flush_tracker,
                        &compaction_manager,
                        &snapshot_tracker,
                        parallelism,
                    );
                }

                log::trace!("flush worker: exiting because keyspace is dropping");
                thread_counter.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            })
            .map(|_| ())
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    // TODO: 3.0.0 if we store the partition as a monotonic integer
    // and the partition's name inside the partition options/manifest
    // we could allow all UTF-8 characters for partition names
    //
    // https://github.com/fjall-rs/fjall/issues/89
    #[test]
    pub fn test_exotic_partition_names() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let config = Config::new(&folder);
        let keyspace = Keyspace::create_or_recover(config)?;

        for name in ["hello$world", "hello#world", "hello.world", "hello_world"] {
            let db = keyspace.open_partition(name, Default::default())?;
            db.insert("a", "a")?;
            assert_eq!(1, db.len()?);
        }

        Ok(())
    }

    #[test]
    pub fn recover_after_rotation_multiple_partitions() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        {
            let config = Config::new(&folder);
            let keyspace = Keyspace::create_or_recover(config)?;
            let db = keyspace.open_partition("default", Default::default())?;
            let db2 = keyspace.open_partition("default2", Default::default())?;

            db.insert("a", "a")?;
            db2.insert("a", "a")?;
            assert_eq!(1, db.len()?);
            assert_eq!(1, db2.len()?);

            assert_eq!(None, db.tree.get_highest_persisted_seqno());
            assert_eq!(None, db2.tree.get_highest_persisted_seqno());

            db.rotate_memtable()?;

            assert_eq!(1, db.len()?);
            assert_eq!(1, db.tree.sealed_memtable_count());

            assert_eq!(1, db2.len()?);
            assert_eq!(0, db2.tree.sealed_memtable_count());

            db2.insert("b", "b")?;
            db2.rotate_memtable()?;

            assert_eq!(1, db.len()?);
            assert_eq!(1, db.tree.sealed_memtable_count());

            assert_eq!(2, db2.len()?);
            assert_eq!(1, db2.tree.sealed_memtable_count());
        }

        {
            // IMPORTANT: We need to allocate enough flush workers
            // because on CI there may not be enough cores by default
            // so the result would be wrong
            let config = Config::new(&folder).flush_workers(16);
            let keyspace = Keyspace::create_or_recover(config)?;
            let db = keyspace.open_partition("default", Default::default())?;
            let db2 = keyspace.open_partition("default2", Default::default())?;

            assert_eq!(1, db.len()?);
            assert_eq!(1, db.tree.sealed_memtable_count());

            assert_eq!(2, db2.len()?);
            assert_eq!(2, db2.tree.sealed_memtable_count());

            assert_eq!(3, keyspace.journal_count());

            keyspace.force_flush();

            assert_eq!(1, db.len()?);
            assert_eq!(0, db.tree.sealed_memtable_count());

            assert_eq!(2, db2.len()?);
            assert_eq!(0, db2.tree.sealed_memtable_count());

            assert_eq!(1, keyspace.journal_count());

            assert_eq!(Some(0), db.tree.get_highest_persisted_seqno());
            assert_eq!(Some(2), db2.tree.get_highest_persisted_seqno());
        }

        Ok(())
    }

    #[test]
    pub fn recover_after_rotation() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        {
            let config = Config::new(&folder);
            let keyspace = Keyspace::create_or_recover(config)?;
            let db = keyspace.open_partition("default", Default::default())?;

            db.insert("a", "a")?;
            assert_eq!(1, db.len()?);

            db.rotate_memtable()?;

            assert_eq!(1, db.len()?);
            assert_eq!(1, db.tree.sealed_memtable_count());
        }

        {
            let config = Config::new(&folder);
            let keyspace = Keyspace::create_or_recover(config)?;
            let db = keyspace.open_partition("default", Default::default())?;

            assert_eq!(1, db.len()?);
            assert_eq!(1, db.tree.sealed_memtable_count());
            assert_eq!(2, keyspace.journal_count());

            keyspace.force_flush();

            assert_eq!(1, db.len()?);
            assert_eq!(0, db.tree.sealed_memtable_count());
            assert_eq!(1, keyspace.journal_count());
        }

        Ok(())
    }

    #[test]
    pub fn force_flush_multiple_partitions() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let config = Config::new(folder);
        let keyspace = Keyspace::create_or_recover(config)?;
        let db = keyspace.open_partition("default", Default::default())?;
        let db2 = keyspace.open_partition("default2", Default::default())?;

        assert_eq!(0, keyspace.write_buffer_size());

        assert_eq!(0, db.segment_count());
        assert_eq!(0, db2.segment_count());

        assert_eq!(0, keyspace.flush_tracker.sealed_journal_count());

        assert_eq!(0, keyspace.flush_tracker.queued_size());

        assert_eq!(0, keyspace.flush_tracker.task_count());

        for _ in 0..100 {
            db.insert(nanoid::nanoid!(), "abc")?;
            db2.insert(nanoid::nanoid!(), "abc")?;
        }

        db.rotate_memtable()?;

        assert_eq!(1, keyspace.flush_tracker.task_count());

        assert_eq!(1, keyspace.flush_tracker.sealed_journal_count());

        for _ in 0..100 {
            db2.insert(nanoid::nanoid!(), "abc")?;
        }

        db2.rotate_memtable()?;

        assert_eq!(2, keyspace.flush_tracker.task_count());

        assert_eq!(2, keyspace.flush_tracker.sealed_journal_count());

        assert_eq!(0, db.segment_count());
        assert_eq!(0, db2.segment_count());

        keyspace.force_flush();

        assert_eq!(0, keyspace.flush_tracker.queued_size());

        assert_eq!(0, keyspace.flush_tracker.task_count());

        assert_eq!(0, keyspace.flush_tracker.sealed_journal_count());

        assert_eq!(0, keyspace.write_buffer_size());
        assert_eq!(1, db.segment_count());
        assert_eq!(1, db2.segment_count());

        Ok(())
    }

    #[test]
    pub fn force_flush() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let config = Config::new(folder);
        let keyspace = Keyspace::create_or_recover(config)?;
        let db = keyspace.open_partition("default", Default::default())?;

        assert_eq!(0, keyspace.write_buffer_size());

        assert_eq!(0, db.segment_count());

        assert_eq!(0, keyspace.flush_tracker.sealed_journal_count());

        assert_eq!(0, keyspace.flush_tracker.queued_size());

        assert_eq!(0, keyspace.flush_tracker.task_count());

        for _ in 0..100 {
            db.insert(nanoid::nanoid!(), "abc")?;
        }

        db.rotate_memtable()?;

        assert_eq!(1, keyspace.flush_tracker.task_count());

        assert_eq!(1, keyspace.flush_tracker.sealed_journal_count());

        assert_eq!(0, db.segment_count());

        keyspace.force_flush();

        assert_eq!(0, keyspace.flush_tracker.queued_size());

        assert_eq!(0, keyspace.flush_tracker.task_count());

        assert_eq!(0, keyspace.flush_tracker.sealed_journal_count());

        assert_eq!(0, keyspace.write_buffer_size());
        assert_eq!(1, db.segment_count());

        Ok(())
    }
}
