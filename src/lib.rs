// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Fjall is an LSM-based embeddable key-value storage engine written in Rust. It features:
//!
//! - Thread-safe BTreeMap-like API
//! - 100% safe & stable Rust
//! - Range & prefix searching with forward and reverse iteration
//! - Cross-partition snapshots (MVCC)
//! - Automatic background maintenance
//! - Single-writer transactions (optional)
//! - Key-value separation for large blob use cases (optional)
//!
//! Each `Keyspace` is a single logical database and is split into `partitions` (a.k.a. column families) - you should probably only use a single keyspace for your application.
//! Each partition is physically a single LSM-tree and its own logical collection; however, write operations across partitions are atomic as they are persisted in a
//! single database-level journal, which will be recovered after a crash.
//!
//! It is not:
//!
//! - a standalone server
//! - a relational database
//! - a wide-column database: it has no notion of columns
//!
//! Keys are limited to 65536 bytes, values are limited to 2^32 bytes. As is normal with any kind of storage engine, larger keys and values have a bigger performance impact.
//!
//! For the underlying LSM-tree implementation, see: <https://crates.io/crates/lsm-tree>.
//!
//! ```
//! use fjall::{Config, PersistMode, Keyspace, PartitionCreateOptions};
//!
//! # let folder = tempfile::tempdir()?;
//! #
//! // A keyspace is a database, which may contain multiple collections ("partitions")
//! // You should probably only use a single keyspace for your application
//! //
//! let keyspace = Config::new(folder).open()?; // or open_transactional for transactional semantics
//!
//! // Each partition is its own physical LSM-tree
//! let items = keyspace.open_partition("my_items", PartitionCreateOptions::default())?;
//!
//! // Write some data
//! items.insert("a", "hello")?;
//!
//! // And retrieve it
//! let bytes = items.get("a")?;
//!
//! // Or remove it again
//! items.remove("a")?;
//!
//! // Search by prefix
//! for kv in items.prefix("prefix") {
//!   // ...
//! }
//!
//! // Search by range
//! for kv in items.range("a"..="z") {
//!   // ...
//! }
//!
//! // Iterators implement DoubleEndedIterator, so you can search backwards, too!
//! for kv in items.prefix("prefix").rev() {
//!   // ...
//! }
//!
//! // Sync the journal to disk to make sure data is definitely durable
//! // When the keyspace is dropped, it will try to persist with `PersistMode::SyncAll` as well
//! keyspace.persist(PersistMode::SyncAll)?;
//! #
//! # Ok::<_, fjall::Error>(())
//! ```

#![doc(html_logo_url = "https://raw.githubusercontent.com/fjall-rs/fjall/main/logo.png")]
#![doc(html_favicon_url = "https://raw.githubusercontent.com/fjall-rs/fjall/main/logo.png")]
#![forbid(unsafe_code)]
#![deny(clippy::all, missing_docs, clippy::cargo)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::indexing_slicing)]
#![warn(clippy::pedantic, clippy::nursery)]
#![warn(clippy::expect_used)]
#![allow(clippy::missing_const_for_fn)]
#![warn(clippy::multiple_crate_versions)]

mod batch;

/// Contains compaction strategies
pub mod compaction;

mod config;

#[cfg(feature = "__internal_whitebox")]
#[doc(hidden)]
pub mod drop;

mod error;
mod file;
mod flush;
mod flush_tracker;
mod gc;
mod iter;
mod journal;
mod keyspace;
mod monitor;
mod partition;
mod path;
mod snapshot_nonce;
mod snapshot_tracker;
mod space_tracker;
mod tracked_snapshot;

#[cfg(any(feature = "single_writer_tx", feature = "ssi_tx"))]
mod tx;

mod version;

pub(crate) type HashMap<K, V> = std::collections::HashMap<K, V, xxhash_rust::xxh3::Xxh3Builder>;

pub use {
    batch::Batch,
    config::Config,
    error::{Error, Result},
    gc::GarbageCollection,
    journal::{error::RecoveryError, writer::PersistMode},
    keyspace::Keyspace,
    partition::{
        options::CreateOptions as PartitionCreateOptions, options::KvSeparationOptions,
        PartitionHandle,
    },
    tracked_snapshot::TrackedSnapshot as Snapshot,
    version::Version,
};

#[cfg(any(feature = "single_writer_tx", feature = "ssi_tx"))]
pub use tx::{
    keyspace::{TransactionalKeyspace, TxKeyspace},
    partition::TransactionalPartitionHandle,
    read_tx::ReadTransaction,
    write_tx::WriteTransaction,
};

/// Alias for [`Batch`]
pub type WriteBatch = Batch;

/// Alias for [`PartitionHandle`]
pub type Partition = PartitionHandle;

/// Alias for [`TransactionalPartitionHandle`]
#[cfg(any(feature = "single_writer_tx", feature = "ssi_tx"))]
pub type TxPartition = TransactionalPartitionHandle;

/// Alias for [`TransactionalPartitionHandle`]
#[cfg(any(feature = "single_writer_tx", feature = "ssi_tx"))]
pub type TxPartitionHandle = TransactionalPartitionHandle;

/// Alias for [`TransactionalPartitionHandle`]
#[cfg(any(feature = "single_writer_tx", feature = "ssi_tx"))]
pub type TransactionalPartition = TransactionalPartitionHandle;

/// A snapshot moment
///
/// See [`Keyspace::instant`].
pub type Instant = lsm_tree::SeqNo;

/// Re-export of [`lsm_tree::Error`]
pub type LsmError = lsm_tree::Error;

#[doc(hidden)]
pub use lsm_tree::AbstractTree;

pub use lsm_tree::{
    AnyTree, BlobCache, BlockCache, CompressionType, KvPair, Slice, TreeType, UserKey, UserValue,
};
