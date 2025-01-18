// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{compaction::Strategy as CompactionStrategy, file::MAGIC_BYTES};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use lsm_tree::{CompressionType, TreeType};

/// Configuration options for key-value-separated partitions.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::module_name_repetitions)]
pub struct KvSeparationOptions {
    /// Compression to use for blobs.
    pub(crate) compression: CompressionType,

    /// Blob file (value log segment) target size in bytes
    #[doc(hidden)]
    pub file_target_size: u64,

    /// Key-value separation threshold in bytes
    #[doc(hidden)]
    pub separation_threshold: u32,
}

impl KvSeparationOptions {
    /// Sets the target size of blob files.
    ///
    /// Smaller blob files allow more granular garbage collection
    /// which allows lower space amp for lower write I/O cost.
    ///
    /// Larger blob files decrease the number of files on disk and thus maintenance
    /// overhead.
    ///
    /// Defaults to 128 MiB.
    #[must_use]
    pub fn file_target_size(mut self, bytes: u64) -> Self {
        self.file_target_size = bytes;
        self
    }

    /// Sets the key-value separation threshold in bytes.
    ///
    /// Smaller value will reduce compaction overhead and thus write amplification,
    /// at the cost of lower read performance.
    ///
    /// Defaults to 1 KiB.
    #[must_use]
    pub fn separation_threshold(mut self, bytes: u32) -> Self {
        self.separation_threshold = bytes;
        self
    }
}

impl Default for KvSeparationOptions {
    fn default() -> Self {
        Self {
            #[cfg(feature = "lz4")]
            compression: CompressionType::Lz4,

            #[cfg(all(feature = "miniz", not(feature = "lz4")))]
            compression: CompressionType::Miniz(6),

            #[cfg(not(any(feature = "lz4", feature = "miniz")))]
            compression: CompressionType::None,

            file_target_size: /* 128 MiB */ 128 * 1_024 * 1_024,

            separation_threshold: /* 1 KiB */ 1_024,
        }
    }
}

impl lsm_tree::coding::Encode for KvSeparationOptions {
    fn encode_into<W: std::io::Write>(&self, writer: &mut W) -> Result<(), lsm_tree::EncodeError> {
        self.compression.encode_into(writer)?;
        writer.write_u64::<BigEndian>(self.file_target_size)?;
        writer.write_u32::<BigEndian>(self.separation_threshold)?;
        Ok(())
    }
}

impl lsm_tree::coding::Decode for KvSeparationOptions {
    fn decode_from<R: std::io::Read>(reader: &mut R) -> Result<Self, lsm_tree::DecodeError>
    where
        Self: Sized,
    {
        let compression = CompressionType::decode_from(reader)?;
        let file_target_size = reader.read_u64::<BigEndian>()?;
        let separation_threshold = reader.read_u32::<BigEndian>()?;

        Ok(Self {
            compression,
            file_target_size,
            separation_threshold,
        })
    }
}

/// Options to configure a partition
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug)]
pub struct CreateOptions {
    /// Maximum size of this partition's memtable - can be changed during runtime
    pub(crate) max_memtable_size: u32,

    /// Block size of data blocks.
    pub(crate) data_block_size: u32,

    /// Block size of index blocks.
    pub(crate) index_block_size: u32,

    /// Amount of levels of the LSM tree (depth of tree).
    pub(crate) level_count: u8,

    /// Bits per key for levels that are not L0, L1, L2
    // NOTE: bloom_bits_per_key is not conditionally compiled,
    // because that would change the file format
    pub(crate) bloom_bits_per_key: i8,

    /// Tree type, see [`TreeType`].
    pub(crate) tree_type: TreeType,

    /// Compression to use.
    pub(crate) compression: CompressionType,

    pub(crate) manual_journal_persist: bool,

    pub(crate) compaction_strategy: CompactionStrategy,

    pub(crate) kv_separation: Option<KvSeparationOptions>,
}

impl lsm_tree::coding::Encode for CreateOptions {
    fn encode_into<W: std::io::Write>(&self, writer: &mut W) -> Result<(), lsm_tree::EncodeError> {
        writer.write_all(MAGIC_BYTES)?;

        writer.write_u8(self.level_count)?;
        writer.write_u8(self.tree_type.into())?;

        writer.write_u32::<BigEndian>(self.max_memtable_size)?;
        writer.write_u32::<BigEndian>(self.data_block_size)?;
        writer.write_u32::<BigEndian>(self.index_block_size)?;

        self.compression.encode_into(writer)?;

        writer.write_u8(u8::from(self.manual_journal_persist))?;

        writer.write_i8(self.bloom_bits_per_key)?;

        // TODO: move into compaction module
        match &self.compaction_strategy {
            CompactionStrategy::Leveled(s) => {
                writer.write_u8(0)?;
                writer.write_u8(s.l0_threshold)?;
                writer.write_u8(s.level_ratio)?;
                writer.write_u32::<BigEndian>(s.target_size)?;
            }
            CompactionStrategy::SizeTiered(s) => {
                writer.write_u8(1)?;
                writer.write_u8(s.level_ratio)?;
                writer.write_u32::<BigEndian>(s.base_size)?;
            }
            CompactionStrategy::Fifo(s) => {
                writer.write_u8(2)?;
                writer.write_u64::<BigEndian>(s.limit)?;

                match s.ttl_seconds {
                    Some(s) => {
                        writer.write_u8(1)?;
                        writer.write_u64::<BigEndian>(s)
                    }
                    None => writer.write_u8(0),
                }?;
            }
        }

        match &self.kv_separation {
            Some(opts) => {
                writer.write_u8(1)?;
                opts.encode_into(writer)?;
            }
            None => {
                writer.write_u8(0)?;
            }
        }

        Ok(())
    }
}

impl lsm_tree::coding::Decode for CreateOptions {
    fn decode_from<R: std::io::Read>(reader: &mut R) -> Result<Self, lsm_tree::DecodeError>
    where
        Self: Sized,
    {
        let mut header = [0; MAGIC_BYTES.len()];
        reader.read_exact(&mut header)?;

        if header != MAGIC_BYTES {
            return Err(lsm_tree::DecodeError::InvalidHeader(
                "PartitionCreateOptions",
            ));
        }

        let level_count = reader.read_u8()?;

        let tree_type = reader.read_u8()?;
        let tree_type: TreeType = tree_type
            .try_into()
            .map_err(|()| lsm_tree::DecodeError::InvalidTag(("TreeType", tree_type)))?;

        let max_memtable_size = reader.read_u32::<BigEndian>()?;
        let data_block_size = reader.read_u32::<BigEndian>()?;
        let index_block_size = reader.read_u32::<BigEndian>()?;

        let compression = CompressionType::decode_from(reader)?;

        let manual_journal_persist = reader.read_u8()? != 0;

        let bloom_bits_per_key = reader.read_i8()?;

        // TODO: move into compaction module
        let compaction_tag = reader.read_u8()?;
        let compaction_strategy = match compaction_tag {
            0 => {
                let l0_threshold = reader.read_u8()?;
                let level_ratio = reader.read_u8()?;
                let target_size = reader.read_u32::<BigEndian>()?;

                CompactionStrategy::Leveled(crate::compaction::Leveled {
                    l0_threshold,
                    target_size,
                    level_ratio,
                    ..Default::default()
                })
            }
            1 => {
                let level_ratio = reader.read_u8()?;
                let base_size = reader.read_u32::<BigEndian>()?;

                CompactionStrategy::SizeTiered(crate::compaction::SizeTiered {
                    base_size,
                    level_ratio,
                })
            }
            2 => {
                let limit = reader.read_u64::<BigEndian>()?;

                let ttl_tag = reader.read_u8()?;
                let ttl_seconds = match ttl_tag {
                    0 => None,
                    1 => Some(reader.read_u64::<BigEndian>()?),
                    _ => return Err(lsm_tree::DecodeError::InvalidTag(("TtlSeconds", ttl_tag))),
                };

                CompactionStrategy::Fifo(crate::compaction::Fifo::new(limit, ttl_seconds))
            }
            _ => {
                return Err(lsm_tree::DecodeError::InvalidTag((
                    "CompactionStrategy",
                    compaction_tag,
                )));
            }
        };

        let kv_sep_tag = reader.read_u8()?;
        let kv_separation = match kv_sep_tag {
            0 => None,
            1 => Some(KvSeparationOptions::decode_from(reader)?),
            _ => {
                return Err(lsm_tree::DecodeError::InvalidTag((
                    "KvSeparationOptions",
                    kv_sep_tag,
                )));
            }
        };

        Ok(Self {
            max_memtable_size,
            data_block_size,
            index_block_size,
            level_count,
            bloom_bits_per_key,
            tree_type,
            compression,
            manual_journal_persist,
            compaction_strategy,
            kv_separation,
        })
    }
}

impl Default for CreateOptions {
    fn default() -> Self {
        let default_tree_config = lsm_tree::Config::default();

        Self {
            manual_journal_persist: false,

            max_memtable_size: /* 16 MiB */ 16 * 1_024 * 1_024,

            data_block_size: default_tree_config.data_block_size,
            index_block_size: default_tree_config.index_block_size,
            bloom_bits_per_key: default_tree_config.bloom_bits_per_key,
            level_count: default_tree_config.level_count,

            tree_type: TreeType::Standard,

            #[cfg(feature = "lz4")]
            compression: CompressionType::Lz4,

            #[cfg(all(feature = "miniz", not(feature = "lz4")))]
            compression: CompressionType::Miniz(6),

            #[cfg(not(any(feature = "lz4", feature = "miniz")))]
            compression: CompressionType::None,

            kv_separation: None,

            compaction_strategy: CompactionStrategy::default(),
        }
    }
}

impl CreateOptions {
    #[must_use]
    #[doc(hidden)]
    pub fn use_bloom_filters(mut self, flag: bool) -> Self {
        self.bloom_bits_per_key = if flag { 10 } else { -1 };
        self
    }

    /// Sets the compression method.
    ///
    /// Once set for a partition, this property is not considered in the future.
    ///
    /// Default = In order: Lz4 -> Miniz -> None, depending on compilation flags
    #[must_use]
    pub fn compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;

        if let Some(opts) = &mut self.kv_separation {
            opts.compression = compression;
        }

        self
    }

    /// Sets the compaction strategy.
    ///
    /// Default = Leveled
    #[must_use]
    pub fn compaction_strategy(mut self, compaction_strategy: CompactionStrategy) -> Self {
        self.compaction_strategy = compaction_strategy;
        self
    }

    /// If `false`, writes will flush data to the operating system.
    ///
    /// Default = false
    ///
    /// Set to `true` to handle persistence manually, e.g. manually using `PersistMode::SyncData`.
    #[must_use]
    pub fn manual_journal_persist(mut self, flag: bool) -> Self {
        self.manual_journal_persist = flag;
        self
    }

    /// Sets the maximum memtable size.
    ///
    /// Default = 16 MiB
    ///
    /// Recommended size 8 - 64 MiB, depending on how much memory
    /// is available.
    ///
    /// Note that the memory usage may temporarily be `max_memtable_size * flush_worker_count`
    /// because of parallel flushing.
    /// Use the keyspace's `max_write_buffer_size` to cap global memory usage.
    ///
    /// Conversely, if `max_memtable_size` is larger than 64 MiB,
    /// it may require increasing the keyspace's `max_write_buffer_size`.
    #[must_use]
    pub fn max_memtable_size(mut self, bytes: u32) -> Self {
        self.max_memtable_size = bytes;
        self
    }

    /// Sets the block size.
    ///
    /// Once set for a partition, this property is not considered in the future.
    ///
    /// Default = 4 KiB
    ///
    /// For point read heavy workloads (get) a sensible default is
    /// somewhere between 4 - 8 KiB, depending on the average value size.
    ///
    /// For scan heavy workloads (range, prefix), use 16 - 64 KiB
    /// which also increases compression efficiency.
    ///
    /// # Panics
    ///
    /// Panics if the block size is smaller than 1 KiB or larger than 512 KiB.
    #[must_use]
    pub fn block_size(mut self, block_size: u32) -> Self {
        assert!(block_size >= 1_024);
        assert!(block_size <= 512 * 1_024);

        self.data_block_size = block_size;
        self.index_block_size = block_size;

        self
    }

    /*   /// Sets the level count (depth of the tree).
    ///
    /// Once set for a partition, this property is not considered in the future.
    ///
    /// Default = 7
    ///
    /// # Panics
    ///
    /// Panics if `n` is less than 2.
    #[must_use]
    pub fn level_count(mut self, n: u8) -> Self {
        assert!(n > 1);

        self.level_count = n;
        self
    } */

    /// Enables key-value separation for this partition.
    ///
    /// Key-value separation is intended for large value scenarios (1 KiB+ per KV).
    /// Large values will be separated into a log-structured value log, which heavily
    /// decreases compaction overhead at the cost of slightly higher read latency
    /// and higher temporary space usage.
    /// Also, garbage collection for deleted or outdated values becomes lazy, so
    /// GC needs to be triggered *manually*.
    ///
    /// Once set for a partition, this property is not considered in the future.
    ///
    /// Default = disabled
    #[must_use]
    pub fn with_kv_separation(mut self, mut opts: KvSeparationOptions) -> Self {
        self.tree_type = TreeType::Blob;

        opts.compression = self.compression;
        self.kv_separation = Some(opts);

        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    #[cfg(not(any(feature = "lz4", feature = "miniz")))]
    fn partition_opts_compression_none() {
        let mut c = CreateOptions::default();
        assert_eq!(c.compression, CompressionType::None);
        assert_eq!(c.kv_separation, None);

        c = c.with_kv_separation(KvSeparationOptions::default());
        assert_eq!(
            c.kv_separation.as_ref().unwrap().compression,
            CompressionType::None,
        );

        c = c.compression(CompressionType::None);
        assert_eq!(c.compression, CompressionType::None);
        assert_eq!(c.kv_separation.unwrap().compression, CompressionType::None);
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    #[cfg(feature = "lz4")]
    fn partition_opts_compression_default() {
        let mut c = CreateOptions::default();
        assert_eq!(c.compression, CompressionType::Lz4);
        assert_eq!(c.kv_separation, None);

        c = c.with_kv_separation(KvSeparationOptions::default());
        assert_eq!(
            c.kv_separation.as_ref().unwrap().compression,
            CompressionType::Lz4,
        );

        c = c.compression(CompressionType::None);
        assert_eq!(c.compression, CompressionType::None);
        assert_eq!(c.kv_separation.unwrap().compression, CompressionType::None);
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    #[cfg(not(feature = "lz4"))]
    #[cfg(feature = "miniz")]
    fn partition_opts_compression_miniz() {
        let mut c = CreateOptions::default();
        assert_eq!(c.compression, CompressionType::Miniz(6));
        assert_eq!(c.kv_separation, None);

        c = c.with_kv_separation(KvSeparationOptions::default());
        assert_eq!(
            c.kv_separation.as_ref().unwrap().compression,
            CompressionType::Miniz(6),
        );

        c = c.compression(CompressionType::None);
        assert_eq!(c.compression, CompressionType::None);
        assert_eq!(c.kv_separation.unwrap().compression, CompressionType::None);
    }

    #[test]
    #[cfg(all(feature = "miniz", feature = "lz4"))]
    fn partition_opts_compression_all() {
        let mut c = CreateOptions::default();
        assert_eq!(c.compression, CompressionType::Lz4);
        assert_eq!(c.kv_separation, None);

        c = c.with_kv_separation(KvSeparationOptions::default());
        assert_eq!(
            c.kv_separation.as_ref().unwrap().compression,
            CompressionType::Lz4,
        );

        c = c.compression(CompressionType::None);
        assert_eq!(c.compression, CompressionType::None);
        assert_eq!(
            c.kv_separation.as_ref().unwrap().compression,
            CompressionType::None
        );

        c = c.compression(CompressionType::Miniz(3));
        assert_eq!(c.compression, CompressionType::Miniz(3));
        assert_eq!(
            c.kv_separation.unwrap().compression,
            CompressionType::Miniz(3)
        );
    }
}
