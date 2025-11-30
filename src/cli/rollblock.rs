use std::time::Duration;

use clap::{Args, ValueEnum, builder::BoolishValueParser};
use serde::Deserialize;

use super::parse_duration;

macro_rules! define_usize_default_with_help {
    ($value_ident:ident, $help_ident:ident, $value:literal, $help_prefix:literal, $help_suffix:literal) => {
        pub const $value_ident: usize = $value;
        pub const $help_ident: &str = concat!($help_prefix, stringify!($value), $help_suffix);
    };
}

macro_rules! define_bool_default_with_help {
    ($value_ident:ident, $help_ident:ident, $value:literal, $help_prefix:literal, $help_suffix:literal) => {
        pub const $value_ident: bool = $value;
        pub const $help_ident: &str = concat!($help_prefix, stringify!($value), $help_suffix);
    };
}

macro_rules! define_async_defaults {
    (
        pending => ($pending_ident:ident, $pending_help_ident:ident, $pending_prefix:literal, $pending_suffix:literal),
        relaxed => ($relaxed_ident:ident, $relaxed_help_ident:ident, $relaxed_prefix:literal, $relaxed_suffix:literal),
        sync_relaxed => ($sync_ident:ident, $sync_help_ident:ident, $sync_prefix:literal, $sync_suffix:literal),
        mode_help => ($mode_help_ident:ident, $mode_prefix:literal, $mode_suffix:literal),
        values => {
            pending: $pending_value:literal,
            relaxed: $relaxed_value:literal,
            sync_relaxed: $sync_value:literal,
        }
    ) => {
        pub const $pending_ident: usize = $pending_value;
        pub const $pending_help_ident: &str =
            concat!($pending_prefix, stringify!($pending_value), $pending_suffix);
        pub const $relaxed_ident: usize = $relaxed_value;
        pub const $relaxed_help_ident: &str =
            concat!($relaxed_prefix, stringify!($relaxed_value), $relaxed_suffix);
        pub const $sync_ident: usize = $sync_value;
        pub const $sync_help_ident: &str =
            concat!($sync_prefix, stringify!($sync_value), $sync_suffix);
        pub const $mode_help_ident: &str = concat!(
            $mode_prefix,
            stringify!($pending_value),
            " sync_every=",
            stringify!($relaxed_value),
            $mode_suffix
        );
    };
}

define_usize_default_with_help!(
    DEFAULT_ROLLBLOCK_SHARDS_COUNT,
    HELP_ROLLBLOCK_SHARDS_COUNT,
    16,
    "Optional. Number of shards to initialize; existing stores keep their recorded layout. [default: ",
    " (new stores)]"
);
define_usize_default_with_help!(
    DEFAULT_ROLLBLOCK_INITIAL_CAPACITY,
    HELP_ROLLBLOCK_INITIAL_CAPACITY,
    5_000_000,
    "Optional. Initial capacity per shard for new stores; existing stores retain their recorded size unless overridden. [default: ",
    " (new stores)]"
);
define_usize_default_with_help!(
    DEFAULT_ROLLBLOCK_THREAD_COUNT,
    HELP_ROLLBLOCK_THREAD_COUNT,
    4,
    "Optional. Worker threads for block application; existing stores reuse the on-disk value unless overridden. [default: ",
    " (new stores)]"
);
define_bool_default_with_help!(
    DEFAULT_ROLLBLOCK_COMPRESS_JOURNAL,
    HELP_ROLLBLOCK_COMPRESS_JOURNAL,
    false,
    "Optional. Enable or disable zstd compression for the journal; existing stores keep their previous setting unless overridden. [default: ",
    " (new stores)]"
);
define_async_defaults!(
    pending => (
        DEFAULT_ROLLBLOCK_ASYNC_MAX_PENDING_BLOCKS,
        HELP_ROLLBLOCK_ASYNC_MAX_PENDING_BLOCKS,
        "Optional. Queue length for async durability modes. [default: ",
        "]"
    ),
    relaxed => (
        DEFAULT_ROLLBLOCK_ASYNC_RELAXED_SYNC_EVERY,
        HELP_ROLLBLOCK_ASYNC_RELAXED_SYNC_EVERY,
        "Optional. fsync cadence (in blocks) for async_relaxed mode. [default: ",
        "]"
    ),
    sync_relaxed => (
        DEFAULT_ROLLBLOCK_SYNCHRONOUS_RELAXED_SYNC_EVERY,
        HELP_ROLLBLOCK_SYNCHRONOUS_RELAXED_SYNC_EVERY,
        "Optional. fsync cadence (in blocks) for synchronous_relaxed mode. [default: ",
        "]"
    ),
    mode_help => (
        HELP_ROLLBLOCK_DURABILITY_MODE,
        "Optional. Select the durability strategy for rollblock persistence. [default: async_relaxed (max_pending_blocks=",
        ")]"
    ),
    values => {
        pending: 1_024,
        relaxed: 100,
        sync_relaxed: 25,
    }
);

#[derive(Clone, Copy, Debug, Deserialize)]
pub enum RollblockMode {
    New,
    Existing,
}

impl Default for RollblockMode {
    fn default() -> Self {
        Self::Existing
    }
}

#[derive(ValueEnum, Clone, Debug, Deserialize)]
pub enum RollblockDurabilityKind {
    #[value(name = "async")]
    Async,
    #[value(name = "async_relaxed")]
    AsyncRelaxed,
    #[value(name = "synchronous")]
    Synchronous,
    #[value(name = "synchronous_relaxed")]
    SynchronousRelaxed,
}

#[derive(Args, Debug, Clone, Default, Deserialize)]
#[command(next_help_heading = "Rollblock parameters")]
#[serde(default)]
pub struct RollblockOptions {
    #[arg(
        long = "rollblock_shards_count",
        alias = "rollblock-shards-count",
        env = "ROLLBLOCK_SHARDS",
        value_name = "COUNT",
        help = HELP_ROLLBLOCK_SHARDS_COUNT
    )]
    pub shards_count: Option<usize>,

    #[arg(
        long = "rollblock_initial_capacity",
        alias = "rollblock-initial-capacity",
        env = "ROLLBLOCK_INITIAL_CAPACITY",
        value_name = "ENTRIES",
        help = HELP_ROLLBLOCK_INITIAL_CAPACITY
    )]
    pub initial_capacity: Option<usize>,

    #[arg(
        long = "rollblock_thread_count",
        alias = "rollblock-thread-count",
        env = "ROLLBLOCK_THREAD_COUNT",
        id = "rollblock_thread_count",
        value_name = "COUNT",
        help = HELP_ROLLBLOCK_THREAD_COUNT
    )]
    pub thread_count: Option<usize>,

    #[arg(
        long = "rollblock_compress_journal",
        alias = "rollblock-compress-journal",
        env = "ROLLBLOCK_COMPRESS_JOURNAL",
        value_name = "BOOL",
        value_parser = BoolishValueParser::new(),
        help = HELP_ROLLBLOCK_COMPRESS_JOURNAL
    )]
    pub compress_journal: Option<bool>,

    #[arg(
        long = "rollblock_snapshot_interval",
        alias = "rollblock-snapshot-interval",
        env = "ROLLBLOCK_SNAPSHOT_INTERVAL",
        value_name = "DURATION",
        value_parser = parse_duration,
        help = "Optional. Frequency of automatic snapshots."
    )]
    #[serde(default, with = "humantime_serde::option")]
    pub snapshot_interval: Option<Duration>,

    #[arg(
        long = "rollblock_max_snapshot_interval",
        alias = "rollblock-max-snapshot-interval",
        env = "ROLLBLOCK_MAX_SNAPSHOT_INTERVAL",
        value_name = "DURATION",
        value_parser = parse_duration,
        help = "Optional. Maximum tolerated lag between snapshots."
    )]
    #[serde(default, with = "humantime_serde::option")]
    pub max_snapshot_interval: Option<Duration>,

    #[arg(
        long = "rollblock_journal_compression_level",
        alias = "rollblock-journal-compression-level",
        env = "ROLLBLOCK_JOURNAL_COMPRESSION_LEVEL",
        value_name = "LEVEL",
        help = "Optional. zstd compression level when compression is enabled."
    )]
    pub journal_compression_level: Option<i32>,

    #[arg(
        long = "rollblock_journal_chunk_size_bytes",
        alias = "rollblock-journal-chunk-size-bytes",
        env = "ROLLBLOCK_JOURNAL_CHUNK_SIZE",
        value_name = "BYTES",
        help = "Optional. Maximum on-disk size for a single journal chunk."
    )]
    pub journal_chunk_size_bytes: Option<u64>,

    #[arg(
        long = "rollblock_lmdb_map_size",
        alias = "rollblock-lmdb-map-size",
        env = "ROLLBLOCK_LMDB_MAP_SIZE",
        value_name = "BYTES",
        help = "Optional. LMDB map size (metadata database upper bound)."
    )]
    pub lmdb_map_size: Option<usize>,

    #[arg(
        long = "rollblock_min_rollback_window",
        alias = "rollblock-min-rollback-window",
        env = "ROLLBLOCK_MIN_ROLLBACK_WINDOW",
        value_name = "BLOCKS",
        help = "Optional. Minimum number of blocks retained for rollback."
    )]
    pub min_rollback_window: Option<u64>,

    #[arg(
        long = "rollblock_prune_interval",
        alias = "rollblock-prune-interval",
        env = "ROLLBLOCK_PRUNE_INTERVAL",
        value_name = "DURATION",
        value_parser = parse_duration,
        help = "Optional. Interval between background pruning passes."
    )]
    #[serde(default, with = "humantime_serde::option")]
    pub prune_interval: Option<Duration>,

    #[arg(
        long = "rollblock_bootstrap_block_profile",
        alias = "rollblock-bootstrap-block-profile",
        env = "ROLLBLOCK_BOOTSTRAP_BLOCK_PROFILE",
        value_name = "BLOCKS",
        help = "Optional. Estimated blocks per journal chunk before history accumulates."
    )]
    pub bootstrap_block_profile: Option<u64>,

    #[arg(
        long = "rollblock_durability_mode",
        alias = "rollblock-durability-mode",
        env = "ROLLBLOCK_DURABILITY_MODE",
        value_enum,
        value_name = "MODE",
        help = HELP_ROLLBLOCK_DURABILITY_MODE
    )]
    pub durability_mode: Option<RollblockDurabilityKind>,

    #[arg(
        long = "rollblock_async_max_pending_blocks",
        alias = "rollblock-async-max-pending-blocks",
        env = "ROLLBLOCK_ASYNC_MAX_PENDING_BLOCKS",
        value_name = "BLOCKS",
        help = HELP_ROLLBLOCK_ASYNC_MAX_PENDING_BLOCKS
    )]
    pub async_max_pending_blocks: Option<usize>,

    #[arg(
        long = "rollblock_async_relaxed_sync_every",
        alias = "rollblock-async-relaxed-sync-every",
        env = "ROLLBLOCK_ASYNC_RELAXED_SYNC_EVERY",
        value_name = "BLOCKS",
        help = HELP_ROLLBLOCK_ASYNC_RELAXED_SYNC_EVERY
    )]
    pub async_relaxed_sync_every: Option<usize>,

    #[arg(
        long = "rollblock_synchronous_relaxed_sync_every",
        alias = "rollblock-synchronous-relaxed-sync-every",
        env = "ROLLBLOCK_SYNCHRONOUS_RELAXED_SYNC_EVERY",
        value_name = "BLOCKS",
        help = HELP_ROLLBLOCK_SYNCHRONOUS_RELAXED_SYNC_EVERY
    )]
    pub synchronous_relaxed_sync_every: Option<usize>,
}
