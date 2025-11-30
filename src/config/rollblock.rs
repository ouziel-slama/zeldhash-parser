use std::path::Path;

use anyhow::{Context, Result, anyhow};
use rand::{Rng, distr::Alphanumeric};
use rollblock::{
    RemoteServerSettings, StoreConfig,
    metadata::{LmdbMetadataStore, MetadataStore},
    orchestrator::DurabilityMode,
};

use crate::cli::{
    DEFAULT_ROLLBLOCK_ASYNC_MAX_PENDING_BLOCKS, DEFAULT_ROLLBLOCK_ASYNC_RELAXED_SYNC_EVERY,
    DEFAULT_ROLLBLOCK_COMPRESS_JOURNAL, DEFAULT_ROLLBLOCK_INITIAL_CAPACITY,
    DEFAULT_ROLLBLOCK_SHARDS_COUNT, DEFAULT_ROLLBLOCK_SYNCHRONOUS_RELAXED_SYNC_EVERY,
    DEFAULT_ROLLBLOCK_THREAD_COUNT, RollblockDurabilityKind, RollblockMode, RollblockOptions,
};

use super::overlay::Overlay;
const EMBEDDED_REMOTE_USERNAME: &str = "proto";
const EMBEDDED_REMOTE_PASSWORD_LEN: usize = 32;

#[derive(Debug, Clone)]
pub struct RollblockSettings {
    pub mode: RollblockMode,
    pub store_config: StoreConfig,
    pub remote_username: String,
    pub remote_password: String,
}

impl RollblockOptions {
    pub fn build(self, data_dir: &Path) -> Result<RollblockSettings> {
        let rollblock_data_dir = data_dir.join("utxodb");
        let mode = if rollblock_data_dir.exists() {
            RollblockMode::Existing
        } else {
            RollblockMode::New
        };

        let mut config = match mode {
            RollblockMode::New => {
                let shards = self.shards_count.unwrap_or(DEFAULT_ROLLBLOCK_SHARDS_COUNT);
                let capacity = self
                    .initial_capacity
                    .unwrap_or(DEFAULT_ROLLBLOCK_INITIAL_CAPACITY);
                let thread_count = self.thread_count.unwrap_or(DEFAULT_ROLLBLOCK_THREAD_COUNT);
                let compress = self
                    .compress_journal
                    .unwrap_or(DEFAULT_ROLLBLOCK_COMPRESS_JOURNAL);
                StoreConfig::new(
                    &rollblock_data_dir,
                    shards,
                    capacity,
                    thread_count,
                    compress,
                )
                .context("failed to create rollblock StoreConfig")?
            }
            RollblockMode::Existing => {
                let mut cfg = StoreConfig::existing(&rollblock_data_dir);
                if let Some(shards) = self.shards_count {
                    let capacity = self.initial_capacity.ok_or_else(|| {
                        anyhow!(
                            "rollblock_initial_capacity is required when overriding shard layout"
                        )
                    })?;
                    cfg = cfg
                        .with_shard_layout(shards, capacity)
                        .context("invalid shard layout override")?;
                }
                if let Some(thread_count) = self.thread_count {
                    cfg.thread_count = thread_count;
                }
                if let Some(compress) = self.compress_journal {
                    cfg = cfg.with_journal_compression(compress);
                }
                cfg
            }
        };

        if let Some(interval) = self.snapshot_interval {
            config = config.with_snapshot_interval(interval);
        }
        if let Some(interval) = self.max_snapshot_interval {
            config = config.with_max_snapshot_interval(interval);
        }
        if let Some(level) = self.journal_compression_level {
            config = config.with_journal_compression_level(level);
        }
        if let Some(bytes) = self.journal_chunk_size_bytes {
            config = config.with_journal_chunk_size(bytes);
        }
        if let Some(size) = self.lmdb_map_size {
            config = config.with_lmdb_map_size(size);
        }
        if let Some(window) = self.min_rollback_window {
            config = config
                .with_min_rollback_window(window)
                .context("invalid rollblock_min_rollback_window")?;
        }
        if let Some(interval) = self.prune_interval {
            config = config
                .with_prune_interval(interval)
                .context("invalid rollblock_prune_interval")?;
        }
        if let Some(profile) = self.bootstrap_block_profile {
            config = config
                .with_bootstrap_block_profile(profile)
                .context("invalid rollblock_bootstrap_block_profile")?;
        }

        let async_pending = self
            .async_max_pending_blocks
            .unwrap_or(DEFAULT_ROLLBLOCK_ASYNC_MAX_PENDING_BLOCKS);
        let async_relaxed_sync_every = self
            .async_relaxed_sync_every
            .unwrap_or(DEFAULT_ROLLBLOCK_ASYNC_RELAXED_SYNC_EVERY);
        let synchronous_relaxed_sync_every = self
            .synchronous_relaxed_sync_every
            .unwrap_or(DEFAULT_ROLLBLOCK_SYNCHRONOUS_RELAXED_SYNC_EVERY);

        let mode_kind = self
            .durability_mode
            .unwrap_or(RollblockDurabilityKind::AsyncRelaxed);

        config = match mode_kind {
            RollblockDurabilityKind::Async => config.with_async_max_pending(async_pending),
            RollblockDurabilityKind::AsyncRelaxed => {
                config.with_async_relaxed(async_pending, async_relaxed_sync_every)
            }
            RollblockDurabilityKind::Synchronous => {
                config.with_durability_mode(DurabilityMode::Synchronous)
            }
            RollblockDurabilityKind::SynchronousRelaxed => {
                config.with_durability_mode(DurabilityMode::SynchronousRelaxed {
                    sync_every_n_blocks: synchronous_relaxed_sync_every.max(1),
                })
            }
        };

        let remote_username = EMBEDDED_REMOTE_USERNAME.to_string();
        let remote_password = generate_remote_password();
        let remote_settings = RemoteServerSettings::default()
            .with_basic_auth(remote_username.clone(), remote_password.clone());
        config = config.with_remote_server(remote_settings);
        config = config
            .enable_remote_server()
            .context("failed to enable rollblock remote server")?;

        Ok(RollblockSettings {
            mode,
            store_config: config,
            remote_username,
            remote_password,
        })
    }
}

impl RollblockSettings {
    pub fn determine_start_height(&self) -> Result<u64> {
        if matches!(self.mode, RollblockMode::New) {
            return Ok(0);
        }

        let metadata_dir = self.store_config.metadata_dir();
        if !metadata_dir.exists() {
            return Ok(0);
        }

        let metadata = LmdbMetadataStore::new_with_map_size(
            &metadata_dir,
            self.store_config.lmdb_map_size.max(1),
        )
        .context("failed to open rollblock metadata store")?;
        let durable_block = metadata
            .current_block()
            .context("failed to read rollblock current block height")?;

        if durable_block == 0 {
            return Ok(0);
        }

        durable_block.checked_add(1).context(
            "rollblock current block reached u64::MAX; cannot derive protoblock start height",
        )
    }
}

impl Overlay for RollblockOptions {
    fn overlay(self, overrides: Self) -> Self {
        Self {
            shards_count: overrides.shards_count.or(self.shards_count),
            initial_capacity: overrides.initial_capacity.or(self.initial_capacity),
            thread_count: overrides.thread_count.or(self.thread_count),
            compress_journal: overrides.compress_journal.or(self.compress_journal),
            snapshot_interval: overrides.snapshot_interval.or(self.snapshot_interval),
            max_snapshot_interval: overrides
                .max_snapshot_interval
                .or(self.max_snapshot_interval),
            journal_compression_level: overrides
                .journal_compression_level
                .or(self.journal_compression_level),
            journal_chunk_size_bytes: overrides
                .journal_chunk_size_bytes
                .or(self.journal_chunk_size_bytes),
            lmdb_map_size: overrides.lmdb_map_size.or(self.lmdb_map_size),
            min_rollback_window: overrides.min_rollback_window.or(self.min_rollback_window),
            prune_interval: overrides.prune_interval.or(self.prune_interval),
            bootstrap_block_profile: overrides
                .bootstrap_block_profile
                .or(self.bootstrap_block_profile),
            durability_mode: overrides.durability_mode.or(self.durability_mode),
            async_max_pending_blocks: overrides
                .async_max_pending_blocks
                .or(self.async_max_pending_blocks),
            async_relaxed_sync_every: overrides
                .async_relaxed_sync_every
                .or(self.async_relaxed_sync_every),
            synchronous_relaxed_sync_every: overrides
                .synchronous_relaxed_sync_every
                .or(self.synchronous_relaxed_sync_every),
        }
    }
}

fn generate_remote_password() -> String {
    let mut rng = rand::rng();
    (0..EMBEDDED_REMOTE_PASSWORD_LEN)
        .map(|_| rng.sample(Alphanumeric))
        .map(char::from)
        .collect()
}
