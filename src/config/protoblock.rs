use anyhow::{Context, Result};
use protoblock::runtime::config::FetcherConfig;

use crate::cli::{
    DEFAULT_PROTOBLOCK_MAX_BATCH_SIZE_MB, DEFAULT_PROTOBLOCK_REORG_WINDOW_SIZE,
    DEFAULT_PROTOBLOCK_RPC_PASSWORD, DEFAULT_PROTOBLOCK_RPC_URL, DEFAULT_PROTOBLOCK_RPC_USER,
    DEFAULT_PROTOBLOCK_THREAD_COUNT, ProtoblockOptions,
};

use super::overlay::Overlay;

#[derive(Debug, Clone)]
pub struct ProtoblockSettings {
    fetcher: FetcherConfig,
}

impl ProtoblockSettings {
    pub fn fetcher_config(&self) -> &FetcherConfig {
        &self.fetcher
    }
}

impl ProtoblockOptions {
    pub fn build(self, start_height: u64) -> Result<ProtoblockSettings> {
        let mut builder = FetcherConfig::builder();

        let rpc_url = self
            .rpc_url
            .unwrap_or_else(|| DEFAULT_PROTOBLOCK_RPC_URL.to_string());
        let rpc_user = self
            .rpc_user
            .unwrap_or_else(|| DEFAULT_PROTOBLOCK_RPC_USER.to_string());
        let rpc_password = self
            .rpc_password
            .unwrap_or_else(|| DEFAULT_PROTOBLOCK_RPC_PASSWORD.to_string());
        let reorg_window_size = self
            .reorg_window_size
            .unwrap_or(DEFAULT_PROTOBLOCK_REORG_WINDOW_SIZE);
        let thread_count = self.thread_count.unwrap_or(DEFAULT_PROTOBLOCK_THREAD_COUNT);
        let max_batch_size_mb = self
            .max_batch_size_mb
            .unwrap_or(DEFAULT_PROTOBLOCK_MAX_BATCH_SIZE_MB);

        builder = builder
            .rpc_url(rpc_url)
            .rpc_user(rpc_user)
            .rpc_password(rpc_password)
            .thread_count(thread_count)
            .max_batch_size_mb(max_batch_size_mb)
            .reorg_window_size(reorg_window_size)
            .start_height(start_height);

        if let Some(timeout) = self.rpc_timeout {
            builder = builder.rpc_timeout(timeout);
        }
        if let Some(interval) = self.metrics_interval {
            builder = builder.metrics_interval(interval);
        }
        if let Some(queue) = self.queue_max_size_mb {
            builder = builder.queue_max_size_mb(queue);
        }
        if let Some(backoff) = self.tip_idle_backoff {
            builder = builder.tip_idle_backoff(backoff);
        }
        if let Some(refresh) = self.tip_refresh_interval {
            builder = builder.tip_refresh_interval(refresh);
        }
        if let Some(bytes) = self.rpc_max_request_body_bytes {
            builder = builder.rpc_max_request_body_bytes(bytes);
        }
        if let Some(bytes) = self.rpc_max_response_body_bytes {
            builder = builder.rpc_max_response_body_bytes(bytes);
        }

        let fetcher = builder
            .build()
            .context("failed to build Protoblock FetcherConfig")?;

        Ok(ProtoblockSettings { fetcher })
    }
}

impl Overlay for ProtoblockOptions {
    fn overlay(self, overrides: Self) -> Self {
        Self {
            rpc_url: overrides.rpc_url.or(self.rpc_url),
            rpc_user: overrides.rpc_user.or(self.rpc_user),
            rpc_password: overrides.rpc_password.or(self.rpc_password),
            thread_count: overrides.thread_count.or(self.thread_count),
            max_batch_size_mb: overrides.max_batch_size_mb.or(self.max_batch_size_mb),
            reorg_window_size: overrides.reorg_window_size.or(self.reorg_window_size),
            rpc_timeout: overrides.rpc_timeout.or(self.rpc_timeout),
            metrics_interval: overrides.metrics_interval.or(self.metrics_interval),
            queue_max_size_mb: overrides.queue_max_size_mb.or(self.queue_max_size_mb),
            tip_idle_backoff: overrides.tip_idle_backoff.or(self.tip_idle_backoff),
            tip_refresh_interval: overrides.tip_refresh_interval.or(self.tip_refresh_interval),
            rpc_max_request_body_bytes: overrides
                .rpc_max_request_body_bytes
                .or(self.rpc_max_request_body_bytes),
            rpc_max_response_body_bytes: overrides
                .rpc_max_response_body_bytes
                .or(self.rpc_max_response_body_bytes),
        }
    }
}
