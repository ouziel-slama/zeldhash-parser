use std::time::Duration;

use clap::Args;
use serde::Deserialize;

use super::parse_duration;

macro_rules! define_str_default_with_help {
    ($value_ident:ident, $help_ident:ident, $value:literal, $help_prefix:literal) => {
        pub const $value_ident: &str = $value;
        pub const $help_ident: &str = concat!($help_prefix, $value, "]");
    };
}

macro_rules! define_usize_default_with_help {
    ($value_ident:ident, $help_ident:ident, $value:literal, $help_prefix:literal) => {
        pub const $value_ident: usize = $value;
        pub const $help_ident: &str = concat!($help_prefix, stringify!($value), "]");
    };
}

define_str_default_with_help!(
    DEFAULT_PROTOBLOCK_RPC_URL,
    HELP_PROTOBLOCK_RPC_URL,
    "http://localhost:8332",
    "Optional. Full Bitcoin Core RPC endpoint (http/https). [default: "
);
define_str_default_with_help!(
    DEFAULT_PROTOBLOCK_RPC_USER,
    HELP_PROTOBLOCK_RPC_USER,
    "rpc",
    "Optional. RPC username used when authenticating to Bitcoin Core. [default: "
);
define_str_default_with_help!(
    DEFAULT_PROTOBLOCK_RPC_PASSWORD,
    HELP_PROTOBLOCK_RPC_PASSWORD,
    "rpc",
    "Optional. RPC password paired with the RPC username. [default: "
);
define_usize_default_with_help!(
    DEFAULT_PROTOBLOCK_THREAD_COUNT,
    HELP_PROTOBLOCK_THREAD_COUNT,
    4,
    "Optional. Number of concurrent block fetcher workers. [default: "
);
define_usize_default_with_help!(
    DEFAULT_PROTOBLOCK_MAX_BATCH_SIZE_MB,
    HELP_PROTOBLOCK_MAX_BATCH_SIZE_MB,
    10,
    "Optional. Target megabytes per JSON-RPC batch. [default: "
);
define_usize_default_with_help!(
    DEFAULT_PROTOBLOCK_REORG_WINDOW_SIZE,
    HELP_PROTOBLOCK_REORG_WINDOW_SIZE,
    100,
    "Optional. Number of historical blocks retained to detect reorgs. [default: "
);

#[derive(Args, Debug, Clone, Default, Deserialize)]
#[command(next_help_heading = "Protoblock parameters")]
#[serde(default)]
pub struct ProtoblockOptions {
    #[arg(
        long = "protoblock_rpc_url",
        alias = "protoblock-rpc-url",
        env = "PROTOBLOCK_RPC_URL",
        value_name = "URL",
        help = HELP_PROTOBLOCK_RPC_URL
    )]
    pub rpc_url: Option<String>,

    #[arg(
        long = "protoblock_rpc_user",
        alias = "protoblock-rpc-user",
        env = "PROTOBLOCK_RPC_USER",
        value_name = "USER",
        help = HELP_PROTOBLOCK_RPC_USER
    )]
    pub rpc_user: Option<String>,

    #[arg(
        long = "protoblock_rpc_password",
        alias = "protoblock-rpc-password",
        env = "PROTOBLOCK_RPC_PASSWORD",
        value_name = "PASSWORD",
        help = HELP_PROTOBLOCK_RPC_PASSWORD
    )]
    pub rpc_password: Option<String>,

    #[arg(
        long = "protoblock_thread_count",
        alias = "protoblock-thread-count",
        env = "PROTOBLOCK_THREAD_COUNT",
        id = "protoblock_thread_count",
        value_name = "COUNT",
        help = HELP_PROTOBLOCK_THREAD_COUNT
    )]
    pub thread_count: Option<usize>,

    #[arg(
        long = "protoblock_max_batch_size_mb",
        alias = "protoblock-max-batch-size-mb",
        env = "PROTOBLOCK_MAX_BATCH_MB",
        value_name = "MEGABYTES",
        help = HELP_PROTOBLOCK_MAX_BATCH_SIZE_MB
    )]
    pub max_batch_size_mb: Option<usize>,

    #[arg(
        long = "protoblock_reorg_window_size",
        alias = "protoblock-reorg-window-size",
        env = "PROTOBLOCK_REORG_WINDOW",
        value_name = "BLOCKS",
        help = HELP_PROTOBLOCK_REORG_WINDOW_SIZE
    )]
    pub reorg_window_size: Option<usize>,

    #[arg(
        long = "protoblock_rpc_timeout",
        alias = "protoblock-rpc-timeout",
        env = "PROTOBLOCK_RPC_TIMEOUT",
        value_name = "DURATION",
        value_parser = parse_duration,
        help = "Optional. Per RPC timeout; accepts human-friendly durations (e.g. 5s, 2m). [default: 10s]"
    )]
    #[serde(default, with = "humantime_serde::option")]
    pub rpc_timeout: Option<Duration>,

    #[arg(
        long = "protoblock_metrics_interval",
        alias = "protoblock-metrics-interval",
        env = "PROTOBLOCK_METRICS_INTERVAL",
        value_name = "DURATION",
        value_parser = parse_duration,
        help = "Optional. Interval between telemetry snapshots. [default: 5s]"
    )]
    #[serde(default, with = "humantime_serde::option")]
    pub metrics_interval: Option<Duration>,

    #[arg(
        long = "protoblock_queue_max_size_mb",
        alias = "protoblock-queue-max-size-mb",
        env = "PROTOBLOCK_QUEUE_MB",
        value_name = "MEGABYTES",
        help = "Optional. Maximum megabytes buffered in the ordered queue. [default: 200 MB]"
    )]
    pub queue_max_size_mb: Option<usize>,

    #[arg(
        long = "protoblock_tip_idle_backoff",
        alias = "protoblock-tip-idle-backoff",
        env = "PROTOBLOCK_TIP_IDLE_BACKOFF",
        value_name = "DURATION",
        value_parser = parse_duration,
        help = "Optional. Backoff between polls once near the blockchain tip. [default: 10s]"
    )]
    #[serde(default, with = "humantime_serde::option")]
    pub tip_idle_backoff: Option<Duration>,

    #[arg(
        long = "protoblock_tip_refresh_interval",
        alias = "protoblock-tip-refresh-interval",
        env = "PROTOBLOCK_TIP_REFRESH_INTERVAL",
        value_name = "DURATION",
        value_parser = parse_duration,
        help = "Optional. Cadence for refreshing the remote tip height. [default: 10s]"
    )]
    #[serde(default, with = "humantime_serde::option")]
    pub tip_refresh_interval: Option<Duration>,

    #[arg(
        long = "protoblock_rpc_max_request_body_bytes",
        alias = "protoblock-rpc-max-request-body-bytes",
        env = "PROTOBLOCK_RPC_MAX_REQUEST_BODY_BYTES",
        value_name = "BYTES",
        help = "Optional. Maximum allowed RPC request body size. [default: 10_485_760 bytes]"
    )]
    pub rpc_max_request_body_bytes: Option<usize>,

    #[arg(
        long = "protoblock_rpc_max_response_body_bytes",
        alias = "protoblock-rpc-max-response-body-bytes",
        env = "PROTOBLOCK_RPC_MAX_RESPONSE_BODY_BYTES",
        value_name = "BYTES",
        help = "Optional. Maximum allowed RPC response body size. [default: 10_485_760 bytes]"
    )]
    pub rpc_max_response_body_bytes: Option<usize>,
}
