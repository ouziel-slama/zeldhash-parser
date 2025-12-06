use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use serde::Deserialize;

use super::{ProtoblockOptions, RollblockOptions};

/// Command-line interface definition.
#[derive(Parser, Debug)]
#[command(
    name = "mhinparser",
    author,
    version,
    about = "MyHashIsNice parser and daemon"
)]
pub struct Cli {
    /// Path to the optional TOML configuration file.
    #[arg(
        long = "config",
        alias = "config-file",
        env = "MHINPARSER_CONFIG",
        global = true,
        value_name = "FILE",
        help = "Optional. Path to the TOML configuration file; defaults to the platform-specific user config directory (ProjectDirs) when omitted."
    )]
    pub config: Option<PathBuf>,

    /// Select the target MHIN network for the entire application.
    #[arg(
        long = "network",
        alias = "mhinprotocol-network",
        env = "MHINPARSER_NETWORK",
        global = true,
        value_enum,
        value_name = "NETWORK",
        help = "Optional. Select the target MHIN network for the application. [default: mainnet]"
    )]
    pub network: Option<MhinNetworkArg>,

    /// Root directory for all application data.
    #[arg(
        long = "data_dir",
        alias = "data-dir",
        env = "MHINPARSER_DATA_DIR",
        global = true,
        value_name = "PATH",
        help = "Optional. Base directory for MHIN server data (Rollblock stores data under <data_dir>/utxodb). Defaults to the platform-specific user data directory (ProjectDirs)."
    )]
    pub data_dir: Option<PathBuf>,

    #[command(flatten)]
    pub protoblock: ProtoblockOptions,

    #[command(flatten)]
    pub rollblock: RollblockOptions,

    /// Run the server in the background without the progress UI.
    #[arg(
        long = "daemon",
        global = true,
        help = "Run the parser as a daemon. Combine with `mhinparser stop` to stop it later."
    )]
    pub daemon: bool,

    /// Internal flag used to mark the detached daemon child.
    #[arg(long = "daemon-child", hide = true, global = true)]
    pub daemon_child: bool,

    /// Optional lifecycle subcommand (e.g. `stop`).
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(ValueEnum, Clone, Debug, Deserialize)]
pub enum MhinNetworkArg {
    Mainnet,
    Testnet4,
    Signet,
    Regtest,
}

/// High-level commands supported by the CLI.
#[derive(Subcommand, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    /// Start the parser (default when no subcommand is provided).
    #[command(alias = "start")]
    Run,
    /// Stop the background daemon by reading the PID file and sending a signal.
    Stop,
}
