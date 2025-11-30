use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use serde::Deserialize;

use super::{ProtoblockOptions, RollblockOptions};

/// Command-line interface definition.
#[derive(Parser, Debug)]
#[command(
    name = "mhinserver",
    author,
    version,
    about = "MyHashIsNice parser and API server"
)]
pub struct Cli {
    /// Path to the optional TOML configuration file.
    #[arg(
        long = "config",
        alias = "config-file",
        env = "MHINSERVER_CONFIG",
        value_name = "FILE",
        help = "Optional. Path to the TOML configuration file; defaults to the platform-specific user config directory (ProjectDirs) when omitted."
    )]
    pub config: Option<PathBuf>,

    /// Select the target MHIN network for the entire application.
    #[arg(
        long = "network",
        alias = "mhinprotocol-network",
        env = "MHINSERVER_NETWORK",
        value_enum,
        value_name = "NETWORK",
        help = "Optional. Select the target MHIN network for the application. [default: mainnet]"
    )]
    pub network: Option<MhinNetworkArg>,

    /// Root directory for all application data.
    #[arg(
        long = "data_dir",
        alias = "data-dir",
        env = "MHINSERVER_DATA_DIR",
        value_name = "PATH",
        help = "Optional. Base directory for MHIN server data (Rollblock stores data under <data_dir>/utxodb). Defaults to the platform-specific user data directory (ProjectDirs)."
    )]
    pub data_dir: Option<PathBuf>,

    #[command(flatten)]
    pub protoblock: ProtoblockOptions,

    #[command(flatten)]
    pub rollblock: RollblockOptions,
}

#[derive(ValueEnum, Clone, Debug, Deserialize)]
pub enum MhinNetworkArg {
    Mainnet,
    Testnet4,
    Signet,
    Regtest,
}
