use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use directories::ProjectDirs;
use serde::Deserialize;

use crate::cli::{Cli, MhinNetworkArg, ProtoblockOptions, RollblockOptions};

use super::overlay::Overlay;
use super::protoblock::ProtoblockSettings;
use super::rollblock::RollblockSettings;

const CONFIG_FILE_NAME: &str = "mhinserver.toml";
const LEGACY_CONFIG_FILES: [&str; 2] = ["mhinserver.toml", "config/mhinserver.toml"];
const PROJECT_QUALIFIER: &str = "io";
const PROJECT_ORGANIZATION: &str = "myhashisnice";
const PROJECT_APPLICATION: &str = "mhinserver";

/// Fully materialized configuration for the application.
#[derive(Debug)]
pub struct AppConfig {
    pub config_file: Option<PathBuf>,
    pub data_dir: PathBuf,
    pub network: mhinprotocol::MhinNetwork,
    pub protoblock: ProtoblockSettings,
    pub rollblock: RollblockSettings,
}

impl AppConfig {
    pub fn load(cli: Cli) -> Result<Self> {
        let Cli {
            config,
            network: cli_network,
            protoblock: cli_protoblock,
            data_dir: cli_data_dir,
            rollblock: cli_rollblock,
        } = cli;

        let (file_config, config_path) = load_file_config(config.as_ref())?;
        let FileConfig {
            data_dir: file_data_dir,
            network: file_network,
            protoblock: file_protoblock,
            rollblock: file_rollblock,
        } = file_config;

        let data_dir = cli_data_dir
            .or(file_data_dir)
            .or_else(default_data_dir_path)
            .ok_or_else(|| {
                anyhow!(
                    "data_dir is required (set --data-dir, MHINSERVER_DATA_DIR, or ensure the OS user data directory is available)"
                )
            })?;

        let rollblock_settings = file_rollblock
            .unwrap_or_default()
            .overlay(cli_rollblock)
            .build(&data_dir)?;

        let protoblock_start_height = rollblock_settings.determine_start_height()?;

        let protoblock_settings = file_protoblock
            .unwrap_or_default()
            .overlay(cli_protoblock)
            .build(protoblock_start_height)?;

        let network = cli_network
            .or(file_network)
            .map(Into::into)
            .unwrap_or(mhinprotocol::MhinNetwork::Mainnet);

        Ok(Self {
            config_file: config_path,
            data_dir,
            network,
            protoblock: protoblock_settings,
            rollblock: rollblock_settings,
        })
    }
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    #[serde(default)]
    data_dir: Option<PathBuf>,
    #[serde(default)]
    network: Option<MhinNetworkArg>,
    #[serde(default)]
    protoblock: Option<ProtoblockOptions>,
    #[serde(default)]
    rollblock: Option<RollblockOptions>,
}

fn load_file_config(path: Option<&PathBuf>) -> Result<(FileConfig, Option<PathBuf>)> {
    if let Some(provided) = path {
        let config = read_toml(provided)?;
        return Ok((config, Some(provided.clone())));
    }

    if let Some(default_path) = default_config_file_path() {
        if default_path.exists() {
            let config = read_toml(&default_path)?;
            return Ok((config, Some(default_path)));
        }
    }

    for candidate in LEGACY_CONFIG_FILES {
        let candidate_path = Path::new(candidate);
        if candidate_path.exists() {
            let config = read_toml(candidate_path)?;
            return Ok((config, Some(candidate_path.to_path_buf())));
        }
    }

    Ok((FileConfig::default(), None))
}

fn read_toml(path: &Path) -> Result<FileConfig> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read config file {}", path.display()))?;
    toml::from_str(&contents)
        .with_context(|| format!("failed to parse config file {}", path.display()))
}

impl From<MhinNetworkArg> for mhinprotocol::MhinNetwork {
    fn from(value: MhinNetworkArg) -> Self {
        match value {
            MhinNetworkArg::Mainnet => mhinprotocol::MhinNetwork::Mainnet,
            MhinNetworkArg::Testnet4 => mhinprotocol::MhinNetwork::Testnet4,
            MhinNetworkArg::Signet => mhinprotocol::MhinNetwork::Signet,
            MhinNetworkArg::Regtest => mhinprotocol::MhinNetwork::Regtest,
        }
    }
}

fn default_project_dirs() -> Option<ProjectDirs> {
    ProjectDirs::from(PROJECT_QUALIFIER, PROJECT_ORGANIZATION, PROJECT_APPLICATION)
}

fn default_config_file_path() -> Option<PathBuf> {
    default_project_dirs().map(|dirs| dirs.config_dir().join(CONFIG_FILE_NAME))
}

fn default_data_dir_path() -> Option<PathBuf> {
    default_project_dirs().map(|dirs| dirs.data_dir().to_path_buf())
}
