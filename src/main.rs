mod cli;
mod config;

use anyhow::Result;
use clap::Parser;

use crate::cli::Cli;
use crate::config::AppConfig;

fn main() -> Result<()> {
    let cli = Cli::parse();
    let app_config = AppConfig::load(cli)?;

    if let Some(path) = &app_config.config_file {
        println!("Loaded configuration from {}", path.display());
    } else {
        println!("Loaded configuration from CLI/environment only");
    }
    println!("Data directory → {}", app_config.data_dir.display());

    let fetcher = app_config.protoblock.fetcher_config();
    println!(
        "Protoblock → rpc_url={}, threads={}",
        fetcher.rpc_url(),
        fetcher.thread_count()
    );

    println!(
        "Rollblock → data_dir={}, mode={:?}, server_enabled={}, remote_user={}, remote_password={}",
        app_config.rollblock.store_config.data_dir.display(),
        app_config.rollblock.mode,
        app_config.rollblock.store_config.enable_server,
        app_config.rollblock.remote_username,
        app_config.rollblock.remote_password
    );

    let network = app_config.network;
    let mhin_config = mhinprotocol::MhinConfig::for_network(network);
    println!("Network → {:?}", network);
    println!(
        "mhinprotocol → min_zero_count={}, base_reward={}, prefix={}",
        mhin_config.min_zero_count,
        mhin_config.base_reward,
        String::from_utf8_lossy(&mhin_config.mhin_prefix)
    );

    Ok(())
}
