mod cli;
mod config;
mod parser;
mod progress;
mod stores;

use anyhow::Result;
use clap::Parser;
use protoblock::Runner;
use std::str::FromStr;
use tokio::runtime::Builder;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::Directive;

use crate::cli::Cli;
use crate::config::AppConfig;
use crate::parser::MhinParser;
use crate::progress::ProgressReporter;

fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();
    let app_config = AppConfig::load(cli)?;

    if let Some(path) = &app_config.config_file {
        println!("Loaded configuration from {}", path.display());
    } else {
        println!("Loaded configuration from CLI/environment only");
    }
    println!("Data directory → {}", app_config.data_dir.display());

    let fetcher_config = app_config.protoblock.fetcher_config().clone();
    let fetcher = &fetcher_config;
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

    let parser = MhinParser::new(app_config)?;

    let runtime = Builder::new_multi_thread().enable_all().build()?;
    runtime.block_on(async move {
        let fetcher_for_progress = fetcher_config.clone();
        let (progress_reporter, progress_handle) =
            ProgressReporter::start(fetcher_for_progress).await?;
        let mut parser = parser;
        parser.attach_progress(progress_handle);
        let mut runner = Runner::new(fetcher_config, parser);
        let run_result = runner.run_until_ctrl_c().await;
        progress_reporter.stop().await;
        run_result
    })?;

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let filter = filter
        .add_directive(Directive::from_str("protoblock=warn").expect("valid protoblock directive"))
        .add_directive(Directive::from_str("rollblock=warn").expect("valid rollblock directive"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .try_init();
}
