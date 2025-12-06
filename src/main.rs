mod cli;
mod config;
mod parser;
mod progress;
mod stores;

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use protoblock::Runner;
use protoblock::runtime::config::FetcherConfig;
use std::env;
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::str::FromStr;
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::Directive;

use crate::cli::{Cli, Command as LifecycleCommand};
use crate::config::{AppConfig, RuntimePaths, load_runtime_paths};
use crate::parser::MhinParser;
use crate::progress::ProgressReporter;
use crate::stores::sqlite::DB_FILE_NAME;

static LOG_GUARD: OnceLock<WorkerGuard> = OnceLock::new();

const STOP_SIGINT_TIMEOUT: Duration = Duration::from_secs(15);
const STOP_SIGTERM_TIMEOUT: Duration = Duration::from_secs(5);
const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(250);

fn main() -> Result<()> {
    let cli = Cli::parse();
    let lifecycle = cli.command.unwrap_or(LifecycleCommand::Run);

    match lifecycle {
        LifecycleCommand::Run => handle_run(cli),
        LifecycleCommand::Stop => handle_stop(cli),
    }
}

fn handle_run(cli: Cli) -> Result<()> {
    let launch = determine_launch(&cli);
    let app_config = AppConfig::load(cli)?;

    match launch {
        LaunchMode::DaemonParent => spawn_daemon(&app_config),
        LaunchMode::DaemonChild => {
            let options = RunOptions::daemon_child(&app_config.runtime);
            run_with_config(app_config, options)
        }
        LaunchMode::Foreground => run_with_config(app_config, RunOptions::foreground()),
    }
}

fn handle_stop(cli: Cli) -> Result<()> {
    let runtime_paths = load_runtime_paths(cli)?;
    stop_daemon(runtime_paths)
}

fn run_with_config(app_config: AppConfig, options: RunOptions) -> Result<()> {
    let RunOptions {
        interactive,
        log_path,
        pid_file,
    } = options;

    init_tracing(log_path.as_deref())?;
    announce_configuration(&app_config, interactive);

    let _pid_guard = pid_file.map(PidFileGuard::new);
    start_runtime(app_config, interactive)
}

fn start_runtime(app_config: AppConfig, interactive: bool) -> Result<()> {
    let fetcher_config = app_config.protoblock.fetcher_config().clone();
    let parser = MhinParser::new(app_config)?;
    let runtime = Builder::new_multi_thread().enable_all().build()?;

    runtime.block_on(async move { run_parser(fetcher_config, parser, interactive).await })?;
    Ok(())
}

async fn run_parser(
    fetcher_config: FetcherConfig,
    mut parser: MhinParser,
    interactive: bool,
) -> Result<()> {
    let mut progress = None;

    if interactive {
        let (reporter, handle) = ProgressReporter::start(fetcher_config.clone()).await?;
        parser.attach_progress(handle);
        progress = Some(reporter);
    }

    let mut runner = Runner::new(fetcher_config, parser);
    let run_result = runner.run_until_ctrl_c().await;

    if let Some(reporter) = progress {
        reporter.stop().await;
    }

    run_result
}

fn announce_configuration(app_config: &AppConfig, interactive: bool) {
    let emit = |line: String| {
        if interactive {
            println!("{line}");
        } else {
            tracing::info!("{line}");
        }
    };

    emit("Starting My Hash Is Nice parser.".to_string());
    if let Some(config_file) = &app_config.config_file {
        emit(format!("Config file: {}", config_file.display()));
    } else {
        emit("Config file: none (using defaults)".to_string());
    }
    emit(format!("UTXO db: {}", utxo_endpoint(app_config)));

    let stats_db = app_config.data_dir.join(DB_FILE_NAME);
    emit(format!("Stats db: {}", stats_db.display()));
    emit(String::new());
}

fn utxo_endpoint(app_config: &AppConfig) -> String {
    let store_config = &app_config.rollblock.store_config;
    if !store_config.enable_server {
        return format!(
            "embedded rollblock server disabled (data dir: {})",
            store_config.data_dir.display()
        );
    }

    match &store_config.remote_server {
        Some(settings) => {
            let scheme = if settings.tls.is_some() { "https" } else { "http" };
            format!(
                "{scheme}://{}:{}@{}",
                settings.auth.username, settings.auth.password, settings.bind_address
            )
        }
        None => "embedded rollblock server settings unavailable".to_string(),
    }
}

fn determine_launch(cli: &Cli) -> LaunchMode {
    if cli.daemon_child {
        LaunchMode::DaemonChild
    } else if cli.daemon {
        LaunchMode::DaemonParent
    } else {
        LaunchMode::Foreground
    }
}

fn init_tracing(log_path: Option<&Path>) -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let filter = filter
        .add_directive(Directive::from_str("protoblock=warn").expect("valid protoblock directive"))
        .add_directive(Directive::from_str("rollblock=warn").expect("valid rollblock directive"));

    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true);

    let init_result = if let Some(path) = log_path {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("failed to open log file {}", path.display()))?;
        let (writer, guard) = tracing_appender::non_blocking(file);
        let _ = LOG_GUARD.set(guard);
        builder.with_writer(writer).try_init()
    } else {
        builder.try_init()
    };

    if init_result.is_err() {
        // The global subscriber was already installed elsewhere (tests, etc.); ignore.
    }

    Ok(())
}

#[derive(Clone, Copy)]
enum LaunchMode {
    Foreground,
    DaemonParent,
    DaemonChild,
}

struct RunOptions {
    interactive: bool,
    log_path: Option<PathBuf>,
    pid_file: Option<PathBuf>,
}

impl RunOptions {
    fn foreground() -> Self {
        Self {
            interactive: true,
            log_path: None,
            pid_file: None,
        }
    }

    fn daemon_child(paths: &RuntimePaths) -> Self {
        Self {
            interactive: false,
            log_path: Some(paths.log_file().to_path_buf()),
            pid_file: Some(paths.pid_file().to_path_buf()),
        }
    }
}

struct PidFileGuard {
    path: PathBuf,
}

impl PidFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for PidFileGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[cfg(unix)]
fn spawn_daemon(app_config: &AppConfig) -> Result<()> {
    use libc::pid_t;

    let pid_path = app_config.runtime.pid_file();
    ensure_pid_slot(pid_path)?;

    let exec_path = env::current_exe().context("failed to resolve current executable path")?;
    let daemon_child_flag = OsString::from("--daemon-child");
    let mut child_args: Vec<OsString> = env::args_os()
        .skip(1)
        .filter(|arg| arg != &daemon_child_flag)
        .collect();
    child_args.push(daemon_child_flag);

    let mut command = ProcessCommand::new(exec_path);
    command.args(child_args);
    command.stdin(Stdio::null());
    command.stdout(Stdio::null());
    command.stderr(Stdio::null());

    let child = command.spawn().context("failed to spawn daemon child")?;
    write_pid_file(pid_path, child.id() as pid_t)?;

    println!("Starting mhinparser in daemon mode (pid {}).", child.id());
    println!("Logs → {}", app_config.runtime.log_file().display());
    println!("PID file → {}", pid_path.display());

    Ok(())
}

#[cfg(not(unix))]
fn spawn_daemon(_app_config: &AppConfig) -> Result<()> {
    bail!("Daemon mode is only supported on Unix-like systems");
}

#[cfg(unix)]
fn stop_daemon(runtime_paths: RuntimePaths) -> Result<()> {
    use libc::{SIGINT, SIGTERM};

    let pid_path = runtime_paths.pid_file();
    let pid = read_pid_file(pid_path)?.ok_or_else(|| {
        anyhow!(
            "No running daemon found (missing PID file at {})",
            pid_path.display()
        )
    })?;

    if !process_alive(pid) {
        cleanup_pid_file(pid_path);
        bail!("Found stale PID file referencing pid {pid}");
    }

    send_signal(pid, SIGINT).context("failed to send SIGINT to daemon")?;
    if wait_for_shutdown(pid, pid_path, STOP_SIGINT_TIMEOUT) {
        cleanup_pid_file(pid_path);
        println!("Sent SIGINT to daemon (pid {}).", pid);
        return Ok(());
    }

    println!("Daemon did not exit after SIGINT; sending SIGTERM.");
    send_signal(pid, SIGTERM).context("failed to send SIGTERM to daemon")?;
    if wait_for_shutdown(pid, pid_path, STOP_SIGTERM_TIMEOUT) {
        cleanup_pid_file(pid_path);
        println!("Daemon stopped after SIGTERM.");
        return Ok(());
    }

    bail!("Daemon did not exit after SIGINT/SIGTERM");
}

#[cfg(not(unix))]
fn stop_daemon(_runtime_paths: RuntimePaths) -> Result<()> {
    bail!("Stopping the daemon is only supported on Unix-like systems");
}

#[cfg(unix)]
fn ensure_pid_slot(pid_path: &Path) -> Result<()> {
    if let Some(pid) = read_pid_file(pid_path)? {
        if process_alive(pid) {
            bail!("mhinparser already running (pid {pid})");
        }
        cleanup_pid_file(pid_path);
    }
    Ok(())
}

#[cfg(unix)]
fn write_pid_file(pid_path: &Path, pid: libc::pid_t) -> Result<()> {
    if let Some(parent) = pid_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create pid directory {}", parent.display()))?;
    }
    fs::write(pid_path, pid.to_string()).with_context(|| {
        format!(
            "failed to write pid file {} for pid {pid}",
            pid_path.display()
        )
    })?;
    Ok(())
}

#[cfg(unix)]
fn read_pid_file(pid_path: &Path) -> Result<Option<libc::pid_t>> {
    if !pid_path.exists() {
        return Ok(None);
    }

    let raw = fs::read_to_string(pid_path)
        .with_context(|| format!("failed to read pid file {}", pid_path.display()))?;
    let pid: libc::pid_t = raw
        .trim()
        .parse::<i64>()
        .with_context(|| format!("invalid pid in {}", pid_path.display()))?
        .try_into()
        .map_err(|_| anyhow!("pid value does not fit pid_t"))?;
    Ok(Some(pid))
}

#[cfg(unix)]
fn send_signal(pid: libc::pid_t, signal: libc::c_int) -> Result<()> {
    unsafe {
        if libc::kill(pid, signal) != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("failed to send signal {signal} to pid {pid}"));
        }
    }
    Ok(())
}

#[cfg(unix)]
fn process_alive(pid: libc::pid_t) -> bool {
    unsafe {
        match libc::kill(pid, 0) {
            0 => true,
            _ => {
                let err = std::io::Error::last_os_error();
                err.raw_os_error().is_some_and(|code| code != libc::ESRCH)
            }
        }
    }
}

#[cfg(unix)]
fn wait_for_shutdown(pid: libc::pid_t, pid_path: &Path, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !process_alive(pid) || !pid_path.exists() {
            return true;
        }
        thread::sleep(WAIT_POLL_INTERVAL);
    }
    false
}

#[cfg(unix)]
fn cleanup_pid_file(pid_path: &Path) {
    if pid_path.exists() {
        let _ = fs::remove_file(pid_path);
    }
}
