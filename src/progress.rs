use anyhow::{Context, Result};
use protoblock::rpc::AsyncRpcClient;
use protoblock::runtime::config::FetcherConfig;
use std::cmp::Ordering as CmpOrdering;
use std::io::{self, Write};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval};

const BAR_WIDTH: usize = 40;
const DISPLAY_WIDTH: usize = 100;
const RENDER_INTERVAL: Duration = Duration::from_millis(750);

/// Spawns background tasks that keep track of parsing progress and render it to stdout.
pub struct ProgressReporter {
    shutdown: Arc<Notify>,
    tip_handle: JoinHandle<()>,
    render_handle: JoinHandle<()>,
}

/// Handle owned by the parser so it can report new heights and rollbacks.
#[derive(Clone)]
pub struct ProgressHandle {
    state: Arc<ProgressState>,
}

impl ProgressReporter {
    /// Starts background tasks and returns both the reporter and a handle the parser can update.
    pub async fn start(fetcher_config: FetcherConfig) -> Result<(Self, ProgressHandle)> {
        let state = Arc::new(ProgressState::new(fetcher_config.start_height()));
        let shutdown = Arc::new(Notify::new());
        let rpc_client = Arc::new(
            AsyncRpcClient::from_config(&fetcher_config)
                .context("failed to build RPC client for progress reporter")?,
        );

        match rpc_client.get_blockchain_tip().await {
            Ok(tip) => state.update_tip(tip),
            Err(err) => tracing::warn!(
                error = %err,
                "failed to fetch initial blockchain tip for progress reporter"
            ),
        }

        let tip_handle = spawn_tip_refresh_loop(
            Arc::clone(&rpc_client),
            Arc::clone(&state),
            fetcher_config.tip_refresh_interval(),
            Arc::clone(&shutdown),
        );
        let render_handle = spawn_render_loop(Arc::clone(&state), Arc::clone(&shutdown));

        let reporter = Self {
            shutdown,
            tip_handle,
            render_handle,
        };
        let handle = ProgressHandle { state };

        Ok((reporter, handle))
    }

    /// Signals both background tasks to stop and waits for them to finish.
    pub async fn stop(self) {
        self.shutdown.notify_waiters();
        let _ = self.tip_handle.await;
        let _ = self.render_handle.await;
        println!();
    }
}

impl ProgressHandle {
    /// Records the latest processed block height.
    pub fn mark_processed(&self, height: u64) {
        let mut current = self.state.last_processed.load(Ordering::SeqCst);
        while matches!(height.cmp(&current), CmpOrdering::Greater) {
            match self.state.last_processed.compare_exchange(
                current,
                height,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Updates the tracker after a rollback.
    pub fn rollback_to(&self, height: u64) {
        self.state.last_processed.store(height, Ordering::SeqCst);
    }
}

fn spawn_tip_refresh_loop(
    rpc_client: Arc<AsyncRpcClient>,
    state: Arc<ProgressState>,
    refresh_interval: Duration,
    shutdown: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = interval(refresh_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    break;
                }
                _ = ticker.tick() => {
                    match rpc_client.get_blockchain_tip().await {
                        Ok(tip) => state.update_tip(tip),
                        Err(err) => tracing::warn!(error = %err, "failed to refresh blockchain tip for progress reporter"),
                    }
                }
            }
        }
    })
}

fn spawn_render_loop(state: Arc<ProgressState>, shutdown: Arc<Notify>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = interval(RENDER_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    break;
                }
                _ = ticker.tick() => {
                    let snapshot = state.snapshot();
                    let line = format_snapshot(snapshot);
                    render_line(&line);
                }
            }
        }
    })
}

fn render_line(line: &str) {
    print!("\r{line:<width$}", line = line, width = DISPLAY_WIDTH);
    let _ = io::stdout().flush();
}

fn format_snapshot(snapshot: ProgressSnapshot) -> String {
    let elapsed = snapshot.start_instant.elapsed();
    let elapsed_display = format_duration(elapsed);
    let elapsed_str = elapsed_display.as_str();
    let run_completed = blocks_completed(snapshot.start_height, snapshot.last_processed);
    let speed = compute_speed(run_completed, elapsed);
    match snapshot.tip_height {
        None => format!(
            "Progress → processed height {} (waiting for remote tip, elapsed {}, speed {:.2} blk/s)",
            snapshot.last_processed, elapsed_str, speed
        ),
        Some(tip) => {
            let total = blocks_total(snapshot.start_height, tip);
            if total == 0 {
                return format!(
                    "Progress → waiting for tip ≥ {} (current tip {}, elapsed {}, speed {:.2} blk/s)",
                    snapshot.start_height, tip, elapsed_str, speed
                );
            }

            let completed_within_target = run_completed.min(total);
            let remaining = total.saturating_sub(completed_within_target);
            let displayed_height = snapshot.last_processed.min(tip);
            let percent = percent_of_tip(displayed_height, tip);
            let bar = render_bar(percent);
            let eta = compute_eta(remaining, speed);
            let eta_display = eta
                .map(format_duration)
                .unwrap_or_else(|| "--:--:--".to_string());

            format!(
                "Progress → {} height {}/{} ({percent:.1}% of tip, remaining {remaining}, elapsed {elapsed_display}, speed {speed:.2} blk/s, eta {eta_display})",
                bar, displayed_height, tip
            )
        }
    }
}

fn render_bar(percent: f64) -> String {
    let capped = percent.clamp(0.0, 100.0);
    let filled = ((capped / 100.0) * BAR_WIDTH as f64).round() as usize;
    let filled = filled.min(BAR_WIDTH);
    let remaining = BAR_WIDTH.saturating_sub(filled);

    let mut bar = String::with_capacity(BAR_WIDTH + 2);
    bar.push('[');
    bar.extend(std::iter::repeat('#').take(filled));
    bar.extend(std::iter::repeat('.').take(remaining));
    bar.push(']');
    bar
}

fn percent_of_tip(progress_height: u64, tip: u64) -> f64 {
    if tip == 0 {
        return if progress_height == 0 { 100.0 } else { 0.0 };
    }

    let clamped = progress_height.min(tip);
    (clamped as f64 / tip as f64) * 100.0
}

fn blocks_completed(start: u64, last_processed: u64) -> u64 {
    if last_processed < start {
        0
    } else {
        last_processed - start + 1
    }
}

fn blocks_total(start: u64, tip: u64) -> u64 {
    if tip < start { 0 } else { tip - start + 1 }
}

fn compute_speed(completed: u64, elapsed: Duration) -> f64 {
    let elapsed_secs = elapsed.as_secs_f64();
    if elapsed_secs <= f64::EPSILON {
        0.0
    } else {
        completed as f64 / elapsed_secs
    }
}

fn compute_eta(remaining: u64, speed: f64) -> Option<Duration> {
    if speed <= f64::EPSILON {
        None
    } else {
        Some(Duration::from_secs_f64(remaining as f64 / speed))
    }
}

fn format_duration(duration: Duration) -> String {
    let total_secs = duration.as_secs();
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

struct ProgressState {
    start_height: u64,
    start_instant: Instant,
    last_processed: AtomicU64,
    tip_height: AtomicU64,
    tip_known: AtomicBool,
}

impl ProgressState {
    fn new(start_height: u64) -> Self {
        Self {
            start_height,
            start_instant: Instant::now(),
            last_processed: AtomicU64::new(start_height.saturating_sub(1)),
            tip_height: AtomicU64::new(0),
            tip_known: AtomicBool::new(false),
        }
    }

    fn update_tip(&self, tip: u64) {
        self.tip_height.store(tip, Ordering::SeqCst);
        self.tip_known.store(true, Ordering::SeqCst);
    }

    fn snapshot(&self) -> ProgressSnapshot {
        let tip_known = self.tip_known.load(Ordering::SeqCst);
        let tip_value = self.tip_height.load(Ordering::SeqCst);
        ProgressSnapshot {
            start_height: self.start_height,
            start_instant: self.start_instant,
            last_processed: self.last_processed.load(Ordering::SeqCst),
            tip_height: tip_known.then_some(tip_value),
        }
    }
}

#[derive(Clone, Copy)]
struct ProgressSnapshot {
    start_height: u64,
    start_instant: Instant,
    last_processed: u64,
    tip_height: Option<u64>,
}
