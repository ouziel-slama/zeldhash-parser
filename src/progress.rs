use crate::stores::sqlite::CumulativeStats;
use anyhow::{Context, Result};
use mhinprotocol::types::Amount;
use protoblock::rpc::AsyncRpcClient;
use protoblock::runtime::config::FetcherConfig;
use ratatui::{
    Frame, Terminal, TerminalOptions, Viewport,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    widgets::{Block, Borders, Cell, Clear, Gauge, Paragraph, Row, Table},
};
use std::cmp::Ordering as CmpOrdering;
use std::io::{self, Stdout};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval};

const RENDER_INTERVAL: Duration = Duration::from_millis(750);
const PANEL_WIDTH: u16 = 100;
const TABLE_SECTION_HEIGHT: u16 = 8;
const PROGRESS_SECTION_HEIGHT: u16 = 6;
const INLINE_VIEWPORT_HEIGHT: u16 = TABLE_SECTION_HEIGHT + PROGRESS_SECTION_HEIGHT + 1;

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
    }
}

impl ProgressHandle {
    /// Records the latest processed block height.
    pub fn mark_processed(&self, height: u64) {
        self.state.bump_last_processed(height);
    }

    /// Updates the tracker after a rollback.
    pub fn rollback_to(&self, height: u64) {
        self.state.last_processed.store(height, Ordering::SeqCst);
        self.state.reset_session_baseline(height);
    }

    /// Publishes the latest cumulative stats so the renderer can display them.
    pub fn update_cumulative(&self, stats: Option<&CumulativeStats>) {
        self.state.set_cumulative(stats.cloned());
    }

    /// Resets the speed baseline so historical blocks do not skew throughput.
    pub fn reset_speed_baseline(&self, height: u64) {
        self.state.reset_session_baseline(height);
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
        if let Err(err) = run_render_loop(state, shutdown).await {
            tracing::warn!(error = %err, "progress reporter render loop failed");
        }
    })
}

async fn run_render_loop(state: Arc<ProgressState>, shutdown: Arc<Notify>) -> Result<()> {
    let mut terminal = setup_terminal().context("failed to start progress terminal")?;
    let render_result = drive_render_loop(&mut terminal, state, shutdown).await;
    let cleanup_result = restore_terminal(terminal);

    render_result?;
    cleanup_result?;
    Ok(())
}

async fn drive_render_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    state: Arc<ProgressState>,
    shutdown: Arc<Notify>,
) -> Result<()> {
    let mut ticker = interval(RENDER_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown.notified() => {
                break;
            }
            _ = ticker.tick() => {
                let snapshot = state.snapshot();
                terminal
                    .draw(|frame| draw_ui(frame, &snapshot))
                    .context("failed to draw progress frame")?;
            }
        }
    }

    Ok(())
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>> {
    let stdout = io::stdout();
    let backend = CrosstermBackend::new(stdout);
    let options = TerminalOptions {
        viewport: Viewport::Inline(INLINE_VIEWPORT_HEIGHT),
    };
    let mut terminal =
        Terminal::with_options(backend, options).context("failed to build ratatui terminal")?;
    terminal
        .clear()
        .context("failed to clear terminal for progress reporter")?;
    Ok(terminal)
}

fn restore_terminal(mut terminal: Terminal<CrosstermBackend<Stdout>>) -> Result<()> {
    terminal
        .show_cursor()
        .context("failed to show cursor after progress reporter")?;
    terminal
        .clear()
        .context("failed to clear terminal after progress reporter")?;
    println!();
    Ok(())
}

fn draw_ui(frame: &mut Frame<'_>, snapshot: &ProgressSnapshot) {
    let show_progress = snapshot
        .tip_height
        .is_some_and(|tip| snapshot.last_processed < tip);
    let panel_area = panel_area(frame.area(), show_progress);
    frame.render_widget(Clear, panel_area);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(section_constraints(panel_area.height, show_progress))
        .split(panel_area);

    let table = build_info_table(snapshot);
    frame.render_widget(table, chunks[0]);

    if show_progress && chunks.len() > 1 {
        render_progress_section(frame, chunks[1], snapshot);
    }
}

fn build_info_table(snapshot: &ProgressSnapshot) -> Table<'static> {
    let stats = snapshot.latest_cumulative.as_ref();
    let tip_display = snapshot
        .tip_height
        .map(format_with_separators)
        .unwrap_or_else(|| "Waiting for remote tip".to_string());
    let last_parsed = format_with_separators(snapshot.last_processed);
    let reward_count = stats
        .map(|s| format_with_separators(s.reward_count()))
        .unwrap_or_else(|| "--".to_string());
    let total_supply = stats
        .map(|s| format_amount(s.total_reward()))
        .unwrap_or_else(|| "--".to_string());
    let max_zero_count = stats
        .map(|s| s.max_zero_count().to_string())
        .unwrap_or_else(|| "--".to_string());
    let nicest_hash = stats
        .and_then(|s| s.nicest_txid().map(str::to_string))
        .unwrap_or_else(|| "-".to_string());
    let unspent = stats
        .map(|s| {
            let spent = s.utxo_spent_count();
            let created = s.new_utxo_count();
            format_with_separators(created.saturating_sub(spent))
        })
        .unwrap_or_else(|| "--".to_string());

    let rows = vec![
        stats_row("Tip", tip_display),
        stats_row("Last parsed block", last_parsed),
        stats_row("Reward count", reward_count),
        stats_row("MHIN supply", total_supply),
        stats_row("Max zero count", max_zero_count),
        stats_row("Last nice hash", nicest_hash),
        stats_row("Unspent UTXO", unspent),
    ];

    Table::new(rows, [Constraint::Length(20), Constraint::Min(10)])
        .column_spacing(2)
        .block(
            Block::default()
                .title("Parsing progress")
                .borders(Borders::ALL),
        )
}

fn stats_row<'a>(label: &'a str, value: String) -> Row<'a> {
    Row::new(vec![
        Cell::from(label).style(Style::default().fg(Color::Gray)),
        Cell::from(value),
    ])
}

fn render_progress_section(frame: &mut Frame<'_>, area: Rect, snapshot: &ProgressSnapshot) {
    if area.height == 0 {
        return;
    }

    let tip = match snapshot.tip_height {
        Some(tip) if snapshot.last_processed < tip => tip,
        _ => return,
    };

    let total = blocks_total(snapshot.start_height, tip);
    if total == 0 {
        return;
    }

    let displayed_height = snapshot.last_processed.min(tip);
    let completed = blocks_completed(snapshot.start_height, displayed_height).min(total);
    let remaining = total.saturating_sub(completed);
    let elapsed = snapshot.start_instant.elapsed();
    let session_completed = snapshot
        .last_processed
        .saturating_sub(snapshot.session_baseline);
    let speed = compute_speed(session_completed, elapsed);
    let eta_display = compute_eta(remaining, speed)
        .map(format_duration)
        .unwrap_or_else(|| "--:--:--".to_string());
    let ratio = (completed as f64 / total as f64).clamp(0.0, 1.0);
    let percent = ratio * 100.0;
    let displayed_height_text = format_with_separators(displayed_height);
    let tip_text = format_with_separators(tip);
    let remaining_text = format_with_separators(remaining);
    let elapsed_display = format_duration(elapsed);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Length(3)])
        .split(area);

    let gauge = Gauge::default()
        .block(
            Block::default()
                .title("Sync progress")
                .borders(Borders::ALL),
        )
        .gauge_style(Style::default().fg(Color::Green))
        .ratio(ratio)
        .label(format!(
            "{percent:.1}% (height {displayed_height_text}/{tip_text})"
        ));
    frame.render_widget(gauge, chunks[0]);

    let info_line = format!(
        "Remaining: {remaining_text} | Speed: {speed:.2} blk/s | Elapsed: {elapsed_display} | ETA: {eta_display}"
    );
    let info = Paragraph::new(info_line)
        .block(Block::default().title("Run details").borders(Borders::ALL));
    frame.render_widget(info, chunks[1]);
}

fn panel_height(show_progress: bool) -> u16 {
    if show_progress {
        TABLE_SECTION_HEIGHT + PROGRESS_SECTION_HEIGHT
    } else {
        TABLE_SECTION_HEIGHT
    }
}

fn panel_area(area: Rect, show_progress: bool) -> Rect {
    let width = PANEL_WIDTH.min(area.width);
    let height = panel_height(show_progress).min(area.height);
    Rect {
        x: area.x,
        y: area.y,
        width,
        height,
    }
}

fn section_constraints(height: u16, show_progress: bool) -> Vec<Constraint> {
    if show_progress {
        let table_height = TABLE_SECTION_HEIGHT.min(height);
        let progress_height = height.saturating_sub(table_height).max(1);
        vec![
            Constraint::Length(table_height),
            Constraint::Length(progress_height),
        ]
    } else {
        vec![Constraint::Length(height)]
    }
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

fn format_with_separators(value: u64) -> String {
    let digits = value.to_string();
    let len = digits.len();
    let mut formatted = String::with_capacity(len + len / 3);
    for (idx, ch) in digits.chars().enumerate() {
        if idx > 0 && (len - idx).is_multiple_of(3) {
            formatted.push(',');
        }
        formatted.push(ch);
    }
    formatted
}

fn format_amount(amount: &Amount) -> String {
    const SCALE: u64 = 100_000_000;
    let whole = *amount / SCALE;
    let fractional = *amount % SCALE;
    let whole_text = format_with_separators(whole);
    format!("{whole_text}.{fractional:08}")
}

struct ProgressState {
    start_height: AtomicU64,
    start_instant: Mutex<Instant>,
    last_processed: AtomicU64,
    tip_height: AtomicU64,
    tip_known: AtomicBool,
    latest_cumulative: Mutex<Option<CumulativeStats>>,
    session_baseline: AtomicU64,
}

impl ProgressState {
    fn new(start_height: u64) -> Self {
        Self {
            start_height: AtomicU64::new(start_height),
            start_instant: Mutex::new(Instant::now()),
            last_processed: AtomicU64::new(start_height.saturating_sub(1)),
            tip_height: AtomicU64::new(0),
            tip_known: AtomicBool::new(false),
            latest_cumulative: Mutex::new(None),
            session_baseline: AtomicU64::new(start_height.saturating_sub(1)),
        }
    }

    fn update_tip(&self, tip: u64) {
        self.tip_height.store(tip, Ordering::SeqCst);
        self.tip_known.store(true, Ordering::SeqCst);
    }

    fn snapshot(&self) -> ProgressSnapshot {
        let tip_known = self.tip_known.load(Ordering::SeqCst);
        let tip_value = self.tip_height.load(Ordering::SeqCst);
        let latest_cumulative = self
            .latest_cumulative
            .lock()
            .expect("latest cumulative mutex poisoned")
            .clone();
        ProgressSnapshot {
            start_height: self.start_height.load(Ordering::SeqCst),
            start_instant: *self
                .start_instant
                .lock()
                .expect("start instant mutex poisoned"),
            last_processed: self.last_processed.load(Ordering::SeqCst),
            tip_height: tip_known.then_some(tip_value),
            latest_cumulative,
            session_baseline: self.session_baseline.load(Ordering::SeqCst),
        }
    }

    fn set_cumulative(&self, stats: Option<CumulativeStats>) {
        if let Some(ref latest) = stats {
            if latest.block_count() > 0 {
                let first_processed = latest
                    .block_index()
                    .saturating_sub(latest.block_count().saturating_sub(1));
                self.bump_start_height(first_processed);
            }
            self.bump_last_processed(latest.block_index());
        }
        *self
            .latest_cumulative
            .lock()
            .expect("latest cumulative mutex poisoned") = stats;
    }

    fn reset_session_baseline(&self, baseline: u64) {
        self.session_baseline.store(baseline, Ordering::SeqCst);
        *self
            .start_instant
            .lock()
            .expect("start instant mutex poisoned") = Instant::now();
    }

    fn bump_start_height(&self, candidate: u64) {
        let mut current = self.start_height.load(Ordering::SeqCst);
        while candidate < current {
            match self.start_height.compare_exchange(
                current,
                candidate,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    fn bump_last_processed(&self, height: u64) {
        let mut current = self.last_processed.load(Ordering::SeqCst);
        while matches!(height.cmp(&current), CmpOrdering::Greater) {
            match self.last_processed.compare_exchange(
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
}

#[derive(Clone)]
struct ProgressSnapshot {
    start_height: u64,
    start_instant: Instant,
    last_processed: u64,
    tip_height: Option<u64>,
    latest_cumulative: Option<CumulativeStats>,
    session_baseline: u64,
}
