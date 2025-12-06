use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{Context, Result, anyhow};
use mhinprotocol::types::{Amount, ProcessedMhinBlock};
use rusqlite::{Connection, Transaction};
use serde::{Deserialize, Serialize};

use super::queries;

static LATEST_CUMULATIVE: OnceLock<Mutex<Option<CumulativeStats>>> = OnceLock::new();

pub const DB_FILE_NAME: &str = "mhinstats.sqlite3";

/// Opens (and creates when missing) the SQLite database used to persist block data.
pub fn get_read_write_connection(data_dir: &Path) -> Result<Connection> {
    fs::create_dir_all(data_dir).with_context(|| {
        format!(
            "unable to create MHIN data directory at {}",
            data_dir.display()
        )
    })?;

    let db_path = data_dir.join(DB_FILE_NAME);
    let connection = Connection::open(&db_path)
        .with_context(|| format!("failed to open SQLite database at {}", db_path.display()))?;

    // Best effort configuration suitable for a single-writer workload.
    let _ = connection.pragma_update(None, "journal_mode", "wal");
    let _ = connection.pragma_update(None, "synchronous", "normal");

    Ok(connection)
}

/// Encapsulates all SQLite operations.
pub struct SqliteStore {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteStore {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Returns the cached cumulative stats, if any.
    pub fn latest_cumulative() -> Option<CumulativeStats> {
        Self::get_latest_cumulative()
    }

    /// Ensures the database schema exists.
    fn ensure_schema(conn: &mut Connection) -> Result<()> {
        let tx = conn
            .transaction()
            .context("failed to start SQLite transaction for ensure_schema")?;

        queries::create_rewards_table(&tx)?;
        queries::create_stats_table(&tx)?;
        queries::create_rewards_block_index_index(&tx)?;

        tx.commit()
            .context("failed to commit SQLite transaction for ensure_schema")?;
        Ok(())
    }

    /// Runs all required one-time initialization helpers for a SQLite connection.
    pub fn initialize(conn: &Arc<Mutex<Connection>>) -> Result<()> {
        let mut guard = conn
            .lock()
            .expect("sqlite connection mutex poisoned during initialization");
        Self::ensure_schema(&mut guard).context("failed to initialize MHIN SQLite schema")?;
        Self::refresh_cumulative_cache(&mut guard)
            .context("failed to warm cumulative stats cache")?;
        Ok(())
    }

    fn refresh_cumulative_cache(conn: &mut Connection) -> Result<()> {
        let latest = Self::fetch_latest_cumulative(conn)?;
        Self::set_cached_cumulative(latest);
        Ok(())
    }

    /// Persists the processed block inside a single SQL transaction and returns the updated cumulative stats.
    pub fn save_block(
        &self,
        block_index: u64,
        block: &ProcessedMhinBlock,
    ) -> Result<CumulativeStats> {
        let block_index_sql = to_sql_i64(block_index, "block_index")?;
        let mut conn = self
            .conn
            .lock()
            .expect("sqlite connection mutex poisoned for save_block");
        let tx = conn
            .transaction()
            .context("failed to start SQLite transaction for save_block")?;

        for reward in &block.rewards {
            let vout = i64::from(reward.vout);
            let zero_count = i64::from(reward.zero_count);
            let reward_value = to_sql_i64(reward.reward, "reward")?;
            let txid = reward.txid.to_string();
            queries::insert_reward(&tx, block_index_sql, &txid, vout, zero_count, reward_value)?;
        }

        let block_stats = BlockStats::from_processed(block_index, block);
        let mut previous_cumul = Self::get_latest_cumulative();
        if previous_cumul.is_none() {
            previous_cumul = Self::latest_cumulative_before(&tx, block_index_sql)
                .context("failed to load cumulative stats")?;
            if previous_cumul.is_some() {
                Self::set_cached_cumulative(previous_cumul.clone());
            }
        }
        let cumul_stats = block_stats.update_cumulative(previous_cumul);

        let block_stats_json = serde_json::to_string(&block_stats)
            .context("failed to serialize block stats to JSON")?;
        let cumul_stats_json = serde_json::to_string(&cumul_stats)
            .context("failed to serialize cumulative stats to JSON")?;
        queries::insert_stats(&tx, block_index_sql, &block_stats_json, &cumul_stats_json)?;

        tx.commit()
            .context("failed to commit SQLite transaction for save_block")?;
        Self::set_cached_cumulative(Some(cumul_stats.clone()));
        Ok(cumul_stats)
    }

    /// Removes every row whose block index is greater than the provided value.
    pub fn rollback(&self, block_index: u64) -> Result<Option<CumulativeStats>> {
        let block_index_sql = to_sql_i64(block_index, "block_index")?;
        let mut conn = self
            .conn
            .lock()
            .expect("sqlite connection mutex poisoned for rollback");
        let tx = conn
            .transaction()
            .context("failed to start SQLite transaction for rollback")?;

        queries::delete_rewards_after_block(&tx, block_index_sql)?;
        queries::delete_stats_after_block(&tx, block_index_sql)?;

        tx.commit()
            .context("failed to commit SQLite rollback transaction")?;
        Self::refresh_cumulative_cache(&mut conn)
            .context("failed to refresh cumulative cache after rollback")?;
        Ok(Self::get_latest_cumulative())
    }

    fn fetch_latest_cumulative(conn: &mut Connection) -> Result<Option<CumulativeStats>> {
        let tx = conn
            .transaction()
            .context("failed to start SQLite transaction for cumulative stats refresh")?;
        let stats = Self::latest_cumulative_before(&tx, i64::MAX)?;
        tx.commit()
            .context("failed to commit SQLite transaction for cumulative stats refresh")?;
        Ok(stats)
    }

    fn get_latest_cumulative() -> Option<CumulativeStats> {
        Self::cumulative_cache()
            .lock()
            .expect("cumulative cache mutex poisoned")
            .clone()
    }

    fn set_cached_cumulative(stats: Option<CumulativeStats>) {
        *Self::cumulative_cache()
            .lock()
            .expect("cumulative cache mutex poisoned") = stats;
    }

    fn cumulative_cache() -> &'static Mutex<Option<CumulativeStats>> {
        LATEST_CUMULATIVE.get_or_init(|| Mutex::new(None))
    }

    fn latest_cumulative_before(
        tx: &Transaction<'_>,
        block_index_sql: i64,
    ) -> Result<Option<CumulativeStats>> {
        let payload = queries::select_latest_cumul_stats(tx, block_index_sql)?;

        if let Some(json) = payload {
            let stats: CumulativeStats = serde_json::from_str(&json)
                .context("failed to deserialize cumulative stats JSON")?;
            Ok(Some(stats))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct BlockStats {
    block_index: u64,
    total_reward: Amount,
    reward_count: u64,
    max_zero_count: u8,
    nicest_txid: Option<String>,
    utxo_spent_count: u64,
    new_utxo_count: u64,
}

impl BlockStats {
    fn from_processed(block_index: u64, block: &ProcessedMhinBlock) -> Self {
        Self {
            block_index,
            total_reward: block.total_reward,
            reward_count: block.rewards.len() as u64,
            max_zero_count: block.max_zero_count,
            nicest_txid: block.nicest_txid.map(|txid| txid.to_string()),
            utxo_spent_count: block.utxo_spent_count,
            new_utxo_count: block.new_utxo_count,
        }
    }

    fn update_cumulative(&self, previous: Option<CumulativeStats>) -> CumulativeStats {
        let mut cumul = previous.unwrap_or_default();
        cumul.block_index = self.block_index;
        cumul.block_count = cumul.block_count.saturating_add(1);
        cumul.total_reward = cumul.total_reward.saturating_add(self.total_reward);
        cumul.reward_count = cumul.reward_count.saturating_add(self.reward_count);
        cumul.utxo_spent_count = cumul.utxo_spent_count.saturating_add(self.utxo_spent_count);
        cumul.new_utxo_count = cumul.new_utxo_count.saturating_add(self.new_utxo_count);

        if self.max_zero_count > cumul.max_zero_count {
            cumul.max_zero_count = self.max_zero_count;
            cumul.nicest_txid = self.nicest_txid.clone();
        }

        cumul
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CumulativeStats {
    block_index: u64,
    block_count: u64,
    total_reward: Amount,
    reward_count: u64,
    max_zero_count: u8,
    nicest_txid: Option<String>,
    utxo_spent_count: u64,
    new_utxo_count: u64,
}

impl CumulativeStats {
    pub fn block_index(&self) -> u64 {
        self.block_index
    }

    pub fn block_count(&self) -> u64 {
        self.block_count
    }

    pub fn total_reward(&self) -> &Amount {
        &self.total_reward
    }

    pub fn reward_count(&self) -> u64 {
        self.reward_count
    }

    pub fn max_zero_count(&self) -> u8 {
        self.max_zero_count
    }

    pub fn nicest_txid(&self) -> Option<&str> {
        self.nicest_txid.as_deref()
    }

    pub fn utxo_spent_count(&self) -> u64 {
        self.utxo_spent_count
    }

    pub fn new_utxo_count(&self) -> u64 {
        self.new_utxo_count
    }
}

fn to_sql_i64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow!("{field} value {value} overflows SQLite INTEGER"))
}
