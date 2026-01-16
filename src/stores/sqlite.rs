//! SQLite storage for ZELD block statistics and rewards.
//!
//! Persists per-block statistics and cumulative protocol metrics in a local
//! SQLite database. The data is used for querying historical ZELD protocol
//! state and displaying progress information.

use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{anyhow, Context, Result};
use rusqlite::{Connection, Transaction};
use serde::{Deserialize, Serialize};
use zeldhash_protocol::types::{Amount, ProcessedZeldBlock};

use super::queries;

static LATEST_CUMULATIVE: OnceLock<Mutex<Option<CumulativeStats>>> = OnceLock::new();

/// Default filename for the SQLite statistics database.
pub const DB_FILE_NAME: &str = "zeldstats.sqlite3";

/// Opens (and creates when missing) the SQLite database used to persist block data.
pub fn get_read_write_connection(data_dir: &Path) -> Result<Connection> {
    fs::create_dir_all(data_dir).with_context(|| {
        format!(
            "unable to create ZELD data directory at {}",
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
        queries::ensure_rewards_address_column(&tx)?;
        queries::create_stats_table(&tx)?;
        queries::create_rewards_block_index_index(&tx)?;
        queries::create_rewards_txid_index(&tx)?;
        queries::create_rewards_zero_count_index(&tx)?;

        tx.commit()
            .context("failed to commit SQLite transaction for ensure_schema")?;
        Ok(())
    }

    /// Runs all required one-time initialization helpers for a SQLite connection.
    pub fn initialize(conn: &Arc<Mutex<Connection>>) -> Result<()> {
        let mut guard = conn
            .lock()
            .expect("sqlite connection mutex poisoned during initialization");
        Self::ensure_schema(&mut guard).context("failed to initialize ZELD SQLite schema")?;
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
        block: &ProcessedZeldBlock,
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
            queries::insert_reward(
                &tx,
                block_index_sql,
                &txid,
                vout,
                zero_count,
                reward_value,
                reward.address.as_deref(),
            )?;
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
    fn from_processed(block_index: u64, block: &ProcessedZeldBlock) -> Self {
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

/// Cumulative statistics aggregated across all processed blocks.
///
/// Tracks running totals for ZELD protocol metrics including total rewards,
/// maximum zero count observed, and UTXO activity.
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
    /// Returns the height of the last processed block.
    pub fn block_index(&self) -> u64 {
        self.block_index
    }

    /// Returns the total number of blocks processed.
    pub fn block_count(&self) -> u64 {
        self.block_count
    }

    /// Returns the cumulative ZELD reward in satoshis.
    pub fn total_reward(&self) -> &Amount {
        &self.total_reward
    }

    /// Returns the total count of individual rewards across all blocks.
    pub fn reward_count(&self) -> u64 {
        self.reward_count
    }

    /// Returns the highest zero count observed in any transaction hash.
    pub fn max_zero_count(&self) -> u8 {
        self.max_zero_count
    }

    /// Returns the transaction ID with the highest zero count ("nicest" hash).
    pub fn nicest_txid(&self) -> Option<&str> {
        self.nicest_txid.as_deref()
    }

    /// Returns the cumulative count of UTXOs spent.
    pub fn utxo_spent_count(&self) -> u64 {
        self.utxo_spent_count
    }

    /// Returns the cumulative count of new UTXOs created.
    pub fn new_utxo_count(&self) -> u64 {
        self.new_utxo_count
    }

    /// Returns a builder for creating `CumulativeStats` instances in tests.
    #[cfg(test)]
    pub(crate) fn builder() -> CumulativeStatsBuilder {
        CumulativeStatsBuilder::default()
    }
}

/// Builder for creating `CumulativeStats` instances in tests.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct CumulativeStatsBuilder {
    inner: CumulativeStats,
}

#[cfg(test)]
impl CumulativeStatsBuilder {
    pub fn block_index(mut self, value: u64) -> Self {
        self.inner.block_index = value;
        self
    }

    pub fn block_count(mut self, value: u64) -> Self {
        self.inner.block_count = value;
        self
    }

    pub fn total_reward(mut self, value: impl Into<Amount>) -> Self {
        self.inner.total_reward = value.into();
        self
    }

    pub fn reward_count(mut self, value: u64) -> Self {
        self.inner.reward_count = value;
        self
    }

    pub fn max_zero_count(mut self, value: u8) -> Self {
        self.inner.max_zero_count = value;
        self
    }

    pub fn nicest_txid(mut self, value: Option<String>) -> Self {
        self.inner.nicest_txid = value;
        self
    }

    pub fn utxo_spent_count(mut self, value: u64) -> Self {
        self.inner.utxo_spent_count = value;
        self
    }

    pub fn new_utxo_count(mut self, value: u64) -> Self {
        self.inner.new_utxo_count = value;
        self
    }

    pub fn build(self) -> CumulativeStats {
        self.inner
    }
}

fn to_sql_i64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow!("{field} value {value} overflows SQLite INTEGER"))
}

#[cfg(test)]
mod tests {
    use super::{to_sql_i64, BlockStats, CumulativeStats, SqliteStore};
    use crate::stores::queries;
    use rusqlite::{params, Connection};

    #[test]
    fn to_sql_i64_accepts_in_range_values() {
        assert_eq!(to_sql_i64(42, "example").expect("convert"), 42);
    }

    #[test]
    fn to_sql_i64_errors_on_overflow() {
        let err = to_sql_i64(u64::MAX, "example").expect_err("overflow should fail");
        assert!(
            err.to_string().contains("overflows SQLite INTEGER"),
            "error message should mention overflow"
        );
    }

    #[test]
    fn block_stats_update_cumulative_sets_initial_values() {
        let stats = BlockStats {
            block_index: 5,
            total_reward: 10u64,
            reward_count: 2,
            max_zero_count: 3,
            nicest_txid: Some("nice".to_string()),
            utxo_spent_count: 4,
            new_utxo_count: 6,
        };

        let cumul = stats.update_cumulative(None);

        assert_eq!(cumul.block_index(), 5);
        assert_eq!(cumul.block_count(), 1);
        assert_eq!(*cumul.total_reward(), 10u64);
        assert_eq!(cumul.reward_count(), 2);
        assert_eq!(cumul.max_zero_count(), 3);
        assert_eq!(cumul.nicest_txid(), Some("nice"));
        assert_eq!(cumul.utxo_spent_count(), 4);
        assert_eq!(cumul.new_utxo_count(), 6);
    }

    #[test]
    fn block_stats_update_cumulative_accumulates_and_preserves_nicest() {
        let previous = CumulativeStats {
            block_index: 9,
            block_count: 3,
            total_reward: 50u64,
            reward_count: 6,
            max_zero_count: 5,
            nicest_txid: Some("prev".to_string()),
            utxo_spent_count: 10,
            new_utxo_count: 12,
        };

        let stats = BlockStats {
            block_index: 10,
            total_reward: 5u64,
            reward_count: 1,
            max_zero_count: 3,
            nicest_txid: Some("new".to_string()),
            utxo_spent_count: 2,
            new_utxo_count: 3,
        };

        let cumul = stats.update_cumulative(Some(previous));

        assert_eq!(cumul.block_index(), 10);
        assert_eq!(cumul.block_count(), 4);
        assert_eq!(*cumul.total_reward(), 55u64);
        assert_eq!(cumul.reward_count(), 7);
        assert_eq!(cumul.max_zero_count(), 5, "should keep previous max");
        assert_eq!(
            cumul.nicest_txid(),
            Some("prev"),
            "should keep previous nicest"
        );
        assert_eq!(cumul.utxo_spent_count(), 12);
        assert_eq!(cumul.new_utxo_count(), 15);
    }

    #[test]
    fn latest_cumulative_before_returns_none_when_empty() {
        let mut conn = Connection::open_in_memory().expect("open db");
        let tx = conn.transaction().expect("start tx");
        queries::create_stats_table(&tx).expect("create stats table");
        tx.commit().expect("commit schema");

        let tx = conn.transaction().expect("start read tx");
        let latest = SqliteStore::latest_cumulative_before(&tx, 10).expect("latest query");
        tx.commit().expect("commit read tx");

        assert!(latest.is_none());
    }

    #[test]
    fn latest_cumulative_before_returns_latest_matching_row() {
        let mut conn = Connection::open_in_memory().expect("open db");
        let tx = conn.transaction().expect("start tx");
        queries::create_stats_table(&tx).expect("create stats table");
        tx.commit().expect("commit schema");

        let older = CumulativeStats {
            block_index: 1,
            block_count: 1,
            total_reward: 5u64,
            reward_count: 2,
            max_zero_count: 1,
            nicest_txid: Some("old".to_string()),
            utxo_spent_count: 0,
            new_utxo_count: 3,
        };
        let newer = CumulativeStats {
            block_index: 3,
            block_count: 2,
            total_reward: 8u64,
            reward_count: 4,
            max_zero_count: 2,
            nicest_txid: Some("new".to_string()),
            utxo_spent_count: 1,
            new_utxo_count: 5,
        };

        let insert_tx = conn.transaction().expect("start insert tx");
        insert_tx
            .execute(
                "INSERT INTO stats (block_index, block_stats, cumul_stats) VALUES (?1, 'old', ?2)",
                params![
                    older.block_index as i64,
                    serde_json::to_string(&older).unwrap()
                ],
            )
            .expect("insert older row");
        insert_tx
            .execute(
                "INSERT INTO stats (block_index, block_stats, cumul_stats) VALUES (?1, 'new', ?2)",
                params![
                    newer.block_index as i64,
                    serde_json::to_string(&newer).unwrap()
                ],
            )
            .expect("insert newer row");
        insert_tx.commit().expect("commit inserts");

        let tx = conn.transaction().expect("start read tx");
        let latest = SqliteStore::latest_cumulative_before(&tx, 4).expect("latest query");
        tx.commit().expect("commit read");

        let latest = latest.expect("should find a row before height 4");
        assert_eq!(latest.block_index(), 3);
        assert_eq!(latest.block_count(), 2);
        assert_eq!(*latest.total_reward(), 8u64);
        assert_eq!(latest.nicest_txid(), Some("new"));
    }

    #[test]
    fn cumulative_stats_builder_creates_correct_instance() {
        let stats = CumulativeStats::builder()
            .block_index(10)
            .block_count(5)
            .total_reward(100u64)
            .reward_count(3)
            .max_zero_count(2)
            .nicest_txid(Some("txid".to_string()))
            .utxo_spent_count(1)
            .new_utxo_count(4)
            .build();
        assert_eq!(stats.block_index(), 10);
        assert_eq!(stats.block_count(), 5);
        assert_eq!(*stats.total_reward(), 100);
        assert_eq!(stats.reward_count(), 3);
        assert_eq!(stats.max_zero_count(), 2);
        assert_eq!(stats.nicest_txid(), Some("txid"));
        assert_eq!(stats.utxo_spent_count(), 1);
        assert_eq!(stats.new_utxo_count(), 4);
    }

    #[test]
    fn cumulative_stats_default_is_zeroed() {
        let stats = CumulativeStats::default();
        assert_eq!(stats.block_index(), 0);
        assert_eq!(stats.block_count(), 0);
        assert_eq!(*stats.total_reward(), 0);
        assert_eq!(stats.reward_count(), 0);
        assert_eq!(stats.max_zero_count(), 0);
        assert_eq!(stats.nicest_txid(), None);
        assert_eq!(stats.utxo_spent_count(), 0);
        assert_eq!(stats.new_utxo_count(), 0);
    }

    #[test]
    fn block_stats_from_processed_captures_values() {
        use bitcoin::{hashes::Hash, Txid};
        use zeldhash_protocol::types::{ProcessedZeldBlock, Reward};

        let txid = Txid::from_slice(&[1u8; 32]).expect("txid");
        let rewards = vec![Reward {
            txid,
            vout: 0,
            zero_count: 5,
            reward: 10,
            address: Some("bc1qtest".to_string()),
        }];
        let block = ProcessedZeldBlock {
            rewards,
            total_reward: 10,
            max_zero_count: 5,
            nicest_txid: Some(txid),
            utxo_spent_count: 2,
            new_utxo_count: 3,
        };

        let stats = BlockStats::from_processed(100, &block);

        assert_eq!(stats.block_index, 100);
        assert_eq!(stats.total_reward, 10);
        assert_eq!(stats.reward_count, 1);
        assert_eq!(stats.max_zero_count, 5);
        assert!(stats.nicest_txid.is_some());
        assert_eq!(stats.utxo_spent_count, 2);
        assert_eq!(stats.new_utxo_count, 3);
    }

    #[test]
    fn block_stats_update_cumulative_updates_nicest_on_higher_zero_count() {
        let previous = CumulativeStats {
            block_index: 9,
            block_count: 3,
            total_reward: 50,
            reward_count: 6,
            max_zero_count: 3,
            nicest_txid: Some("prev".to_string()),
            utxo_spent_count: 10,
            new_utxo_count: 12,
        };

        let stats = BlockStats {
            block_index: 10,
            total_reward: 5,
            reward_count: 1,
            max_zero_count: 5, // Higher than previous (3)
            nicest_txid: Some("new_nice".to_string()),
            utxo_spent_count: 2,
            new_utxo_count: 3,
        };

        let cumul = stats.update_cumulative(Some(previous));

        assert_eq!(cumul.max_zero_count(), 5, "should update to new higher max");
        assert_eq!(
            cumul.nicest_txid(),
            Some("new_nice"),
            "should update nicest txid"
        );
    }

    #[test]
    fn get_read_write_connection_creates_db_file() {
        use super::{get_read_write_connection, DB_FILE_NAME};
        use tempfile::TempDir;

        let temp = TempDir::new().expect("temp dir");
        let _conn = get_read_write_connection(temp.path()).expect("connection");

        let db_path = temp.path().join(DB_FILE_NAME);
        assert!(db_path.exists(), "database file should be created");
    }

    #[test]
    fn sqlite_store_initialize_creates_tables() {
        use super::get_read_write_connection;
        use std::sync::{Arc, Mutex};
        use tempfile::TempDir;

        let temp = TempDir::new().expect("temp dir");
        let conn = get_read_write_connection(temp.path()).expect("connection");
        let conn = Arc::new(Mutex::new(conn));

        SqliteStore::initialize(&conn).expect("initialize");

        // Verify tables exist by querying sqlite_master
        let guard = conn.lock().expect("lock");
        let rewards_exists: bool = guard
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='rewards'",
                [],
                |_| Ok(true),
            )
            .unwrap_or(false);
        let stats_exists: bool = guard
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stats'",
                [],
                |_| Ok(true),
            )
            .unwrap_or(false);
        let rewards_block_index_exists: bool = guard
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='index' AND name='rewards_block_index_idx'",
                [],
                |_| Ok(true),
            )
            .unwrap_or(false);
        let rewards_txid_exists: bool = guard
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='index' AND name='rewards_txid_idx'",
                [],
                |_| Ok(true),
            )
            .unwrap_or(false);
        let rewards_zero_count_exists: bool = guard
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='index' AND name='rewards_zero_count_idx'",
                [],
                |_| Ok(true),
            )
            .unwrap_or(false);

        assert!(rewards_exists, "rewards table should exist");
        assert!(stats_exists, "stats table should exist");
        assert!(
            rewards_block_index_exists,
            "rewards block index should exist"
        );
        assert!(rewards_txid_exists, "rewards txid index should exist");
        assert!(
            rewards_zero_count_exists,
            "rewards zero_count index should exist"
        );
    }

    #[test]
    fn sqlite_store_save_and_rollback() {
        use super::get_read_write_connection;
        use bitcoin::{hashes::Hash, Txid};
        use std::sync::{Arc, Mutex};
        use tempfile::TempDir;
        use zeldhash_protocol::types::{ProcessedZeldBlock, Reward};

        let temp = TempDir::new().expect("temp dir");
        let conn = get_read_write_connection(temp.path()).expect("connection");
        let conn = Arc::new(Mutex::new(conn));
        SqliteStore::initialize(&conn).expect("initialize");

        let store = SqliteStore::new(Arc::clone(&conn));

        // Save block 0
        let txid = Txid::from_slice(&[1u8; 32]).expect("txid");
        let block0 = ProcessedZeldBlock {
            rewards: vec![Reward {
                txid,
                vout: 0,
                zero_count: 3,
                reward: 100,
                address: Some("bc1qblock0".to_string()),
            }],
            total_reward: 100,
            max_zero_count: 3,
            nicest_txid: Some(txid),
            utxo_spent_count: 1,
            new_utxo_count: 2,
        };
        let cumul0 = store.save_block(0, &block0).expect("save block 0");
        assert_eq!(cumul0.block_index(), 0);
        assert_eq!(cumul0.block_count(), 1);

        // Save block 1
        let txid2 = Txid::from_slice(&[2u8; 32]).expect("txid2");
        let block1 = ProcessedZeldBlock {
            rewards: vec![Reward {
                txid: txid2,
                vout: 1,
                zero_count: 4,
                reward: 50,
                address: None,
            }],
            total_reward: 50,
            max_zero_count: 4,
            nicest_txid: Some(txid2),
            utxo_spent_count: 0,
            new_utxo_count: 1,
        };
        let cumul1 = store.save_block(1, &block1).expect("save block 1");
        assert_eq!(cumul1.block_index(), 1);
        assert_eq!(cumul1.block_count(), 2);
        assert_eq!(*cumul1.total_reward(), 150);

        // Rollback to before block 1
        let after_rollback = store.rollback(0).expect("rollback");
        let after_rollback = after_rollback.expect("should have stats after rollback");
        assert_eq!(after_rollback.block_index(), 0);
        assert_eq!(after_rollback.block_count(), 1);
        assert_eq!(*after_rollback.total_reward(), 100);
    }
}
