//! SQL query helpers for the SQLite stats database.
//!
//! Provides low-level database operations for reward and statistics tables.

use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension, Transaction};

/// Creates the `rewards` table if it does not exist.
pub fn create_rewards_table(tx: &Transaction<'_>) -> Result<()> {
    tx.execute(
        r#"
        CREATE TABLE IF NOT EXISTS rewards (
            block_index INTEGER NOT NULL,
            txid TEXT NOT NULL,
            vout INTEGER NOT NULL,
            zero_count INTEGER NOT NULL,
            reward INTEGER NOT NULL,
            address TEXT,
            PRIMARY KEY (block_index, txid, vout)
        )
        "#,
        [],
    )
    .context("failed to ensure rewards table")?;
    Ok(())
}

/// Ensures the `address` column exists on the `rewards` table for migrations.
pub fn ensure_rewards_address_column(tx: &Transaction<'_>) -> Result<()> {
    let has_address: Option<i64> = tx
        .query_row(
            "SELECT 1 FROM pragma_table_info('rewards') WHERE name = 'address' LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .context("failed to inspect rewards table schema")?;

    if has_address.is_none() {
        tx.execute("ALTER TABLE rewards ADD COLUMN address TEXT", [])
            .context("failed to add rewards.address column")?;
    }
    Ok(())
}

/// Creates the `stats` table if it does not exist.
pub fn create_stats_table(tx: &Transaction<'_>) -> Result<()> {
    tx.execute(
        r#"
        CREATE TABLE IF NOT EXISTS stats (
            block_index INTEGER PRIMARY KEY,
            block_stats TEXT NOT NULL,
            cumul_stats TEXT NOT NULL
        )
        "#,
        [],
    )
    .context("failed to ensure stats table")?;
    Ok(())
}

/// Creates an index on `rewards(block_index)` for efficient rollback queries.
pub fn create_rewards_block_index_index(tx: &Transaction<'_>) -> Result<()> {
    tx.execute(
        "CREATE INDEX IF NOT EXISTS rewards_block_index_idx ON rewards(block_index)",
        [],
    )
    .context("failed to ensure rewards block index")?;
    Ok(())
}

/// Creates an index on `rewards(txid)` for fast lookups by transaction id.
pub fn create_rewards_txid_index(tx: &Transaction<'_>) -> Result<()> {
    tx.execute(
        "CREATE INDEX IF NOT EXISTS rewards_txid_idx ON rewards(txid)",
        [],
    )
    .context("failed to ensure rewards txid index")?;
    Ok(())
}

/// Creates an index on `rewards(zero_count DESC)` for sorting by zero count.
pub fn create_rewards_zero_count_index(tx: &Transaction<'_>) -> Result<()> {
    tx.execute(
        "CREATE INDEX IF NOT EXISTS rewards_zero_count_idx ON rewards(zero_count DESC)",
        [],
    )
    .context("failed to ensure rewards zero_count index")?;
    Ok(())
}

/// Deletes all reward rows with `block_index > threshold`.
pub fn delete_rewards_after_block(tx: &Transaction<'_>, block_index: i64) -> Result<()> {
    tx.execute(
        "DELETE FROM rewards WHERE block_index > ?1",
        params![block_index],
    )
    .context("failed to delete rewards during rollback")?;
    Ok(())
}

/// Deletes all stats rows with `block_index > threshold`.
pub fn delete_stats_after_block(tx: &Transaction<'_>, block_index: i64) -> Result<()> {
    tx.execute(
        "DELETE FROM stats WHERE block_index > ?1",
        params![block_index],
    )
    .context("failed to delete stats during rollback")?;
    Ok(())
}

/// Inserts a single reward row into the `rewards` table.
pub fn insert_reward(
    tx: &Transaction<'_>,
    block_index: i64,
    txid: &str,
    vout: i64,
    zero_count: i64,
    reward: i64,
    address: Option<&str>,
) -> Result<()> {
    tx.execute(
        "INSERT INTO rewards (block_index, txid, vout, zero_count, reward, address) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![block_index, txid, vout, zero_count, reward, address],
    )
    .with_context(|| {
        format!(
            "failed to insert reward row for txid {txid} vout {vout}"
        )
    })?;
    Ok(())
}

/// Inserts block-level and cumulative statistics as JSON strings.
pub fn insert_stats(
    tx: &Transaction<'_>,
    block_index: i64,
    block_stats: &str,
    cumul_stats: &str,
) -> Result<()> {
    tx.execute(
        "INSERT INTO stats (block_index, block_stats, cumul_stats) VALUES (?1, ?2, ?3)",
        params![block_index, block_stats, cumul_stats],
    )
    .context("failed to insert stats row")?;
    Ok(())
}

/// Returns the most recent cumulative stats JSON for blocks before `block_index`.
pub fn select_latest_cumul_stats(tx: &Transaction<'_>, block_index: i64) -> Result<Option<String>> {
    tx.query_row(
        "SELECT cumul_stats FROM stats WHERE block_index < ?1 ORDER BY block_index DESC LIMIT 1",
        params![block_index],
        |row| row.get(0),
    )
    .optional()
    .context("failed to fetch cumulative stats")
}

#[cfg(test)]
mod tests {
    use rusqlite::{params, Connection};

    use super::*;

    #[test]
    fn create_tables_and_index() {
        let mut conn = Connection::open_in_memory().expect("in-memory db");
        let tx = conn
            .transaction()
            .expect("transaction creation should succeed");

        create_rewards_table(&tx).expect("create rewards");
        ensure_rewards_address_column(&tx).expect("ensure address column");
        create_stats_table(&tx).expect("create stats");
        create_rewards_block_index_index(&tx).expect("create index");
        create_rewards_txid_index(&tx).expect("create txid index");
        create_rewards_zero_count_index(&tx).expect("create zero_count index");

        let rewards_exists: i64 = tx
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='rewards'",
                [],
                |row| row.get(0),
            )
            .expect("rewards table query");
        let stats_exists: i64 = tx
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='stats'",
                [],
                |row| row.get(0),
            )
            .expect("stats table query");
        let index_exists: i64 = tx
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='rewards_block_index_idx'",
                [],
                |row| row.get(0),
            )
            .expect("index query");
        let txid_index_exists: i64 = tx
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='rewards_txid_idx'",
                [],
                |row| row.get(0),
            )
            .expect("txid index query");
        let zero_count_index_exists: i64 = tx
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='rewards_zero_count_idx'",
                [],
                |row| row.get(0),
            )
            .expect("zero_count index query");

        assert_eq!(rewards_exists, 1);
        assert_eq!(stats_exists, 1);
        assert_eq!(index_exists, 1);
        assert_eq!(txid_index_exists, 1);
        assert_eq!(zero_count_index_exists, 1);
    }

    #[test]
    fn ensure_rewards_address_column_adds_missing_column() {
        let mut conn = Connection::open_in_memory().expect("in-memory db");
        let tx = conn
            .transaction()
            .expect("transaction creation should succeed");
        tx.execute(
            r#"
            CREATE TABLE rewards (
                block_index INTEGER NOT NULL,
                txid TEXT NOT NULL,
                vout INTEGER NOT NULL,
                zero_count INTEGER NOT NULL,
                reward INTEGER NOT NULL,
                PRIMARY KEY (block_index, txid, vout)
            )
            "#,
            [],
        )
        .expect("create legacy rewards");

        ensure_rewards_address_column(&tx).expect("add address column");

        let has_address: Option<i64> = tx
            .query_row(
                "SELECT 1 FROM pragma_table_info('rewards') WHERE name = 'address' LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .expect("query pragma");
        assert!(has_address.is_some());
    }

    #[test]
    fn select_latest_cumul_stats_returns_latest_before_block() {
        let mut conn = Connection::open_in_memory().expect("in-memory db");
        let tx = conn
            .transaction()
            .expect("transaction creation should succeed");
        create_stats_table(&tx).expect("create stats");

        tx.execute(
            "INSERT INTO stats (block_index, block_stats, cumul_stats) VALUES (?1, 'b1', 'c1')",
            params![1],
        )
        .expect("insert block 1");
        tx.execute(
            "INSERT INTO stats (block_index, block_stats, cumul_stats) VALUES (?1, 'b2', 'c2')",
            params![2],
        )
        .expect("insert block 2");

        let latest = select_latest_cumul_stats(&tx, 3).expect("select latest");
        assert_eq!(latest.as_deref(), Some("c2"));

        let latest_before_two = select_latest_cumul_stats(&tx, 2).expect("select before 2");
        assert_eq!(latest_before_two.as_deref(), Some("c1"));
    }

    #[test]
    fn delete_helpers_remove_rows_after_block() {
        let mut conn = Connection::open_in_memory().expect("in-memory db");
        let tx = conn
            .transaction()
            .expect("transaction creation should succeed");
        create_rewards_table(&tx).expect("create rewards");
        create_stats_table(&tx).expect("create stats");

        tx.execute(
            "INSERT INTO rewards (block_index, txid, vout, zero_count, reward) VALUES (?1, 'tx1', 0, 0, 1)",
            params![1],
        ).expect("insert reward 1");
        tx.execute(
            "INSERT INTO rewards (block_index, txid, vout, zero_count, reward) VALUES (?1, 'tx2', 0, 0, 1)",
            params![2],
        ).expect("insert reward 2");
        tx.execute(
            "INSERT INTO stats (block_index, block_stats, cumul_stats) VALUES (?1, 'b1', 'c1')",
            params![1],
        )
        .expect("insert stats 1");
        tx.execute(
            "INSERT INTO stats (block_index, block_stats, cumul_stats) VALUES (?1, 'b2', 'c2')",
            params![2],
        )
        .expect("insert stats 2");

        delete_rewards_after_block(&tx, 1).expect("delete rewards");
        delete_stats_after_block(&tx, 1).expect("delete stats");

        let rewards_count: i64 = tx
            .query_row("SELECT count(*) FROM rewards", [], |row| row.get(0))
            .expect("rewards count");
        let stats_count: i64 = tx
            .query_row("SELECT count(*) FROM stats", [], |row| row.get(0))
            .expect("stats count");

        assert_eq!(rewards_count, 1);
        assert_eq!(stats_count, 1);
    }

    #[test]
    fn insert_reward_creates_row() {
        let mut conn = Connection::open_in_memory().expect("in-memory db");
        let tx = conn
            .transaction()
            .expect("transaction creation should succeed");
        create_rewards_table(&tx).expect("create rewards");

        insert_reward(&tx, 100, "txid123", 0, 5, 1000, Some("bc1qtest")).expect("insert reward");

        let count: i64 = tx
            .query_row("SELECT count(*) FROM rewards", [], |row| row.get(0))
            .expect("rewards count");
        assert_eq!(count, 1);

        let (txid, zero_count, reward, address): (String, i64, i64, Option<String>) = tx
            .query_row(
                "SELECT txid, zero_count, reward, address FROM rewards WHERE block_index = 100",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("fetch reward");
        assert_eq!(txid, "txid123");
        assert_eq!(zero_count, 5);
        assert_eq!(reward, 1000);
        assert_eq!(address.as_deref(), Some("bc1qtest"));
    }

    #[test]
    fn insert_reward_with_no_address() {
        let mut conn = Connection::open_in_memory().expect("in-memory db");
        let tx = conn
            .transaction()
            .expect("transaction creation should succeed");
        create_rewards_table(&tx).expect("create rewards");

        insert_reward(&tx, 101, "txid456", 1, 3, 500, None).expect("insert reward");

        let address: Option<String> = tx
            .query_row(
                "SELECT address FROM rewards WHERE block_index = 101",
                [],
                |row| row.get(0),
            )
            .expect("fetch address");
        assert!(address.is_none());
    }

    #[test]
    fn insert_stats_creates_row() {
        let mut conn = Connection::open_in_memory().expect("in-memory db");
        let tx = conn
            .transaction()
            .expect("transaction creation should succeed");
        create_stats_table(&tx).expect("create stats");

        insert_stats(&tx, 50, r#"{"key":"value"}"#, r#"{"cumul":"data"}"#).expect("insert stats");

        let count: i64 = tx
            .query_row("SELECT count(*) FROM stats", [], |row| row.get(0))
            .expect("stats count");
        assert_eq!(count, 1);

        let (block_stats, cumul_stats): (String, String) = tx
            .query_row(
                "SELECT block_stats, cumul_stats FROM stats WHERE block_index = 50",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("fetch stats");
        assert_eq!(block_stats, r#"{"key":"value"}"#);
        assert_eq!(cumul_stats, r#"{"cumul":"data"}"#);
    }

    #[test]
    fn select_latest_cumul_stats_returns_none_for_empty_table() {
        let mut conn = Connection::open_in_memory().expect("in-memory db");
        let tx = conn
            .transaction()
            .expect("transaction creation should succeed");
        create_stats_table(&tx).expect("create stats");

        let result = select_latest_cumul_stats(&tx, 100).expect("select");
        assert!(result.is_none());
    }
}
