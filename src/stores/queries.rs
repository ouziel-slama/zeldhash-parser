use anyhow::{Context, Result};
use rusqlite::{OptionalExtension, Transaction, params};

pub fn create_rewards_table(tx: &Transaction<'_>) -> Result<()> {
    tx.execute(
        r#"
        CREATE TABLE IF NOT EXISTS rewards (
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
    .context("failed to ensure rewards table")?;
    Ok(())
}

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

pub fn create_rewards_block_index_index(tx: &Transaction<'_>) -> Result<()> {
    tx.execute(
        "CREATE INDEX IF NOT EXISTS rewards_block_index_idx ON rewards(block_index)",
        [],
    )
    .context("failed to ensure rewards block index")?;
    Ok(())
}

pub fn delete_rewards_after_block(tx: &Transaction<'_>, block_index: i64) -> Result<()> {
    tx.execute(
        "DELETE FROM rewards WHERE block_index > ?1",
        params![block_index],
    )
    .context("failed to delete rewards during rollback")?;
    Ok(())
}

pub fn delete_stats_after_block(tx: &Transaction<'_>, block_index: i64) -> Result<()> {
    tx.execute(
        "DELETE FROM stats WHERE block_index > ?1",
        params![block_index],
    )
    .context("failed to delete stats during rollback")?;
    Ok(())
}

pub fn insert_reward(
    tx: &Transaction<'_>,
    block_index: i64,
    txid: &str,
    vout: i64,
    zero_count: i64,
    reward: i64,
) -> Result<()> {
    tx.execute(
        "INSERT INTO rewards (block_index, txid, vout, zero_count, reward) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![block_index, txid, vout, zero_count, reward],
    )
    .with_context(|| {
        format!(
            "failed to insert reward row for txid {txid} vout {vout}"
        )
    })?;
    Ok(())
}

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

pub fn select_latest_cumul_stats(tx: &Transaction<'_>, block_index: i64) -> Result<Option<String>> {
    tx.query_row(
        "SELECT cumul_stats FROM stats WHERE block_index < ?1 ORDER BY block_index DESC LIMIT 1",
        params![block_index],
        |row| row.get(0),
    )
    .optional()
    .context("failed to fetch cumulative stats")
}
