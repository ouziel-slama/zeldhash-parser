//! Core block processing logic for the ZELDHASH protocol.
//!
//! This module implements the [`BlockProtocol`] trait from protoblock,
//! providing the main parsing pipeline that:
//!
//! 1. Pre-processes blocks in parallel to extract ZELD-relevant data
//! 2. Processes blocks sequentially to update the UTXO set and statistics
//! 3. Handles chain reorganizations through rollback support

use crate::config::AppConfig;
use crate::progress::ProgressHandle;
use crate::stores::sqlite::{get_read_write_connection, SqliteStore};
use crate::stores::utxo::UTXOStore;
use anyhow::{Context, Result};
use bitcoin::Block;
use protoblock::{
    preprocessors::sized_queue::QueueByteSize,
    runtime::protocol::{
        BlockProtocol, ProtocolError, ProtocolFuture, ProtocolPreProcessFuture, ProtocolStage,
    },
};
use rollblock::BlockStoreFacade;
use rusqlite::Connection;
use std::sync::{Arc, Mutex};
use zeldhash_protocol::{
    config::ZeldConfig,
    protocol::ZeldProtocol,
    types::{PreProcessedZeldBlock, ZeldInput, ZeldOutput, ZeldTransaction},
};

/// Parser driving block ingestion for ZELD.
pub struct ZeldParser {
    protocol: ZeldProtocol,
    store: UTXOStore,
    progress: Option<ProgressHandle>,
    sqlite_conn: Arc<Mutex<Connection>>,
}

impl ZeldParser {
    /// Builds a parser from the full application configuration.
    pub fn new(app_config: AppConfig) -> Result<Self> {
        let zeld_config = ZeldConfig::for_network(app_config.network);
        let store_config = app_config.rollblock.store_config.clone();
        let store =
            BlockStoreFacade::new(store_config).context("failed to initialize rollblock store")?;
        let store = UTXOStore::new(store);
        let sqlite_conn = get_read_write_connection(&app_config.data_dir)
            .context("failed to open ZELD SQLite store")?;
        let sqlite_conn = Arc::new(Mutex::new(sqlite_conn));
        SqliteStore::initialize(&sqlite_conn).context("failed to initialize ZELD SQLite store")?;
        Ok(Self {
            protocol: ZeldProtocol::new(zeld_config),
            store,
            progress: None,
            sqlite_conn,
        })
    }

    /// Wires a progress handle so the parser can report processed heights.
    pub fn attach_progress(&mut self, progress: ProgressHandle) {
        if let Some(stats) = SqliteStore::latest_cumulative() {
            progress.update_cumulative(Some(&stats));
            progress.mark_processed(stats.block_index());
            progress.reset_speed_baseline(stats.block_index());
        }
        self.progress = Some(progress);
    }

    fn protocol_error(stage: ProtocolStage, err: impl Into<anyhow::Error>) -> ProtocolError {
        ProtocolError::new(stage, err.into())
    }
}

impl BlockProtocol for ZeldParser {
    type PreProcessed = PreProcessedBlock;

    fn pre_process(
        &self,
        block: Block,
        _height: u64,
    ) -> ProtocolPreProcessFuture<Self::PreProcessed> {
        let protocol = self.protocol.clone();
        Box::pin(async move {
            let parsed = protocol.pre_process_block(&block);
            Ok(PreProcessedBlock::new(parsed))
        })
    }

    fn process(&mut self, data: Self::PreProcessed, height: u64) -> ProtocolFuture<'_> {
        Box::pin(async move {
            self.store
                .start_block(height)
                .map_err(|err| Self::protocol_error(ProtocolStage::Process, err))?;

            let pre_processed = data.into_inner();
            let processed_block = {
                let mut store_view = self.store.view();
                self.protocol.process_block(&pre_processed, &mut store_view)
            };

            self.store
                .end_block()
                .map_err(|err| Self::protocol_error(ProtocolStage::Process, err))?;

            let sqlite_store = SqliteStore::new(Arc::clone(&self.sqlite_conn));
            let cumul_stats = sqlite_store
                .save_block(height, &processed_block)
                .map_err(|err| Self::protocol_error(ProtocolStage::Process, err))?;

            if let Some(handle) = &self.progress {
                handle.update_cumulative(Some(&cumul_stats));
                handle.mark_processed(height);
            }

            Ok(())
        })
    }

    fn rollback(&mut self, block_height: u64) -> ProtocolFuture<'_> {
        Box::pin(async move {
            self.store
                .rollback(block_height)
                .map_err(|err| Self::protocol_error(ProtocolStage::Rollback, err))?;

            let sqlite_store = SqliteStore::new(Arc::clone(&self.sqlite_conn));
            let latest_cumul = sqlite_store
                .rollback(block_height)
                .map_err(|err| Self::protocol_error(ProtocolStage::Rollback, err))?;

            if let Some(handle) = &self.progress {
                handle.update_cumulative(latest_cumul.as_ref());
                handle.rollback_to(block_height.saturating_sub(1));
            }

            Ok(())
        })
    }

    fn shutdown(&mut self) -> ProtocolFuture<'_> {
        Box::pin(async move {
            self.store
                .close()
                .map_err(|err| Self::protocol_error(ProtocolStage::Shutdown, err))?;
            Ok(())
        })
    }
}

/// Wrapper used to attach queue sizing metadata to pre-processed blocks.
#[derive(Clone)]
pub struct PreProcessedBlock {
    inner: PreProcessedZeldBlock,
}

impl PreProcessedBlock {
    pub fn new(inner: PreProcessedZeldBlock) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> PreProcessedZeldBlock {
        self.inner
    }
}

impl QueueByteSize for PreProcessedBlock {
    fn queue_bytes(&self) -> usize {
        let mut total = core::mem::size_of::<PreProcessedZeldBlock>();

        for tx in &self.inner.transactions {
            total = total
                .saturating_add(core::mem::size_of::<ZeldTransaction>())
                .saturating_add(
                    tx.inputs
                        .len()
                        .saturating_mul(core::mem::size_of::<ZeldInput>()),
                )
                .saturating_add(
                    tx.outputs
                        .len()
                        .saturating_mul(core::mem::size_of::<ZeldOutput>()),
                );
        }

        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{hashes::Hash, Txid};
    use core::mem::size_of;

    fn sample_tx(id: u8, inputs: usize, outputs: usize) -> ZeldTransaction {
        let txid = Txid::from_slice(&[id; 32]).expect("txid");
        let inputs = (0..inputs)
            .map(|i| ZeldInput {
                utxo_key: [i as u8; 12],
            })
            .collect();
        let outputs = (0..outputs)
            .map(|i| ZeldOutput {
                utxo_key: [i as u8; 12],
                value: 1,
                reward: 1,
                distribution: 0,
                vout: i as u32,
                address: None,
            })
            .collect();

        ZeldTransaction {
            txid,
            inputs,
            outputs,
            zero_count: id,
            reward: 1,
            has_op_return_distribution: false,
        }
    }

    #[test]
    fn queue_bytes_counts_transactions_inputs_and_outputs() {
        let block = PreProcessedZeldBlock {
            transactions: vec![sample_tx(1, 1, 2), sample_tx(2, 2, 1)],
            max_zero_count: 2,
        };
        let pre_processed = PreProcessedBlock::new(block.clone());

        let mut expected = size_of::<PreProcessedZeldBlock>();
        for tx in &block.transactions {
            expected += size_of::<ZeldTransaction>();
            expected += tx.inputs.len() * size_of::<ZeldInput>();
            expected += tx.outputs.len() * size_of::<ZeldOutput>();
        }

        assert_eq!(pre_processed.queue_bytes(), expected);
    }

    #[test]
    fn pre_processed_block_into_inner_returns_original() {
        let block = PreProcessedZeldBlock {
            transactions: vec![sample_tx(3, 2, 3)],
            max_zero_count: 5,
        };
        let pre_processed = PreProcessedBlock::new(block.clone());
        let recovered = pre_processed.into_inner();

        assert_eq!(recovered.transactions.len(), 1);
        assert_eq!(recovered.max_zero_count, 5);
    }

    #[test]
    fn pre_processed_block_is_clone() {
        let block = PreProcessedZeldBlock {
            transactions: vec![sample_tx(1, 1, 1)],
            max_zero_count: 1,
        };
        let pre_processed = PreProcessedBlock::new(block);
        let cloned = pre_processed.clone();

        assert_eq!(pre_processed.queue_bytes(), cloned.queue_bytes());
    }

    #[test]
    fn queue_bytes_handles_empty_block() {
        let block = PreProcessedZeldBlock {
            transactions: vec![],
            max_zero_count: 0,
        };
        let pre_processed = PreProcessedBlock::new(block);

        // Empty block should just be the size of PreProcessedZeldBlock itself
        assert_eq!(
            pre_processed.queue_bytes(),
            size_of::<PreProcessedZeldBlock>()
        );
    }

    #[test]
    fn queue_bytes_handles_many_inputs_outputs() {
        let block = PreProcessedZeldBlock {
            transactions: vec![sample_tx(1, 100, 200)],
            max_zero_count: 1,
        };
        let pre_processed = PreProcessedBlock::new(block);

        let expected = size_of::<PreProcessedZeldBlock>()
            + size_of::<ZeldTransaction>()
            + 100 * size_of::<ZeldInput>()
            + 200 * size_of::<ZeldOutput>();

        assert_eq!(pre_processed.queue_bytes(), expected);
    }

    #[test]
    fn pre_processed_block_new_creates_wrapper() {
        let block = PreProcessedZeldBlock {
            transactions: vec![],
            max_zero_count: 0,
        };
        let pre_processed = PreProcessedBlock::new(block);
        assert_eq!(
            pre_processed.queue_bytes(),
            size_of::<PreProcessedZeldBlock>()
        );
    }

    #[test]
    fn sample_tx_creates_valid_transaction() {
        let tx = sample_tx(5, 3, 4);
        assert_eq!(tx.inputs.len(), 3);
        assert_eq!(tx.outputs.len(), 4);
        assert_eq!(tx.zero_count, 5);
    }

    #[test]
    fn queue_bytes_with_multiple_transactions() {
        let block = PreProcessedZeldBlock {
            transactions: vec![sample_tx(1, 2, 3), sample_tx(2, 4, 5), sample_tx(3, 6, 7)],
            max_zero_count: 3,
        };
        let pre_processed = PreProcessedBlock::new(block.clone());

        let mut expected = size_of::<PreProcessedZeldBlock>();
        for tx in &block.transactions {
            expected += size_of::<ZeldTransaction>();
            expected += tx.inputs.len() * size_of::<ZeldInput>();
            expected += tx.outputs.len() * size_of::<ZeldOutput>();
        }

        assert_eq!(pre_processed.queue_bytes(), expected);
    }

    #[test]
    fn pre_processed_block_clone_preserves_data() {
        let block = PreProcessedZeldBlock {
            transactions: vec![sample_tx(2, 1, 2)],
            max_zero_count: 2,
        };
        let original = PreProcessedBlock::new(block);
        let cloned = original.clone();

        assert_eq!(original.queue_bytes(), cloned.queue_bytes());
        assert_eq!(original.inner.max_zero_count, cloned.inner.max_zero_count);
        assert_eq!(
            original.inner.transactions.len(),
            cloned.inner.transactions.len()
        );
    }

    #[test]
    fn queue_bytes_handles_transaction_with_no_inputs() {
        let tx = ZeldTransaction {
            txid: Txid::from_slice(&[1u8; 32]).expect("txid"),
            inputs: vec![],
            outputs: vec![ZeldOutput {
                utxo_key: [0u8; 12],
                value: 100,
                reward: 10,
                distribution: 0,
                vout: 0,
                address: None,
            }],
            zero_count: 1,
            reward: 10,
            has_op_return_distribution: false,
        };

        let block = PreProcessedZeldBlock {
            transactions: vec![tx],
            max_zero_count: 1,
        };
        let pre_processed = PreProcessedBlock::new(block);

        let expected = size_of::<PreProcessedZeldBlock>()
            + size_of::<ZeldTransaction>()
            + size_of::<ZeldOutput>();

        assert_eq!(pre_processed.queue_bytes(), expected);
    }

    #[test]
    fn queue_bytes_handles_transaction_with_no_outputs() {
        let tx = ZeldTransaction {
            txid: Txid::from_slice(&[2u8; 32]).expect("txid"),
            inputs: vec![ZeldInput {
                utxo_key: [1u8; 12],
            }],
            outputs: vec![],
            zero_count: 0,
            reward: 0,
            has_op_return_distribution: false,
        };

        let block = PreProcessedZeldBlock {
            transactions: vec![tx],
            max_zero_count: 0,
        };
        let pre_processed = PreProcessedBlock::new(block);

        let expected = size_of::<PreProcessedZeldBlock>()
            + size_of::<ZeldTransaction>()
            + size_of::<ZeldInput>();

        assert_eq!(pre_processed.queue_bytes(), expected);
    }

    #[test]
    fn pre_processed_block_inner_access() {
        let block = PreProcessedZeldBlock {
            transactions: vec![sample_tx(5, 2, 3)],
            max_zero_count: 5,
        };
        let pre_processed = PreProcessedBlock::new(block);

        // Access inner directly
        assert_eq!(pre_processed.inner.max_zero_count, 5);
        assert_eq!(pre_processed.inner.transactions.len(), 1);
        assert_eq!(pre_processed.inner.transactions[0].zero_count, 5);
    }
}
