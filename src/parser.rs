use crate::config::AppConfig;
use crate::progress::ProgressHandle;
use crate::stores::utxo::UTXOStore;
use anyhow::{Context, Result};
use bitcoin::Block;
use mhinprotocol::{
    config::MhinConfig,
    protocol::MhinProtocol,
    types::{MhinInput, MhinOutput, MhinTransaction, PreProcessedMhinBlock},
};
use protoblock::{
    preprocessors::sized_queue::QueueByteSize,
    runtime::protocol::{
        BlockProtocol, ProtocolError, ProtocolFuture, ProtocolPreProcessFuture, ProtocolStage,
    },
};
use rollblock::MhinStoreBlockFacade;

/// Parser driving block ingestion for MHIN.
pub struct MhinParser {
    protocol: MhinProtocol,
    store: UTXOStore,
    progress: Option<ProgressHandle>,
}

impl MhinParser {
    /// Builds a parser from the full application configuration.
    pub fn new(app_config: AppConfig) -> Result<Self> {
        let mhin_config = MhinConfig::for_network(app_config.network);
        let store_config = app_config.rollblock.store_config.clone();
        let store = MhinStoreBlockFacade::new(store_config)
            .context("failed to initialize rollblock store")?;
        let store = UTXOStore::new(store);
        Ok(Self {
            protocol: MhinProtocol::new(mhin_config),
            store,
            progress: None,
        })
    }

    /// Wires a progress handle so the parser can report processed heights.
    pub fn attach_progress(&mut self, progress: ProgressHandle) {
        self.progress = Some(progress);
    }

    fn protocol_error(stage: ProtocolStage, err: impl Into<anyhow::Error>) -> ProtocolError {
        ProtocolError::new(stage, err.into())
    }
}

impl BlockProtocol for MhinParser {
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

    fn process<'a>(&'a mut self, data: Self::PreProcessed, height: u64) -> ProtocolFuture<'a> {
        Box::pin(async move {
            self.store
                .start_block(height)
                .map_err(|err| Self::protocol_error(ProtocolStage::Process, err))?;

            let pre_processed = data.into_inner();
            {
                let mut store_view = self.store.view();
                let _ = self.protocol.process_block(&pre_processed, &mut store_view);
            }

            self.store
                .end_block()
                .map_err(|err| Self::protocol_error(ProtocolStage::Process, err))?;

            if let Some(handle) = &self.progress {
                handle.mark_processed(height);
            }

            Ok(())
        })
    }

    fn rollback<'a>(&'a mut self, block_height: u64) -> ProtocolFuture<'a> {
        Box::pin(async move {
            self.store
                .rollback(block_height)
                .map_err(|err| Self::protocol_error(ProtocolStage::Rollback, err))?;

            if let Some(handle) = &self.progress {
                handle.rollback_to(block_height.saturating_sub(1));
            }

            Ok(())
        })
    }

    fn shutdown<'a>(&'a mut self) -> ProtocolFuture<'a> {
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
    inner: PreProcessedMhinBlock,
}

impl PreProcessedBlock {
    pub fn new(inner: PreProcessedMhinBlock) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> PreProcessedMhinBlock {
        self.inner
    }
}

impl QueueByteSize for PreProcessedBlock {
    fn queue_bytes(&self) -> usize {
        let mut total = core::mem::size_of::<PreProcessedMhinBlock>();

        for tx in &self.inner.transactions {
            total = total
                .saturating_add(core::mem::size_of::<MhinTransaction>())
                .saturating_add(
                    tx.inputs
                        .len()
                        .saturating_mul(core::mem::size_of::<MhinInput>()),
                )
                .saturating_add(
                    tx.outputs
                        .len()
                        .saturating_mul(core::mem::size_of::<MhinOutput>()),
                );
        }

        total
    }
}
