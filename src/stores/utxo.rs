use std::ops::{Deref, DerefMut};

use mhinprotocol::{
    store::MhinStore,
    types::{Amount, UtxoKey},
};
use rollblock::MhinStoreBlockFacade;
use rollblock::types::{Operation as StoreOperation, Value as StoreValue};

/// Owns the rollblock facade and hands out lightweight store views as needed.
pub(crate) struct UTXOStore {
    facade: MhinStoreBlockFacade,
}

impl UTXOStore {
    pub(crate) fn new(facade: MhinStoreBlockFacade) -> Self {
        Self { facade }
    }

    pub(crate) fn view(&self) -> UTXOStoreView<'_> {
        UTXOStoreView {
            facade: &self.facade,
        }
    }
}

impl Deref for UTXOStore {
    type Target = MhinStoreBlockFacade;

    fn deref(&self) -> &Self::Target {
        &self.facade
    }
}

impl DerefMut for UTXOStore {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.facade
    }
}

/// Thin wrapper to expose `MhinStore` functionality on top of `MhinStoreBlockFacade`.
pub(crate) struct UTXOStoreView<'a> {
    facade: &'a MhinStoreBlockFacade,
}

impl<'a> UTXOStoreView<'a> {
    fn decode_value(&self, key: &UtxoKey, value: StoreValue) -> Amount {
        if value.is_delete() {
            return 0;
        }

        value.to_u64().unwrap_or_else(|| {
            panic!(
                "invalid rollblock value for key {key:?}: rollblock value exceeds 64-bit width ({} bytes)",
                value.len()
            )
        })
    }
}

impl<'a> MhinStore for UTXOStoreView<'a> {
    fn get(&mut self, key: &UtxoKey) -> Amount {
        let value = self.facade.get(*key).unwrap_or_else(|err| {
            // Panic intentionally: rollblock read failures signal critical data corruption that must be investigated by an operator.
            panic!("rollblock get failed for key {key:?}: {err}");
        });
        self.decode_value(key, value)
    }

    fn pop(&mut self, key: &UtxoKey) -> Amount {
        let value = self.facade.pop(*key).unwrap_or_else(|err| {
            // Panic intentionally: rollblock read failures signal critical data corruption that must be investigated by an operator.
            panic!("rollblock pop failed for key {key:?}: {err}");
        });
        self.decode_value(key, value)
    }

    fn set(&mut self, key: UtxoKey, value: Amount) {
        if let Err(err) = self.facade.set(StoreOperation {
            key,
            value: StoreValue::from(value),
        }) {
            // Panic intentionally: write failures mean the backing store is inconsistent and requires manual operator intervention.
            panic!("rollblock set failed: {err}");
        }
    }
}
