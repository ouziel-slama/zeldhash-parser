//! UTXO storage backed by rollblock for O(1) lookups and instant rollbacks.
//!
//! Wraps the [`rollblock`] key-value store to provide ZELD-aware UTXO management
//! with support for efficient chain reorganization handling.

use std::ops::{Deref, DerefMut};

use rollblock::types::{Operation as StoreOperation, StoreKey, Value as StoreValue};
use rollblock::BlockStoreFacade;
use zeldhash_protocol::{
    store::ZeldStore,
    types::{Balance, UtxoKey},
};

/// Owns the rollblock facade and hands out lightweight store views as needed.
pub(crate) struct UTXOStore {
    facade: BlockStoreFacade,
}

impl UTXOStore {
    pub(crate) fn new(facade: BlockStoreFacade) -> Self {
        Self { facade }
    }

    pub(crate) fn view(&self) -> UTXOStoreView<'_> {
        UTXOStoreView {
            facade: &self.facade,
        }
    }
}

impl Deref for UTXOStore {
    type Target = BlockStoreFacade;

    fn deref(&self) -> &Self::Target {
        &self.facade
    }
}

impl DerefMut for UTXOStore {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.facade
    }
}

/// Thin wrapper to expose `ZeldStore` functionality on top of `BlockStoreFacade`.
pub(crate) struct UTXOStoreView<'a> {
    facade: &'a BlockStoreFacade,
}

impl UTXOStoreView<'_> {
    #[inline]
    fn to_store_key(key: &UtxoKey) -> StoreKey {
        StoreKey::from_prefix(*key)
    }

    fn decode_value(&self, key: &UtxoKey, value: StoreValue) -> Balance {
        if value.is_delete() {
            return 0;
        }

        let bytes: &[u8] = value.as_ref();
        if bytes.len() > 8 {
            panic!(
                "invalid rollblock value for key {key:?}: expected up to 8 bytes, got {}",
                bytes.len()
            );
        }
        let mut buf = [0u8; 8];
        buf[..bytes.len()].copy_from_slice(bytes);
        Balance::from_le_bytes(buf)
    }

    fn encode_balance(value: Balance) -> StoreValue {
        StoreValue::from_le_bytes(value.to_le_bytes())
    }
}

impl ZeldStore for UTXOStoreView<'_> {
    fn get(&mut self, key: &UtxoKey) -> Balance {
        let store_key = Self::to_store_key(key);
        let value = self.facade.get(store_key).unwrap_or_else(|err| {
            // Panic intentionally: rollblock read failures signal critical data corruption that must be investigated by an operator.
            panic!("rollblock get failed for key {key:?}: {err}");
        });
        self.decode_value(key, value)
    }

    fn set(&mut self, key: UtxoKey, value: Balance) {
        let store_key = Self::to_store_key(&key);
        if let Err(err) = self.facade.set(StoreOperation {
            key: store_key,
            value: Self::encode_balance(value),
        }) {
            // Panic intentionally: write failures mean the backing store is inconsistent and requires manual operator intervention.
            panic!("rollblock set failed: {err}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rollblock::StoreConfig;
    use tempfile::TempDir;

    fn create_test_store(temp_dir: &TempDir) -> UTXOStore {
        let config = StoreConfig::new(temp_dir.path(), 2, 1000, 2, false).expect("store config");
        let facade = BlockStoreFacade::new(config).expect("create facade");
        UTXOStore::new(facade)
    }

    #[test]
    fn utxo_store_deref_returns_facade() {
        let temp = TempDir::new().expect("temp dir");
        let store = create_test_store(&temp);
        // Deref should return a reference to the facade - checking current_block confirms it works
        let facade_ref: &BlockStoreFacade = &store;
        assert!(facade_ref.current_block().is_ok());
    }

    #[test]
    fn utxo_store_view_returns_zero_for_empty_value() {
        let temp = TempDir::new().expect("temp dir");
        let store = create_test_store(&temp);
        let view = store.view();
        // An empty StoreValue represents a deleted/missing key
        let empty_value = StoreValue::from_vec(Vec::new());
        let key: UtxoKey = [0u8; 12];
        let balance = view.decode_value(&key, empty_value);
        assert_eq!(balance, 0);
    }

    #[test]
    fn utxo_store_view_decodes_positive_balance() {
        let temp = TempDir::new().expect("temp dir");
        let store = create_test_store(&temp);
        let view = store.view();
        let key: UtxoKey = [1u8; 12];
        let value = UTXOStoreView::encode_balance(42);
        let balance = view.decode_value(&key, value);
        assert_eq!(balance, 42);
    }

    #[test]
    fn utxo_store_view_decodes_negative_balance() {
        let temp = TempDir::new().expect("temp dir");
        let store = create_test_store(&temp);
        let view = store.view();
        let key: UtxoKey = [1u8; 12];
        let value = UTXOStoreView::encode_balance(-50);
        let balance = view.decode_value(&key, value);
        assert_eq!(balance, -50);
    }

    #[test]
    fn utxo_store_set_get_roundtrip() {
        let temp = TempDir::new().expect("temp dir");
        let store = create_test_store(&temp);

        store.start_block(0).expect("start block");

        let key: UtxoKey = [2u8; 12];
        {
            let mut view = store.view();
            view.set(key, 100);
        }

        store.end_block().expect("end block");

        // Start a new block to read the value
        store.start_block(1).expect("start next block");
        {
            let mut view = store.view();
            let amount = view.get(&key);
            assert_eq!(amount, 100);
        }
        store.end_block().expect("end next block");
    }

    #[test]
    fn utxo_store_spent_balance_becomes_negative() {
        let temp = TempDir::new().expect("temp dir");
        let store = create_test_store(&temp);

        store.start_block(0).expect("start block");
        let key: UtxoKey = [3u8; 12];
        {
            let mut view = store.view();
            view.set(key, 200);
        }
        store.end_block().expect("end block");

        // Spend the UTXO by setting a negative tombstone
        store.start_block(1).expect("start spend block");
        {
            let mut view = store.view();
            let balance = view.get(&key);
            assert_eq!(balance, 200);
            // Mark as spent with negative tombstone
            view.set(key, -balance);
        }
        store.end_block().expect("end spend block");

        // After spending, the value should be negative
        store.start_block(2).expect("start verify block");
        {
            let mut view = store.view();
            let balance = view.get(&key);
            assert_eq!(balance, -200);
        }
        store.end_block().expect("end verify block");
    }

    #[test]
    fn utxo_store_deref_mut_allows_mutation() {
        let temp = TempDir::new().expect("temp dir");
        let mut store = create_test_store(&temp);
        // DerefMut should allow us to call mutable methods on the facade
        let facade_mut: &mut BlockStoreFacade = &mut store;
        facade_mut
            .start_block(0)
            .expect("start block via deref_mut");
        facade_mut.end_block().expect("end block via deref_mut");
    }
}
