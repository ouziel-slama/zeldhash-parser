# zeldhash-parser

[![Tests](https://github.com/ouziel-slama/zeldhash-parser/actions/workflows/tests.yml/badge.svg)](https://github.com/ouziel-slama/zeldhash-parser/actions/workflows/tests.yml)
[![Coverage](https://codecov.io/github/ouziel-slama/zeldhash-parser/graph/badge.svg?token=QKMVNDU06E)](https://codecov.io/github/ouziel-slama/zeldhash-parser)
[![Format](https://github.com/ouziel-slama/zeldhash-parser/actions/workflows/fmt.yml/badge.svg)](https://github.com/ouziel-slama/zeldhash-parser/actions/workflows/fmt.yml)
[![Clippy](https://github.com/ouziel-slama/zeldhash-parser/actions/workflows/clippy.yml/badge.svg)](https://github.com/ouziel-slama/zeldhash-parser/actions/workflows/clippy.yml)
[![Crates.io](https://img.shields.io/crates/v/zeldhash-parser.svg)](https://crates.io/crates/zeldhash-parser)

A high-performance Bitcoin blockchain parser implementing the **[ZeldHash](https://zeldhash.com/)** protocol.

Built on [`zeldhash_protocol`](https://crates.io/crates/zeldhash_protocol), this parser leverages [`protoblock`](https://crates.io/crates/protoblock) for blazing-fast block fetching and [`rollblock`](https://crates.io/crates/rollblock) for efficient UTXO management with instant rollback support.

## Features

- 🚀 **Maximum Performance** — Parallel block fetching via `protoblock` with configurable thread pools
- ⚡ **Instant Rollbacks** — `rollblock` provides chain reorganization handling
- 🌐 **Multi-Network** — Supports Mainnet, Testnet4, Signet, and Regtest
- 📊 **Live Progress** — Terminal UI powered by `ratatui`
- 🔧 **Daemon Mode** — Run as a background service with signal handling

## Installation

```bash
cargo install zeldhash-parser
```

Or build from source:

```bash
git clone https://github.com/ouziel-slama/zeldhash-parser
cd zeldhash-parser
cargo build --release
```

## Quick Start

```bash
# Parse mainnet (connects to local Bitcoin Core RPC)
zeldhash-parser

# Parse testnet4
zeldhash-parser --network testnet4

# Run as daemon
zeldhash-parser --daemon

# Stop daemon
zeldhash-parser stop
```

## Configuration

Configuration can be provided via CLI flags, environment variables, or a TOML file.

Default config location: `~/.config/zeldhash/zeldhash-parser/zeldhash-parser.toml`

Example configuration with production-optimized values (defaults are more conservative):

```toml
# Network: mainnet, testnet4, signet, regtest
network = "mainnet"

# Data directory for SQLite stats and rollblock storage
data_dir = "/path/to/data"

[protoblock]
rpc_url = "http://127.0.0.1:8332"
rpc_user = "bitcoin"
rpc_password = "password"
thread_count = 8              # default: 4
max_batch_size_mb = 256       # default: 10

[rollblock]
user = "zeld"                 # change this in production
password = "zeld"             # change this in production
port = 9443
shards_count = 16
thread_count = 4
initial_capacity = 100_000_000  # default: 5_000_000
```

The embedded rollblock server binds to `127.0.0.1` with basic authentication. By default it uses the `zeld`/`zeld` credentials on port `9443`; change these via `--rollblock_user`, `--rollblock_password`, and `--rollblock_port`. The parser emits a startup warning when the defaults are still in use.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                       zeldhash-parser                            │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  protoblock │  │ zeldhash_protocol│  │     rollblock       │  │
│  │  ───────────│  │  ───────────│  │  ─────────────────  │  │
│  │  Fast block │  │  ZELD       │  │  UTXO store with    │  │
│  │  fetching & │  │  protocol   │  │  instant rollback   │  │
│  │  processing │  │  logic      │  │  support            │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│                           │                                  │
│                    ┌──────┴──────┐                          │
│                    │   SQLite    │                          │
│                    │   (stats)   │                          │
│                    └─────────────┘                          │
└─────────────────────────────────────────────────────────────┘
```

## CLI Reference

```
Usage: zeldhash-parser [OPTIONS] [COMMAND]

Commands:
  run   Start the parser (default)
  stop  Stop the running daemon

Options:
  -c, --config <FILE>       Path to config file
  -d, --data-dir <DIR>      Data directory
  -n, --network <NETWORK>   Bitcoin network [mainnet|testnet4|signet|regtest]
      --daemon              Run as background daemon
  -h, --help                Print help
  -V, --version             Print version
```

## Requirements

- Rust 1.75+ (2021 edition)
- Bitcoin Core node with RPC enabled
- ~50GB+ disk space for mainnet UTXO set

## Data Storage

### SQLite Stats Database

The parser stores protocol statistics in `zeldstats.sqlite3` with two tables:

```sql
-- Individual rewards per block
CREATE TABLE rewards (
    block_index INTEGER NOT NULL,
    txid TEXT NOT NULL,
    vout INTEGER NOT NULL,
    zero_count INTEGER NOT NULL,
    reward INTEGER NOT NULL,
    address TEXT,                -- Bitcoin address (NULL for non-standard scripts)
    PRIMARY KEY (block_index, txid, vout)
);

-- Per-block and cumulative stats (JSON-encoded)
CREATE TABLE stats (
    block_index INTEGER PRIMARY KEY,
    block_stats TEXT NOT NULL,   -- {"block_index":..., "total_reward":..., "reward_count":..., ...}
    cumul_stats TEXT NOT NULL    -- cumulative totals up to this block
);
```

**Example: Query rewards with sqlite3**

```bash
sqlite3 /path/to/data/zeldstats.sqlite3 \
  "SELECT txid, zero_count, reward, address FROM rewards WHERE block_index = 870000;"
```

### Rollblock UTXO Store

UTXOs are stored in a [`rollblock`](https://crates.io/crates/rollblock) key-value store for O(1) lookups and instant rollbacks.

**Key format:** Each UTXO is identified by a 12-byte key computed with the helper exposed by `zeldhash-protocol`:

```rust
use bitcoin::Txid;
use xxhash_rust::xxh3::xxh3_128;

fn compute_utxo_key(txid: &Txid, vout: u32) -> [u8; 12] {
    let mut payload = [0u8; 36];
    payload[..32].copy_from_slice(txid.as_ref());       // 32-byte txid
    payload[32..].copy_from_slice(&vout.to_le_bytes()); // 4-byte vout (LE)
    let hash = xxh3_128(&payload).to_le_bytes();        // xxHash128 → take first 12 bytes
    let mut key = [0u8; 12];
    key.copy_from_slice(&hash[..12]);                   // truncate to 96 bits
    key
}
```

**Value format:** The ZELD UTXO balance stored as a little-endian `u64`.

**Example: Read UTXO balances with rollblock client**

```rust
use rollblock::client::{ClientConfig, RemoteStoreClient};
use rollblock::net::BasicAuthConfig;
use zeldhash_protocol::protocol::compute_utxo_key;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to rollblock server
    let auth = BasicAuthConfig::new("your_user", "your_password");
    let config = ClientConfig::without_tls(auth);
    let mut client = RemoteStoreClient::connect("127.0.0.1:9443", config)?;

    // Compute UTXO key for txid:vout
    let txid_hex = "abc123..."; // your txid
    let vout: u32 = 0;
    let txid = bitcoin::Txid::from_hex(txid_hex)?;
    let key = compute_utxo_key(&txid, vout);

    // Fetch balance
    let value = client.get_one(key)?;
    let balance = u64::from_le_bytes(value.try_into().unwrap_or([0; 8]));
    println!("Balance: {} sats", balance);

    client.close()?;
    Ok(())
}
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

