# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] - 2026-01-16

### Added

- Added `address` column to `rewards` table to store Bitcoin addresses for reward outputs
- Automatic migration for existing databases via `ensure_rewards_address_column()`

### Changed

- Bumped `zeldhash-protocol` to 0.6.0
- Refactored UTXO store to use signed `Balance` type (i64) instead of unsigned `Amount` (u64)
- Spent UTXOs are now marked with negative balance tombstones instead of being deleted

### Removed

- Removed `ZeldStore::pop()` method in favor of negative balance tombstones

### Notes

- **Full reparse required**: This version requires a complete reparse of the blockchain due to UTXO storage model changes

## [0.4.0] - 2025-01-10

### Changed

- Bumped `zeldhash-protocol` to 0.5.0

## [0.3.1] - 2025-12-30

### Added

- Added index on `rewards(txid)` for fast transaction lookups
- Added index on `rewards(zero_count DESC)` for efficient sorting by zero count

## [0.3.0] - 2025-12-27

### Changed

- Bumped `zeldhash-protocol` to 0.4.0
- Bumped minimum Rust version to 1.83
- Added `rust-toolchain.toml` to enforce Rust 1.83

### Notes

- **Full reparse required**: This version requires a complete reparse of the blockchain due to protocol changes

## [0.2.1] - 2025-12-13

### Changed

- Bumped `rollblock` to 0.4.1 and `zeldhash-protocol` to 0.3.1

## [0.2.0] - 2025-12-13

### Breaking

- Renamed the project and crate from `mhinparser` to `zeldhash-parser`
- Switched protocol dependency from `mhinprotocol` to `zeldhash-protocol` with 12-byte UTXO keys; existing rollblock stores must be rebuilt
- Configuration file and runtime paths now use the `zeldhash` namespace and `zeldhash-parser.toml`
- SQLite stats database filename is now `zeldstats.sqlite3`

### Changed

- Bumped `rollblock` to 0.4.0 (key-12 feature) and `zeldhash-protocol` to 0.3.0
- Updated docs, CLI, and runtime messages to the new ZeldHash branding

## [0.1.1] - 2025-12-06

### Changed

- Updated `rollblock` dependency to version 0.3.4

## [0.1.0] - 2025-12-06

### Added

- Initial release of zeldhash-parser
- Bitcoin blockchain parsing with the **ZeldHash** protocol
- High-performance block fetching via `protoblock`
- UTXO management with instant rollback support via `rollblock`
- Multi-network support: Mainnet, Testnet4, Signet, Regtest
- Interactive terminal progress UI with `ratatui`
- Daemon mode with SIGINT/SIGTERM handling
- SQLite storage for block statistics
- TOML configuration file support
- CLI with environment variable overrides

[0.5.0]: https://github.com/ouziel-slama/zeldhash-parser/releases/tag/v0.5.0
[0.4.0]: https://github.com/ouziel-slama/zeldhash-parser/releases/tag/v0.4.0
[0.3.1]: https://github.com/ouziel-slama/zeldhash-parser/releases/tag/v0.3.1
[0.3.0]: https://github.com/ouziel-slama/zeldhash-parser/releases/tag/v0.3.0
[0.2.1]: https://github.com/ouziel-slama/zeldhash-parser/releases/tag/v0.2.1
[0.2.0]: https://github.com/ouziel-slama/zeldhash-parser/releases/tag/v0.2.0
[0.1.1]: https://github.com/ouziel-slama/zeldhash-parser/releases/tag/v0.1.1
[0.1.0]: https://github.com/ouziel-slama/zeldhash-parser/releases/tag/v0.1.0

