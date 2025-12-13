# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.2.1]: https://github.com/ouziel-slama/zeldhash-parser/releases/tag/v0.2.1
[0.2.0]: https://github.com/ouziel-slama/zeldhash-parser/releases/tag/v0.2.0
[0.1.1]: https://github.com/ouziel-slama/zeldhash-parser/releases/tag/v0.1.1
[0.1.0]: https://github.com/ouziel-slama/zeldhash-parser/releases/tag/v0.1.0

