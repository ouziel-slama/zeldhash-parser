# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-12-06

### Added

- Initial release of mhinparser
- Bitcoin blockchain parsing with the **My Hash Is Nice** protocol
- High-performance block fetching via `protoblock`
- UTXO management with instant rollback support via `rollblock`
- Multi-network support: Mainnet, Testnet4, Signet, Regtest
- Interactive terminal progress UI with `ratatui`
- Daemon mode with SIGINT/SIGTERM handling
- SQLite storage for block statistics
- TOML configuration file support
- CLI with environment variable overrides

[0.1.0]: https://github.com/ouziel-slama/mhinparser/releases/tag/v0.1.0

