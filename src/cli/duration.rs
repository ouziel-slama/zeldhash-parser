use std::time::Duration;

/// Parses human-friendly duration strings (e.g. `30s`, `5m`, `1h`).
pub fn parse_duration(value: &str) -> Result<Duration, String> {
    humantime::parse_duration(value).map_err(|err| err.to_string())
}
