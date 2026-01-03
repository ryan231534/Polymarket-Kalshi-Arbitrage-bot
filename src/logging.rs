//! Institutional-grade structured logging with rotation and correlation.
//!
//! Features:
//! - Dual output: console + rotating file
//! - Format control: pretty (default) or JSON
//! - Run correlation: UUID run_id on every log line
//! - Daily log rotation with non-blocking writer
//!
//! Environment variables:
//! - LOG_FORMAT=pretty|json (default: pretty)
//! - LOG_DIR=/path/to/logs (default: ./logs)
//! - RUN_ID=<uuid> (default: auto-generated)
//! - RUST_LOG=level (default: info)

use std::io;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};
use uuid::Uuid;

/// Logging format options
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Pretty,
    Json,
}

impl LogFormat {
    pub fn from_env() -> Self {
        match std::env::var("LOG_FORMAT")
            .unwrap_or_default()
            .to_lowercase()
            .as_str()
        {
            "json" => LogFormat::Json,
            _ => LogFormat::Pretty,
        }
    }
}

/// Logging configuration
#[derive(Debug, Clone)]
pub struct LogConfig {
    pub format: LogFormat,
    pub log_dir: String,
    pub run_id: Uuid,
    pub filter: String,
}

impl LogConfig {
    pub fn from_env() -> Self {
        let format = LogFormat::from_env();
        let log_dir = std::env::var("LOG_DIR").unwrap_or_else(|_| "./logs".to_string());
        let run_id = std::env::var("RUN_ID")
            .ok()
            .and_then(|s| Uuid::parse_str(&s).ok())
            .unwrap_or_else(Uuid::new_v4);
        let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| {
            "info,prediction_market_arbitrage=info,hyper=warn,reqwest=warn".to_string()
        });

        Self {
            format,
            log_dir,
            run_id,
            filter,
        }
    }
}

/// Initialize structured logging with file rotation and run correlation.
///
/// Returns a WorkerGuard that must be kept alive for the program lifetime
/// to ensure non-blocking writer flushes before exit.
///
/// # Example
/// ```no_run
/// use prediction_market_arbitrage::logging;
///
/// #[tokio::main]
/// async fn main() {
///     let _guard = logging::init_logging();
///     tracing::info!("Logging initialized");
///     // ... application logic ...
/// }
/// ```
pub fn init_logging() -> WorkerGuard {
    let config = LogConfig::from_env();

    // Create log directory if needed
    if let Err(e) = std::fs::create_dir_all(&config.log_dir) {
        eprintln!("Failed to create log directory: {}", e);
    }

    // Daily rotating file appender
    let file_appender = tracing_appender::rolling::daily(&config.log_dir, "arb_bot.log");
    let (non_blocking_file, guard) = tracing_appender::non_blocking(file_appender);

    // Build the EnvFilter
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&config.filter))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    // Build layers based on format
    match config.format {
        LogFormat::Pretty => {
            // Console layer: pretty format
            let console_layer = fmt::layer()
                .with_writer(io::stdout)
                .with_target(false)
                .with_thread_ids(false)
                .with_line_number(false)
                .with_ansi(true)
                .pretty()
                .with_filter(env_filter.clone());

            // File layer: compact format with all fields
            let file_layer = fmt::layer()
                .with_writer(non_blocking_file)
                .with_target(true)
                .with_thread_ids(true)
                .with_line_number(true)
                .with_ansi(false)
                .compact()
                .with_filter(env_filter);

            tracing_subscriber::registry()
                .with(console_layer)
                .with(file_layer)
                .init();
        }
        LogFormat::Json => {
            // Console layer: JSON format
            let console_layer = fmt::layer()
                .with_writer(io::stdout)
                .with_target(true)
                .with_thread_ids(true)
                .with_line_number(true)
                .with_ansi(false)
                .json()
                .flatten_event(true)
                .with_current_span(true)
                .with_span_list(false)
                .with_filter(env_filter.clone());

            // File layer: JSON format (same as console)
            let file_layer = fmt::layer()
                .with_writer(non_blocking_file)
                .with_target(true)
                .with_thread_ids(true)
                .with_line_number(true)
                .with_ansi(false)
                .json()
                .flatten_event(true)
                .with_current_span(true)
                .with_span_list(false)
                .with_filter(env_filter);

            tracing_subscriber::registry()
                .with(console_layer)
                .with(file_layer)
                .init();
        }
    }

    // Log initialization info
    tracing::info!(
        run_id = %config.run_id,
        log_format = ?config.format,
        log_dir = %config.log_dir,
        filter = %config.filter,
        "Logging initialized"
    );

    guard
}

/// Get the current run ID from environment or generate a new one.
/// This is used for creating the root span.
pub fn get_run_id() -> Uuid {
    std::env::var("RUN_ID")
        .ok()
        .and_then(|s| Uuid::parse_str(&s).ok())
        .unwrap_or_else(Uuid::new_v4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_format_from_env() {
        std::env::remove_var("LOG_FORMAT");
        assert_eq!(LogFormat::from_env(), LogFormat::Pretty);

        std::env::set_var("LOG_FORMAT", "json");
        assert_eq!(LogFormat::from_env(), LogFormat::Json);

        std::env::set_var("LOG_FORMAT", "JSON");
        assert_eq!(LogFormat::from_env(), LogFormat::Json);

        std::env::set_var("LOG_FORMAT", "pretty");
        assert_eq!(LogFormat::from_env(), LogFormat::Pretty);

        std::env::remove_var("LOG_FORMAT");
    }

    #[test]
    fn test_log_config_from_env() {
        std::env::remove_var("LOG_DIR");
        std::env::remove_var("RUN_ID");
        std::env::remove_var("RUST_LOG");

        let config = LogConfig::from_env();
        assert_eq!(config.log_dir, "./logs");
        assert!(config.filter.contains("info"));

        std::env::set_var("LOG_DIR", "/tmp/test_logs");
        let config = LogConfig::from_env();
        assert_eq!(config.log_dir, "/tmp/test_logs");

        std::env::remove_var("LOG_DIR");
    }

    #[test]
    fn test_get_run_id() {
        std::env::remove_var("RUN_ID");
        let id1 = get_run_id();
        let id2 = get_run_id();
        // Each call generates a new UUID
        assert_ne!(id1, id2);

        // Test with explicit RUN_ID
        let test_uuid = Uuid::new_v4();
        std::env::set_var("RUN_ID", test_uuid.to_string());
        let id3 = get_run_id();
        assert_eq!(id3, test_uuid);

        std::env::remove_var("RUN_ID");
    }
}
