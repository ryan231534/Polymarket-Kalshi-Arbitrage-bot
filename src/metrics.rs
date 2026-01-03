//! Production metrics and monitoring for the arbitrage trading system.
//!
//! This module provides comprehensive observability including counters, gauges,
//! and histograms for tracking arbitrage opportunities, execution performance,
//! P&L, API latencies, and system health.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Global metrics registry for the arbitrage trading system
#[derive(Debug, Clone)]
pub struct Metrics {
    /// Arbitrage opportunities detected
    pub opportunities_detected: Arc<Counter>,
    /// Arbitrage opportunities executed
    pub opportunities_executed: Arc<Counter>,
    /// Execution attempts (may include retries)
    pub execution_attempts: Arc<Counter>,
    /// Successful executions
    pub execution_successes: Arc<Counter>,
    /// Failed executions
    pub execution_failures: Arc<Counter>,

    /// Total realized P&L in cents
    pub realized_pnl_cents: Arc<Gauge>,
    /// Total unrealized P&L in cents
    pub unrealized_pnl_cents: Arc<Gauge>,
    /// Total fees paid in cents
    pub total_fees_cents: Arc<Gauge>,

    /// Discovery operations completed
    pub discovery_runs: Arc<Counter>,
    /// Markets discovered
    pub markets_discovered: Arc<Gauge>,
    /// Discovery errors
    pub discovery_errors: Arc<Counter>,

    /// Kalshi API calls
    pub kalshi_api_calls: Arc<Counter>,
    /// Kalshi API errors
    pub kalshi_api_errors: Arc<Counter>,
    /// Kalshi API retries
    pub kalshi_api_retries: Arc<Counter>,

    /// Polymarket API calls
    pub poly_api_calls: Arc<Counter>,
    /// Polymarket API errors
    pub poly_api_errors: Arc<Counter>,
    /// Polymarket API retries
    pub poly_api_retries: Arc<Counter>,

    /// WebSocket reconnections (Kalshi)
    pub kalshi_ws_reconnects: Arc<Counter>,
    /// WebSocket reconnections (Polymarket)
    pub poly_ws_reconnects: Arc<Counter>,

    /// Circuit breaker trips
    pub circuit_breaker_trips: Arc<Counter>,
    /// Active positions count
    pub active_positions: Arc<Gauge>,
}

impl Metrics {
    /// Create a new metrics registry
    pub fn new() -> Self {
        Self {
            opportunities_detected: Arc::new(Counter::new("opportunities_detected")),
            opportunities_executed: Arc::new(Counter::new("opportunities_executed")),
            execution_attempts: Arc::new(Counter::new("execution_attempts")),
            execution_successes: Arc::new(Counter::new("execution_successes")),
            execution_failures: Arc::new(Counter::new("execution_failures")),

            realized_pnl_cents: Arc::new(Gauge::new("realized_pnl_cents")),
            unrealized_pnl_cents: Arc::new(Gauge::new("unrealized_pnl_cents")),
            total_fees_cents: Arc::new(Gauge::new("total_fees_cents")),

            discovery_runs: Arc::new(Counter::new("discovery_runs")),
            markets_discovered: Arc::new(Gauge::new("markets_discovered")),
            discovery_errors: Arc::new(Counter::new("discovery_errors")),

            kalshi_api_calls: Arc::new(Counter::new("kalshi_api_calls")),
            kalshi_api_errors: Arc::new(Counter::new("kalshi_api_errors")),
            kalshi_api_retries: Arc::new(Counter::new("kalshi_api_retries")),

            poly_api_calls: Arc::new(Counter::new("poly_api_calls")),
            poly_api_errors: Arc::new(Counter::new("poly_api_errors")),
            poly_api_retries: Arc::new(Counter::new("poly_api_retries")),

            kalshi_ws_reconnects: Arc::new(Counter::new("kalshi_ws_reconnects")),
            poly_ws_reconnects: Arc::new(Counter::new("poly_ws_reconnects")),

            circuit_breaker_trips: Arc::new(Counter::new("circuit_breaker_trips")),
            active_positions: Arc::new(Gauge::new("active_positions")),
        }
    }

    /// Export metrics in Prometheus text format
    pub fn export_prometheus(&self) -> String {
        let mut output = String::new();

        // Trading metrics
        output.push_str(&format!(
            "# HELP opportunities_detected Total arbitrage opportunities detected\n\
             # TYPE opportunities_detected counter\n\
             opportunities_detected {}\n\n",
            self.opportunities_detected.get()
        ));

        output.push_str(&format!(
            "# HELP opportunities_executed Total arbitrage opportunities executed\n\
             # TYPE opportunities_executed counter\n\
             opportunities_executed {}\n\n",
            self.opportunities_executed.get()
        ));

        output.push_str(&format!(
            "# HELP execution_attempts Total execution attempts\n\
             # TYPE execution_attempts counter\n\
             execution_attempts {}\n\n",
            self.execution_attempts.get()
        ));

        output.push_str(&format!(
            "# HELP execution_successes Total successful executions\n\
             # TYPE execution_successes counter\n\
             execution_successes {}\n\n",
            self.execution_successes.get()
        ));

        output.push_str(&format!(
            "# HELP execution_failures Total failed executions\n\
             # TYPE execution_failures counter\n\
             execution_failures {}\n\n",
            self.execution_failures.get()
        ));

        // P&L metrics
        output.push_str(&format!(
            "# HELP realized_pnl_cents Realized profit/loss in cents\n\
             # TYPE realized_pnl_cents gauge\n\
             realized_pnl_cents {}\n\n",
            self.realized_pnl_cents.get()
        ));

        output.push_str(&format!(
            "# HELP unrealized_pnl_cents Unrealized profit/loss in cents\n\
             # TYPE unrealized_pnl_cents gauge\n\
             unrealized_pnl_cents {}\n\n",
            self.unrealized_pnl_cents.get()
        ));

        output.push_str(&format!(
            "# HELP total_fees_cents Total fees paid in cents\n\
             # TYPE total_fees_cents gauge\n\
             total_fees_cents {}\n\n",
            self.total_fees_cents.get()
        ));

        // Discovery metrics
        output.push_str(&format!(
            "# HELP discovery_runs Total discovery operations\n\
             # TYPE discovery_runs counter\n\
             discovery_runs {}\n\n",
            self.discovery_runs.get()
        ));

        output.push_str(&format!(
            "# HELP markets_discovered Number of markets discovered\n\
             # TYPE markets_discovered gauge\n\
             markets_discovered {}\n\n",
            self.markets_discovered.get()
        ));

        output.push_str(&format!(
            "# HELP discovery_errors Total discovery errors\n\
             # TYPE discovery_errors counter\n\
             discovery_errors {}\n\n",
            self.discovery_errors.get()
        ));

        // API metrics
        output.push_str(&format!(
            "# HELP kalshi_api_calls Total Kalshi API calls\n\
             # TYPE kalshi_api_calls counter\n\
             kalshi_api_calls {}\n\n",
            self.kalshi_api_calls.get()
        ));

        output.push_str(&format!(
            "# HELP kalshi_api_errors Total Kalshi API errors\n\
             # TYPE kalshi_api_errors counter\n\
             kalshi_api_errors {}\n\n",
            self.kalshi_api_errors.get()
        ));

        output.push_str(&format!(
            "# HELP poly_api_calls Total Polymarket API calls\n\
             # TYPE poly_api_calls counter\n\
             poly_api_calls {}\n\n",
            self.poly_api_calls.get()
        ));

        output.push_str(&format!(
            "# HELP poly_api_errors Total Polymarket API errors\n\
             # TYPE poly_api_errors counter\n\
             poly_api_errors {}\n\n",
            self.poly_api_errors.get()
        ));

        // WebSocket metrics
        output.push_str(&format!(
            "# HELP kalshi_ws_reconnects Kalshi WebSocket reconnections\n\
             # TYPE kalshi_ws_reconnects counter\n\
             kalshi_ws_reconnects {}\n\n",
            self.kalshi_ws_reconnects.get()
        ));

        output.push_str(&format!(
            "# HELP poly_ws_reconnects Polymarket WebSocket reconnections\n\
             # TYPE poly_ws_reconnects counter\n\
             poly_ws_reconnects {}\n\n",
            self.poly_ws_reconnects.get()
        ));

        // System metrics
        output.push_str(&format!(
            "# HELP circuit_breaker_trips Circuit breaker activations\n\
             # TYPE circuit_breaker_trips counter\n\
             circuit_breaker_trips {}\n\n",
            self.circuit_breaker_trips.get()
        ));

        output.push_str(&format!(
            "# HELP active_positions Number of active positions\n\
             # TYPE active_positions gauge\n\
             active_positions {}\n\n",
            self.active_positions.get()
        ));

        output
    }

    /// Print human-readable metrics summary
    pub fn print_summary(&self) {
        tracing::info!("📊 === METRICS SUMMARY ===");
        tracing::info!("Trading:");
        tracing::info!(
            "  Opportunities detected: {}",
            self.opportunities_detected.get()
        );
        tracing::info!(
            "  Opportunities executed: {}",
            self.opportunities_executed.get()
        );
        tracing::info!(
            "  Execution success rate: {:.1}%",
            self.execution_success_rate()
        );
        tracing::info!(
            "  Realized P&L: ${:.2}",
            self.realized_pnl_cents.get() as f64 / 100.0
        );
        tracing::info!(
            "  Unrealized P&L: ${:.2}",
            self.unrealized_pnl_cents.get() as f64 / 100.0
        );
        tracing::info!(
            "  Total fees: ${:.2}",
            self.total_fees_cents.get() as f64 / 100.0
        );
        tracing::info!("  Active positions: {}", self.active_positions.get());

        tracing::info!("Discovery:");
        tracing::info!("  Discovery runs: {}", self.discovery_runs.get());
        tracing::info!("  Markets discovered: {}", self.markets_discovered.get());
        tracing::info!("  Discovery errors: {}", self.discovery_errors.get());

        tracing::info!("API Health:");
        tracing::info!(
            "  Kalshi calls: {} (errors: {}, retries: {})",
            self.kalshi_api_calls.get(),
            self.kalshi_api_errors.get(),
            self.kalshi_api_retries.get()
        );
        tracing::info!(
            "  Polymarket calls: {} (errors: {}, retries: {})",
            self.poly_api_calls.get(),
            self.poly_api_errors.get(),
            self.poly_api_retries.get()
        );
        tracing::info!(
            "  Kalshi WS reconnects: {}",
            self.kalshi_ws_reconnects.get()
        );
        tracing::info!(
            "  Polymarket WS reconnects: {}",
            self.poly_ws_reconnects.get()
        );

        tracing::info!("System:");
        tracing::info!(
            "  Circuit breaker trips: {}",
            self.circuit_breaker_trips.get()
        );
    }

    /// Calculate execution success rate as percentage
    fn execution_success_rate(&self) -> f64 {
        let total = self.execution_attempts.get();
        if total == 0 {
            return 0.0;
        }
        (self.execution_successes.get() as f64 / total as f64) * 100.0
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Atomic counter for monotonically increasing metrics
#[derive(Debug)]
pub struct Counter {
    name: &'static str,
    value: AtomicU64,
}

impl Counter {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            value: AtomicU64::new(0),
        }
    }

    /// Increment counter by 1
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment counter by n
    pub fn add(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Get current value
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Get metric name
    pub fn name(&self) -> &'static str {
        self.name
    }
}

/// Atomic gauge for metrics that can go up or down
#[derive(Debug)]
pub struct Gauge {
    name: &'static str,
    value: AtomicU64,
}

impl Gauge {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            value: AtomicU64::new(0),
        }
    }

    /// Set gauge to specific value
    pub fn set(&self, value: i64) {
        // Store as u64 but interpret as i64 (two's complement)
        self.value.store(value as u64, Ordering::Relaxed);
    }

    /// Increment gauge by 1
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement gauge by 1
    pub fn dec(&self) {
        self.value.fetch_sub(1, Ordering::Relaxed);
    }

    /// Add to gauge
    pub fn add(&self, n: i64) {
        if n >= 0 {
            self.value.fetch_add(n as u64, Ordering::Relaxed);
        } else {
            self.value.fetch_sub((-n) as u64, Ordering::Relaxed);
        }
    }

    /// Get current value (as signed integer)
    pub fn get(&self) -> i64 {
        self.value.load(Ordering::Relaxed) as i64
    }

    /// Get metric name
    pub fn name(&self) -> &'static str {
        self.name
    }
}

/// Timer for measuring operation duration
pub struct Timer {
    start: Instant,
}

impl Timer {
    /// Start a new timer
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Get elapsed time in milliseconds
    pub fn elapsed_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    /// Get elapsed time in microseconds
    pub fn elapsed_us(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_basic() {
        let counter = Counter::new("test");
        assert_eq!(counter.get(), 0);

        counter.inc();
        assert_eq!(counter.get(), 1);

        counter.add(5);
        assert_eq!(counter.get(), 6);
    }

    #[test]
    fn test_counter_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let counter = Arc::new(Counter::new("test"));
        let mut handles = vec![];

        // Spawn 10 threads, each incrementing 100 times
        for _ in 0..10 {
            let counter = counter.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    counter.inc();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(counter.get(), 1000);
    }

    #[test]
    fn test_gauge_positive() {
        let gauge = Gauge::new("test");
        assert_eq!(gauge.get(), 0);

        gauge.set(42);
        assert_eq!(gauge.get(), 42);

        gauge.inc();
        assert_eq!(gauge.get(), 43);

        gauge.add(7);
        assert_eq!(gauge.get(), 50);
    }

    #[test]
    fn test_gauge_negative() {
        let gauge = Gauge::new("test");

        gauge.set(-10);
        assert_eq!(gauge.get(), -10);

        gauge.add(5);
        assert_eq!(gauge.get(), -5);

        gauge.dec();
        assert_eq!(gauge.get(), -6);

        gauge.add(-4);
        assert_eq!(gauge.get(), -10);
    }

    #[test]
    fn test_gauge_overflow() {
        let gauge = Gauge::new("test");

        gauge.set(i64::MAX);
        assert_eq!(gauge.get(), i64::MAX);

        gauge.set(i64::MIN);
        assert_eq!(gauge.get(), i64::MIN);
    }

    #[test]
    fn test_timer() {
        let timer = Timer::start();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let elapsed = timer.elapsed_ms();

        // Should be at least 10ms, but allow some slack
        assert!(elapsed >= 10, "elapsed: {}", elapsed);
        assert!(elapsed < 100, "elapsed: {}", elapsed);
    }

    #[test]
    fn test_metrics_creation() {
        let metrics = Metrics::new();

        assert_eq!(metrics.opportunities_detected.get(), 0);
        assert_eq!(metrics.execution_attempts.get(), 0);
        assert_eq!(metrics.realized_pnl_cents.get(), 0);
    }

    #[test]
    fn test_metrics_trading_flow() {
        let metrics = Metrics::new();

        // Simulate detecting an opportunity
        metrics.opportunities_detected.inc();
        assert_eq!(metrics.opportunities_detected.get(), 1);

        // Simulate executing
        metrics.opportunities_executed.inc();
        metrics.execution_attempts.inc();
        metrics.execution_successes.inc();

        // Simulate P&L
        metrics.realized_pnl_cents.set(500); // $5.00 profit
        metrics.total_fees_cents.set(50); // $0.50 fees

        assert_eq!(metrics.execution_success_rate(), 100.0);
        assert_eq!(metrics.realized_pnl_cents.get(), 500);
        assert_eq!(metrics.total_fees_cents.get(), 50);
    }

    #[test]
    fn test_prometheus_export() {
        let metrics = Metrics::new();

        metrics.opportunities_detected.add(5);
        metrics.opportunities_executed.add(3);
        metrics.realized_pnl_cents.set(1000);

        let output = metrics.export_prometheus();

        assert!(output.contains("opportunities_detected 5"));
        assert!(output.contains("opportunities_executed 3"));
        assert!(output.contains("realized_pnl_cents 1000"));
        assert!(output.contains("# TYPE opportunities_detected counter"));
        assert!(output.contains("# HELP realized_pnl_cents"));
    }

    #[test]
    fn test_execution_success_rate() {
        let metrics = Metrics::new();

        // Zero attempts
        assert_eq!(metrics.execution_success_rate(), 0.0);

        // 3 successes out of 4 attempts
        metrics.execution_attempts.add(4);
        metrics.execution_successes.add(3);
        assert_eq!(metrics.execution_success_rate(), 75.0);

        // All fail
        metrics.execution_attempts.add(6);
        assert_eq!(metrics.execution_success_rate(), 30.0); // 3/10
    }
}
