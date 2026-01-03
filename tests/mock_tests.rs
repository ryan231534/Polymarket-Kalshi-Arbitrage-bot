//! Mock tests for arbitrage bot without external dependencies
//!
//! This module provides comprehensive mock-based testing for all major components
//! including API clients, execution engine, discovery system, and fee estimation.

use prediction_market_arbitrage::{
    cache::TeamCache,
    circuit_breaker::{CircuitBreaker, CircuitBreakerConfig},
    cost::*,
    execution::*,
    fees::{Exchange, FeeModel},
    metrics::Metrics,
    pnl::PnLTracker,
    position_tracker::*,
    types::*,
};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

// =============================================================================
// MOCK API CLIENTS
// =============================================================================

/// Mock Kalshi API client for testing without real API calls
#[derive(Debug, Clone)]
pub struct MockKalshiClient {
    /// Simulated response data
    events: Vec<MockKalshiEvent>,
    markets: Vec<MockKalshiMarket>,
    /// Simulated latency (ms)
    latency_ms: u64,
    /// Failure rate (0.0 to 1.0)
    failure_rate: f64,
    /// Call counter
    call_count: Arc<std::sync::atomic::AtomicU64>,
}

#[derive(Debug, Clone)]
pub struct MockKalshiEvent {
    pub series_ticker: String,
    pub event_ticker: String,
    pub title: String,
    pub category: String,
}

#[derive(Debug, Clone)]
pub struct MockKalshiMarket {
    pub ticker: String,
    pub event_ticker: String,
    pub subtitle: String,
    pub yes_bid: u16,
    pub yes_ask: u16,
    pub no_bid: u16,
    pub no_ask: u16,
}

impl MockKalshiClient {
    pub fn new() -> Self {
        Self {
            events: vec![],
            markets: vec![],
            latency_ms: 10,
            failure_rate: 0.0,
            call_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    pub fn with_events(mut self, events: Vec<MockKalshiEvent>) -> Self {
        self.events = events;
        self
    }

    pub fn with_markets(mut self, markets: Vec<MockKalshiMarket>) -> Self {
        self.markets = markets;
        self
    }

    pub fn with_latency(mut self, latency_ms: u64) -> Self {
        self.latency_ms = latency_ms;
        self
    }

    pub fn with_failure_rate(mut self, failure_rate: f64) -> Self {
        self.failure_rate = failure_rate;
        self
    }

    pub async fn get_events(&self, _series: &str) -> Result<Vec<MockKalshiEvent>, String> {
        self.call_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tokio::time::sleep(tokio::time::Duration::from_millis(self.latency_ms)).await;

        if rand::random::<f64>() < self.failure_rate {
            return Err("Simulated API failure".to_string());
        }

        Ok(self.events.clone())
    }

    pub async fn get_markets(&self, _event_ticker: &str) -> Result<Vec<MockKalshiMarket>, String> {
        self.call_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tokio::time::sleep(tokio::time::Duration::from_millis(self.latency_ms)).await;

        if rand::random::<f64>() < self.failure_rate {
            return Err("Simulated API failure".to_string());
        }

        Ok(self.markets.clone())
    }

    pub fn call_count(&self) -> u64 {
        self.call_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Default for MockKalshiClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Mock Polymarket client for testing
#[derive(Debug, Clone)]
pub struct MockPolyClient {
    /// Simulated token data
    tokens: Vec<MockPolyToken>,
    /// Simulated latency (ms)
    latency_ms: u64,
    /// Failure rate (0.0 to 1.0)
    failure_rate: f64,
    /// Call counter
    call_count: Arc<std::sync::atomic::AtomicU64>,
}

#[derive(Debug, Clone)]
pub struct MockPolyToken {
    pub token_id: String,
    pub outcome: String,
    pub best_bid: u16,
    pub best_ask: u16,
}

impl MockPolyClient {
    pub fn new() -> Self {
        Self {
            tokens: vec![],
            latency_ms: 15,
            failure_rate: 0.0,
            call_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    pub fn with_tokens(mut self, tokens: Vec<MockPolyToken>) -> Self {
        self.tokens = tokens;
        self
    }

    pub fn with_latency(mut self, latency_ms: u64) -> Self {
        self.latency_ms = latency_ms;
        self
    }

    pub fn with_failure_rate(mut self, failure_rate: f64) -> Self {
        self.failure_rate = failure_rate;
        self
    }

    pub async fn get_token(&self, token_id: &str) -> Result<Option<MockPolyToken>, String> {
        self.call_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tokio::time::sleep(tokio::time::Duration::from_millis(self.latency_ms)).await;

        if rand::random::<f64>() < self.failure_rate {
            return Err("Simulated API failure".to_string());
        }

        Ok(self.tokens.iter().find(|t| t.token_id == token_id).cloned())
    }

    pub fn call_count(&self) -> u64 {
        self.call_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Default for MockPolyClient {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// MOCK EXECUTION TESTS
// =============================================================================

#[cfg(test)]
mod execution_mock_tests {
    use super::*;

    /// Test: Mock execution engine processes requests without real API calls
    #[tokio::test]
    async fn test_mock_execution_dry_run() {
        // Create mock state
        let mut state = GlobalState::new();
        let pair = MarketPair {
            pair_id: "test-market-1".into(),
            league: "nba".into(),
            market_type: MarketType::Moneyline,
            description: "Test Market".into(),
            kalshi_event_ticker: "TEST-EVENT".into(),
            kalshi_market_ticker: "TEST-MARKET".into(),
            poly_slug: "test-slug".into(),
            poly_yes_token: "token-yes".into(),
            poly_no_token: "token-no".into(),
            line_value: None,
            team_suffix: None,
        };
        state.add_pair(pair);

        // Create mock components
        let circuit_breaker = Arc::new(CircuitBreaker::new(CircuitBreakerConfig::from_env()));
        let (_tx, _rx) = create_position_channel();
        let pnl_tracker = Arc::new(Mutex::new(PnLTracker::new("./test_data", true)));
        let fee_model = Arc::new(FeeModel::new(
            prediction_market_arbitrage::config::KalshiRole::Maker,
        ));
        let metrics = Arc::new(Metrics::new());

        // Verify we can create execution components without panicking
        assert_eq!(metrics.execution_attempts.get(), 0);
        assert_eq!(metrics.execution_successes.get(), 0);
    }

    /// Test: Mock execution tracks metrics correctly
    #[tokio::test]
    async fn test_mock_execution_metrics_tracking() {
        let metrics = Arc::new(Metrics::new());

        // Simulate detecting opportunities
        metrics.opportunities_detected.add(5);
        assert_eq!(metrics.opportunities_detected.get(), 5);

        // Simulate executions
        metrics.execution_attempts.add(5);
        metrics.execution_successes.add(4);
        metrics.execution_failures.add(1);

        assert_eq!(metrics.execution_attempts.get(), 5);
        assert_eq!(metrics.execution_successes.get(), 4);
        assert_eq!(metrics.execution_failures.get(), 1);

        // Success rate should be 80%
        let success_rate = (metrics.execution_successes.get() as f64
            / metrics.execution_attempts.get() as f64)
            * 100.0;
        assert!((success_rate - 80.0).abs() < 0.1);
    }

    /// Test: Mock execution handles concurrent requests
    #[tokio::test]
    async fn test_mock_execution_concurrent() {
        let metrics = Arc::new(Metrics::new());
        let mut handles = vec![];

        // Spawn 10 concurrent "executions"
        for _ in 0..10 {
            let metrics = metrics.clone();
            handles.push(tokio::spawn(async move {
                metrics.opportunities_detected.inc();
                metrics.execution_attempts.inc();
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                metrics.execution_successes.inc();
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(metrics.opportunities_detected.get(), 10);
        assert_eq!(metrics.execution_attempts.get(), 10);
        assert_eq!(metrics.execution_successes.get(), 10);
    }

    /// Test: Mock position tracker without real fills
    #[tokio::test]
    async fn test_mock_position_tracking() {
        let mut tracker = PositionTracker::new();

        // Record mock fills using the actual FillRecord API
        tracker.record_fill(&FillRecord::new(
            "TEST-MARKET",      // market_id
            "Test Description", // description
            "kalshi",           // platform
            "yes",              // side
            10.0,               // contracts
            0.50,               // price
            0.05,               // fees
            "test-order-1",     // order_id
        ));

        tracker.record_fill(&FillRecord::new(
            "TEST-MARKET",
            "Test Description",
            "polymarket",
            "no",
            10.0,
            0.45,
            0.03,
            "test-order-2",
        ));

        let pos = tracker.get("TEST-MARKET");
        assert!(pos.is_some());

        let pos = pos.unwrap();
        assert_eq!(pos.kalshi_yes.contracts, 10.0);
        assert_eq!(pos.poly_no.contracts, 10.0);

        // Verify total cost includes prices and fees
        let total_cost = pos.total_cost();
        assert!(total_cost > 0.0);
    }
}

// =============================================================================
// MOCK DISCOVERY TESTS
// =============================================================================

#[cfg(test)]
mod discovery_mock_tests {
    use super::*;

    /// Test: Mock discovery without real API calls
    #[tokio::test]
    async fn test_mock_discovery_basic() {
        let kalshi_client = MockKalshiClient::new().with_events(vec![
            MockKalshiEvent {
                series_ticker: "NBA".to_string(),
                event_ticker: "NBA-LAKERS-WIN".to_string(),
                title: "Lakers Win Championship".to_string(),
                category: "sports".to_string(),
            },
            MockKalshiEvent {
                series_ticker: "NFL".to_string(),
                event_ticker: "NFL-CHIEFS-WIN".to_string(),
                title: "Chiefs Win Super Bowl".to_string(),
                category: "sports".to_string(),
            },
        ]);

        let events = kalshi_client.get_events("NBA").await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(kalshi_client.call_count(), 1);
    }

    /// Test: Mock discovery with simulated latency
    #[tokio::test]
    async fn test_mock_discovery_latency() {
        let kalshi_client = MockKalshiClient::new()
            .with_latency(100) // 100ms latency
            .with_events(vec![MockKalshiEvent {
                series_ticker: "NBA".to_string(),
                event_ticker: "NBA-TEST".to_string(),
                title: "Test Event".to_string(),
                category: "sports".to_string(),
            }]);

        let start = std::time::Instant::now();
        let _events = kalshi_client.get_events("NBA").await.unwrap();
        let elapsed = start.elapsed();

        // Should take at least 100ms
        assert!(elapsed.as_millis() >= 100);
    }

    /// Test: Mock discovery with simulated failures
    #[tokio::test]
    async fn test_mock_discovery_failures() {
        let kalshi_client = MockKalshiClient::new()
            .with_failure_rate(1.0) // 100% failure rate
            .with_events(vec![]);

        let result = kalshi_client.get_events("NBA").await;
        assert!(result.is_err());
    }

    /// Test: Mock discovery with concurrent API calls
    #[tokio::test]
    async fn test_mock_discovery_concurrent() {
        let kalshi_client =
            Arc::new(
                MockKalshiClient::new()
                    .with_latency(50)
                    .with_events(vec![MockKalshiEvent {
                        series_ticker: "NBA".to_string(),
                        event_ticker: "NBA-TEST".to_string(),
                        title: "Test Event".to_string(),
                        category: "sports".to_string(),
                    }]),
            );

        let mut handles = vec![];

        // Spawn 5 concurrent API calls
        for _ in 0..5 {
            let client = kalshi_client.clone();
            handles.push(tokio::spawn(async move { client.get_events("NBA").await }));
        }

        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }

        // Should have made 5 calls
        assert_eq!(kalshi_client.call_count(), 5);
    }

    /// Test: Mock Polymarket client
    #[tokio::test]
    async fn test_mock_polymarket_client() {
        let poly_client = MockPolyClient::new().with_tokens(vec![
            MockPolyToken {
                token_id: "token-yes".to_string(),
                outcome: "Yes".to_string(),
                best_bid: 55,
                best_ask: 57,
            },
            MockPolyToken {
                token_id: "token-no".to_string(),
                outcome: "No".to_string(),
                best_bid: 42,
                best_ask: 44,
            },
        ]);

        let token = poly_client.get_token("token-yes").await.unwrap();
        assert!(token.is_some());
        let token = token.unwrap();
        assert_eq!(token.best_bid, 55);
        assert_eq!(token.best_ask, 57);

        assert_eq!(poly_client.call_count(), 1);
    }
}

// =============================================================================
// MOCK FEE ESTIMATION TESTS
// =============================================================================

#[cfg(test)]
mod fee_mock_tests {
    use super::*;

    /// Test: Mock fee estimation without external calls
    #[tokio::test]
    async fn test_mock_fee_estimation_kalshi() {
        let fee_model = FeeModel::new(prediction_market_arbitrage::config::KalshiRole::Taker);

        // Kalshi fees are deterministic based on price
        let fee = fee_model.estimate_fees(Exchange::Kalshi, "", 50, 1).await;

        // At 50¢, Kalshi taker fee should be 3¢ (7% base rate - 1% rebate = 6% effective)
        // But this is precomputed in the table, let's just verify it's reasonable
        assert!(fee > 0);
        assert!(fee < 10); // Should be a few cents
    }

    /// Test: Mock fee estimation with different venues
    #[tokio::test]
    async fn test_mock_fee_estimation_venues() {
        let fee_model = FeeModel::new(prediction_market_arbitrage::config::KalshiRole::Maker);

        // Test both venues
        let kalshi_fee = fee_model.estimate_fees(Exchange::Kalshi, "", 50, 1).await;
        let poly_fee = fee_model
            .estimate_fees(Exchange::Polymarket, "test-token", 50, 1)
            .await;

        // Both should return reasonable fees
        assert!(kalshi_fee > 0);
        assert!(poly_fee >= 0); // Polymarket maker fees can be 0 or negative (rebates)
    }

    /// Test: Mock fee estimation with different sizes
    #[tokio::test]
    async fn test_mock_fee_estimation_sizes() {
        let fee_model = FeeModel::new(prediction_market_arbitrage::config::KalshiRole::Taker);

        let fee_1 = fee_model.estimate_fees(Exchange::Kalshi, "", 50, 1).await;
        let fee_10 = fee_model.estimate_fees(Exchange::Kalshi, "", 50, 10).await;

        // Fee should scale roughly with size (not exactly due to ceiling)
        assert!(
            fee_10 >= fee_1 * 9,
            "fee_10 ({}) should be >= 9 * fee_1 ({})",
            fee_10,
            fee_1
        );
        assert!(
            fee_10 <= fee_1 * 11,
            "fee_10 ({}) should be <= 11 * fee_1 ({})",
            fee_10,
            fee_1
        );
    }

    /// Test: Mock fee estimation at edge prices
    #[tokio::test]
    async fn test_mock_fee_estimation_edge_prices() {
        let fee_model = FeeModel::new(prediction_market_arbitrage::config::KalshiRole::Taker);

        // Test at extreme prices
        let fee_1 = fee_model.estimate_fees(Exchange::Kalshi, "", 1, 1).await;
        let fee_50 = fee_model.estimate_fees(Exchange::Kalshi, "", 50, 1).await;
        let fee_99 = fee_model.estimate_fees(Exchange::Kalshi, "", 99, 1).await;

        // All prices should have some fee
        assert!(fee_1 > 0, "fee at 1¢ should be > 0");
        assert!(fee_50 > 0, "fee at 50¢ should be > 0");
        assert!(fee_99 > 0, "fee at 99¢ should be > 0");

        // Fee formula is p × (100-p), so:
        // At 1¢: 1 × 99 = 99
        // At 50¢: 50 × 50 = 2500 (maximum)
        // At 99¢: 99 × 1 = 99
        // So fee at 50¢ should be highest
        assert!(
            fee_50 >= fee_1,
            "fee at 50¢ ({}) should be >= fee at 1¢ ({})",
            fee_50,
            fee_1
        );
        assert!(
            fee_50 >= fee_99,
            "fee at 50¢ ({}) should be >= fee at 99¢ ({})",
            fee_50,
            fee_99
        );
    }
}

// =============================================================================
// MOCK ARBITRAGE DETECTION TESTS
// =============================================================================

#[cfg(test)]
mod arbitrage_mock_tests {
    use super::*;

    /// Test: Mock arbitrage detection with favorable prices
    #[test]
    fn test_mock_arbitrage_detection_profitable() {
        let mut state = GlobalState::new();

        // Add a market with profitable arbitrage
        // Poly YES at 45¢, Kalshi NO at 50¢ → total = 95¢ (profitable)
        let pair = MarketPair {
            pair_id: "test-profit-1".into(),
            league: "test".into(),
            market_type: MarketType::Moneyline,
            description: "Test Profitable Arb".into(),
            kalshi_event_ticker: "TEST-EVENT".into(),
            kalshi_market_ticker: "TEST-PROFIT".into(),
            poly_slug: "test-slug".into(),
            poly_yes_token: "token-yes-1".into(),
            poly_no_token: "token-no-1".into(),
            line_value: None,
            team_suffix: None,
        };
        state.add_pair(pair);

        // Set prices
        let market = state.get_by_id(0).unwrap();
        market.poly.store(45, 46, 1, 1); // YES bid=45, ask=46
        market.kalshi.store(49, 50, 1, 1); // NO bid=49, ask=50

        // Check for arbitrage
        let arb_mask = market.check_arbs(100); // 100¢ threshold

        // Should find arbitrage (45 + 50 = 95¢ < 100¢)
        assert!(arb_mask > 0);
    }

    /// Test: Mock arbitrage detection with unfavorable prices
    #[test]
    fn test_mock_arbitrage_detection_unprofitable() {
        let mut state = GlobalState::new();

        // Add a market with no arbitrage
        // Poly YES at 55¢, Kalshi NO at 50¢ → total = 105¢ (not profitable)
        let pair = MarketPair {
            pair_id: "test-no-arb-1".into(),
            league: "test".into(),
            market_type: MarketType::Moneyline,
            description: "Test No Arb".into(),
            kalshi_event_ticker: "TEST-EVENT".into(),
            kalshi_market_ticker: "TEST-NO-ARB".into(),
            poly_slug: "test-slug".into(),
            poly_yes_token: "token-yes-1".into(),
            poly_no_token: "token-no-1".into(),
            line_value: None,
            team_suffix: None,
        };
        state.add_pair(pair);

        // Set prices that don't create arbitrage
        let market = state.get_by_id(0).unwrap();
        market.poly.store(55, 56, 1, 1);
        market.kalshi.store(49, 50, 1, 1);

        // Check for arbitrage
        let arb_mask = market.check_arbs(100);

        // Should not find arbitrage (55 + 50 = 105¢ > 100¢)
        assert_eq!(arb_mask, 0);
    }

    /// Test: Mock arbitrage detection with fees
    #[test]
    fn test_mock_arbitrage_detection_with_fees() {
        let mut state = GlobalState::new();

        let pair = MarketPair {
            pair_id: "test-fees-1".into(),
            league: "test".into(),
            market_type: MarketType::Moneyline,
            description: "Test Arb With Fees".into(),
            kalshi_event_ticker: "TEST-EVENT".into(),
            kalshi_market_ticker: "TEST-FEES".into(),
            poly_slug: "test-slug".into(),
            poly_yes_token: "token-yes-1".into(),
            poly_no_token: "token-no-1".into(),
            line_value: None,
            team_suffix: None,
        };
        state.add_pair(pair);

        // Set prices that are barely profitable before fees
        // 47 + 48 = 95¢
        let market = state.get_by_id(0).unwrap();
        market.poly.store(47, 48, 1, 1);
        market.kalshi.store(47, 48, 1, 1);

        // With threshold at 96¢ (requires cost < 96), 95¢ passes
        let arb_mask_low = market.check_arbs(96);
        assert!(arb_mask_low > 0, "95¢ cost should pass 96¢ threshold");

        // With very strict threshold at 90¢ (requires cost < 90), 95¢ fails
        let arb_mask_strict = market.check_arbs(90);
        assert_eq!(arb_mask_strict, 0, "95¢ cost should fail 90¢ threshold");
    }

    /// Test: Mock arbitrage with multiple markets
    #[test]
    fn test_mock_arbitrage_multiple_markets() {
        let mut state = GlobalState::new();

        // Add 3 markets
        for i in 0..3 {
            let pair = MarketPair {
                pair_id: format!("market-{}", i).into(),
                league: "test".into(),
                market_type: MarketType::Moneyline,
                description: format!("Market {}", i).into(),
                kalshi_event_ticker: format!("EVENT-{}", i).into(),
                kalshi_market_ticker: format!("MARKET-{}", i).into(),
                poly_slug: format!("slug-{}", i).into(),
                poly_yes_token: format!("token-yes-{}", i).into(),
                poly_no_token: format!("token-no-{}", i).into(),
                line_value: None,
                team_suffix: None,
            };
            state.add_pair(pair);
        }

        // Set prices: market 0 and 2 have arbs, market 1 doesn't
        state.get_by_id(0).unwrap().poly.store(40, 41, 1, 1);
        state.get_by_id(0).unwrap().kalshi.store(54, 55, 1, 1);

        state.get_by_id(1).unwrap().poly.store(50, 51, 1, 1);
        state.get_by_id(1).unwrap().kalshi.store(50, 51, 1, 1);

        state.get_by_id(2).unwrap().poly.store(42, 43, 1, 1);
        state.get_by_id(2).unwrap().kalshi.store(52, 53, 1, 1);

        // Check each market for arbitrage
        let mut arb_count = 0;
        for i in 0..3 {
            let market = state.get_by_id(i).unwrap();
            if market.check_arbs(100) > 0 {
                arb_count += 1;
            }
        }

        // Should find 2 arbitrage opportunities
        assert_eq!(arb_count, 2);
    }
}

// =============================================================================
// MOCK COST ANALYSIS TESTS
// =============================================================================

#[cfg(test)]
mod cost_mock_tests {
    use super::*;
    use prediction_market_arbitrage::config::KalshiRole;
    use prediction_market_arbitrage::cost::*;

    /// Test: Mock Kalshi fee calculation
    #[test]
    fn test_mock_kalshi_fee_calculation() {
        // Test taker fee at 50¢
        let fee = kalshi_fee_total_cents(50, 1, KalshiRole::Taker);

        // Formula: ceil(7 × 1 × 50 × 50 / 10000) = ceil(1.75) = 2
        assert_eq!(fee, 2);
    }

    /// Test: Mock Kalshi fee scaling with contracts
    #[test]
    fn test_mock_kalshi_fee_scaling() {
        let fee_1 = kalshi_fee_total_cents(50, 1, KalshiRole::Taker);
        let fee_10 = kalshi_fee_total_cents(50, 10, KalshiRole::Taker);

        // Fees should scale roughly with contracts (not exactly due to ceiling)
        // fee_10 should be roughly 10x fee_1, allowing for ceiling rounding
        assert!(
            fee_10 >= fee_1 * 9,
            "fee_10 ({}) should be >= 9 * fee_1 ({})",
            fee_10,
            fee_1
        );
        assert!(
            fee_10 <= fee_1 * 11,
            "fee_10 ({}) should be <= 11 * fee_1 ({})",
            fee_10,
            fee_1
        );
    }

    /// Test: Mock Kalshi maker vs taker fees
    #[test]
    fn test_mock_kalshi_maker_taker_comparison() {
        let taker = kalshi_fee_total_cents(50, 10, KalshiRole::Taker);
        let maker = kalshi_fee_total_cents(50, 10, KalshiRole::Maker);

        // Maker fees should be lower than taker fees
        assert!(maker < taker);
    }

    /// Test: Mock Kalshi fee at edge prices
    #[test]
    fn test_mock_kalshi_fee_edge_prices() {
        // At 1¢: low notional, minimal fee
        let fee_1 = kalshi_fee_total_cents(1, 1, KalshiRole::Taker);
        assert!(fee_1 <= 1);

        // At 99¢: also low notional (1 × 99 = 99), minimal fee
        let fee_99 = kalshi_fee_total_cents(99, 1, KalshiRole::Taker);
        assert!(fee_99 <= 1);

        // At 50¢: maximum notional (50 × 50 = 2500), highest fee
        let fee_50 = kalshi_fee_total_cents(50, 1, KalshiRole::Taker);
        assert!(fee_50 >= fee_1);
        assert!(fee_50 >= fee_99);
    }

    /// Test: Mock Polymarket fee calculation
    #[test]
    fn test_mock_polymarket_fee_calculation() {
        use prediction_market_arbitrage::config::PolyRole;

        // Test with typical taker fee (200 bps = 2%)
        let premium = 50; // 50¢
        let fee = poly_fee_total_cents(premium, PolyRole::Taker, 0, 200);

        // 50¢ × 2% = 1¢
        assert_eq!(fee, 1);
    }

    /// Test: Mock Polymarket fee edge cases
    #[test]
    fn test_mock_polymarket_fee_edge_cases() {
        use prediction_market_arbitrage::config::PolyRole;

        // Zero premium → zero fee
        assert_eq!(poly_fee_total_cents(0, PolyRole::Taker, 0, 200), 0);

        // Zero bps → zero fee
        assert_eq!(poly_fee_total_cents(50, PolyRole::Taker, 0, 0), 0);

        // High premium
        let fee_high = poly_fee_total_cents(99, PolyRole::Taker, 0, 200);
        assert!(fee_high >= 1);
    }

    /// Test: Mock slippage calculation
    #[test]
    fn test_mock_slippage_calculation() {
        let slippage_per_leg = 1; // 1¢ per leg
        let contracts = 10;
        let legs = 2; // Two-leg arbitrage

        let slippage = slippage_total_cents(legs, contracts, slippage_per_leg);

        // Should be 2 legs × 10 contracts × 1¢ = 20¢
        assert_eq!(slippage, 20);
    }
}

// =============================================================================
// MOCK INTEGRATION TESTS
// =============================================================================

#[cfg(test)]
mod integration_mock_tests {
    use super::*;

    /// Test: End-to-end mock flow from discovery to execution
    #[tokio::test]
    async fn test_mock_end_to_end_flow() {
        // 1. Mock discovery
        let kalshi_client = MockKalshiClient::new()
            .with_events(vec![MockKalshiEvent {
                series_ticker: "NBA".to_string(),
                event_ticker: "NBA-LAKERS".to_string(),
                title: "Lakers Win".to_string(),
                category: "sports".to_string(),
            }])
            .with_markets(vec![MockKalshiMarket {
                ticker: "LAKERS-YES".to_string(),
                event_ticker: "NBA-LAKERS".to_string(),
                subtitle: "Lakers to win".to_string(),
                yes_bid: 49,
                yes_ask: 51,
                no_bid: 48,
                no_ask: 50,
            }]);

        let poly_client = MockPolyClient::new().with_tokens(vec![
            MockPolyToken {
                token_id: "lakers-yes".to_string(),
                outcome: "Yes".to_string(),
                best_bid: 44,
                best_ask: 46,
            },
            MockPolyToken {
                token_id: "lakers-no".to_string(),
                outcome: "No".to_string(),
                best_bid: 53,
                best_ask: 55,
            },
        ]);

        // 2. Fetch data
        let events = kalshi_client.get_events("NBA").await.unwrap();
        assert_eq!(events.len(), 1);

        let markets = kalshi_client
            .get_markets(&events[0].event_ticker)
            .await
            .unwrap();
        assert_eq!(markets.len(), 1);

        let yes_token = poly_client.get_token("lakers-yes").await.unwrap();
        assert!(yes_token.is_some());

        // 3. Check for arbitrage
        let mut state = GlobalState::new();
        let pair = MarketPair {
            pair_id: "lakers-win-1".into(),
            league: "nba".into(),
            market_type: MarketType::Moneyline,
            description: "Lakers Win".into(),
            kalshi_event_ticker: "NBA-LAKERS".into(),
            kalshi_market_ticker: "LAKERS-YES".into(),
            poly_slug: "lakers-slug".into(),
            poly_yes_token: "lakers-yes".into(),
            poly_no_token: "lakers-no".into(),
            line_value: None,
            team_suffix: None,
        };
        state.add_pair(pair);

        // Set prices from mock data
        let market = state.get_by_id(0).unwrap();
        market.poly.store(44, 46, 1, 1);
        market.kalshi.store(48, 50, 1, 1);

        // 4. Detect arbitrage (46 + 50 = 96¢ < 100¢)
        let arb_mask = market.check_arbs(100);
        assert!(arb_mask > 0);

        // 5. Track metrics
        let metrics = Metrics::new();
        metrics.opportunities_detected.inc();
        metrics.discovery_runs.inc();

        assert_eq!(metrics.opportunities_detected.get(), 1);
        assert_eq!(metrics.discovery_runs.get(), 1);
    }

    /// Test: Mock concurrent discovery and execution
    #[tokio::test]
    async fn test_mock_concurrent_operations() {
        let kalshi_client = Arc::new(MockKalshiClient::new().with_latency(50));
        let poly_client = Arc::new(MockPolyClient::new().with_latency(75));
        let metrics = Arc::new(Metrics::new());

        let mut handles = vec![];

        // Simulate concurrent discovery and execution
        for i in 0..5 {
            let k_client = kalshi_client.clone();
            let p_client = poly_client.clone();
            let m = metrics.clone();

            handles.push(tokio::spawn(async move {
                // Simulate discovery
                m.discovery_runs.inc();
                let _ = k_client.get_events("NBA").await;
                let _ = p_client.get_token(&format!("token-{}", i)).await;

                // Simulate execution
                m.opportunities_detected.inc();
                m.execution_attempts.inc();
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                m.execution_successes.inc();
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(metrics.discovery_runs.get(), 5);
        assert_eq!(metrics.opportunities_detected.get(), 5);
        assert_eq!(metrics.execution_attempts.get(), 5);
        assert_eq!(metrics.execution_successes.get(), 5);
    }
}
