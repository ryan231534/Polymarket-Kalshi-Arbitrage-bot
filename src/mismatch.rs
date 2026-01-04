//! Mismatch detection and deterministic unwind ladder for partial fills.
//!
//! When cross-venue or same-venue arb legs fill unequally, this module:
//! 1. Detects and classifies mismatch severity
//! 2. Builds a deterministic price ladder for unwinding
//! 3. Executes IOC/FAK orders to flatten leftover exposure
//!
//! ## Mismatch Classification
//! - `None`: No mismatch (fills matched)
//! - `Normal`: Mismatch <= MAX_MISMATCH_CONTRACTS, use standard ladder
//! - `Severe`: Mismatch > MAX_MISMATCH_CONTRACTS, use emergency ladder + trip breaker

use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info, warn};

use crate::circuit_breaker::CircuitBreaker;
use crate::kalshi::KalshiApiClient;
use crate::pnl::{ContractSide, PnLTracker, Side as PnLSide, Venue as PnLVenue};
use crate::polymarket_clob::SharedAsyncClient;
use crate::types::cents_to_price;

// === Core Types ===

/// Trading venue
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Venue {
    Kalshi,
    Polymarket,
}

impl From<Venue> for PnLVenue {
    fn from(v: Venue) -> Self {
        match v {
            Venue::Kalshi => PnLVenue::Kalshi,
            Venue::Polymarket => PnLVenue::Polymarket,
        }
    }
}

/// Contract side (YES or NO outcome)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Yes,
    No,
}

impl From<Side> for ContractSide {
    fn from(s: Side) -> Self {
        match s {
            Side::Yes => ContractSide::Yes,
            Side::No => ContractSide::No,
        }
    }
}

/// Direction of leftover exposure
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dir {
    /// Long exposure: need to SELL to flatten
    Long,
    /// Short exposure: need to BUY to cover
    #[allow(dead_code)]
    Short,
}

/// Leftover exposure after a partial fill
#[derive(Debug, Clone)]
pub struct Exposure {
    /// Which venue has the exposure
    pub venue: Venue,
    /// Market identifier (ticker for Kalshi, token_id for Poly)
    pub market_key: String,
    /// Contract side (YES or NO)
    pub side: Side,
    /// Direction of exposure (Long = need to sell, Short = need to buy)
    pub dir: Dir,
    /// Number of contracts to unwind
    pub qty: u32,
    /// Reference price in cents (typically the fill price of the overfilled leg)
    pub ref_price_cents: u16,
}

/// Mismatch severity classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MismatchSeverity {
    /// No mismatch
    None,
    /// Mismatch <= max threshold, use standard unwind
    Normal,
    /// Mismatch > max threshold, use emergency unwind + trip breaker
    Severe,
}

/// Result of an unwind operation
#[derive(Debug, Clone)]
pub struct UnwindResult {
    /// Initial quantity to unwind
    #[allow(dead_code)]
    pub initial_qty: u32,
    /// Quantity successfully filled
    pub filled_qty: u32,
    /// Quantity remaining after unwind
    pub remaining_qty: u32,
    /// Number of ladder steps attempted
    #[allow(dead_code)]
    pub steps_attempted: u32,
    /// Worst (most aggressive) price used
    pub worst_price_used: u16,
    /// Estimated total cost in cents (negative = proceeds from selling)
    #[allow(dead_code)]
    pub total_cost_cents: i64,
    /// Elapsed time in milliseconds
    pub elapsed_ms: u64,
}

/// Configuration for unwind operations
#[derive(Debug, Clone)]
pub struct UnwindConfig {
    pub max_steps: u32,
    pub step_cents: u16,
    pub panic_price_buy: u16,
    pub panic_price_sell: u16,
    pub backoff_ms: u64,
}

impl UnwindConfig {
    /// Create config from environment variables
    pub fn from_env() -> Self {
        Self {
            max_steps: crate::config::unwind_max_steps(),
            step_cents: crate::config::unwind_step_cents(),
            panic_price_buy: crate::config::unwind_panic_price_buy_cents(),
            panic_price_sell: crate::config::unwind_panic_price_sell_cents(),
            backoff_ms: crate::config::unwind_backoff_ms(),
        }
    }

    /// Create emergency config (more aggressive for severe mismatch)
    pub fn emergency_from_env() -> Self {
        let multiplier = crate::config::unwind_emergency_multiplier();
        let base = Self::from_env();
        Self {
            max_steps: base.max_steps.saturating_mul(multiplier),
            // Step size multiplied but clamped to avoid insane jumps
            step_cents: base.step_cents.saturating_mul(multiplier as u16).min(10),
            panic_price_buy: base.panic_price_buy,
            panic_price_sell: base.panic_price_sell,
            // Faster backoff for emergency
            backoff_ms: base.backoff_ms / 2,
        }
    }
}

// === Mismatch Classification ===

/// Classify mismatch severity
///
/// # Arguments
/// * `mismatch` - Absolute difference in filled contracts between legs
/// * `max_allowed` - Maximum mismatch before severe classification
///
/// # Returns
/// Severity level determining unwind strategy
pub fn classify_mismatch(mismatch: u32, max_allowed: u32) -> MismatchSeverity {
    if mismatch == 0 {
        MismatchSeverity::None
    } else if mismatch <= max_allowed {
        MismatchSeverity::Normal
    } else {
        MismatchSeverity::Severe
    }
}

// === Deterministic Ladder Price Generator ===

/// Build deterministic unwind price ladder.
///
/// For BUY to cover (Short exposure):
///   Start at ref_price + step, increase until panic_price (99)
///
/// For SELL to flatten (Long exposure):
///   Start at ref_price - step, decrease until panic_price (1)
///
/// Rules:
/// - No duplicates
/// - Always ends with panic_price
/// - Clamped to [1..99]
/// - Stable ordering (deterministic)
///
/// # Arguments
/// * `dir` - Direction of needed action (Long = sell, Short = buy)
/// * `ref_price` - Reference price in cents (typically last fill price)
/// * `step` - Price step size in cents
/// * `max_steps` - Maximum number of steps (not including panic)
/// * `panic_price` - Final aggressive price to guarantee fill
///
/// # Returns
/// Vec of prices in execution order
pub fn build_unwind_prices(
    dir: Dir,
    ref_price: u16,
    step: u16,
    max_steps: u32,
    panic_price: u16,
) -> Vec<u16> {
    let mut prices = Vec::with_capacity(max_steps as usize + 1);

    match dir {
        Dir::Short => {
            // Need to BUY: start above ref, go up to panic (99)
            for i in 1..=max_steps {
                let price = ref_price
                    .saturating_add(step.saturating_mul(i as u16))
                    .min(99);
                if prices.last() != Some(&price) {
                    prices.push(price);
                }
                // Stop early if we've hit the ceiling
                if price >= 99 {
                    break;
                }
            }
            // Always end with panic price
            if prices.last() != Some(&panic_price) {
                prices.push(panic_price);
            }
        }
        Dir::Long => {
            // Need to SELL: start below ref, go down to panic (1)
            for i in 1..=max_steps {
                let price = ref_price
                    .saturating_sub(step.saturating_mul(i as u16))
                    .max(1);
                if prices.last() != Some(&price) {
                    prices.push(price);
                }
                // Stop early if we've hit the floor
                if price <= 1 {
                    break;
                }
            }
            // Always end with panic price
            if prices.last() != Some(&panic_price) {
                prices.push(panic_price);
            }
        }
    }

    prices
}

// === Unwind Executors ===

/// Execute unwind on Kalshi using IOC orders
pub async fn unwind_kalshi(
    exposure: &Exposure,
    prices: &[u16],
    kalshi: &KalshiApiClient,
    cfg: &UnwindConfig,
    pnl_tracker: Option<&tokio::sync::Mutex<PnLTracker>>,
    market_key_for_pnl: &str,
) -> UnwindResult {
    let start = Instant::now();
    let mut remaining = exposure.qty;
    let mut total_filled = 0u32;
    let mut total_cost: i64 = 0;
    let mut steps_attempted = 0u32;
    let mut worst_price = 0u16;

    let side_str = match exposure.side {
        Side::Yes => "yes",
        Side::No => "no",
    };

    for (idx, &price) in prices.iter().enumerate() {
        if remaining == 0 {
            break;
        }

        steps_attempted = idx as u32 + 1;

        info!(
            event = "unwind_step",
            venue = "kalshi",
            step_idx = idx,
            price = price,
            attempted_qty = remaining,
            side = side_str,
            dir = ?exposure.dir,
            "Unwind ladder step"
        );

        let result = match exposure.dir {
            Dir::Long => {
                // Sell to flatten
                kalshi
                    .sell_ioc(
                        &exposure.market_key,
                        side_str,
                        price as i64,
                        remaining as i64,
                    )
                    .await
            }
            Dir::Short => {
                // Buy to cover
                kalshi
                    .buy_ioc(
                        &exposure.market_key,
                        side_str,
                        price as i64,
                        remaining as i64,
                    )
                    .await
            }
        };

        match result {
            Ok(resp) => {
                let filled = resp.order.filled_count() as u32;
                if filled > 0 {
                    let fill_cost = resp.order.taker_fill_cost.unwrap_or(0)
                        + resp.order.maker_fill_cost.unwrap_or(0);

                    // For sells, cost is negative (proceeds)
                    let signed_cost = match exposure.dir {
                        Dir::Long => -(fill_cost as i64), // Selling = proceeds
                        Dir::Short => fill_cost as i64,   // Buying = cost
                    };

                    total_filled += filled;
                    remaining = remaining.saturating_sub(filled);
                    total_cost += signed_cost;
                    worst_price = price;

                    // Record in P&L tracker
                    if let Some(pnl) = pnl_tracker {
                        let mut tracker = pnl.lock().await;
                        let pnl_side = match exposure.dir {
                            Dir::Long => PnLSide::Sell,
                            Dir::Short => PnLSide::Buy,
                        };
                        tracker.on_fill(
                            PnLVenue::Kalshi,
                            market_key_for_pnl,
                            &resp.order.order_id,
                            pnl_side,
                            exposure.side.into(),
                            filled as i64,
                            price as i64,
                            0, // Fee calculated separately
                        );
                    }

                    info!(
                        event = "unwind_step",
                        venue = "kalshi",
                        step_idx = idx,
                        price = price,
                        filled_qty = filled,
                        remaining_qty = remaining,
                        "Unwind step filled"
                    );
                }
            }
            Err(e) => {
                warn!(
                    event = "unwind_step_error",
                    venue = "kalshi",
                    step_idx = idx,
                    price = price,
                    error = %e,
                    "Unwind step failed"
                );
            }
        }

        // Backoff between steps
        if remaining > 0 && idx < prices.len() - 1 {
            tokio::time::sleep(tokio::time::Duration::from_millis(cfg.backoff_ms)).await;
        }
    }

    let elapsed_ms = start.elapsed().as_millis() as u64;

    UnwindResult {
        initial_qty: exposure.qty,
        filled_qty: total_filled,
        remaining_qty: remaining,
        steps_attempted,
        worst_price_used: worst_price,
        total_cost_cents: total_cost,
        elapsed_ms,
    }
}

/// Execute unwind on Polymarket using FAK orders
pub async fn unwind_poly(
    exposure: &Exposure,
    prices: &[u16],
    poly: &SharedAsyncClient,
    cfg: &UnwindConfig,
    pnl_tracker: Option<&tokio::sync::Mutex<PnLTracker>>,
    market_key_for_pnl: &str,
) -> UnwindResult {
    let start = Instant::now();
    let mut remaining = exposure.qty;
    let mut total_filled = 0u32;
    let mut total_cost: i64 = 0;
    let mut steps_attempted = 0u32;
    let mut worst_price = 0u16;

    for (idx, &price) in prices.iter().enumerate() {
        if remaining == 0 {
            break;
        }

        steps_attempted = idx as u32 + 1;
        let price_f64 = cents_to_price(price);

        info!(
            event = "unwind_step",
            venue = "polymarket",
            step_idx = idx,
            price = price,
            attempted_qty = remaining,
            dir = ?exposure.dir,
            "Unwind ladder step"
        );

        let result = match exposure.dir {
            Dir::Long => {
                // Sell to flatten
                poly.sell_fak(&exposure.market_key, price_f64, remaining as f64)
                    .await
            }
            Dir::Short => {
                // Buy to cover
                poly.buy_fak(&exposure.market_key, price_f64, remaining as f64)
                    .await
            }
        };

        match result {
            Ok(fill) => {
                let filled = fill.filled_size as u32;
                if filled > 0 {
                    let fill_cost = (fill.fill_cost * 100.0) as i64;

                    // For sells, cost is negative (proceeds)
                    let signed_cost = match exposure.dir {
                        Dir::Long => -fill_cost, // Selling = proceeds
                        Dir::Short => fill_cost, // Buying = cost
                    };

                    total_filled += filled;
                    remaining = remaining.saturating_sub(filled);
                    total_cost += signed_cost;
                    worst_price = price;

                    // Record in P&L tracker
                    if let Some(pnl) = pnl_tracker {
                        let mut tracker = pnl.lock().await;
                        let pnl_side = match exposure.dir {
                            Dir::Long => PnLSide::Sell,
                            Dir::Short => PnLSide::Buy,
                        };
                        tracker.on_fill(
                            PnLVenue::Polymarket,
                            market_key_for_pnl,
                            &fill.order_id,
                            pnl_side,
                            exposure.side.into(),
                            filled as i64,
                            price as i64,
                            0, // No trading fee on Poly
                        );
                    }

                    info!(
                        event = "unwind_step",
                        venue = "polymarket",
                        step_idx = idx,
                        price = price,
                        filled_qty = filled,
                        remaining_qty = remaining,
                        "Unwind step filled"
                    );
                }
            }
            Err(e) => {
                warn!(
                    event = "unwind_step_error",
                    venue = "polymarket",
                    step_idx = idx,
                    price = price,
                    error = %e,
                    "Unwind step failed"
                );
            }
        }

        // Backoff between steps
        if remaining > 0 && idx < prices.len() - 1 {
            tokio::time::sleep(tokio::time::Duration::from_millis(cfg.backoff_ms)).await;
        }
    }

    let elapsed_ms = start.elapsed().as_millis() as u64;

    UnwindResult {
        initial_qty: exposure.qty,
        filled_qty: total_filled,
        remaining_qty: remaining,
        steps_attempted,
        worst_price_used: worst_price,
        total_cost_cents: total_cost,
        elapsed_ms,
    }
}

/// Execute unwind based on venue
pub async fn execute_unwind(
    exposure: &Exposure,
    cfg: &UnwindConfig,
    kalshi: Option<&KalshiApiClient>,
    poly: Option<&SharedAsyncClient>,
    pnl_tracker: Option<&tokio::sync::Mutex<PnLTracker>>,
    market_key_for_pnl: &str,
) -> UnwindResult {
    let panic_price = match exposure.dir {
        Dir::Short => cfg.panic_price_buy,
        Dir::Long => cfg.panic_price_sell,
    };

    let prices = build_unwind_prices(
        exposure.dir,
        exposure.ref_price_cents,
        cfg.step_cents,
        cfg.max_steps,
        panic_price,
    );

    info!(
        event = "unwind_start",
        venue = ?exposure.venue,
        contract_side = ?exposure.side,
        dir = ?exposure.dir,
        qty = exposure.qty,
        ref_price = exposure.ref_price_cents,
        steps = prices.len(),
        step_cents = cfg.step_cents,
        "Starting unwind"
    );

    let result = match exposure.venue {
        Venue::Kalshi => {
            if let Some(k) = kalshi {
                unwind_kalshi(exposure, &prices, k, cfg, pnl_tracker, market_key_for_pnl).await
            } else {
                error!("Kalshi client not available for unwind");
                UnwindResult {
                    initial_qty: exposure.qty,
                    filled_qty: 0,
                    remaining_qty: exposure.qty,
                    steps_attempted: 0,
                    worst_price_used: 0,
                    total_cost_cents: 0,
                    elapsed_ms: 0,
                }
            }
        }
        Venue::Polymarket => {
            if let Some(p) = poly {
                unwind_poly(exposure, &prices, p, cfg, pnl_tracker, market_key_for_pnl).await
            } else {
                error!("Polymarket client not available for unwind");
                UnwindResult {
                    initial_qty: exposure.qty,
                    filled_qty: 0,
                    remaining_qty: exposure.qty,
                    steps_attempted: 0,
                    worst_price_used: 0,
                    total_cost_cents: 0,
                    elapsed_ms: 0,
                }
            }
        }
    };

    info!(
        event = "unwind_complete",
        venue = ?exposure.venue,
        filled_qty = result.filled_qty,
        remaining_qty = result.remaining_qty,
        worst_price_used = result.worst_price_used,
        elapsed_ms = result.elapsed_ms,
        "Unwind complete"
    );

    // Trip breaker if we couldn't flatten
    if result.remaining_qty > 0 {
        error!(
            event = "mismatch_persisted",
            venue = ?exposure.venue,
            remaining_qty = result.remaining_qty,
            "Failed to fully unwind exposure"
        );
    }

    result
}

/// Handle mismatch with full playbook including severity classification and breaker integration
pub async fn handle_mismatch(
    market_id: u16,
    arb_type: crate::types::ArbType,
    yes_filled: i64,
    no_filled: i64,
    yes_price: u16,
    no_price: u16,
    poly_yes_token: &str,
    poly_no_token: &str,
    kalshi_ticker: &str,
    kalshi: &Arc<KalshiApiClient>,
    poly: &Arc<SharedAsyncClient>,
    circuit_breaker: &Arc<CircuitBreaker>,
    pnl_tracker: &Arc<tokio::sync::Mutex<PnLTracker>>,
    market_key_for_pnl: &str,
) {
    use crate::types::ArbType;

    let mismatch = (yes_filled - no_filled).unsigned_abs() as u32;
    if mismatch == 0 {
        return;
    }

    let max_mismatch = crate::config::max_mismatch_contracts();
    let severity = classify_mismatch(mismatch, max_mismatch);

    info!(
        event = "mismatch_detected",
        market_id = market_id,
        arb_type = ?arb_type,
        intended_contracts = yes_filled.max(no_filled),
        leg_a_filled = yes_filled,
        leg_b_filled = no_filled,
        mismatch = mismatch,
        severity = ?severity,
        "Mismatch detected"
    );

    // Build exposure based on which leg overfilled
    let exposure = if yes_filled > no_filled {
        // YES leg overfilled - we're long YES, need to sell YES to flatten
        let (venue, market_key) = match arb_type {
            ArbType::PolyYesKalshiNo | ArbType::PolyOnly => {
                (Venue::Polymarket, poly_yes_token.to_string())
            }
            ArbType::KalshiYesPolyNo | ArbType::KalshiOnly => {
                (Venue::Kalshi, kalshi_ticker.to_string())
            }
        };
        Exposure {
            venue,
            market_key,
            side: Side::Yes,
            dir: Dir::Long, // Long = need to sell
            qty: mismatch,
            ref_price_cents: yes_price,
        }
    } else {
        // NO leg overfilled - we're long NO, need to sell NO to flatten
        let (venue, market_key) = match arb_type {
            ArbType::PolyYesKalshiNo | ArbType::KalshiOnly => {
                (Venue::Kalshi, kalshi_ticker.to_string())
            }
            ArbType::KalshiYesPolyNo | ArbType::PolyOnly => {
                (Venue::Polymarket, poly_no_token.to_string())
            }
        };
        Exposure {
            venue,
            market_key,
            side: Side::No,
            dir: Dir::Long, // Long = need to sell
            qty: mismatch,
            ref_price_cents: no_price,
        }
    };

    // Select config based on severity
    let cfg = match severity {
        MismatchSeverity::None => return,
        MismatchSeverity::Normal => UnwindConfig::from_env(),
        MismatchSeverity::Severe => {
            // Trip circuit breaker for severe mismatch
            warn!(
                event = "severe_mismatch_trip",
                market_id = market_id,
                mismatch = mismatch,
                max_allowed = max_mismatch,
                "Severe mismatch - tripping circuit breaker"
            );
            circuit_breaker.record_error().await;
            UnwindConfig::emergency_from_env()
        }
    };

    // Execute unwind
    let result = execute_unwind(
        &exposure,
        &cfg,
        Some(kalshi.as_ref()),
        Some(poly.as_ref()),
        Some(pnl_tracker.as_ref()),
        market_key_for_pnl,
    )
    .await;

    // If we failed to flatten, trip the breaker
    if result.remaining_qty > 0 {
        error!(
            event = "unwind_failed",
            market_id = market_id,
            remaining_qty = result.remaining_qty,
            "Failed to unwind - tripping circuit breaker"
        );
        circuit_breaker.record_error().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Mismatch Classification Tests
    // =========================================================================

    #[test]
    fn test_classify_mismatch_none() {
        assert_eq!(classify_mismatch(0, 1), MismatchSeverity::None);
        assert_eq!(classify_mismatch(0, 5), MismatchSeverity::None);
    }

    #[test]
    fn test_classify_mismatch_normal() {
        assert_eq!(classify_mismatch(1, 1), MismatchSeverity::Normal);
        assert_eq!(classify_mismatch(1, 5), MismatchSeverity::Normal);
        assert_eq!(classify_mismatch(5, 5), MismatchSeverity::Normal);
    }

    #[test]
    fn test_classify_mismatch_severe() {
        assert_eq!(classify_mismatch(2, 1), MismatchSeverity::Severe);
        assert_eq!(classify_mismatch(6, 5), MismatchSeverity::Severe);
        assert_eq!(classify_mismatch(100, 1), MismatchSeverity::Severe);
    }

    // =========================================================================
    // Ladder Price Generator Tests
    // =========================================================================

    #[test]
    fn test_build_unwind_prices_buy_ladder() {
        // Short exposure: need to BUY to cover
        // ref=50, step=2, 5 steps, panic=99
        let prices = build_unwind_prices(Dir::Short, 50, 2, 5, 99);

        // Should be: 52, 54, 56, 58, 60, 99
        assert!(prices.len() >= 2, "Should have at least 2 prices");
        assert_eq!(*prices.last().unwrap(), 99, "Must end with panic price 99");

        // Check ascending order
        for i in 1..prices.len() {
            assert!(prices[i] >= prices[i - 1], "Buy ladder must be ascending");
        }

        // Check no duplicates
        let mut seen = std::collections::HashSet::new();
        for p in &prices {
            assert!(seen.insert(*p), "No duplicates allowed: {}", p);
        }
    }

    #[test]
    fn test_build_unwind_prices_buy_clamps_to_99() {
        // Start near ceiling
        let prices = build_unwind_prices(Dir::Short, 95, 2, 10, 99);

        // All prices should be <= 99
        for p in &prices {
            assert!(*p <= 99, "Price {} exceeds 99", p);
        }
        assert_eq!(*prices.last().unwrap(), 99);
    }

    #[test]
    fn test_build_unwind_prices_buy_ends_with_panic() {
        let prices = build_unwind_prices(Dir::Short, 50, 1, 3, 99);
        assert_eq!(
            *prices.last().unwrap(),
            99,
            "Buy ladder must end with panic=99"
        );
    }

    #[test]
    fn test_build_unwind_prices_sell_ladder() {
        // Long exposure: need to SELL to flatten
        // ref=50, step=2, 5 steps, panic=1
        let prices = build_unwind_prices(Dir::Long, 50, 2, 5, 1);

        // Should be: 48, 46, 44, 42, 40, 1
        assert!(prices.len() >= 2, "Should have at least 2 prices");
        assert_eq!(*prices.last().unwrap(), 1, "Must end with panic price 1");

        // Check descending order
        for i in 1..prices.len() {
            assert!(prices[i] <= prices[i - 1], "Sell ladder must be descending");
        }

        // Check no duplicates
        let mut seen = std::collections::HashSet::new();
        for p in &prices {
            assert!(seen.insert(*p), "No duplicates allowed: {}", p);
        }
    }

    #[test]
    fn test_build_unwind_prices_sell_clamps_to_1() {
        // Start near floor
        let prices = build_unwind_prices(Dir::Long, 5, 2, 10, 1);

        // All prices should be >= 1
        for p in &prices {
            assert!(*p >= 1, "Price {} below 1", p);
        }
        assert_eq!(*prices.last().unwrap(), 1);
    }

    #[test]
    fn test_build_unwind_prices_sell_ends_with_panic() {
        let prices = build_unwind_prices(Dir::Long, 50, 1, 3, 1);
        assert_eq!(
            *prices.last().unwrap(),
            1,
            "Sell ladder must end with panic=1"
        );
    }

    #[test]
    fn test_build_unwind_prices_no_duplicates() {
        // Edge case: start at panic price
        let prices_buy = build_unwind_prices(Dir::Short, 99, 1, 5, 99);
        assert_eq!(prices_buy, vec![99], "Should only have panic price");

        let prices_sell = build_unwind_prices(Dir::Long, 1, 1, 5, 1);
        assert_eq!(prices_sell, vec![1], "Should only have panic price");
    }

    #[test]
    fn test_build_unwind_prices_stable_ordering() {
        // Verify deterministic output
        let prices1 = build_unwind_prices(Dir::Short, 50, 2, 5, 99);
        let prices2 = build_unwind_prices(Dir::Short, 50, 2, 5, 99);
        assert_eq!(prices1, prices2, "Ladder must be deterministic");

        let prices3 = build_unwind_prices(Dir::Long, 50, 2, 5, 1);
        let prices4 = build_unwind_prices(Dir::Long, 50, 2, 5, 1);
        assert_eq!(prices3, prices4, "Ladder must be deterministic");
    }

    #[test]
    fn test_build_unwind_prices_single_step() {
        let prices = build_unwind_prices(Dir::Short, 50, 10, 1, 99);
        // Should be: 60, 99
        assert!(prices.len() >= 1);
        assert_eq!(*prices.last().unwrap(), 99);

        let prices = build_unwind_prices(Dir::Long, 50, 10, 1, 1);
        // Should be: 40, 1
        assert!(prices.len() >= 1);
        assert_eq!(*prices.last().unwrap(), 1);
    }

    // =========================================================================
    // UnwindConfig Tests
    // =========================================================================

    #[test]
    fn test_unwind_config_default() {
        // Can't test from_env reliably due to caching, but we can verify struct works
        let cfg = UnwindConfig {
            max_steps: 5,
            step_cents: 1,
            panic_price_buy: 99,
            panic_price_sell: 1,
            backoff_ms: 25,
        };
        assert_eq!(cfg.max_steps, 5);
        assert_eq!(cfg.step_cents, 1);
    }

    // =========================================================================
    // Exposure Tests
    // =========================================================================

    #[test]
    fn test_exposure_construction() {
        let exp = Exposure {
            venue: Venue::Kalshi,
            market_key: "KXTEST-YES".to_string(),
            side: Side::Yes,
            dir: Dir::Long,
            qty: 10,
            ref_price_cents: 50,
        };
        assert_eq!(exp.venue, Venue::Kalshi);
        assert_eq!(exp.qty, 10);
        assert_eq!(exp.ref_price_cents, 50);
    }
}
