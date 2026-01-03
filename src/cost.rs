//! All-in cost model for arbitrage trades.
//!
//! This module provides accurate fee calculations for both Kalshi and Polymarket,
//! slippage estimation, and expected profit calculations using integer cents (no floats).
//!
//! ## Fee Formulas
//!
//! ### Kalshi
//! - Taker: ceil(7 × C × p × (100-p) / 10000) cents
//! - Maker: ceil(7 × C × p × (100-p) / 40000) cents
//!
//! Where C = contracts, p = price in cents [1..99]
//!
//! ### Polymarket
//! - Fee = ceil(premium_cents × bps / 10000) cents
//!
//! ## Slippage
//! - Slippage = legs × contracts × slippage_per_leg cents
//!
//! TODO: Polymarket tick sizes can be < 1 cent and would require Decimal/bps pricing
//! for full fidelity. Current implementation uses cents as worst-case tick rounding.

use crate::config::{KalshiRole, PolyRole};
use crate::types::{ArbType, FastExecutionRequest};

/// Configuration for all-in cost calculations
#[derive(Debug, Clone, Copy)]
pub struct AllInCfg {
    /// Kalshi fee role (taker or maker)
    pub kalshi_role: KalshiRole,
    /// Polymarket fee role (taker or maker)
    pub poly_role: PolyRole,
    /// Polymarket maker fee in basis points
    pub poly_maker_bps: u16,
    /// Polymarket taker fee in basis points
    pub poly_taker_bps: u16,
    /// Slippage allowance per leg in cents (includes tick rounding)
    pub slippage_cents_per_leg: u16,
}

impl AllInCfg {
    /// Create config from environment variables
    pub fn from_env() -> Self {
        Self {
            kalshi_role: crate::config::kalshi_fee_role(),
            poly_role: PolyRole::Taker, // Default to taker for IOC/FAK orders
            poly_maker_bps: crate::config::poly_maker_fee_bps(),
            poly_taker_bps: crate::config::poly_taker_fee_bps(),
            slippage_cents_per_leg: crate::config::slippage_cents_per_leg(),
        }
    }
}

impl Default for AllInCfg {
    fn default() -> Self {
        Self {
            kalshi_role: KalshiRole::Taker,
            poly_role: PolyRole::Taker,
            poly_maker_bps: 0,
            poly_taker_bps: 0,
            slippage_cents_per_leg: 1,
        }
    }
}

/// Calculate total Kalshi fee for a trade in cents.
///
/// Uses exact ceil math for the formula:
/// - Taker: ceil(7 × C × p × (100-p) / 10000)
/// - Maker: ceil(7 × C × p × (100-p) / 40000)
///
/// This avoids per-contract rounding errors by computing the total first.
///
/// # Arguments
/// * `price_cents` - Price in cents [1..99]
/// * `contracts` - Number of contracts
/// * `role` - Taker or Maker
///
/// # Returns
/// Total fee in cents
#[inline]
pub fn kalshi_fee_total_cents(price_cents: u16, contracts: u32, role: KalshiRole) -> u32 {
    // Guard against invalid prices
    if price_cents == 0 || price_cents >= 100 {
        return 0;
    }

    let p = price_cents as u64;
    let c = contracts as u64;

    // Numerator: 7 × C × p × (100-p)
    let numerator = 7 * c * p * (100 - p);

    // Denominator depends on role
    let denom = match role {
        KalshiRole::Taker => 10_000u64,
        KalshiRole::Maker => 40_000u64, // 4x lower fee for makers
    };

    // Ceiling division: (numerator + denom - 1) / denom
    ((numerator + denom - 1) / denom) as u32
}

/// Calculate total Polymarket fee for a trade in cents.
///
/// Fee = ceil(premium_cents × bps / 10000)
///
/// # Arguments
/// * `premium_cents` - Total premium in cents (price × contracts)
/// * `role` - Taker or Maker
/// * `maker_bps` - Maker fee in basis points
/// * `taker_bps` - Taker fee in basis points
///
/// # Returns
/// Total fee in cents
#[inline]
pub fn poly_fee_total_cents(premium_cents: u32, role: PolyRole, maker_bps: u16, taker_bps: u16) -> u32 {
    let bps = match role {
        PolyRole::Maker => maker_bps,
        PolyRole::Taker => taker_bps,
    };

    if bps == 0 {
        return 0;
    }

    let numerator = premium_cents as u64 * bps as u64;
    // Ceiling division
    ((numerator + 9999) / 10000) as u32
}

/// Calculate total slippage allowance in cents.
///
/// Slippage = legs × contracts × slippage_per_leg
///
/// # Arguments
/// * `legs` - Number of legs (typically 2 for arb trades)
/// * `contracts` - Number of contracts
/// * `slippage_cents_per_leg` - Slippage allowance per leg in cents
///
/// # Returns
/// Total slippage in cents
#[inline]
pub fn slippage_total_cents(legs: u32, contracts: u32, slippage_cents_per_leg: u16) -> u32 {
    legs * contracts * slippage_cents_per_leg as u32
}

/// Calculate the all-in total cost for an arbitrage trade in cents.
///
/// Cost = sum(premiums) + sum(fees) + slippage
///
/// For each leg:
/// - Premium = price_cents × contracts
/// - Kalshi legs: apply kalshi_fee_total_cents
/// - Poly legs: apply poly_fee_total_cents
///
/// All arb types have 2 legs:
/// - Cross-venue: 1 Kalshi + 1 Poly
/// - Same-venue: 2 legs on same platform
///
/// # Arguments
/// * `req` - The execution request with prices and arb type
/// * `contracts` - Number of contracts to trade
/// * `cfg` - All-in cost configuration
///
/// # Returns
/// Total all-in cost in cents
#[inline]
pub fn all_in_cost_total_cents(req: &FastExecutionRequest, contracts: u32, cfg: &AllInCfg) -> u32 {
    let yes_premium = req.yes_price as u32 * contracts;
    let no_premium = req.no_price as u32 * contracts;
    let total_premium = yes_premium + no_premium;

    // Calculate fees based on arb type
    let fees = match req.arb_type {
        ArbType::PolyYesKalshiNo => {
            // Poly YES leg + Kalshi NO leg
            let poly_fee = poly_fee_total_cents(yes_premium, cfg.poly_role, cfg.poly_maker_bps, cfg.poly_taker_bps);
            let kalshi_fee = kalshi_fee_total_cents(req.no_price, contracts, cfg.kalshi_role);
            poly_fee + kalshi_fee
        }
        ArbType::KalshiYesPolyNo => {
            // Kalshi YES leg + Poly NO leg
            let kalshi_fee = kalshi_fee_total_cents(req.yes_price, contracts, cfg.kalshi_role);
            let poly_fee = poly_fee_total_cents(no_premium, cfg.poly_role, cfg.poly_maker_bps, cfg.poly_taker_bps);
            kalshi_fee + poly_fee
        }
        ArbType::PolyOnly => {
            // Both legs on Polymarket
            let yes_fee = poly_fee_total_cents(yes_premium, cfg.poly_role, cfg.poly_maker_bps, cfg.poly_taker_bps);
            let no_fee = poly_fee_total_cents(no_premium, cfg.poly_role, cfg.poly_maker_bps, cfg.poly_taker_bps);
            yes_fee + no_fee
        }
        ArbType::KalshiOnly => {
            // Both legs on Kalshi
            let yes_fee = kalshi_fee_total_cents(req.yes_price, contracts, cfg.kalshi_role);
            let no_fee = kalshi_fee_total_cents(req.no_price, contracts, cfg.kalshi_role);
            yes_fee + no_fee
        }
    };

    // All arb types have 2 legs
    let slippage = slippage_total_cents(2, contracts, cfg.slippage_cents_per_leg);

    total_premium + fees + slippage
}

/// Calculate expected profit for an arbitrage trade in cents.
///
/// Profit = payout - all_in_cost
/// Payout = 100 × contracts (one side wins, pays $1 per contract)
///
/// # Arguments
/// * `req` - The execution request
/// * `contracts` - Number of contracts
/// * `cfg` - All-in cost configuration
///
/// # Returns
/// Expected profit in cents (can be negative)
#[inline]
pub fn expected_profit_total_cents(req: &FastExecutionRequest, contracts: u32, cfg: &AllInCfg) -> i32 {
    let payout = 100 * contracts;
    let cost = all_in_cost_total_cents(req, contracts, cfg);
    payout as i32 - cost as i32
}

/// Result of all-in cost analysis for logging
#[derive(Debug, Clone)]
pub struct AllInAnalysis {
    pub contracts: u32,
    pub all_in_cost_total: u32,
    pub cost_per_contract: u32,
    pub profit_total: i32,
    pub profit_per_contract: i32,
    pub exec_threshold_cents: u16,
    pub min_profit_cents: u16,
    pub passes_threshold: bool,
    pub passes_min_profit: bool,
}

/// Analyze an execution request against all-in cost thresholds.
///
/// # Arguments
/// * `req` - The execution request
/// * `contracts` - Number of contracts
/// * `cfg` - All-in cost configuration
/// * `exec_threshold_cents` - Maximum cost per contract to execute
/// * `min_profit_cents` - Minimum profit per contract to execute
///
/// # Returns
/// Analysis result with pass/fail status
pub fn analyze_all_in(
    req: &FastExecutionRequest,
    contracts: u32,
    cfg: &AllInCfg,
    exec_threshold_cents: u16,
    min_profit_cents: u16,
) -> AllInAnalysis {
    let all_in_cost_total = all_in_cost_total_cents(req, contracts, cfg);
    let profit_total = expected_profit_total_cents(req, contracts, cfg);

    let cost_per_contract = if contracts > 0 {
        all_in_cost_total / contracts
    } else {
        0
    };

    let profit_per_contract = if contracts > 0 {
        profit_total / contracts as i32
    } else {
        0
    };

    let passes_threshold = cost_per_contract <= exec_threshold_cents as u32;
    let passes_min_profit = profit_total >= (min_profit_cents as i32 * contracts as i32);

    AllInAnalysis {
        contracts,
        all_in_cost_total,
        cost_per_contract,
        profit_total,
        profit_per_contract,
        exec_threshold_cents,
        min_profit_cents,
        passes_threshold,
        passes_min_profit,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Kalshi Fee Tests
    // =========================================================================

    #[test]
    fn test_kalshi_fee_taker_50c_1_contract() {
        // At p=50¢, formula: ceil(7 × 1 × 50 × 50 / 10000) = ceil(17500/10000) = ceil(1.75) = 2
        let fee = kalshi_fee_total_cents(50, 1, KalshiRole::Taker);
        assert_eq!(fee, 2, "Taker fee at 50¢ for 1 contract should be 2¢");
    }

    #[test]
    fn test_kalshi_fee_maker_50c_1_contract() {
        // At p=50¢, formula: ceil(7 × 1 × 50 × 50 / 40000) = ceil(17500/40000) = ceil(0.4375) = 1
        let fee = kalshi_fee_total_cents(50, 1, KalshiRole::Maker);
        assert_eq!(fee, 1, "Maker fee at 50¢ for 1 contract should be 1¢");
    }

    #[test]
    fn test_kalshi_fee_taker_vs_maker_4x_denom() {
        // Maker fee should always be <= taker fee (4x lower denominator means 4x lower fee)
        for p in 1..100u16 {
            let taker = kalshi_fee_total_cents(p, 10, KalshiRole::Taker);
            let maker = kalshi_fee_total_cents(p, 10, KalshiRole::Maker);
            assert!(
                maker <= taker,
                "Maker fee {} should be <= taker fee {} at price {}",
                maker, taker, p
            );
        }
    }

    #[test]
    fn test_kalshi_fee_taker_exact_ceil_math() {
        // Test exact ceil math at various prices
        // p=25: 7 × 1 × 25 × 75 / 10000 = 13125/10000 = 1.3125 → ceil = 2
        assert_eq!(kalshi_fee_total_cents(25, 1, KalshiRole::Taker), 2);

        // p=10: 7 × 1 × 10 × 90 / 10000 = 6300/10000 = 0.63 → ceil = 1
        assert_eq!(kalshi_fee_total_cents(10, 1, KalshiRole::Taker), 1);

        // p=90: 7 × 1 × 90 × 10 / 10000 = 6300/10000 = 0.63 → ceil = 1
        assert_eq!(kalshi_fee_total_cents(90, 1, KalshiRole::Taker), 1);
    }

    #[test]
    fn test_kalshi_fee_multiple_contracts() {
        // 10 contracts at 50¢: 7 × 10 × 50 × 50 / 10000 = 175000/10000 = 17.5 → ceil = 18
        let fee = kalshi_fee_total_cents(50, 10, KalshiRole::Taker);
        assert_eq!(fee, 18, "Taker fee at 50¢ for 10 contracts should be 18¢");
    }

    #[test]
    fn test_kalshi_fee_no_per_contract_rounding_errors() {
        // Total fee for 10 contracts should equal ceil(7×10×p×(100-p)/10000)
        // NOT 10 × ceil(7×1×p×(100-p)/10000)
        // At p=50: total = ceil(175000/10000) = 18
        // Per-contract approach would give 10 × 2 = 20
        let total = kalshi_fee_total_cents(50, 10, KalshiRole::Taker);
        let per_contract = kalshi_fee_total_cents(50, 1, KalshiRole::Taker);
        // Total should be 18, not 20
        assert_eq!(total, 18);
        assert_eq!(per_contract, 2);
        assert!(total < per_contract * 10, "Total fee should avoid per-contract rounding errors");
    }

    #[test]
    fn test_kalshi_fee_boundary_prices() {
        // Price 0 should return 0 (invalid)
        assert_eq!(kalshi_fee_total_cents(0, 10, KalshiRole::Taker), 0);
        // Price 100 should return 0 (invalid - no fee at certainty)
        assert_eq!(kalshi_fee_total_cents(100, 10, KalshiRole::Taker), 0);
        // Price 1: 7 × 1 × 1 × 99 / 10000 = 693/10000 = 0.0693 → ceil = 1
        assert_eq!(kalshi_fee_total_cents(1, 1, KalshiRole::Taker), 1);
        // Price 99: same as price 1 due to symmetry
        assert_eq!(kalshi_fee_total_cents(99, 1, KalshiRole::Taker), 1);
    }

    // =========================================================================
    // Polymarket Fee Tests
    // =========================================================================

    #[test]
    fn test_poly_fee_bps_calculation() {
        // premium=10000 cents ($100), bps=10 -> fee=ceil(10000*10/10000)=10 cents
        let fee = poly_fee_total_cents(10000, PolyRole::Taker, 0, 10);
        assert_eq!(fee, 10, "10 bps on $100 should be 10¢");
    }

    #[test]
    fn test_poly_fee_ceil_math() {
        // premium=100 cents, bps=10 -> 100*10/10000=0.1 -> ceil=1
        let fee = poly_fee_total_cents(100, PolyRole::Taker, 0, 10);
        assert_eq!(fee, 1, "Ceil(0.1) should be 1");
    }

    #[test]
    fn test_poly_fee_zero_bps() {
        // Zero bps should return zero fee
        let fee = poly_fee_total_cents(10000, PolyRole::Taker, 0, 0);
        assert_eq!(fee, 0, "Zero bps should return zero fee");
    }

    #[test]
    fn test_poly_fee_maker_vs_taker() {
        // Maker: 5 bps, Taker: 10 bps
        let maker_fee = poly_fee_total_cents(10000, PolyRole::Maker, 5, 10);
        let taker_fee = poly_fee_total_cents(10000, PolyRole::Taker, 5, 10);
        assert_eq!(maker_fee, 5, "Maker should use maker_bps=5");
        assert_eq!(taker_fee, 10, "Taker should use taker_bps=10");
    }

    // =========================================================================
    // Slippage Tests
    // =========================================================================

    #[test]
    fn test_slippage_calculation() {
        // 2 legs, 10 contracts, 1¢ per leg = 20¢
        let slip = slippage_total_cents(2, 10, 1);
        assert_eq!(slip, 20);
    }

    #[test]
    fn test_slippage_zero() {
        let slip = slippage_total_cents(2, 10, 0);
        assert_eq!(slip, 0);
    }

    // =========================================================================
    // Expected Profit Tests
    // =========================================================================

    fn make_req(yes_price: u16, no_price: u16, arb_type: ArbType) -> FastExecutionRequest {
        FastExecutionRequest {
            market_id: 0,
            yes_price,
            no_price,
            yes_size: 10000,
            no_size: 10000,
            arb_type,
            detected_ns: 0,
        }
    }

    #[test]
    fn test_expected_profit_no_fees_no_slippage() {
        // YES=40¢, NO=50¢ -> payout=100¢, cost=90¢, profit=10¢ per contract
        let req = make_req(40, 50, ArbType::PolyOnly);
        let cfg = AllInCfg {
            poly_maker_bps: 0,
            poly_taker_bps: 0,
            slippage_cents_per_leg: 0,
            ..Default::default()
        };
        let profit = expected_profit_total_cents(&req, 1, &cfg);
        assert_eq!(profit, 10, "Profit should be 100 - (40+50) = 10¢");
    }

    #[test]
    fn test_expected_profit_multiple_contracts() {
        // 10 contracts: payout=1000¢, cost=900¢, profit=100¢
        let req = make_req(40, 50, ArbType::PolyOnly);
        let cfg = AllInCfg {
            poly_maker_bps: 0,
            poly_taker_bps: 0,
            slippage_cents_per_leg: 0,
            ..Default::default()
        };
        let profit = expected_profit_total_cents(&req, 10, &cfg);
        assert_eq!(profit, 100);
    }

    #[test]
    fn test_expected_profit_with_slippage() {
        // 10 contracts, slippage=1¢/leg, 2 legs -> slippage=20¢
        // profit = 100 - (40+50+2) = 8¢ per contract with slippage
        let req = make_req(40, 50, ArbType::PolyOnly);
        let cfg = AllInCfg {
            poly_maker_bps: 0,
            poly_taker_bps: 0,
            slippage_cents_per_leg: 1,
            ..Default::default()
        };
        let profit = expected_profit_total_cents(&req, 10, &cfg);
        // payout=1000, premium=900, slippage=20, fees=0 -> profit=80
        assert_eq!(profit, 80);
    }

    #[test]
    fn test_expected_profit_with_kalshi_fees() {
        // KalshiOnly: both legs on Kalshi, fees apply
        // YES=40¢, NO=50¢, 10 contracts
        // YES fee: ceil(7×10×40×60/10000) = ceil(16800/10000) = 2
        // NO fee: ceil(7×10×50×50/10000) = ceil(17500/10000) = 2
        // (Actually let's recalc: 7*10*40*60=168000, /10000=16.8, ceil=17)
        // (And: 7*10*50*50=175000, /10000=17.5, ceil=18)
        let req = make_req(40, 50, ArbType::KalshiOnly);
        let cfg = AllInCfg {
            kalshi_role: KalshiRole::Taker,
            slippage_cents_per_leg: 0,
            ..Default::default()
        };
        let profit = expected_profit_total_cents(&req, 10, &cfg);
        // payout=1000, premium=900, fees=17+18=35, slippage=0 -> profit=65
        assert_eq!(profit, 65);
    }

    #[test]
    fn test_expected_profit_negative() {
        // Unprofitable trade: YES=55¢, NO=55¢ -> cost=110¢ > 100¢
        let req = make_req(55, 55, ArbType::PolyOnly);
        let cfg = AllInCfg {
            slippage_cents_per_leg: 0,
            ..Default::default()
        };
        let profit = expected_profit_total_cents(&req, 1, &cfg);
        assert_eq!(profit, -10, "Profit should be negative for unprofitable trade");
    }

    #[test]
    fn test_expected_profit_cross_platform_poly_yes_kalshi_no() {
        // PolyYesKalshiNo: Poly YES leg + Kalshi NO leg
        // YES=40¢ (Poly), NO=50¢ (Kalshi)
        // Kalshi fee on NO: ceil(7×10×50×50/10000) = 18
        // Poly fee: 0 (default bps=0)
        let req = make_req(40, 50, ArbType::PolyYesKalshiNo);
        let cfg = AllInCfg {
            kalshi_role: KalshiRole::Taker,
            slippage_cents_per_leg: 0,
            ..Default::default()
        };
        let profit = expected_profit_total_cents(&req, 10, &cfg);
        // payout=1000, premium=900, fees=18, slippage=0 -> profit=82
        assert_eq!(profit, 82);
    }

    // =========================================================================
    // All-In Analysis Tests
    // =========================================================================

    #[test]
    fn test_analyze_all_in_passes() {
        let req = make_req(40, 50, ArbType::PolyOnly);
        let cfg = AllInCfg::default();
        let analysis = analyze_all_in(&req, 10, &cfg, 100, 1);

        assert!(analysis.passes_threshold, "Should pass threshold check");
        assert!(analysis.passes_min_profit, "Should pass min profit check");
    }

    #[test]
    fn test_analyze_all_in_fails_threshold() {
        // Set very low threshold
        let req = make_req(55, 55, ArbType::PolyOnly);
        let cfg = AllInCfg::default();
        let analysis = analyze_all_in(&req, 10, &cfg, 95, 1);

        // cost_per_contract = (1100+20)/10 = 112 > 95
        assert!(!analysis.passes_threshold, "Should fail threshold check");
    }

    #[test]
    fn test_analyze_all_in_fails_min_profit() {
        // Marginal profit below minimum
        let req = make_req(49, 50, ArbType::PolyOnly);
        let cfg = AllInCfg {
            slippage_cents_per_leg: 0,
            ..Default::default()
        };
        // 10 contracts: profit = 1000 - 990 = 10, need 10 × 5 = 50 min profit
        let analysis = analyze_all_in(&req, 10, &cfg, 100, 5);
        assert!(!analysis.passes_min_profit, "Should fail min profit check");
    }
}
