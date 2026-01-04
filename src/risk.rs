//! Dynamic position sizing with hard risk cap.
//!
//! This module implements conservative position sizing that limits
//! worst-case loss per trade to a fraction of the available bankroll.
//!
//! # Example
//! ```
//! use prediction_market_arbitrage::risk::{SizingConfig, BankrollConfig, size_for_trade};
//!
//! let cfg = SizingConfig::default();
//! let bk = BankrollConfig::default();
//! let decision = size_for_trade(&cfg, &bk);
//! assert!(decision.qty >= 0);
//! ```

use std::env;

/// Position sizing configuration (all from environment variables)
#[derive(Debug, Clone)]
pub struct SizingConfig {
    /// Maximum fraction of bankroll to risk per trade (in basis points, 1000 = 10%)
    pub max_fraction_bps: u32,
    /// Maximum quantity per trade (hard cap)
    pub max_qty_per_trade: u32,
    /// Minimum quantity per trade (below this, skip trade)
    pub min_qty_per_trade: u32,
    /// Worst-case loss per contract in cents (typically 100 = $1.00)
    pub worst_case_loss_per_contract_cents: i64,
}

impl Default for SizingConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

impl SizingConfig {
    /// Load sizing configuration from environment variables
    pub fn from_env() -> Self {
        let max_fraction_bps = env::var("MAX_FRACTION_BPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000); // 10% default

        let max_qty_per_trade = env::var("MAX_QTY_PER_TRADE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);

        let min_qty_per_trade = env::var("MIN_QTY_PER_TRADE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);

        let worst_case_loss_per_contract_cents = env::var("WORST_CASE_LOSS_PER_CONTRACT_CENTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100); // $1.00 default

        Self {
            max_fraction_bps,
            max_qty_per_trade,
            min_qty_per_trade,
            worst_case_loss_per_contract_cents,
        }
    }

    /// Create config with explicit values (for testing)
    #[allow(dead_code)]
    pub fn new(
        max_fraction_bps: u32,
        max_qty_per_trade: u32,
        min_qty_per_trade: u32,
        worst_case_loss_per_contract_cents: i64,
    ) -> Self {
        Self {
            max_fraction_bps,
            max_qty_per_trade,
            min_qty_per_trade,
            worst_case_loss_per_contract_cents,
        }
    }
}

/// Bankroll configuration (cash available on each platform)
#[derive(Debug, Clone)]
pub struct BankrollConfig {
    /// Cash available on Kalshi in cents
    pub kalshi_cash_cents: i64,
    /// Cash available on Polymarket in cents
    pub poly_cash_cents: i64,
}

impl Default for BankrollConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

impl BankrollConfig {
    /// Load bankroll configuration from environment variables
    pub fn from_env() -> Self {
        let kalshi_cash_cents = env::var("KALSHI_CASH_CENTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1500); // $15.00 default

        let poly_cash_cents = env::var("POLY_CASH_CENTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1500); // $15.00 default

        Self {
            kalshi_cash_cents,
            poly_cash_cents,
        }
    }

    /// Create config with explicit values (for testing)
    #[allow(dead_code)]
    pub fn new(kalshi_cash_cents: i64, poly_cash_cents: i64) -> Self {
        Self {
            kalshi_cash_cents,
            poly_cash_cents,
        }
    }

    /// Get the effective bankroll (minimum of both platforms)
    pub fn effective_bankroll_cents(&self) -> i64 {
        self.kalshi_cash_cents.min(self.poly_cash_cents)
    }
}

/// Result of position sizing calculation
#[derive(Debug, Clone)]
pub struct SizingDecision {
    /// Number of contracts to trade (0 = skip trade)
    pub qty: u32,
    /// Effective bankroll used for calculation (cents)
    pub bankroll_cents: i64,
    /// Risk budget for this trade (cents)
    pub risk_budget_cents: i64,
    /// Worst-case loss per contract (cents)
    pub worst_case_loss_cents: i64,
    /// Reason for the sizing decision
    pub reason: &'static str,
}

impl SizingDecision {
    /// Check if trade should be executed
    pub fn should_trade(&self) -> bool {
        self.qty > 0
    }
}

/// Calculate position size for a trade using hard risk cap.
///
/// # Algorithm (integer-only math)
/// 1. bankroll_cents = min(kalshi_cash, poly_cash)
/// 2. risk_budget_cents = bankroll_cents * max_fraction_bps / 10_000
/// 3. qty = risk_budget_cents / worst_case_loss_per_contract_cents
/// 4. qty = clamp(qty, 0..max_qty_per_trade)
/// 5. if qty < min_qty_per_trade => qty=0, reason="below_min"
///
/// # Example
/// - bankroll = $15.00 (1500¢)
/// - max_fraction_bps = 1000 (10%)
/// - risk_budget = 1500 * 1000 / 10_000 = 150¢
/// - worst_case_loss = 100¢ per contract
/// - qty = 150 / 100 = 1 contract
pub fn size_for_trade(cfg: &SizingConfig, bk: &BankrollConfig) -> SizingDecision {
    // Step 1: Effective bankroll is minimum of both platforms
    let bankroll_cents = bk.effective_bankroll_cents();

    // Step 2: Calculate risk budget using integer math (bps / 10_000)
    // risk_budget = bankroll * fraction_bps / 10_000
    let risk_budget_cents = (bankroll_cents * cfg.max_fraction_bps as i64) / 10_000;

    // Step 3: Calculate quantity based on worst-case loss
    let worst_case_loss_cents = cfg.worst_case_loss_per_contract_cents;

    // Avoid division by zero
    let raw_qty = if worst_case_loss_cents > 0 {
        (risk_budget_cents / worst_case_loss_cents) as u32
    } else {
        0
    };

    // Step 4: Clamp to max_qty_per_trade
    let clamped_qty = raw_qty.min(cfg.max_qty_per_trade);

    // Step 5: Check minimum threshold
    let (qty, reason) = if clamped_qty < cfg.min_qty_per_trade {
        (0, "below_min")
    } else {
        (clamped_qty, "ok")
    };

    SizingDecision {
        qty,
        bankroll_cents,
        risk_budget_cents,
        worst_case_loss_cents,
        reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bankroll_1500_produces_qty_1() {
        // $15 bankroll, 10% risk, $1 worst-case loss => qty=1
        let cfg = SizingConfig::new(1000, 10, 1, 100);
        let bk = BankrollConfig::new(1500, 1500);

        let decision = size_for_trade(&cfg, &bk);

        assert_eq!(decision.bankroll_cents, 1500);
        assert_eq!(decision.risk_budget_cents, 150); // 1500 * 1000 / 10_000
        assert_eq!(decision.qty, 1); // 150 / 100 = 1
        assert_eq!(decision.reason, "ok");
        assert!(decision.should_trade());
    }

    #[test]
    fn test_bankroll_5000_produces_qty_5() {
        // $50 bankroll, 10% risk, $1 worst-case loss => qty=5
        let cfg = SizingConfig::new(1000, 10, 1, 100);
        let bk = BankrollConfig::new(5000, 5000);

        let decision = size_for_trade(&cfg, &bk);

        assert_eq!(decision.bankroll_cents, 5000);
        assert_eq!(decision.risk_budget_cents, 500); // 5000 * 1000 / 10_000
        assert_eq!(decision.qty, 5); // 500 / 100 = 5
        assert_eq!(decision.reason, "ok");
    }

    #[test]
    fn test_clamp_by_max_qty() {
        // $200 bankroll, 10% risk => qty=20, but max=10
        let cfg = SizingConfig::new(1000, 10, 1, 100);
        let bk = BankrollConfig::new(20000, 20000);

        let decision = size_for_trade(&cfg, &bk);

        assert_eq!(decision.risk_budget_cents, 2000); // 20000 * 1000 / 10_000
                                                      // Raw qty would be 20, but clamped to 10
        assert_eq!(decision.qty, 10);
        assert_eq!(decision.reason, "ok");
    }

    #[test]
    fn test_below_min_returns_qty_0() {
        // $5 bankroll, 10% risk => risk_budget=50, qty=0 (below min=1)
        let cfg = SizingConfig::new(1000, 10, 1, 100);
        let bk = BankrollConfig::new(500, 500);

        let decision = size_for_trade(&cfg, &bk);

        assert_eq!(decision.bankroll_cents, 500);
        assert_eq!(decision.risk_budget_cents, 50); // 500 * 1000 / 10_000
                                                    // 50 / 100 = 0, which is below min=1
        assert_eq!(decision.qty, 0);
        assert_eq!(decision.reason, "below_min");
        assert!(!decision.should_trade());
    }

    #[test]
    fn test_uses_minimum_platform_bankroll() {
        // Kalshi has $50, Poly has $15 => use $15
        let cfg = SizingConfig::new(1000, 10, 1, 100);
        let bk = BankrollConfig::new(5000, 1500);

        let decision = size_for_trade(&cfg, &bk);

        assert_eq!(decision.bankroll_cents, 1500); // min(5000, 1500)
        assert_eq!(decision.qty, 1);
    }

    #[test]
    fn test_different_worst_case_loss() {
        // $50 bankroll, 10% risk, $0.50 worst-case loss => qty=10
        let cfg = SizingConfig::new(1000, 10, 1, 50); // 50¢ worst-case
        let bk = BankrollConfig::new(5000, 5000);

        let decision = size_for_trade(&cfg, &bk);

        assert_eq!(decision.risk_budget_cents, 500);
        assert_eq!(decision.qty, 10); // 500 / 50 = 10, clamped to max=10
    }

    #[test]
    fn test_higher_min_qty_threshold() {
        // $15 bankroll, 10% risk => qty=1, but min=2
        let cfg = SizingConfig::new(1000, 10, 2, 100); // min_qty=2
        let bk = BankrollConfig::new(1500, 1500);

        let decision = size_for_trade(&cfg, &bk);

        assert_eq!(decision.qty, 0); // 1 < 2 (min)
        assert_eq!(decision.reason, "below_min");
    }

    #[test]
    fn test_zero_worst_case_loss_returns_zero() {
        // Edge case: worst_case_loss = 0 should not panic
        let cfg = SizingConfig::new(1000, 10, 1, 0);
        let bk = BankrollConfig::new(5000, 5000);

        let decision = size_for_trade(&cfg, &bk);

        assert_eq!(decision.qty, 0);
        assert_eq!(decision.reason, "below_min");
    }

    #[test]
    fn test_5_percent_risk_fraction() {
        // $100 bankroll, 5% risk (500 bps), $1 worst-case => qty=5
        let cfg = SizingConfig::new(500, 10, 1, 100); // 500 bps = 5%
        let bk = BankrollConfig::new(10000, 10000);

        let decision = size_for_trade(&cfg, &bk);

        assert_eq!(decision.risk_budget_cents, 500); // 10000 * 500 / 10_000
        assert_eq!(decision.qty, 5);
    }
}
