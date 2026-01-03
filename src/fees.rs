//! Fee model for both Polymarket and Kalshi exchanges.
//!
//! # Unified All-In Cost Model - Single Source of Truth
//!
//! This module provides the **unified all-in cost model** used consistently across:
//! - **Detection** (hot path): Fast, cached, never blocks
//! - **Execution** (gate): Uses cached fees with async refresh
//! - **P&L** (recording): Prevents double-counting via fee-included tracking
//!
//! ## Key Components
//!
//! ### `AllInCostModel`
//! The single source of truth for all fee and cost calculations. Replaces:
//! - Old `AllInCfg` (execution)
//! - Direct fee table lookups (detection)
//! - Ad-hoc fee calculations (P&L)
//!
//! ### `CostMode`
//! Determines caching/blocking behavior:
//! - `DetectionFast`: NEVER blocks, uses cached or conservative fallback
//! - `ExecutionTruth`: Uses cached, triggers async refresh if needed
//! - `PnLTruth`: Uses actual fill data to avoid double-counting
//!
//! ### `AllInCost`
//! Cost breakdown with `fee_included_in_total` flag to prevent double-counting:
//! ```rust,ignore
//! // Case 1: Fee computed separately
//! let cost = AllInCost::from_gross_and_fee(1000, 50);
//! // total_cents = 1050, fee_included_in_total = false
//!
//! // Case 2: Fee already in total (Kalshi fill_cost)
//! let cost = AllInCost::from_total_with_fee_included(1050, 50);
//! // total_cents = 1050, fee_included_in_total = true (don't add again!)
//! ```
//!
//! ## Usage Examples
//!
//! ### Detection (Hot Path)
//! ```rust,ignore
//! let cost_model = AllInCostModel::new(fee_model, kalshi_role);
//! let cost = cost_model.compute_leg_cost(
//!     FeeVenue::Polymarket,
//!     token_id,
//!     price_cents,
//!     qty,
//!     CostMode::DetectionFast, // NEVER blocks
//! );
//! ```
//!
//! ### Execution Gate
//! ```rust,ignore
//! let (leg1, leg2, total) = cost_model.compute_arb_cost(
//!     arb_type,
//!     yes_price,
//!     no_price,
//!     qty,
//!     poly_yes_token,
//!     poly_no_token,
//!     CostMode::ExecutionTruth,
//! );
//! if total > threshold { reject(); }
//! ```
//!
//! ### P&L Recording (No Double-Counting)
//! ```rust,ignore
//! let cost = if fill_includes_fee {
//!     AllInCost::from_total_with_fee_included(fill_total, inferred_fee)
//! } else {
//!     AllInCost::from_gross_and_fee(gross, fee)
//! };
//! // fee_included_in_total flag ensures we don't add fee twice
//! ```

use rustc_hash::FxHashMap;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::config::{KalshiRole, GAMMA_API_BASE};
use crate::cost::{kalshi_fee_total_cents, poly_fee_total_cents};
use crate::types::{ArbType, PriceCents};

/// Conservative fallback Polymarket fee (50 bps = 0.5%) if metadata unavailable
const POLY_FALLBACK_TAKER_BPS: u16 = 50;
const POLY_FALLBACK_MAKER_BPS: u16 = 20;

// =============================================================================
// Unified All-In Cost Model - Single Source of Truth
// =============================================================================

/// Cost calculation mode - determines caching/blocking behavior
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostMode {
    /// Detection fast path - NEVER blocks, uses cached fees or conservative fallback
    DetectionFast,
    /// Execution gate - uses cached fees, triggers async refresh if needed
    ExecutionTruth,
    /// P&L recording - uses actual fill data to avoid double-counting
    PnLTruth,
}

/// Venue identifier for fee calculations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FeeVenue {
    Kalshi,
    Polymarket,
}

/// Trade side
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeSide {
    Buy,
    Sell,
}

/// All-in cost breakdown
#[derive(Debug, Clone, Copy)]
pub struct AllInCost {
    /// Gross notional in cents (price × qty)
    pub gross_cents: i64,
    /// Fee in cents (computed or from fill)
    pub fee_cents: i64,
    /// Total cost in cents (gross + fee, unless fee already included)
    pub total_cents: i64,
    /// Whether fee is already included in total (important for P&L)
    pub fee_included_in_total: bool,
}

impl AllInCost {
    /// Create from separate gross and fee
    pub fn from_gross_and_fee(gross_cents: i64, fee_cents: i64) -> Self {
        Self {
            gross_cents,
            fee_cents,
            total_cents: gross_cents + fee_cents,
            fee_included_in_total: false,
        }
    }

    /// Create when fee is already included in total
    pub fn from_total_with_fee_included(total_cents: i64, inferred_fee_cents: i64) -> Self {
        Self {
            gross_cents: total_cents - inferred_fee_cents,
            fee_cents: inferred_fee_cents,
            total_cents,
            fee_included_in_total: true,
        }
    }
}

/// Which exchange to estimate fees for
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Exchange {
    Kalshi,
    Polymarket,
}

/// Trade action (buy or sell)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Buy,
    Sell,
}

/// Polymarket market fee configuration
#[derive(Debug, Clone, Default)]
pub struct PolyFeeConfig {
    /// Maker fee in basis points (0 = free)
    pub maker_bps: u16,
    /// Taker fee in basis points
    pub taker_bps: u16,
    /// Whether this is a fallback (logged once)
    pub is_fallback: bool,
}

/// Response from Gamma API for market details
#[derive(Debug, Deserialize)]
struct GammaMarketDetails {
    #[serde(rename = "makerBaseFee")]
    maker_base_fee: Option<f64>,
    #[serde(rename = "takerBaseFee")]
    taker_base_fee: Option<f64>,
}

/// Fee model with caching and market-specific fee fetching
pub struct FeeModel {
    /// HTTP client for Gamma API
    http: reqwest::Client,
    /// Cached Polymarket fee configs by token_id
    poly_cache: Arc<RwLock<FxHashMap<String, PolyFeeConfig>>>,
    /// Markets for which we've logged fallback warnings
    fallback_logged: Arc<RwLock<FxHashMap<String, bool>>>,
    /// Default Kalshi role (taker or maker)
    kalshi_role: KalshiRole,
}

impl FeeModel {
    /// Create a new fee model
    pub fn new(kalshi_role: KalshiRole) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .expect("Failed to build HTTP client for fee model"),
            poly_cache: Arc::new(RwLock::new(FxHashMap::default())),
            fallback_logged: Arc::new(RwLock::new(FxHashMap::default())),
            kalshi_role,
        }
    }

    /// Estimate fees for a trade in cents.
    ///
    /// # Arguments
    /// * `exchange` - Which exchange (Kalshi or Polymarket)
    /// * `market_id` - Market identifier (Kalshi ticker or Polymarket token_id)
    /// * `price_cents` - Price in cents [1..99]
    /// * `contracts` - Number of contracts
    ///
    /// # Returns
    /// Total fee in cents
    pub async fn estimate_fees(
        &self,
        exchange: Exchange,
        market_id: &str,
        price_cents: PriceCents,
        contracts: u32,
    ) -> u32 {
        match exchange {
            Exchange::Kalshi => kalshi_fee_total_cents(price_cents, contracts, self.kalshi_role),
            Exchange::Polymarket => {
                let config = self.get_poly_fee_config(market_id).await;
                let premium_cents = price_cents as u32 * contracts;

                // Assume taker for IOC/FAK orders (which are used for arb execution)
                poly_fee_total_cents(
                    premium_cents,
                    crate::config::PolyRole::Taker,
                    config.maker_bps,
                    config.taker_bps,
                )
            }
        }
    }

    /// Get Polymarket fee config for a token, with caching
    async fn get_poly_fee_config(&self, token_id: &str) -> PolyFeeConfig {
        // Check cache first
        {
            let cache = self.poly_cache.read().await;
            if let Some(config) = cache.get(token_id) {
                return config.clone();
            }
        }

        // Fetch from API
        let config = match self.fetch_poly_fees(token_id).await {
            Ok(cfg) => {
                // Log first successful fetch for this token
                let mut logged = self.fallback_logged.write().await;
                if !logged.contains_key(token_id) {
                    info!(
                        "[FEES] Polymarket token {} fees: maker={}bps, taker={}bps",
                        &token_id[..8.min(token_id.len())],
                        cfg.maker_bps,
                        cfg.taker_bps
                    );
                    logged.insert(token_id.to_string(), true);
                }
                cfg
            }
            Err(e) => {
                // Log warning once per market
                let mut logged = self.fallback_logged.write().await;
                if !logged.contains_key(token_id) {
                    warn!(
                        "[FEES] Failed to fetch Polymarket fees for token {}: {}. Using conservative fallback ({}bps taker, {}bps maker)",
                        &token_id[..8.min(token_id.len())],
                        e,
                        POLY_FALLBACK_TAKER_BPS,
                        POLY_FALLBACK_MAKER_BPS
                    );
                    logged.insert(token_id.to_string(), true);
                }
                PolyFeeConfig {
                    maker_bps: POLY_FALLBACK_MAKER_BPS,
                    taker_bps: POLY_FALLBACK_TAKER_BPS,
                    is_fallback: true,
                }
            }
        };

        // Cache it
        {
            let mut cache = self.poly_cache.write().await;
            cache.insert(token_id.to_string(), config.clone());
        }

        config
    }

    /// Fetch Polymarket fees from Gamma API
    async fn fetch_poly_fees(&self, token_id: &str) -> anyhow::Result<PolyFeeConfig> {
        let url = format!("{}/markets?clob_token_ids={}", GAMMA_API_BASE, token_id);

        let resp = self.http.get(&url).send().await?;

        if !resp.status().is_success() {
            anyhow::bail!("HTTP {}", resp.status());
        }

        let markets: Vec<GammaMarketDetails> = resp.json().await?;

        if markets.is_empty() {
            anyhow::bail!("No market found for token");
        }

        let market = &markets[0];

        // Convert from decimal to basis points (e.g., 0.01 -> 100 bps = 1%)
        let maker_bps = market
            .maker_base_fee
            .map(|f| (f * 10000.0).round() as u16)
            .unwrap_or(0);

        let taker_bps = market
            .taker_base_fee
            .map(|f| (f * 10000.0).round() as u16)
            .unwrap_or(0);

        Ok(PolyFeeConfig {
            maker_bps,
            taker_bps,
            is_fallback: false,
        })
    }

    /// Estimate fees for both legs of an arbitrage trade
    ///
    /// Returns (poly_fee_cents, kalshi_fee_cents, total_fee_cents)
    pub async fn estimate_arb_fees(
        &self,
        poly_token_id: &str,
        poly_price_cents: PriceCents,
        kalshi_price_cents: PriceCents,
        contracts: u32,
    ) -> (u32, u32, u32) {
        let poly_fee = self
            .estimate_fees(
                Exchange::Polymarket,
                poly_token_id,
                poly_price_cents,
                contracts,
            )
            .await;

        let kalshi_fee = self
            .estimate_fees(Exchange::Kalshi, "", kalshi_price_cents, contracts)
            .await;

        (poly_fee, kalshi_fee, poly_fee + kalshi_fee)
    }

    /// Get cached Polymarket fee BPS (non-blocking, used in hot path)
    pub fn get_poly_fee_bps_cached(&self, token_id: &str) -> Option<(u16, u16)> {
        // Try to read cache without blocking
        if let Ok(cache) = self.poly_cache.try_read() {
            if let Some(config) = cache.get(token_id) {
                return Some((config.maker_bps, config.taker_bps));
            }
        }
        None
    }

    /// Request async refresh of Polymarket fees (non-blocking)
    pub fn request_poly_fee_refresh(&self, token_id: String) {
        // Only spawn if we're in a tokio runtime context
        if tokio::runtime::Handle::try_current().is_ok() {
            let self_clone = Self {
                http: self.http.clone(),
                poly_cache: self.poly_cache.clone(),
                fallback_logged: self.fallback_logged.clone(),
                kalshi_role: self.kalshi_role,
            };

            // Spawn background task to refresh
            tokio::spawn(async move {
                let _ = self_clone.get_poly_fee_config(&token_id).await;
            });
        }
        // If not in tokio context (e.g., sync tests), just skip the refresh
    }
}

// =============================================================================
// Unified All-In Cost Model - Used by Detection, Execution, and P&L
// =============================================================================

/// Unified all-in cost model - single source of truth for fee calculations
pub struct AllInCostModel {
    fee_model: Arc<FeeModel>,
    kalshi_role: KalshiRole,
}

impl AllInCostModel {
    /// Create new unified cost model
    pub fn new(fee_model: Arc<FeeModel>, kalshi_role: KalshiRole) -> Self {
        Self {
            fee_model,
            kalshi_role,
        }
    }

    /// Compute all-in cost for a single leg
    ///
    /// This is the SINGLE SOURCE OF TRUTH for cost calculations.
    /// Used by detection (fast, cached), execution (truth), and P&L (truth).
    pub fn compute_leg_cost(
        &self,
        venue: FeeVenue,
        instrument_id: &str,
        price_cents: i64,
        qty: i64,
        mode: CostMode,
    ) -> AllInCost {
        let gross_cents = price_cents * qty;

        let fee_cents = match venue {
            FeeVenue::Kalshi => {
                // Kalshi fees are deterministic based on price and quantity
                kalshi_fee_total_cents(price_cents as u16, qty as u32, self.kalshi_role) as i64
            }
            FeeVenue::Polymarket => {
                match mode {
                    CostMode::DetectionFast => {
                        // NEVER block - use cached or fallback
                        if let Some((maker_bps, taker_bps)) =
                            self.fee_model.get_poly_fee_bps_cached(instrument_id)
                        {
                            // Use cached fees
                            let premium = gross_cents as u32;
                            poly_fee_total_cents(
                                premium,
                                crate::config::PolyRole::Taker,
                                maker_bps,
                                taker_bps,
                            ) as i64
                        } else {
                            // Cache miss - request async refresh and use conservative fallback
                            self.fee_model
                                .request_poly_fee_refresh(instrument_id.to_string());

                            let premium = gross_cents as u32;
                            poly_fee_total_cents(
                                premium,
                                crate::config::PolyRole::Taker,
                                POLY_FALLBACK_MAKER_BPS,
                                POLY_FALLBACK_TAKER_BPS,
                            ) as i64
                        }
                    }
                    CostMode::ExecutionTruth | CostMode::PnLTruth => {
                        // Use cached fees if available, else fallback
                        let (maker_bps, taker_bps) = self
                            .fee_model
                            .get_poly_fee_bps_cached(instrument_id)
                            .unwrap_or((POLY_FALLBACK_MAKER_BPS, POLY_FALLBACK_TAKER_BPS));

                        // Trigger refresh if not cached
                        if self
                            .fee_model
                            .get_poly_fee_bps_cached(instrument_id)
                            .is_none()
                        {
                            self.fee_model
                                .request_poly_fee_refresh(instrument_id.to_string());
                        }

                        let premium = gross_cents as u32;
                        poly_fee_total_cents(
                            premium,
                            crate::config::PolyRole::Taker,
                            maker_bps,
                            taker_bps,
                        ) as i64
                    }
                }
            }
        };

        AllInCost::from_gross_and_fee(gross_cents, fee_cents)
    }

    /// Compute total all-in cost for an arbitrage trade (both legs)
    pub fn compute_arb_cost(
        &self,
        arb_type: ArbType,
        yes_price: i64,
        no_price: i64,
        qty: i64,
        poly_token_yes: &str,
        poly_token_no: &str,
        mode: CostMode,
    ) -> (AllInCost, AllInCost, i64) {
        // Determine which legs are used based on arb type
        let (leg1_cost, leg2_cost) = match arb_type {
            ArbType::PolyYesKalshiNo => {
                let poly_yes = self.compute_leg_cost(
                    FeeVenue::Polymarket,
                    poly_token_yes,
                    yes_price,
                    qty,
                    mode,
                );
                let kalshi_no = self.compute_leg_cost(FeeVenue::Kalshi, "", no_price, qty, mode);
                (poly_yes, kalshi_no)
            }
            ArbType::KalshiYesPolyNo => {
                let kalshi_yes = self.compute_leg_cost(FeeVenue::Kalshi, "", yes_price, qty, mode);
                let poly_no =
                    self.compute_leg_cost(FeeVenue::Polymarket, poly_token_no, no_price, qty, mode);
                (kalshi_yes, poly_no)
            }
            ArbType::PolyOnly => {
                let poly_yes = self.compute_leg_cost(
                    FeeVenue::Polymarket,
                    poly_token_yes,
                    yes_price,
                    qty,
                    mode,
                );
                let poly_no =
                    self.compute_leg_cost(FeeVenue::Polymarket, poly_token_no, no_price, qty, mode);
                (poly_yes, poly_no)
            }
            ArbType::KalshiOnly => {
                let kalshi_yes = self.compute_leg_cost(FeeVenue::Kalshi, "", yes_price, qty, mode);
                let kalshi_no = self.compute_leg_cost(FeeVenue::Kalshi, "", no_price, qty, mode);
                (kalshi_yes, kalshi_no)
            }
        };

        let total_cost = leg1_cost.total_cents + leg2_cost.total_cents;
        (leg1_cost, leg2_cost, total_cost)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pnl::{ContractSide, PnLTracker, Side as PnLSide, Venue as PnLVenue};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_kalshi_fees() {
        let model = FeeModel::new(KalshiRole::Taker);

        // Test at 50 cents
        let fee = model.estimate_fees(Exchange::Kalshi, "", 50, 1).await;

        // Expected: ceil(7 × 1 × 50 × 50 / 10000) = 2 cents
        assert_eq!(fee, 2);

        // Test with 10 contracts
        let fee = model.estimate_fees(Exchange::Kalshi, "", 50, 10).await;

        // Expected: ceil(7 × 10 × 50 × 50 / 10000) = 18 cents
        assert_eq!(fee, 18);
    }

    #[tokio::test]
    async fn test_poly_fallback_fees() {
        let model = FeeModel::new(KalshiRole::Taker);

        // Use a fake token ID that won't exist in API
        let fee = model
            .estimate_fees(Exchange::Polymarket, "fake_token_12345", 60, 10)
            .await;

        // Premium = 60¢ × 10 = 600¢
        // Fallback taker fee = 50 bps
        // Expected: ceil(600 × 50 / 10000) = 3¢
        assert_eq!(fee, 3);
    }

    #[tokio::test]
    async fn test_arb_fees_estimation() {
        let model = FeeModel::new(KalshiRole::Taker);

        let (poly_fee, kalshi_fee, total) =
            model.estimate_arb_fees("fake_token_xyz", 45, 50, 10).await;

        // Poly: premium = 45 × 10 = 450¢, fee = ceil(450 × 50 / 10000) = 3¢
        assert_eq!(poly_fee, 3);

        // Kalshi: ceil(7 × 10 × 50 × 50 / 10000) = 18¢
        assert_eq!(kalshi_fee, 18);

        // Total
        assert_eq!(total, 21);
    }

    #[test]
    fn test_fee_config_default() {
        let config = PolyFeeConfig::default();
        assert_eq!(config.maker_bps, 0);
        assert_eq!(config.taker_bps, 0);
        assert!(!config.is_fallback);
    }

    #[tokio::test]
    async fn test_opportunity_with_fees() {
        // Test that opportunity total includes fees correctly
        let model = FeeModel::new(KalshiRole::Taker);

        // Scenario: PolyYes=45¢, KalshiNo=50¢
        let poly_yes_price: u16 = 45;
        let kalshi_no_price: u16 = 50;
        let contracts: u32 = 10;

        // Without fees: 45 + 50 = 95¢ per contract (5¢ profit)
        // With fees:
        let poly_fee = model
            .estimate_fees(
                Exchange::Polymarket,
                "fake_token",
                poly_yes_price,
                contracts,
            )
            .await;
        let kalshi_fee = model
            .estimate_fees(Exchange::Kalshi, "", kalshi_no_price, contracts)
            .await;

        // Poly: 450¢ premium × 50bps = 3¢
        // Kalshi: ceil(7 × 10 × 50 × 50 / 10000) = 18¢
        assert_eq!(poly_fee, 3);
        assert_eq!(kalshi_fee, 18);

        // Total cost per contract = (450 + 500 + 3 + 18) / 10 = 97.1¢
        let total_cost_cents = (poly_yes_price as u32 * contracts)
            + (kalshi_no_price as u32 * contracts)
            + poly_fee
            + kalshi_fee;
        let cost_per_contract = total_cost_cents / contracts;

        // Should be 97¢ per contract
        assert_eq!(cost_per_contract, 97);
    }

    #[tokio::test]
    async fn test_pnl_fees_accumulation() {
        // Test that P&L tracker correctly accumulates fees
        let temp_dir = std::env::temp_dir().join("pnl_fees_test");
        let _ = std::fs::create_dir_all(&temp_dir);
        let dir_str = temp_dir.to_str().unwrap();

        let tracker = Arc::new(Mutex::new(PnLTracker::new(dir_str, true)));

        // Simulate two fills with fees
        {
            let mut pnl = tracker.lock().await;
            pnl.on_fill(
                PnLVenue::Polymarket,
                "test_market_1",
                "order1",
                PnLSide::Buy,
                ContractSide::Yes,
                10,
                45, // 45¢ price
                3,  // 3¢ fee (from our fee model)
            );

            pnl.on_fill(
                PnLVenue::Kalshi,
                "test_market_1",
                "order2",
                PnLSide::Buy,
                ContractSide::No,
                10,
                50, // 50¢ price
                18, // 18¢ fee (from our fee model)
            );
        }

        // Check P&L summary includes fees
        let summary = {
            let pnl = tracker.lock().await;
            pnl.summary()
        };

        // Total fees should be 3 + 18 = 21¢
        assert_eq!(summary.fees_cents, 21);

        // Net P&L should account for fees
        // Realized P&L = 0 (no position closed)
        // Unrealized P&L = depends on marks (we haven't set any)
        // Fees = 21¢
        assert_eq!(summary.fees_cents, 21);
        assert_eq!(summary.open_positions, 2); // Two positions (YES and NO)

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_pnl_snapshot_persists_fees() {
        // Test that fees are persisted in snapshots
        let temp_dir = std::env::temp_dir().join("pnl_snapshot_fees_test");
        let _ = std::fs::create_dir_all(&temp_dir);
        let dir_str = temp_dir.to_str().unwrap();

        // Create tracker and add fills with fees
        {
            let mut tracker = PnLTracker::new(dir_str, true);

            tracker.on_fill(
                PnLVenue::Polymarket,
                "test_market",
                "order1",
                PnLSide::Buy,
                ContractSide::Yes,
                10,
                45,
                3, // fee
            );

            tracker.on_fill(
                PnLVenue::Kalshi,
                "test_market",
                "order2",
                PnLSide::Buy,
                ContractSide::No,
                10,
                50,
                18, // fee
            );

            // Save snapshot
            assert!(tracker.save_snapshot());
        }

        // Load from snapshot in new tracker
        {
            let tracker = PnLTracker::new(dir_str, true);
            let summary = tracker.summary();

            // Fees should be restored from snapshot
            assert_eq!(summary.fees_cents, 21);
            assert_eq!(summary.open_positions, 2);
        }

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_unified_cost_model_kalshi() {
        let fee_model = Arc::new(FeeModel::new(KalshiRole::Taker));
        let cost_model = AllInCostModel::new(fee_model, KalshiRole::Taker);

        // Test Kalshi leg cost calculation
        let cost = cost_model.compute_leg_cost(
            FeeVenue::Kalshi,
            "",
            50,  // 50 cents
            100, // 100 contracts
            CostMode::DetectionFast,
        );

        assert_eq!(cost.gross_cents, 5000, "Gross should be 50 * 100 = 5000");
        assert_eq!(
            cost.fee_cents, 175,
            "Fee should be ceil(7*100*50*50/10000) = 175"
        );
        assert_eq!(cost.total_cents, 5175, "Total should be 5000 + 175");
        assert!(!cost.fee_included_in_total, "Fee not included in total");
    }

    #[test]
    fn test_unified_cost_model_poly_fallback() {
        let fee_model = Arc::new(FeeModel::new(KalshiRole::Taker));
        let cost_model = AllInCostModel::new(fee_model, KalshiRole::Taker);

        // Test Polymarket with cache miss (uses fallback)
        let cost = cost_model.compute_leg_cost(
            FeeVenue::Polymarket,
            "fake_token_123",
            40,  // 40 cents
            100, // 100 contracts
            CostMode::DetectionFast,
        );

        assert_eq!(cost.gross_cents, 4000, "Gross should be 40 * 100 = 4000");
        // Fallback is 50 bps: ceil(4000 * 50 / 10000) = 20
        assert_eq!(cost.fee_cents, 20, "Fee should use fallback 50 bps");
        assert_eq!(cost.total_cents, 4020, "Total should be 4000 + 20");
    }

    #[test]
    fn test_all_in_cost_from_gross_and_fee() {
        let cost = AllInCost::from_gross_and_fee(1000, 50);
        assert_eq!(cost.gross_cents, 1000);
        assert_eq!(cost.fee_cents, 50);
        assert_eq!(cost.total_cents, 1050);
        assert!(!cost.fee_included_in_total);
    }

    #[test]
    fn test_all_in_cost_fee_included() {
        let cost = AllInCost::from_total_with_fee_included(1050, 50);
        assert_eq!(cost.gross_cents, 1000);
        assert_eq!(cost.fee_cents, 50);
        assert_eq!(cost.total_cents, 1050);
        assert!(
            cost.fee_included_in_total,
            "Fee should be marked as included"
        );
    }

    #[test]
    fn test_unified_arb_cost_cross_venue() {
        let fee_model = Arc::new(FeeModel::new(KalshiRole::Taker));
        let cost_model = AllInCostModel::new(fee_model, KalshiRole::Taker);

        let (leg1, leg2, total) = cost_model.compute_arb_cost(
            ArbType::PolyYesKalshiNo,
            40, // yes price
            50, // no price
            10, // qty
            "poly_yes_token",
            "poly_no_token",
            CostMode::DetectionFast,
        );

        // Poly YES: 40 * 10 = 400, fee ~2 (fallback 50bps)
        assert_eq!(leg1.gross_cents, 400);
        assert_eq!(leg1.fee_cents, 2);

        // Kalshi NO: 50 * 10 = 500, fee = 18
        assert_eq!(leg2.gross_cents, 500);
        assert_eq!(leg2.fee_cents, 18);

        // Total: 400 + 2 + 500 + 18 = 920
        assert_eq!(total, 920);
    }

    #[test]
    fn test_unified_model_prevents_double_counting() {
        // Test that AllInCost properly tracks whether fees are included

        // Case 1: Fees computed separately (NOT included in total)
        let cost1 = AllInCost::from_gross_and_fee(1000, 50);
        assert!(!cost1.fee_included_in_total);
        assert_eq!(cost1.total_cents, 1050); // gross + fee

        // In P&L, we would record:
        // - cost_basis: cost1.total_cents = 1050
        // - fees: cost1.fee_cents = 50

        // Case 2: Fee already included in fill total (Kalshi fill_cost scenario)
        let cost2 = AllInCost::from_total_with_fee_included(1050, 50);
        assert!(cost2.fee_included_in_total);
        assert_eq!(cost2.total_cents, 1050); // already includes fee
        assert_eq!(cost2.gross_cents, 1000); // inferred

        // In P&L, we would record:
        // - cost_basis: cost2.total_cents = 1050 (fee already in here)
        // - fees: cost2.fee_cents = 50 (for tracking only, not added again)

        // This prevents double-counting: we don't add fee_cents to total_cents twice
    }

    #[test]
    fn test_cost_mode_detection_never_blocks() {
        // Verify DetectionFast mode uses cached/fallback without blocking
        let fee_model = Arc::new(FeeModel::new(KalshiRole::Taker));
        let cost_model = AllInCostModel::new(fee_model, KalshiRole::Taker);

        // With empty cache, should immediately return fallback
        let start = std::time::Instant::now();
        let cost = cost_model.compute_leg_cost(
            FeeVenue::Polymarket,
            "uncached_token",
            50,
            100,
            CostMode::DetectionFast,
        );
        let elapsed = start.elapsed();

        // Should be instant (< 1ms) since it doesn't block
        assert!(elapsed.as_millis() < 10, "Detection should be instant");
        assert!(cost.fee_cents > 0, "Should have fallback fee");
    }
}
