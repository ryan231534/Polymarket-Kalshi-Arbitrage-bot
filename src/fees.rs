//! Fee model for both Polymarket and Kalshi exchanges.
//!
//! This module provides unified fee estimation across both platforms with:
//! - Market-specific fee fetching and caching for Polymarket
//! - Conservative fallback when fees can't be fetched
//! - Single API: estimate_fees(exchange, market_id, side, action, price_cents, contracts) -> fee_cents

use rustc_hash::FxHashMap;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::config::{KalshiRole, GAMMA_API_BASE};
use crate::cost::{kalshi_fee_total_cents, poly_fee_total_cents};
use crate::types::PriceCents;

/// Conservative fallback Polymarket fee (50 bps = 0.5%) if metadata unavailable
const POLY_FALLBACK_TAKER_BPS: u16 = 50;
const POLY_FALLBACK_MAKER_BPS: u16 = 20;

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
}
