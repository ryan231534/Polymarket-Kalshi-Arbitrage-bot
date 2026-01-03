//! High-performance order execution engine for arbitrage opportunities.
//!
//! This module handles concurrent order execution across both platforms,
//! position reconciliation, and automatic exposure management.

use anyhow::{anyhow, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use tracing::{error, info, warn};

use crate::circuit_breaker::CircuitBreaker;
use crate::config::{execution_threshold_cents, min_profit_cents};
use crate::cost::{analyze_all_in, AllInCfg};
use crate::kalshi::KalshiApiClient;
use crate::mismatch::handle_mismatch;
use crate::pnl::{ContractSide, PnLTracker, Side as PnLSide, Venue};
use crate::polymarket_clob::SharedAsyncClient;
use crate::position_tracker::{FillRecord, PositionChannel};
use crate::risk::{size_for_trade, BankrollConfig, SizingConfig};
use crate::types::{
    cents_to_price, kalshi_fee_cents, ArbType, FastExecutionRequest, GlobalState, MarketPair,
};

// =============================================================================
// EXECUTION ENGINE
// =============================================================================

/// High-precision monotonic clock for latency measurement and performance tracking
pub struct NanoClock {
    start: Instant,
}

impl NanoClock {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    #[inline(always)]
    pub fn now_ns(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }
}

impl Default for NanoClock {
    fn default() -> Self {
        Self::new()
    }
}

/// Core execution engine for processing arbitrage opportunities
pub struct ExecutionEngine {
    kalshi: Arc<KalshiApiClient>,
    poly_async: Arc<SharedAsyncClient>,
    state: Arc<GlobalState>,
    circuit_breaker: Arc<CircuitBreaker>,
    position_channel: PositionChannel,
    pnl_tracker: Arc<Mutex<PnLTracker>>,
    in_flight: Arc<[AtomicU64; 8]>,
    clock: NanoClock,
    pub dry_run: bool,
    test_mode: bool,
}

impl ExecutionEngine {
    pub fn new(
        kalshi: Arc<KalshiApiClient>,
        poly_async: Arc<SharedAsyncClient>,
        state: Arc<GlobalState>,
        circuit_breaker: Arc<CircuitBreaker>,
        position_channel: PositionChannel,
        pnl_tracker: Arc<Mutex<PnLTracker>>,
        dry_run: bool,
    ) -> Self {
        let test_mode = std::env::var("TEST_ARB")
            .map(|v| v == "1" || v == "true")
            .unwrap_or(false);

        Self {
            kalshi,
            poly_async,
            state,
            circuit_breaker,
            position_channel,
            pnl_tracker,
            in_flight: Arc::new(std::array::from_fn(|_| AtomicU64::new(0))),
            clock: NanoClock::new(),
            dry_run,
            test_mode,
        }
    }

    /// Process an execution request
    #[inline]
    pub async fn process(&self, req: FastExecutionRequest) -> Result<ExecutionResult> {
        let market_id = req.market_id;

        // Deduplication check (512 markets via 8x u64 bitmask)
        if market_id < 512 {
            let slot = (market_id / 64) as usize;
            let bit = market_id % 64;
            let mask = 1u64 << bit;
            let prev = self.in_flight[slot].fetch_or(mask, Ordering::AcqRel);
            if prev & mask != 0 {
                return Ok(ExecutionResult {
                    market_id,
                    success: false,
                    profit_cents: 0,
                    latency_ns: self.clock.now_ns() - req.detected_ns,
                    error: Some("Already in-flight"),
                });
            }
        }

        // Get market pair
        let market = self
            .state
            .get_by_id(market_id)
            .ok_or_else(|| anyhow!("Unknown market_id {}", market_id))?;

        let pair = market
            .pair
            .as_ref()
            .ok_or_else(|| anyhow!("No pair for market_id {}", market_id))?;

        // Calculate profit
        let profit_cents = req.profit_cents();
        if profit_cents < 1 {
            self.release_in_flight(market_id);
            return Ok(ExecutionResult {
                market_id,
                success: false,
                profit_cents: 0,
                latency_ns: self.clock.now_ns() - req.detected_ns,
                error: Some("Profit below threshold"),
            });
        }

        // === Dynamic Position Sizing (Risk-Based) ===
        // Size each arb attempt so worst-case loss per trade <= 10% of bankroll
        let sizing_cfg = SizingConfig::from_env();
        let bankroll_cfg = BankrollConfig::from_env();
        let sizing_decision = size_for_trade(&sizing_cfg, &bankroll_cfg);

        // Log the sizing decision
        info!(
            event = "sizing_decision",
            qty = sizing_decision.qty,
            bankroll_cents = sizing_decision.bankroll_cents,
            risk_budget_cents = sizing_decision.risk_budget_cents,
            worst_case_loss_cents = sizing_decision.worst_case_loss_cents,
            reason = sizing_decision.reason,
            "Position sizing"
        );

        // Skip trade if sizing returns qty=0
        if !sizing_decision.should_trade() {
            self.release_in_flight(market_id);
            return Ok(ExecutionResult {
                market_id,
                success: false,
                profit_cents: 0,
                latency_ns: self.clock.now_ns() - req.detected_ns,
                error: Some("Sizing: below_min"),
            });
        }

        // Calculate max contracts from size (min of both sides, limited by sizing)
        let liquidity_contracts = (req.yes_size.min(req.no_size) / 100) as i64;
        let mut max_contracts = liquidity_contracts.min(sizing_decision.qty as i64);

        // Safety: In test mode, cap position size at 10 contracts
        // Note: Polymarket enforces a $1 minimum order value. At 40¢ per contract,
        // a single contract ($0.40) would be rejected. Using 10 contracts ensures
        // we meet the minimum requirement at any reasonable price level.
        if self.test_mode && max_contracts > 10 {
            warn!(
                "[EXEC] ⚠️ TEST_MODE: Position size capped from {} to 10 contracts",
                max_contracts
            );
            max_contracts = 10;
        }

        if max_contracts < 1 {
            warn!(
                "[EXEC] Liquidity fail: {:?} | yes_size={}¢ no_size={}¢",
                req.arb_type, req.yes_size, req.no_size
            );
            self.release_in_flight(market_id);
            return Ok(ExecutionResult {
                market_id,
                success: false,
                profit_cents: 0,
                latency_ns: self.clock.now_ns() - req.detected_ns,
                error: Some("Insufficient liquidity"),
            });
        }

        // === All-In Cost Gate ===
        // Compute accurate fees (Kalshi + Poly), slippage, and expected profit
        // This is the FINAL profitability check before execution
        let all_in_cfg = AllInCfg::from_env();
        let exec_threshold = execution_threshold_cents();
        let min_profit = min_profit_cents();
        let analysis = analyze_all_in(&req, max_contracts as u32, &all_in_cfg, exec_threshold, min_profit);

        if !analysis.passes_threshold || !analysis.passes_min_profit {
            // Structured rejection event with full cost breakdown
            info!(
                event = "exec_rejected_all_in",
                market_id = market_id,
                arb_type = ?req.arb_type,
                contracts = analysis.contracts,
                all_in_cost_total = analysis.all_in_cost_total,
                cost_per_contract = analysis.cost_per_contract,
                profit_total = analysis.profit_total,
                profit_per_contract = analysis.profit_per_contract,
                exec_threshold_cents = analysis.exec_threshold_cents,
                min_profit_cents = analysis.min_profit_cents,
                passes_threshold = analysis.passes_threshold,
                passes_min_profit = analysis.passes_min_profit,
                slippage_cents_per_leg = all_in_cfg.slippage_cents_per_leg,
                kalshi_role = ?all_in_cfg.kalshi_role,
                poly_maker_bps = all_in_cfg.poly_maker_bps,
                poly_taker_bps = all_in_cfg.poly_taker_bps,
                "Rejected by all-in cost model"
            );

            self.release_in_flight(market_id);
            return Ok(ExecutionResult {
                market_id,
                success: false,
                profit_cents: 0,
                latency_ns: self.clock.now_ns() - req.detected_ns,
                error: Some(if !analysis.passes_threshold {
                    "All-in cost exceeds threshold"
                } else {
                    "Profit below minimum"
                }),
            });
        }

        // Circuit breaker check
        if let Err(_reason) = self
            .circuit_breaker
            .can_execute(&pair.pair_id, max_contracts)
            .await
        {
            self.release_in_flight(market_id);
            return Ok(ExecutionResult {
                market_id,
                success: false,
                profit_cents: 0,
                latency_ns: self.clock.now_ns() - req.detected_ns,
                error: Some("Circuit breaker"),
            });
        }

        let latency_to_exec = self.clock.now_ns() - req.detected_ns;

        // Structured execution attempt event
        info!(
            event = "exec_attempt",
            market_id = market_id,
            market_desc = %pair.description,
            arb_type = ?req.arb_type,
            yes_price_cents = req.yes_price,
            no_price_cents = req.no_price,
            yes_size_cents = req.yes_size,
            no_size_cents = req.no_size,
            profit_cents = profit_cents,
            contracts = max_contracts,
            latency_us = latency_to_exec / 1000,
            "Execution attempt"
        );

        info!(
            "[EXEC] 🎯 {} | {:?} y={}¢ n={}¢ | profit={}¢ | {}x | {}µs",
            pair.description,
            req.arb_type,
            req.yes_price,
            req.no_price,
            profit_cents,
            max_contracts,
            latency_to_exec / 1000
        );

        if self.dry_run {
            info!(
                "[EXEC] 🏃 DRY RUN - would execute {} contracts",
                max_contracts
            );
            self.release_in_flight_delayed(market_id);
            return Ok(ExecutionResult {
                market_id,
                success: true,
                profit_cents,
                latency_ns: latency_to_exec,
                error: Some("DRY_RUN"),
            });
        }

        // Execute both legs concurrently
        let result = self
            .execute_both_legs_async(&req, pair, max_contracts)
            .await;

        // Release in-flight after delay
        self.release_in_flight_delayed(market_id);

        match result {
            // Note: For same-platform arbs (PolyOnly/KalshiOnly), these are YES/NO fills, not platform fills
            Ok((yes_filled, no_filled, yes_cost, no_cost, yes_order_id, no_order_id)) => {
                let matched = yes_filled.min(no_filled);
                let success = matched > 0;
                let actual_profit = matched as i16 * 100 - (yes_cost + no_cost) as i16;

                // === Automatic exposure management for mismatched fills ===
                // If one leg fills more than the other, use the mismatch playbook
                // to flatten exposure with a deterministic unwind ladder
                if yes_filled != no_filled && (yes_filled > 0 || no_filled > 0) {
                    let excess = (yes_filled - no_filled).abs();
                    let (leg1_name, leg2_name) = match req.arb_type {
                        ArbType::PolyYesKalshiNo => ("P_yes", "K_no"),
                        ArbType::KalshiYesPolyNo => ("K_yes", "P_no"),
                        ArbType::PolyOnly => ("P_yes", "P_no"),
                        ArbType::KalshiOnly => ("K_yes", "K_no"),
                    };

                    // Structured fill mismatch event
                    warn!(
                        event = "fill_mismatch",
                        market_id = market_id,
                        arb_type = ?req.arb_type,
                        yes_filled = yes_filled,
                        no_filled = no_filled,
                        excess = excess,
                        leg1 = leg1_name,
                        leg2 = leg2_name,
                        "Fill mismatch detected, initiating unwind playbook"
                    );

                    warn!(
                        "[EXEC] ⚠️ Fill mismatch: {}={} {}={} (excess={})",
                        leg1_name, yes_filled, leg2_name, no_filled, excess
                    );

                    // Spawn mismatch handler in background with deterministic ladder unwind
                    let kalshi = self.kalshi.clone();
                    let poly_async = self.poly_async.clone();
                    let circuit_breaker = self.circuit_breaker.clone();
                    let pnl_tracker = self.pnl_tracker.clone();
                    let arb_type = req.arb_type;
                    let yes_price = req.yes_price;
                    let no_price = req.no_price;
                    let poly_yes_token = pair.poly_yes_token.to_string();
                    let poly_no_token = pair.poly_no_token.to_string();
                    let kalshi_ticker = pair.kalshi_market_ticker.to_string();
                    let market_key_for_pnl = pair.pair_id.clone();

                    tokio::spawn(async move {
                        handle_mismatch(
                            market_id,
                            arb_type,
                            yes_filled,
                            no_filled,
                            yes_price,
                            no_price,
                            &poly_yes_token,
                            &poly_no_token,
                            &kalshi_ticker,
                            &kalshi,
                            &poly_async,
                            &circuit_breaker,
                            &pnl_tracker,
                            &market_key_for_pnl,
                        )
                        .await;
                    });
                }

                if success {
                    self.circuit_breaker
                        .record_success(
                            &pair.pair_id,
                            matched,
                            matched,
                            actual_profit as f64 / 100.0,
                        )
                        .await;
                }

                if matched > 0 {
                    let (platform1, side1, platform2, side2) = match req.arb_type {
                        ArbType::PolyYesKalshiNo => ("polymarket", "yes", "kalshi", "no"),
                        ArbType::KalshiYesPolyNo => ("kalshi", "yes", "polymarket", "no"),
                        ArbType::PolyOnly => ("polymarket", "yes", "polymarket", "no"),
                        ArbType::KalshiOnly => ("kalshi", "yes", "kalshi", "no"),
                    };

                    // Structured execution result event
                    info!(
                        event = "exec_result",
                        market_id = market_id,
                        success = success,
                        arb_type = ?req.arb_type,
                        yes_filled = yes_filled,
                        no_filled = no_filled,
                        matched = matched,
                        yes_cost_cents = yes_cost,
                        no_cost_cents = no_cost,
                        actual_profit_cents = actual_profit,
                        latency_us = (self.clock.now_ns() - req.detected_ns) / 1000,
                        "Execution result"
                    );

                    // Calculate fees for P&L tracking
                    // Kalshi fee is based on price; Polymarket has no trading fee (only gas)
                    let (fee1, fee2) = match req.arb_type {
                        ArbType::PolyYesKalshiNo => {
                            // Leg 1: Poly YES (no fee), Leg 2: Kalshi NO (fee)
                            (0i64, kalshi_fee_cents(req.no_price) as i64 * matched)
                        }
                        ArbType::KalshiYesPolyNo => {
                            // Leg 1: Kalshi YES (fee), Leg 2: Poly NO (no fee)
                            (kalshi_fee_cents(req.yes_price) as i64 * matched, 0i64)
                        }
                        ArbType::PolyOnly => {
                            // Both legs on Polymarket (no fees)
                            (0i64, 0i64)
                        }
                        ArbType::KalshiOnly => {
                            // Both legs on Kalshi (both have fees)
                            (
                                kalshi_fee_cents(req.yes_price) as i64 * matched,
                                kalshi_fee_cents(req.no_price) as i64 * matched,
                            )
                        }
                    };

                    // Record fills in legacy position tracker
                    self.position_channel.record_fill(FillRecord::new(
                        &pair.pair_id,
                        &pair.description,
                        platform1,
                        side1,
                        matched as f64,
                        yes_cost as f64 / 100.0 / yes_filled.max(1) as f64,
                        fee1 as f64 / 100.0,
                        &yes_order_id,
                    ));
                    self.position_channel.record_fill(FillRecord::new(
                        &pair.pair_id,
                        &pair.description,
                        platform2,
                        side2,
                        matched as f64,
                        no_cost as f64 / 100.0 / no_filled.max(1) as f64,
                        fee2 as f64 / 100.0,
                        &no_order_id,
                    ));

                    // Record fills in P&L tracker (integer cents)
                    {
                        let mut pnl: tokio::sync::MutexGuard<'_, PnLTracker> =
                            self.pnl_tracker.lock().await;
                        let venue1 = if platform1 == "kalshi" {
                            Venue::Kalshi
                        } else {
                            Venue::Polymarket
                        };
                        let venue2 = if platform2 == "kalshi" {
                            Venue::Kalshi
                        } else {
                            Venue::Polymarket
                        };
                        let contract_side1 = if side1 == "yes" {
                            ContractSide::Yes
                        } else {
                            ContractSide::No
                        };
                        let contract_side2 = if side2 == "yes" {
                            ContractSide::Yes
                        } else {
                            ContractSide::No
                        };

                        // Calculate average price per contract in cents
                        let price1_cents = if yes_filled > 0 {
                            yes_cost / yes_filled
                        } else {
                            req.yes_price as i64
                        };
                        let price2_cents = if no_filled > 0 {
                            no_cost / no_filled
                        } else {
                            req.no_price as i64
                        };

                        pnl.on_fill(
                            venue1,
                            &pair.pair_id,
                            &yes_order_id,
                            PnLSide::Buy,
                            contract_side1,
                            matched,
                            price1_cents,
                            fee1 / matched.max(1), // fee per contract
                        );

                        // Structured P&L fill event
                        info!(
                            event = "pnl_fill",
                            venue = ?venue1,
                            market_key = %pair.pair_id,
                            side = ?PnLSide::Buy,
                            contract_side = ?contract_side1,
                            qty = matched,
                            price_cents = price1_cents,
                            fee_cents = fee1 / matched.max(1),
                            order_id = %yes_order_id,
                            "P&L fill recorded"
                        );

                        pnl.on_fill(
                            venue2,
                            &pair.pair_id,
                            &no_order_id,
                            PnLSide::Buy,
                            contract_side2,
                            matched,
                            price2_cents,
                            fee2 / matched.max(1), // fee per contract
                        );

                        // Structured P&L fill event
                        info!(
                            event = "pnl_fill",
                            venue = ?venue2,
                            market_key = %pair.pair_id,
                            side = ?PnLSide::Buy,
                            contract_side = ?contract_side2,
                            qty = matched,
                            price_cents = price2_cents,
                            fee_cents = fee2 / matched.max(1),
                            order_id = %no_order_id,
                            "P&L fill recorded"
                        );
                    }
                }

                Ok(ExecutionResult {
                    market_id,
                    success,
                    profit_cents: actual_profit,
                    latency_ns: self.clock.now_ns() - req.detected_ns,
                    error: if success {
                        None
                    } else {
                        Some("Partial/no fill")
                    },
                })
            }
            Err(_e) => {
                self.circuit_breaker.record_error().await;
                Ok(ExecutionResult {
                    market_id,
                    success: false,
                    profit_cents: 0,
                    latency_ns: self.clock.now_ns() - req.detected_ns,
                    error: Some("Execution failed"),
                })
            }
        }
    }

    async fn execute_both_legs_async(
        &self,
        req: &FastExecutionRequest,
        pair: &MarketPair,
        contracts: i64,
    ) -> Result<(i64, i64, i64, i64, String, String)> {
        match req.arb_type {
            // === CROSS-PLATFORM: Poly YES + Kalshi NO ===
            ArbType::PolyYesKalshiNo => {
                let kalshi_fut = self.kalshi.buy_ioc(
                    &pair.kalshi_market_ticker,
                    "no",
                    req.no_price as i64,
                    contracts,
                );
                let poly_fut = self.poly_async.buy_fak(
                    &pair.poly_yes_token,
                    cents_to_price(req.yes_price),
                    contracts as f64,
                );
                let (kalshi_res, poly_res) = tokio::join!(kalshi_fut, poly_fut);
                self.extract_cross_results(kalshi_res, poly_res)
            }

            // === CROSS-PLATFORM: Kalshi YES + Poly NO ===
            ArbType::KalshiYesPolyNo => {
                let kalshi_fut = self.kalshi.buy_ioc(
                    &pair.kalshi_market_ticker,
                    "yes",
                    req.yes_price as i64,
                    contracts,
                );
                let poly_fut = self.poly_async.buy_fak(
                    &pair.poly_no_token,
                    cents_to_price(req.no_price),
                    contracts as f64,
                );
                let (kalshi_res, poly_res) = tokio::join!(kalshi_fut, poly_fut);
                self.extract_cross_results(kalshi_res, poly_res)
            }

            // === SAME-PLATFORM: Poly YES + Poly NO ===
            ArbType::PolyOnly => {
                let yes_fut = self.poly_async.buy_fak(
                    &pair.poly_yes_token,
                    cents_to_price(req.yes_price),
                    contracts as f64,
                );
                let no_fut = self.poly_async.buy_fak(
                    &pair.poly_no_token,
                    cents_to_price(req.no_price),
                    contracts as f64,
                );
                let (yes_res, no_res) = tokio::join!(yes_fut, no_fut);
                self.extract_poly_only_results(yes_res, no_res)
            }

            // === SAME-PLATFORM: Kalshi YES + Kalshi NO ===
            ArbType::KalshiOnly => {
                let yes_fut = self.kalshi.buy_ioc(
                    &pair.kalshi_market_ticker,
                    "yes",
                    req.yes_price as i64,
                    contracts,
                );
                let no_fut = self.kalshi.buy_ioc(
                    &pair.kalshi_market_ticker,
                    "no",
                    req.no_price as i64,
                    contracts,
                );
                let (yes_res, no_res) = tokio::join!(yes_fut, no_fut);
                self.extract_kalshi_only_results(yes_res, no_res)
            }
        }
    }

    /// Extract results from cross-platform execution
    fn extract_cross_results(
        &self,
        kalshi_res: Result<crate::kalshi::KalshiOrderResponse>,
        poly_res: Result<crate::polymarket_clob::PolyFillAsync>,
    ) -> Result<(i64, i64, i64, i64, String, String)> {
        let (kalshi_filled, kalshi_cost, kalshi_order_id) = match kalshi_res {
            Ok(resp) => {
                let filled = resp.order.filled_count();
                let cost = resp.order.taker_fill_cost.unwrap_or(0)
                    + resp.order.maker_fill_cost.unwrap_or(0);
                (filled, cost, resp.order.order_id)
            }
            Err(e) => {
                warn!("[EXEC] Kalshi failed: {}", e);
                (0, 0, String::new())
            }
        };

        let (poly_filled, poly_cost, poly_order_id) = match poly_res {
            Ok(fill) => (
                (fill.filled_size as i64),
                (fill.fill_cost * 100.0) as i64,
                fill.order_id,
            ),
            Err(e) => {
                warn!("[EXEC] Poly failed: {}", e);
                (0, 0, String::new())
            }
        };

        Ok((
            kalshi_filled,
            poly_filled,
            kalshi_cost,
            poly_cost,
            kalshi_order_id,
            poly_order_id,
        ))
    }

    /// Extract results from Poly-only execution (same-platform)
    fn extract_poly_only_results(
        &self,
        yes_res: Result<crate::polymarket_clob::PolyFillAsync>,
        no_res: Result<crate::polymarket_clob::PolyFillAsync>,
    ) -> Result<(i64, i64, i64, i64, String, String)> {
        let (yes_filled, yes_cost, yes_order_id) = match yes_res {
            Ok(fill) => (
                (fill.filled_size as i64),
                (fill.fill_cost * 100.0) as i64,
                fill.order_id,
            ),
            Err(e) => {
                warn!("[EXEC] Poly YES failed: {}", e);
                (0, 0, String::new())
            }
        };

        let (no_filled, no_cost, no_order_id) = match no_res {
            Ok(fill) => (
                (fill.filled_size as i64),
                (fill.fill_cost * 100.0) as i64,
                fill.order_id,
            ),
            Err(e) => {
                warn!("[EXEC] Poly NO failed: {}", e);
                (0, 0, String::new())
            }
        };

        // For same-platform, return YES as "kalshi" slot and NO as "poly" slot
        // This keeps the existing result handling logic working
        Ok((
            yes_filled,
            no_filled,
            yes_cost,
            no_cost,
            yes_order_id,
            no_order_id,
        ))
    }

    /// Extract results from Kalshi-only execution (same-platform)
    fn extract_kalshi_only_results(
        &self,
        yes_res: Result<crate::kalshi::KalshiOrderResponse>,
        no_res: Result<crate::kalshi::KalshiOrderResponse>,
    ) -> Result<(i64, i64, i64, i64, String, String)> {
        let (yes_filled, yes_cost, yes_order_id) = match yes_res {
            Ok(resp) => {
                let filled = resp.order.filled_count();
                let cost = resp.order.taker_fill_cost.unwrap_or(0)
                    + resp.order.maker_fill_cost.unwrap_or(0);
                (filled, cost, resp.order.order_id)
            }
            Err(e) => {
                warn!("[EXEC] Kalshi YES failed: {}", e);
                (0, 0, String::new())
            }
        };

        let (no_filled, no_cost, no_order_id) = match no_res {
            Ok(resp) => {
                let filled = resp.order.filled_count();
                let cost = resp.order.taker_fill_cost.unwrap_or(0)
                    + resp.order.maker_fill_cost.unwrap_or(0);
                (filled, cost, resp.order.order_id)
            }
            Err(e) => {
                warn!("[EXEC] Kalshi NO failed: {}", e);
                (0, 0, String::new())
            }
        };

        // For same-platform, return YES as "kalshi" slot and NO as "poly" slot
        Ok((
            yes_filled,
            no_filled,
            yes_cost,
            no_cost,
            yes_order_id,
            no_order_id,
        ))
    }

    #[inline(always)]
    fn release_in_flight(&self, market_id: u16) {
        if market_id < 512 {
            let slot = (market_id / 64) as usize;
            let bit = market_id % 64;
            let mask = !(1u64 << bit);
            self.in_flight[slot].fetch_and(mask, Ordering::Release);
        }
    }

    fn release_in_flight_delayed(&self, market_id: u16) {
        if market_id < 512 {
            let in_flight = self.in_flight.clone();
            let slot = (market_id / 64) as usize;
            let bit = market_id % 64;
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                let mask = !(1u64 << bit);
                in_flight[slot].fetch_and(mask, Ordering::Release);
            });
        }
    }
}

/// Result of an execution attempt
#[derive(Debug, Clone, Copy)]
pub struct ExecutionResult {
    /// Market identifier
    pub market_id: u16,
    /// Whether execution was successful
    pub success: bool,
    /// Realized profit in cents
    pub profit_cents: i16,
    /// Total latency from detection to completion in nanoseconds
    pub latency_ns: u64,
    /// Error message if execution failed
    pub error: Option<&'static str>,
}

/// Create a new execution request channel with bounded capacity
pub fn create_execution_channel() -> (
    mpsc::Sender<FastExecutionRequest>,
    mpsc::Receiver<FastExecutionRequest>,
) {
    mpsc::channel(256)
}

/// Main execution event loop - processes arbitrage opportunities as they arrive
pub async fn run_execution_loop(
    mut rx: mpsc::Receiver<FastExecutionRequest>,
    engine: Arc<ExecutionEngine>,
) {
    info!(
        "[EXEC] Execution engine started (dry_run={})",
        engine.dry_run
    );

    while let Some(req) = rx.recv().await {
        let engine = engine.clone();

        // Process immediately in spawned task
        tokio::spawn(async move {
            match engine.process(req).await {
                Ok(result) if result.success => {
                    info!(
                        "[EXEC] ✅ market_id={} profit={}¢ latency={}µs",
                        result.market_id,
                        result.profit_cents,
                        result.latency_ns / 1000
                    );
                }
                Ok(result) => {
                    if result.error != Some("Already in-flight") {
                        warn!(
                            "[EXEC] ⚠️ market_id={}: {:?}",
                            result.market_id, result.error
                        );
                    }
                }
                Err(e) => {
                    error!("[EXEC] ❌ Error: {}", e);
                }
            }
        });
    }

    info!("[EXEC] Execution engine stopped");
}
