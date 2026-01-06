//! Prediction Market Arbitrage Trading System
//!
//! A high-performance, production-ready arbitrage trading system for cross-platform
//! prediction markets. This system monitors price discrepancies between Kalshi and
//! Polymarket, executing risk-free arbitrage opportunities in real-time.
//!
//! ## Strategy
//!
//! The core arbitrage strategy exploits the fundamental property of prediction markets:
//! YES + NO = $1.00 (guaranteed). Arbitrage opportunities exist when:
//!
//! ```
//! Best YES ask (Platform A) + Best NO ask (Platform B) < $1.00
//! ```
//!
//! ## Architecture
//!
//! - **Real-time price monitoring** via WebSocket connections to both platforms
//! - **Lock-free orderbook cache** using atomic operations for zero-copy updates
//! - **SIMD-accelerated arbitrage detection** for sub-millisecond latency
//! - **Concurrent order execution** with automatic position reconciliation
//! - **Circuit breaker protection** with configurable risk limits
//! - **Market discovery system** with intelligent caching and incremental updates
//! - **Live sports discovery** with cross-exchange joining for real-time sports markets

mod cache;
mod circuit_breaker;
mod config;
mod cost;
mod discovery;
mod execution;
mod fees;
mod kalshi;
mod live_discovery;
mod logging;
mod metrics;
mod mismatch;
mod pnl;
mod polymarket;
mod polymarket_clob;
mod position_tracker;
mod prefetch;
mod retry;
mod risk;
mod types;

use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, info_span, warn};

use cache::TeamCache;
use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use config::{
    enabled_leagues_from_env, format_threshold_cents, kalshi_fee_role, profit_threshold_cents,
    threshold_profit_percent, ENABLED_LEAGUES, WS_RECONNECT_DELAY_SECS,
};
use discovery::DiscoveryClient;
use execution::{create_execution_channel, run_execution_loop, ExecutionEngine};
use fees::{Exchange, FeeModel};
use kalshi::{KalshiApiClient, KalshiConfig};
use live_discovery::{
    matched_markets_to_pairs, LeagueSpec, LiveDiscoveryConfig, LiveDiscoveryEngine,
};
use metrics::Metrics;
use pnl::PnLTracker;
use polymarket::{run_ws as run_poly_ws, GammaClient};
use polymarket_clob::{PolymarketAsyncClient, PreparedCreds, SharedAsyncClient};
use position_tracker::{create_position_channel, position_writer_loop, PositionTracker};
use types::{GlobalState, MarketPair};

/// Polymarket CLOB API host
const POLY_CLOB_HOST: &str = "https://clob.polymarket.com";
/// Polygon chain ID
const POLYGON_CHAIN_ID: u64 = 137;

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env FIRST before any config/logging initialization
    // This ensures all environment variables from .env are available for OnceLock caching
    dotenvy::dotenv().ok();

    // Initialize structured logging with file rotation
    // Keep guard alive for program lifetime to ensure non-blocking writer flushes
    let _log_guard = logging::init_logging();
    let run_id = logging::get_run_id();

    // Initialize metrics
    let metrics = Arc::new(Metrics::new());
    info!("📊 Metrics system initialized");

    // Get canonical threshold (single source of truth)
    let threshold_cents = profit_threshold_cents();
    let threshold_display = format_threshold_cents(threshold_cents);
    let profit_pct = threshold_profit_percent(threshold_cents);

    // Get leagues from environment or use default (all leagues)
    let enabled_leagues = enabled_leagues_from_env();
    let leagues_to_monitor: Vec<String> = if enabled_leagues.is_empty() {
        ENABLED_LEAGUES.iter().map(|s| s.to_string()).collect()
    } else {
        enabled_leagues.clone()
    };

    // Check for dry run mode
    let dry_run = std::env::var("DRY_RUN")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(true);

    // Check for discovery-only mode (exits after discovery, no WS/execution)
    let discovery_only = std::env::var("DISCOVERY_ONLY")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false);

    // Create root span for the entire application lifetime
    let root_span = info_span!(
        "arb_bot",
        run_id = %run_id,
        version = "2.0",
        dry_run = dry_run,
        discovery_only = discovery_only,
        leagues = ?leagues_to_monitor,
        threshold_cents = threshold_cents,
    );

    // Enter the root span for the duration of main
    let _enter = root_span.enter();

    info!("🚀 Prediction Market Arbitrage System v2.0");

    if discovery_only {
        info!("   Mode: DISCOVERY ONLY (will exit after discovery)");
    }

    info!(
        "   Profit threshold: <{} ({:.1}% minimum profit)",
        threshold_display, profit_pct
    );

    if leagues_to_monitor.is_empty() {
        info!("   Monitored leagues: ALL");
    } else {
        info!("   Monitored leagues: {:?}", leagues_to_monitor);
    }

    if dry_run {
        info!("   Mode: DRY RUN (set DRY_RUN=0 to execute)");
    } else {
        warn!("   Mode: LIVE EXECUTION");
    }

    // Load Kalshi credentials
    let kalshi_config = KalshiConfig::from_env()?;
    info!("[KALSHI] API key loaded");

    // Load Polymarket credentials
    let poly_private_key = std::env::var("POLY_PRIVATE_KEY").context("POLY_PRIVATE_KEY not set")?;
    let poly_funder =
        std::env::var("POLY_FUNDER").context("POLY_FUNDER not set (your wallet address)")?;

    // Create async Polymarket client and derive API credentials
    info!("[POLYMARKET] Creating async client and deriving API credentials...");
    let poly_async_client = PolymarketAsyncClient::new(
        POLY_CLOB_HOST,
        POLYGON_CHAIN_ID,
        &poly_private_key,
        &poly_funder,
    )?;
    let api_creds = poly_async_client.derive_api_key(0).await?;
    let prepared_creds = PreparedCreds::from_api_creds(&api_creds)?;
    let poly_async = Arc::new(SharedAsyncClient::new(
        poly_async_client,
        prepared_creds,
        POLYGON_CHAIN_ID,
    ));

    // Load neg_risk cache from Python script output
    match poly_async.load_cache(".clob_market_cache.json") {
        Ok(count) => info!("[POLYMARKET] Loaded {} neg_risk entries from cache", count),
        Err(e) => warn!("[POLYMARKET] Could not load neg_risk cache: {}", e),
    }

    info!("[POLYMARKET] Client ready for {}", &poly_funder[..10]);

    // Load team code mapping cache
    let team_cache = TeamCache::load();
    info!("📂 Loaded {} team code mappings", team_cache.len());

    // Create Kalshi API client
    let kalshi_api = Arc::new(KalshiApiClient::new(kalshi_config));

    // Run discovery (with caching support)
    let force_discovery = std::env::var("FORCE_DISCOVERY")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false);

    info!(
        "🔍 Market discovery{}...",
        if force_discovery {
            " (forced refresh)"
        } else {
            ""
        }
    );

    metrics.discovery_runs.inc();

    // === Live Discovery for Sports Leagues ===
    // Check if any monitored leagues are sports leagues
    let live_config = LiveDiscoveryConfig::from_env();
    let sports_leagues: Vec<LeagueSpec> = leagues_to_monitor
        .iter()
        .filter_map(|code| LeagueSpec::from_code(code))
        .filter(|spec| spec.is_sports)
        .collect();

    let has_sports_leagues = !sports_leagues.is_empty();
    let mut live_pairs: Vec<MarketPair> = Vec::new();

    if has_sports_leagues && live_config.live_join_enabled {
        info!(
            "🏀 Live sports discovery enabled for {} leagues: {:?}",
            sports_leagues.len(),
            sports_leagues
                .iter()
                .map(|s| &s.league_code)
                .collect::<Vec<_>>()
        );

        // Create Gamma client for Polymarket discovery
        let gamma = Arc::new(GammaClient::new());
        let live_engine = LiveDiscoveryEngine::with_config(
            kalshi_api.clone(),
            gamma.clone(),
            live_config.clone(),
        );

        // Run live discovery for all sports leagues
        let live_results = live_engine.discover_all_leagues(&sports_leagues).await?;

        // Aggregate results
        let mut total_kalshi_events = 0;
        let mut total_poly_events = 0;
        let mut total_matched = 0;

        for result in &live_results {
            total_kalshi_events += result.kalshi_discovered_count;
            total_poly_events += result.poly_discovered_count;
            total_matched += result.matched_markets.len();

            // Convert matched markets to MarketPairs
            let pairs = matched_markets_to_pairs(&result.matched_markets);
            live_pairs.extend(pairs);
        }

        info!(
            event = "live_discovery_summary",
            kalshi_open = total_kalshi_events,
            poly_active = total_poly_events,
            matched = total_matched,
            pairs_generated = live_pairs.len(),
            "📊 Live sports discovery: Kalshi={} open, Poly={} active, {} matched → {} pairs",
            total_kalshi_events,
            total_poly_events,
            total_matched,
            live_pairs.len()
        );

        // Log 3 sample pairs
        for (i, pair) in live_pairs.iter().take(3).enumerate() {
            info!(
                event = "live_pair_sample",
                index = i,
                kalshi_ticker = %pair.kalshi_market_ticker,
                poly_yes_token = %pair.poly_yes_token,
                "  Sample {}: {} (K:{} P:{}...)",
                i + 1,
                pair.description,
                pair.kalshi_market_ticker,
                &pair.poly_yes_token[..pair.poly_yes_token.len().min(16)]
            );
        }
    }

    // === Standard Discovery (slug-based) ===
    // Run for non-sports leagues, or as fallback if live yields 0 and fallback is enabled
    let non_sports_leagues: Vec<&str> = leagues_to_monitor
        .iter()
        .map(|s| s.as_str())
        .filter(|code| !LeagueSpec::is_sports_league(code))
        .collect();

    let need_standard_discovery =
        !non_sports_leagues.is_empty() || (live_pairs.is_empty() && live_config.fallback_slug_map);

    let standard_pairs = if need_standard_discovery {
        let discovery =
            DiscoveryClient::new(KalshiApiClient::new(KalshiConfig::from_env()?), team_cache);

        // Use original leagues for fallback, or non-sports for normal operation
        let discovery_leagues: Vec<&str> = if live_pairs.is_empty() && live_config.fallback_slug_map
        {
            warn!("⚠️ Live discovery yielded 0 pairs, falling back to slug-based discovery");
            leagues_to_monitor.iter().map(|s| s.as_str()).collect()
        } else {
            non_sports_leagues.clone()
        };

        if discovery_leagues.is_empty() {
            vec![]
        } else {
            let result = if force_discovery {
                discovery.discover_all_force(&discovery_leagues).await
            } else {
                discovery.discover_all(&discovery_leagues).await
            };

            if !result.errors.is_empty() {
                for err in &result.errors {
                    warn!("   ⚠️ Standard discovery error: {}", err);
                }
                metrics.discovery_errors.add(result.errors.len() as u64);
            }

            result.pairs
        }
    } else {
        vec![]
    };

    // === Merge Results ===
    let mut all_pairs: Vec<MarketPair> = Vec::new();
    all_pairs.extend(live_pairs);
    all_pairs.extend(standard_pairs);

    metrics.markets_discovered.set(all_pairs.len() as i64);

    info!("📊 Market discovery complete:");
    info!("   - Total matched market pairs: {}", all_pairs.len());

    if all_pairs.is_empty() {
        error!("No market pairs found!");
        return Ok(());
    }

    // Display discovered market pairs
    info!("📋 Discovered market pairs:");
    for pair in &all_pairs {
        info!(
            "   ✅ {} | {} | Kalshi: {}",
            pair.description, pair.market_type, pair.kalshi_market_ticker
        );
    }

    // === DISCOVERY_ONLY MODE: Exit after printing results ===
    if discovery_only {
        info!("🔍 DISCOVERY_ONLY mode: printing detailed results and exiting");
        info!(
            event = "discovery_only_summary",
            total_pairs = all_pairs.len(),
            "=== Discovery Summary ==="
        );

        // Print 3 sample pairs with full token details
        info!("📋 Sample matched pairs with subscription IDs:");
        for (i, pair) in all_pairs.iter().take(3).enumerate() {
            info!(
                event = "discovery_only_sample",
                index = i,
                kalshi_ticker = %pair.kalshi_market_ticker,
                poly_yes_token = %pair.poly_yes_token,
                poly_no_token = %pair.poly_no_token,
                league = %pair.league,
                market_type = %pair.market_type,
                "  [{}/{}] {} | K:{} | P_yes:{} | P_no:{}",
                i + 1,
                all_pairs.len().min(3),
                pair.description,
                pair.kalshi_market_ticker,
                pair.poly_yes_token,
                pair.poly_no_token
            );
        }

        info!("✅ Discovery complete. Exiting without starting WS/execution.");
        return Ok(());
    }

    // Replace result.pairs with all_pairs for downstream usage
    let result_pairs = all_pairs;

    // Build global state
    let state = Arc::new({
        let mut s = GlobalState::new();
        for pair in result_pairs {
            s.add_pair(pair);
        }
        info!(
            "📡 Global state initialized: tracking {} markets",
            s.market_count()
        );
        s
    });

    // === Log WS subscription details ===
    {
        let market_count = state.market_count();
        let kalshi_tickers: Vec<String> = state
            .markets
            .iter()
            .take(market_count)
            .filter_map(|m| m.pair.as_ref().map(|p| p.kalshi_market_ticker.to_string()))
            .collect();
        let poly_tokens: Vec<String> = state
            .markets
            .iter()
            .take(market_count)
            .filter_map(|m| m.pair.as_ref())
            .flat_map(|p| vec![p.poly_yes_token.to_string(), p.poly_no_token.to_string()])
            .collect();

        info!(
            event = "ws_subscriptions_prepared",
            kalshi_tickers_count = kalshi_tickers.len(),
            poly_tokens_count = poly_tokens.len(),
            "📡 WebSocket subscriptions: {} Kalshi tickers, {} Polymarket tokens",
            kalshi_tickers.len(),
            poly_tokens.len()
        );

        // Log sample subscriptions
        if let Some(first_market) = state
            .markets
            .iter()
            .take(market_count)
            .find_map(|m| m.pair.as_ref())
        {
            info!(
                event = "ws_subscription_sample",
                kalshi_ticker = %first_market.kalshi_market_ticker,
                poly_yes_token = %first_market.poly_yes_token,
                poly_no_token = %first_market.poly_no_token,
                description = %first_market.description,
                "  Sample: {} (K:{} P_yes:{}...)",
                first_market.description,
                first_market.kalshi_market_ticker,
                &first_market.poly_yes_token[..first_market.poly_yes_token.len().min(20)]
            );
        }
    }

    // Initialize execution infrastructure
    let (exec_tx, exec_rx) = create_execution_channel();
    let circuit_breaker = Arc::new(CircuitBreaker::new(CircuitBreakerConfig::from_env()));

    let position_tracker = Arc::new(RwLock::new(PositionTracker::new()));
    let (position_channel, position_rx) = create_position_channel();

    tokio::spawn(position_writer_loop(position_rx, position_tracker));

    // Initialize P&L tracker
    let pnl_dir = std::env::var("PNL_DIR").unwrap_or_else(|_| "./data".to_string());
    let pnl_flush_secs: u64 = std::env::var("PNL_FLUSH_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let pnl_tracker = Arc::new(tokio::sync::Mutex::new(PnLTracker::new(&pnl_dir, dry_run)));
    info!(
        "📊 P&L tracker initialized: dir={}, flush_interval={}s, dry_run={}",
        pnl_dir, pnl_flush_secs, dry_run
    );

    // Initialize fee model
    let fee_model = Arc::new(FeeModel::new(kalshi_fee_role()));
    info!(
        "💰 Fee model initialized with Kalshi role={:?}",
        kalshi_fee_role()
    );

    // Note: threshold_cents already initialized at startup (line ~70)
    info!(
        "   Execution threshold: {} (same as profit threshold)",
        threshold_display
    );

    let engine = Arc::new(ExecutionEngine::new(
        kalshi_api.clone(),
        poly_async,
        state.clone(),
        circuit_breaker.clone(),
        position_channel,
        pnl_tracker.clone(),
        fee_model.clone(),
        metrics.clone(),
        dry_run,
    ));

    let exec_handle = tokio::spawn(run_execution_loop(exec_rx, engine));

    // === TEST MODE: Synthetic arbitrage injection ===
    // TEST_ARB=1 to enable, TEST_ARB_TYPE=poly_yes_kalshi_no|kalshi_yes_poly_no|poly_only|kalshi_only
    let test_arb = std::env::var("TEST_ARB")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false);
    if test_arb {
        let test_state = state.clone();
        let test_exec_tx = exec_tx.clone();
        let test_dry_run = dry_run;

        // Parse arb type from environment (default: poly_yes_kalshi_no)
        let arb_type_str =
            std::env::var("TEST_ARB_TYPE").unwrap_or_else(|_| "poly_yes_kalshi_no".to_string());

        tokio::spawn(async move {
            use types::{ArbType, FastExecutionRequest};

            // Wait for WebSocket connections to establish and populate orderbooks
            info!("[TEST] Injecting synthetic arbitrage opportunity in 10 seconds...");
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

            // Parse arb type
            let arb_type = match arb_type_str.to_lowercase().as_str() {
                "poly_yes_kalshi_no" | "pykn" | "0" => ArbType::PolyYesKalshiNo,
                "kalshi_yes_poly_no" | "kypn" | "1" => ArbType::KalshiYesPolyNo,
                "poly_only" | "poly" | "2" => ArbType::PolyOnly,
                "kalshi_only" | "kalshi" | "3" => ArbType::KalshiOnly,
                _ => {
                    warn!(
                        "[TEST] Unknown TEST_ARB_TYPE='{}', defaulting to PolyYesKalshiNo",
                        arb_type_str
                    );
                    warn!("[TEST] Valid values: poly_yes_kalshi_no, kalshi_yes_poly_no, poly_only, kalshi_only");
                    ArbType::PolyYesKalshiNo
                }
            };

            // Set prices based on arb type for realistic test scenarios
            let (yes_price, no_price, description) = match arb_type {
                ArbType::PolyYesKalshiNo => {
                    (40, 50, "P_yes=40¢ + K_no=50¢ + fee≈2¢ = 92¢ → 8¢ profit")
                }
                ArbType::KalshiYesPolyNo => {
                    (40, 50, "K_yes=40¢ + P_no=50¢ + fee≈2¢ = 92¢ → 8¢ profit")
                }
                ArbType::PolyOnly => (
                    48,
                    50,
                    "P_yes=48¢ + P_no=50¢ + fee=0¢ = 98¢ → 2¢ profit (NO FEES!)",
                ),
                ArbType::KalshiOnly => (
                    44,
                    44,
                    "K_yes=44¢ + K_no=44¢ + fee≈4¢ = 92¢ → 8¢ profit (DOUBLE FEES)",
                ),
            };

            // Find first market with valid state
            let market_count = test_state.market_count();
            for market_id in 0..market_count {
                if let Some(market) = test_state.get_by_id(market_id as u16) {
                    if let Some(pair) = &market.pair {
                        // SIZE: 1000 cents = 10 contracts (Poly $1 min requires ~3 contracts at 40¢)
                        let fake_req = FastExecutionRequest {
                            market_id: market_id as u16,
                            yes_price,
                            no_price,
                            yes_size: 1000, // 1000¢ = 10 contracts
                            no_size: 1000,  // 1000¢ = 10 contracts
                            arb_type,
                            detected_ns: 0,
                        };

                        warn!(
                            "[TEST] 🧪 Injecting synthetic {:?} arbitrage for: {}",
                            arb_type, pair.description
                        );
                        warn!("[TEST]    Scenario: {}", description);
                        warn!("[TEST]    Position size capped to 10 contracts for safety");
                        warn!("[TEST]    Execution mode: DRY_RUN={}", test_dry_run);

                        if let Err(e) = test_exec_tx.send(fake_req).await {
                            error!("[TEST] Failed to send fake arb: {}", e);
                        }
                        break;
                    }
                }
            }
        });
    }

    // Initialize Kalshi WebSocket connection (config reused on reconnects)
    let kalshi_state = state.clone();
    let kalshi_exec_tx = exec_tx.clone();
    let kalshi_threshold = threshold_cents;
    let kalshi_ws_config = KalshiConfig::from_env()?;
    let kalshi_handle = tokio::spawn(async move {
        loop {
            match kalshi::run_ws(
                &kalshi_ws_config,
                kalshi_state.clone(),
                kalshi_exec_tx.clone(),
                kalshi_threshold,
            )
            .await
            {
                Ok(_) => {
                    info!(
                        event = "ws_disconnected",
                        venue = "kalshi",
                        "WebSocket clean disconnect"
                    );
                }
                Err(e) => {
                    error!(event = "ws_error", venue = "kalshi", error = %e, "WebSocket error, reconnecting");
                    error!("[KALSHI] WebSocket disconnected: {} - reconnecting...", e);
                }
            }
            info!(
                event = "ws_reconnect",
                venue = "kalshi",
                delay_secs = WS_RECONNECT_DELAY_SECS,
                "Reconnecting WebSocket"
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(WS_RECONNECT_DELAY_SECS)).await;
        }
    });

    // Initialize Polymarket WebSocket connection
    let poly_state = state.clone();
    let poly_exec_tx = exec_tx.clone();
    let poly_threshold = threshold_cents;
    let poly_handle = tokio::spawn(async move {
        loop {
            match polymarket::run_ws(poly_state.clone(), poly_exec_tx.clone(), poly_threshold).await
            {
                Ok(_) => {
                    info!(
                        event = "ws_disconnected",
                        venue = "polymarket",
                        "WebSocket clean disconnect"
                    );
                }
                Err(e) => {
                    error!(event = "ws_error", venue = "polymarket", error = %e, "WebSocket error, reconnecting");
                    error!(
                        "[POLYMARKET] WebSocket disconnected: {} - reconnecting...",
                        e
                    );
                }
            }
            info!(
                event = "ws_reconnect",
                venue = "polymarket",
                delay_secs = WS_RECONNECT_DELAY_SECS,
                "Reconnecting WebSocket"
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(WS_RECONNECT_DELAY_SECS)).await;
        }
    });

    // Periodic market rediscovery to pick up new opportunities
    // Note: This replaces the entire state with newly discovered markets
    // WebSocket handlers will automatically reconnect and repopulate orderbooks
    let rediscovery_state_ref = state.clone();
    let rediscovery_leagues = leagues_to_monitor.clone();
    let rediscovery_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(3600)); // 1 hour
        interval.tick().await; // Skip first tick (already did discovery at startup)

        loop {
            interval.tick().await;
            info!("🔄 Running periodic market rediscovery...");

            // Reload team cache and create fresh Kalshi client for each rediscovery
            let team_cache = TeamCache::load();
            let kalshi_config = match KalshiConfig::from_env() {
                Ok(cfg) => cfg,
                Err(e) => {
                    error!("Failed to load Kalshi config for rediscovery: {}", e);
                    continue;
                }
            };

            let discovery = DiscoveryClient::new(KalshiApiClient::new(kalshi_config), team_cache);

            let leagues_refs: Vec<&str> = rediscovery_leagues.iter().map(|s| s.as_str()).collect();
            match discovery.discover_all_force(&leagues_refs).await {
                result if !result.pairs.is_empty() => {
                    let old_count = rediscovery_state_ref.market_count();

                    info!(
                        "📊 Rediscovery complete: found {} market pairs (previous: {})",
                        result.pairs.len(),
                        old_count
                    );

                    // Log newly discovered markets
                    for pair in &result.pairs {
                        info!(
                            "   ✅ {} | {} | Kalshi: {}",
                            pair.description, pair.market_type, pair.kalshi_market_ticker
                        );
                    }

                    if !result.errors.is_empty() {
                        for err in &result.errors {
                            warn!("   ⚠️ {}", err);
                        }
                    }

                    info!(
                        "ℹ️  Note: New markets will be picked up automatically after reconnection"
                    );
                    info!("ℹ️  To activate new markets now, restart the bot");
                }
                result => {
                    warn!("⚠️ Rediscovery found no market pairs");
                    for err in &result.errors {
                        warn!("   {}", err);
                    }
                }
            }
        }
    });

    // === Live Sports Discovery Refresh Loop ===
    // More frequent refresh for sports leagues using live join
    let live_refresh_state = state.clone();
    let live_refresh_kalshi = kalshi_api.clone();
    let live_refresh_leagues = sports_leagues.clone();
    let live_refresh_config = live_config.clone();
    let live_refresh_handle = if has_sports_leagues && live_config.live_join_enabled {
        let refresh_secs = live_config.refresh_interval_secs;
        Some(tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(refresh_secs));
            interval.tick().await; // Skip first tick (already did discovery at startup)

            loop {
                interval.tick().await;
                info!(
                    event = "live_refresh_start",
                    "🔄 Running live sports discovery refresh..."
                );

                // Create fresh Gamma client for each refresh
                let gamma = Arc::new(GammaClient::new());
                let engine = LiveDiscoveryEngine::with_config(
                    live_refresh_kalshi.clone(),
                    gamma,
                    live_refresh_config.clone(),
                );

                // Run discovery for all sports leagues
                match engine.discover_all_leagues(&live_refresh_leagues).await {
                    Ok(results) => {
                        let mut total_kalshi = 0;
                        let mut total_poly = 0;
                        let mut total_matched = 0;
                        let mut new_pairs: Vec<MarketPair> = Vec::new();

                        for result in &results {
                            total_kalshi += result.kalshi_discovered_count;
                            total_poly += result.poly_discovered_count;
                            total_matched += result.matched_markets.len();
                            new_pairs.extend(matched_markets_to_pairs(&result.matched_markets));
                        }

                        let current_count = live_refresh_state.market_count();

                        info!(
                            event = "live_refresh_complete",
                            kalshi_open = total_kalshi,
                            poly_active = total_poly,
                            matched = total_matched,
                            new_pairs = new_pairs.len(),
                            current_subscribed = current_count,
                            "📊 Live refresh: K={}, P={}, matched={}, pairs={} (current={})",
                            total_kalshi,
                            total_poly,
                            total_matched,
                            new_pairs.len(),
                            current_count
                        );

                        // Diff against currently subscribed pairs
                        let current_tickers: std::collections::HashSet<String> = live_refresh_state
                            .markets
                            .iter()
                            .take(current_count)
                            .filter_map(|m| {
                                m.pair.as_ref().map(|p| p.kalshi_market_ticker.to_string())
                            })
                            .collect();

                        let new_tickers: std::collections::HashSet<String> = new_pairs
                            .iter()
                            .map(|p| p.kalshi_market_ticker.to_string())
                            .collect();

                        let added: Vec<_> = new_tickers.difference(&current_tickers).collect();
                        let removed: Vec<_> = current_tickers.difference(&new_tickers).collect();

                        if !added.is_empty() {
                            info!(
                                event = "live_refresh_new_markets",
                                count = added.len(),
                                "🆕 {} new markets detected (requires restart to subscribe)",
                                added.len()
                            );
                            for ticker in added.iter().take(5) {
                                info!("   + {}", ticker);
                            }
                        }

                        if !removed.is_empty() {
                            warn!(
                                event = "live_refresh_stale_markets",
                                count = removed.len(),
                                "⚠️ {} stale markets detected (no longer in discovery)",
                                removed.len()
                            );
                            for ticker in removed.iter().take(5) {
                                warn!("   - {}", ticker);
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            event = "live_refresh_error",
                            error = %e,
                            "Failed to run live sports refresh: {}",
                            e
                        );
                    }
                }
            }
        }))
    } else {
        None
    };

    // System health monitoring and arbitrage diagnostics
    let heartbeat_state = state.clone();
    let heartbeat_threshold = threshold_cents;
    let heartbeat_pnl = pnl_tracker.clone();
    let heartbeat_metrics = metrics.clone();
    let heartbeat_flush_secs = pnl_flush_secs;
    let heartbeat_fee_model = fee_model.clone();
    let heartbeat_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let market_count = heartbeat_state.market_count();
            let mut with_kalshi = 0;
            let mut with_poly = 0;
            let mut with_both = 0;
            // Track best arbitrage opportunity: (total_cost, market_id, p_yes, k_no, k_yes, p_no, poly_fee, kalshi_fee, is_poly_yes_kalshi_no)
            let mut best_arb: Option<(u16, u16, u16, u16, u16, u16, u16, u16, bool)> = None;

            for market in heartbeat_state.markets.iter().take(market_count) {
                let (k_yes, k_no, _, _) = market.kalshi.load();
                let (p_yes, p_no, _, _) = market.poly.load();
                let has_k = k_yes > 0 && k_no > 0;
                let has_p = p_yes > 0 && p_no > 0;
                if k_yes > 0 || k_no > 0 {
                    with_kalshi += 1;
                }
                if p_yes > 0 || p_no > 0 {
                    with_poly += 1;
                }
                if has_k && has_p {
                    with_both += 1;

                    // Get token IDs for fee estimation
                    let (poly_yes_token, poly_no_token) = market
                        .pair
                        .as_ref()
                        .map(|p| (p.poly_yes_token.as_ref(), p.poly_no_token.as_ref()))
                        .unwrap_or(("", ""));

                    // Calculate fees for both directions (PolyYes+KalshiNo and KalshiYes+PolyNo)
                    let poly_yes_fee = heartbeat_fee_model
                        .estimate_fees(Exchange::Polymarket, poly_yes_token, p_yes, 1)
                        .await as u16;
                    let kalshi_no_fee = heartbeat_fee_model
                        .estimate_fees(Exchange::Kalshi, "", k_no, 1)
                        .await as u16;

                    let kalshi_yes_fee = heartbeat_fee_model
                        .estimate_fees(Exchange::Kalshi, "", k_yes, 1)
                        .await as u16;
                    let poly_no_fee = heartbeat_fee_model
                        .estimate_fees(Exchange::Polymarket, poly_no_token, p_no, 1)
                        .await as u16;

                    // Cost = leg1 + leg2 + fees
                    let cost1 = p_yes + k_no + poly_yes_fee + kalshi_no_fee;
                    let cost2 = k_yes + p_no + kalshi_yes_fee + poly_no_fee;

                    let (best_cost, poly_fee, kalshi_fee, is_poly_yes) = if cost1 <= cost2 {
                        (cost1, poly_yes_fee, kalshi_no_fee, true)
                    } else {
                        (cost2, poly_no_fee, kalshi_yes_fee, false)
                    };

                    if best_arb.is_none() || best_cost < best_arb.as_ref().unwrap().0 {
                        best_arb = Some((
                            best_cost,
                            market.market_id,
                            p_yes,
                            k_no,
                            k_yes,
                            p_no,
                            poly_fee,
                            kalshi_fee,
                            is_poly_yes,
                        ));
                    }
                }
            }

            info!("💓 System heartbeat | Markets: {} total, {} with Kalshi prices, {} with Polymarket prices, {} with both | threshold={}¢",
                  market_count, with_kalshi, with_poly, with_both, heartbeat_threshold);

            // P&L summary
            let pnl_summary_data;
            {
                let pnl: tokio::sync::MutexGuard<'_, PnLTracker> = heartbeat_pnl.lock().await;
                let pnl_summary = pnl.format_summary();
                let venue_summary = pnl.format_venue_summary();
                info!("📊 P&L | {} | {}", pnl_summary, venue_summary);

                // Capture lightweight summary for structured event and metrics
                pnl_summary_data = pnl.summary();

                // Update metrics
                heartbeat_metrics
                    .realized_pnl_cents
                    .set(pnl_summary_data.realized_cents as i64);
                heartbeat_metrics
                    .unrealized_pnl_cents
                    .set(pnl_summary_data.unrealized_cents as i64);
                heartbeat_metrics
                    .total_fees_cents
                    .set(pnl_summary_data.fees_cents as i64);
                heartbeat_metrics
                    .active_positions
                    .set(pnl_summary_data.open_positions as i64);

                // Save snapshot if needed
                if pnl.should_save_snapshot(heartbeat_flush_secs) {
                    drop(pnl); // Release lock before saving
                    let pnl_write: tokio::sync::MutexGuard<'_, PnLTracker> =
                        heartbeat_pnl.lock().await;
                    if pnl_write.save_snapshot() {
                        info!("💾 P&L snapshot saved");
                    }
                }
            }

            // Print metrics summary every 10 minutes
            if heartbeat_metrics.opportunities_detected.get() > 0 {
                heartbeat_metrics.print_summary();
            }

            // Emit structured heartbeat event for monitoring/alerting
            info!(
                event = "heartbeat",
                markets_total = market_count,
                markets_kalshi = with_kalshi,
                markets_poly = with_poly,
                markets_both = with_both,
                threshold_cents = heartbeat_threshold,
                pnl_realized_cents = pnl_summary_data.realized_cents,
                pnl_unrealized_cents = pnl_summary_data.unrealized_cents,
                pnl_fees_cents = pnl_summary_data.fees_cents,
                pnl_net_cents = pnl_summary_data.realized_cents + pnl_summary_data.unrealized_cents,
                open_positions = pnl_summary_data.open_positions,
                "Structured heartbeat"
            );

            if let Some((
                cost,
                market_id,
                p_yes,
                k_no,
                k_yes,
                p_no,
                poly_fee,
                kalshi_fee,
                is_poly_yes,
            )) = best_arb
            {
                let gap = cost as i16 - heartbeat_threshold as i16;
                let desc = heartbeat_state
                    .get_by_id(market_id)
                    .and_then(|m| m.pair.as_ref())
                    .map(|p| &*p.description)
                    .unwrap_or("Unknown");
                let leg_breakdown = if is_poly_yes {
                    format!(
                        "P_yes({}¢) + K_no({}¢) + P_fee({}¢) + K_fee({}¢) = {}¢",
                        p_yes, k_no, poly_fee, kalshi_fee, cost
                    )
                } else {
                    format!(
                        "K_yes({}¢) + P_no({}¢) + P_fee({}¢) + K_fee({}¢) = {}¢",
                        k_yes, p_no, poly_fee, kalshi_fee, cost
                    )
                };
                if gap <= 10 {
                    info!("   📊 Best opportunity: {} | {} | gap={:+}¢ | [Poly_yes={}¢ Kalshi_no={}¢ Kalshi_yes={}¢ Poly_no={}¢]",
                          desc, leg_breakdown, gap, p_yes, k_no, k_yes, p_no);
                } else {
                    info!(
                        "   📊 Best opportunity: {} | {} | gap={:+}¢ (market efficient)",
                        desc, leg_breakdown, gap
                    );
                }
            } else if with_both == 0 {
                warn!("   ⚠️  No markets with both Kalshi and Polymarket prices - verify WebSocket connections");
            }
        }
    });

    // Main event loop - run until termination
    info!("✅ All systems operational - entering main event loop");
    let _ = tokio::join!(
        kalshi_handle,
        poly_handle,
        heartbeat_handle,
        rediscovery_handle,
        exec_handle
    );

    Ok(())
}
