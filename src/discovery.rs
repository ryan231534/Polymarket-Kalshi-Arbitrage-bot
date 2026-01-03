//! Intelligent market discovery and matching system.
//!
//! This module handles the discovery of matching markets between Kalshi and Polymarket,
//! with support for caching, incremental updates, and parallel processing.

use anyhow::Result;
use chrono::{NaiveDate, Utc};
use futures_util::{stream, StreamExt};
use governor::{
    clock::DefaultClock, middleware::NoOpMiddleware, state::NotKeyed, Quota, RateLimiter,
};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::cache::TeamCache;
use crate::config::{get_league_config, get_league_configs, LeagueConfig};
use crate::kalshi::KalshiApiClient;
use crate::polymarket::GammaClient;
use crate::prefetch;
use crate::types::{DiscoveryResult, KalshiEvent, KalshiMarket, MarketPair, MarketType};

/// Max concurrent Gamma API requests
const GAMMA_CONCURRENCY: usize = 20;

/// Kalshi rate limit: 2 requests per second (very conservative - they rate limit aggressively)
/// Must be conservative because discovery runs many leagues/series in parallel
/// Override with env var KALSHI_DISCOVERY_RPS
const KALSHI_RATE_LIMIT_PER_SEC: u32 = 2;

/// Max concurrent Kalshi API requests GLOBALLY across all leagues/series
/// This is the hard cap - prevents bursting even when rate limiter has tokens
/// Override with env var KALSHI_DISCOVERY_CONCURRENCY
const KALSHI_GLOBAL_CONCURRENCY: usize = 1;

/// Default discovery horizon in days (only discover events within this timeframe)
/// Override with env var DISCOVERY_HORIZON_DAYS
const DEFAULT_DISCOVERY_HORIZON_DAYS: u32 = 3;

/// Get Kalshi rate limit from env or default
fn get_kalshi_rate_limit() -> u32 {
    std::env::var("KALSHI_DISCOVERY_RPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0) // Validate: must be > 0
        .unwrap_or(KALSHI_RATE_LIMIT_PER_SEC)
}

/// Get Kalshi concurrency limit from env or default
fn get_kalshi_concurrency() -> usize {
    std::env::var("KALSHI_DISCOVERY_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0) // Validate: must be > 0
        .unwrap_or(KALSHI_GLOBAL_CONCURRENCY)
}

/// Get discovery horizon days from env or default
fn get_discovery_horizon_days() -> u32 {
    std::env::var("DISCOVERY_HORIZON_DAYS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0) // Validate: must be > 0
        .unwrap_or(DEFAULT_DISCOVERY_HORIZON_DAYS)
}

/// Cache file path
const DISCOVERY_CACHE_PATH: &str = ".discovery_cache.json";

/// Cache TTL in seconds (2 hours - new markets appear every ~2 hours)
const CACHE_TTL_SECS: u64 = 2 * 60 * 60;

/// Task for parallel Gamma lookup
struct GammaLookupTask {
    event: Arc<KalshiEvent>,
    market: KalshiMarket,
    poly_slug: String,
    market_type: MarketType,
    league: String,
}

/// Type alias for Kalshi rate limiter
type KalshiRateLimiter =
    RateLimiter<NotKeyed, governor::state::InMemoryState, DefaultClock, NoOpMiddleware>;

/// Persistent cache for discovered market pairs (schema v2 with config scoping)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiscoveryCache {
    /// Schema version (current: 2)
    #[serde(default)]
    schema_version: u32,
    /// Unix timestamp when cache was created
    timestamp_secs: u64,
    /// Cached market pairs
    pairs: Vec<MarketPair>,
    /// Set of known Kalshi market tickers (for incremental updates)
    known_kalshi_tickers: Vec<String>,
    /// Leagues this cache was created for (normalized, sorted; empty = all)
    #[serde(default)]
    leagues: Vec<String>,
    /// Discovery horizon in days
    #[serde(default)]
    horizon_days: u32,
}

/// Legacy cache format (schema v1) - for backward compatibility detection
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct DiscoveryCacheLegacy {
    timestamp_secs: u64,
    pairs: Vec<MarketPair>,
    known_kalshi_tickers: Vec<String>,
}

impl DiscoveryCache {
    fn new(pairs: Vec<MarketPair>, leagues: Vec<String>, horizon_days: u32) -> Self {
        let known_kalshi_tickers: Vec<String> = pairs
            .iter()
            .map(|p| p.kalshi_market_ticker.to_string())
            .collect();
        Self {
            schema_version: 2,
            timestamp_secs: current_unix_secs(),
            pairs,
            known_kalshi_tickers,
            leagues: normalize_leagues(leagues),
            horizon_days,
        }
    }

    fn is_expired(&self) -> bool {
        let now = current_unix_secs();
        now.saturating_sub(self.timestamp_secs) > CACHE_TTL_SECS
    }

    fn age_secs(&self) -> u64 {
        current_unix_secs().saturating_sub(self.timestamp_secs)
    }

    fn has_ticker(&self, ticker: &str) -> bool {
        self.known_kalshi_tickers.iter().any(|t| t == ticker)
    }

    /// Check if cache matches the current discovery config
    fn matches_config(&self, leagues: &[String], horizon_days: u32) -> bool {
        if self.schema_version != 2 {
            return false;
        }
        if self.horizon_days != horizon_days {
            return false;
        }
        let normalized_leagues = normalize_leagues(leagues.to_vec());
        self.leagues == normalized_leagues
    }
}

/// Normalize leagues list: lowercase, sorted, deduplicated
fn normalize_leagues(mut leagues: Vec<String>) -> Vec<String> {
    leagues = leagues.into_iter().map(|s| s.to_lowercase()).collect();
    leagues.sort();
    leagues.dedup();
    leagues
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Market discovery and matching client for cross-platform market identification
pub struct DiscoveryClient {
    kalshi: Arc<KalshiApiClient>,
    gamma: Arc<GammaClient>,
    pub team_cache: Arc<TeamCache>,
    kalshi_limiter: Arc<KalshiRateLimiter>,
    kalshi_semaphore: Arc<Semaphore>, // Global concurrency limit for Kalshi
    gamma_semaphore: Arc<Semaphore>,
}

impl DiscoveryClient {
    pub fn new(kalshi: KalshiApiClient, team_cache: TeamCache) -> Self {
        // Get rate limits from env
        let rate_limit = get_kalshi_rate_limit();
        let concurrency = get_kalshi_concurrency();
        let horizon_days = get_discovery_horizon_days();

        info!(
            "🔧 Discovery config: rate_limit={}req/s, concurrency={}, horizon={}days",
            rate_limit, concurrency, horizon_days
        );

        // Create token bucket rate limiter for Kalshi
        let quota = Quota::per_second(NonZeroU32::new(rate_limit).unwrap());
        let kalshi_limiter = Arc::new(RateLimiter::direct(quota));

        Self {
            kalshi: Arc::new(kalshi),
            gamma: Arc::new(GammaClient::new()),
            team_cache: Arc::new(team_cache),
            kalshi_limiter,
            kalshi_semaphore: Arc::new(Semaphore::new(concurrency)),
            gamma_semaphore: Arc::new(Semaphore::new(GAMMA_CONCURRENCY)),
        }
    }

    /// Load and validate cache from disk (async)
    /// Returns Some(cache) only if cache is valid AND matches current config
    async fn load_cache(leagues: &[String], horizon_days: u32) -> Option<DiscoveryCache> {
        let data = tokio::fs::read_to_string(DISCOVERY_CACHE_PATH).await.ok()?;

        // Try to deserialize as v2 (with schema_version)
        if let Ok(cache) = serde_json::from_str::<DiscoveryCache>(&data) {
            // Validate config match
            if cache.matches_config(leagues, horizon_days) {
                info!(
                    "📂 Cache config match: schema_version={}, leagues={:?}, horizon_days={}",
                    cache.schema_version, cache.leagues, cache.horizon_days
                );
                return Some(cache);
            } else {
                info!("⚠️  Cache config mismatch (will run fresh discovery):");
                info!(
                    "   Cache: schema_version={}, leagues={:?}, horizon_days={}",
                    cache.schema_version, cache.leagues, cache.horizon_days
                );
                info!(
                    "   Current: schema_version=2, leagues={:?}, horizon_days={}",
                    normalize_leagues(leagues.to_vec()),
                    horizon_days
                );
                return None;
            }
        }

        // Try legacy v1 format
        if serde_json::from_str::<DiscoveryCacheLegacy>(&data).is_ok() {
            info!("⚠️  Legacy cache (v1) detected - config unknown, running fresh discovery");
        } else {
            warn!("⚠️  Failed to parse cache file - corrupted or invalid format");
        }

        None
    }

    /// Save cache to disk atomically (async)
    async fn save_cache(cache: &DiscoveryCache) -> Result<()> {
        let data = serde_json::to_string_pretty(cache)?;
        let tmp_path = format!("{}.tmp", DISCOVERY_CACHE_PATH);

        // Write to tmp file first
        tokio::fs::write(&tmp_path, data).await?;

        // Atomic rename
        tokio::fs::rename(&tmp_path, DISCOVERY_CACHE_PATH).await?;

        Ok(())
    }

    /// Discover all market pairs with caching support
    ///
    /// Strategy:
    /// 1. Check if cache bypass is enabled (DISCOVERY_CACHE_BYPASS=1)
    /// 2. Try to load cache from disk and validate config
    /// 3. If cache exists, is fresh (<2 hours), and matches config, use it
    /// 4. If cache exists but is stale, load it + fetch incremental updates
    /// 5. If no cache or config mismatch, do full discovery
    pub async fn discover_all(&self, leagues: &[&str]) -> DiscoveryResult {
        let horizon_days = get_discovery_horizon_days();
        let effective_leagues: Vec<String> = leagues.iter().map(|s| s.to_string()).collect();

        // Check for cache bypass
        let bypass_cache = std::env::var("DISCOVERY_CACHE_BYPASS")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false);

        if bypass_cache {
            info!("🔄 DISCOVERY_CACHE_BYPASS=1, skipping cache load");
        } else {
            // Try to load existing cache
            let cached = Self::load_cache(&effective_leagues, horizon_days).await;

            match cached {
                Some(cache) if !cache.is_expired() => {
                    // Cache is fresh and matches config - use it directly
                    info!(
                        "📂 Loaded {} pairs from cache (age: {}s)",
                        cache.pairs.len(),
                        cache.age_secs()
                    );
                    return DiscoveryResult {
                        pairs: cache.pairs,
                        kalshi_events_found: 0, // From cache
                        poly_matches: 0,
                        poly_misses: 0,
                        errors: vec![],
                    };
                }
                Some(cache) => {
                    // Cache is stale - do incremental discovery
                    info!(
                        "📂 Cache expired (age: {}s), doing incremental refresh...",
                        cache.age_secs()
                    );
                    return self
                        .discover_incremental(leagues, cache, &effective_leagues, horizon_days)
                        .await;
                }
                None => {
                    // No cache or config mismatch - do full discovery
                    info!("📂 No valid cache, doing full discovery...");
                }
            }
        }

        // Full discovery (no cache)
        let result = self.discover_full(leagues).await;

        // Save to cache
        if !result.pairs.is_empty() {
            let cache = DiscoveryCache::new(
                result.pairs.clone(),
                effective_leagues.clone(),
                horizon_days,
            );
            if let Err(e) = Self::save_cache(&cache).await {
                warn!("Failed to save discovery cache: {}", e);
            } else {
                info!("💾 Saved {} pairs to cache", result.pairs.len());
            }
        }

        result
    }

    /// Force full discovery (ignores cache)
    pub async fn discover_all_force(&self, leagues: &[&str]) -> DiscoveryResult {
        let horizon_days = get_discovery_horizon_days();
        let effective_leagues: Vec<String> = leagues.iter().map(|s| s.to_string()).collect();

        info!("🔄 Forced full discovery (ignoring cache)...");
        let result = self.discover_full(leagues).await;

        // Save to cache
        if !result.pairs.is_empty() {
            let cache = DiscoveryCache::new(result.pairs.clone(), effective_leagues, horizon_days);
            if let Err(e) = Self::save_cache(&cache).await {
                warn!("Failed to save discovery cache: {}", e);
            } else {
                info!("💾 Saved {} pairs to cache", result.pairs.len());
            }
        }

        result
    }

    /// Full discovery without cache
    async fn discover_full(&self, leagues: &[&str]) -> DiscoveryResult {
        // Check if prefetch is enabled (default: true for performance)
        let use_prefetch = std::env::var("USE_PREFETCH_DISCOVERY")
            .map(|v| v != "0" && v.to_lowercase() != "false")
            .unwrap_or(true);

        if use_prefetch {
            info!("🚀 Using batch prefetch for discovery");
            return self.discover_full_with_prefetch(leagues).await;
        }

        info!("🐌 Using legacy sequential discovery");
        self.discover_full_legacy(leagues).await
    }

    /// Full discovery using batch prefetch (10-20x faster)
    async fn discover_full_with_prefetch(&self, leagues: &[&str]) -> DiscoveryResult {
        let start = std::time::Instant::now();

        let configs: Vec<_> = if leagues.is_empty() {
            get_league_configs()
        } else {
            leagues
                .iter()
                .filter_map(|l| get_league_config(l))
                .collect()
        };

        // Build list of all series to prefetch
        let mut series_list = Vec::new();
        let market_types = [
            MarketType::Moneyline,
            MarketType::Spread,
            MarketType::Total,
            MarketType::Btts,
        ];

        for config in &configs {
            for market_type in &market_types {
                if let Some(series) = self.get_series_for_type(config, *market_type) {
                    series_list.push(series.to_string());
                }
            }
        }

        // Phase 1: Prefetch all Kalshi events and markets
        info!("📡 Phase 1: Prefetching Kalshi events/markets...");
        let phase1_result = match prefetch::prefetch_all(
            self.kalshi.clone(),
            self.gamma.clone(),
            &series_list,
            &[], // No poly slugs yet - will do in phase 2
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!("Prefetch failed, falling back to legacy discovery: {}", e);
                return self.discover_full_legacy(leagues).await;
            }
        };

        // Phase 2: Build poly slugs from Kalshi data and prefetch Polymarket tokens
        info!("📡 Phase 2: Building poly slugs and prefetching tokens...");
        let phase2_start = std::time::Instant::now();
        let horizon_days = get_discovery_horizon_days();
        let mut poly_slugs = Vec::new();

        for config in &configs {
            for market_type in &market_types {
                let series = match self.get_series_for_type(config, *market_type) {
                    Some(s) => s,
                    None => continue,
                };

                // Process all markets from phase1 for this series
                for (event_ticker, markets) in phase1_result.kalshi_markets.iter() {
                    if !event_ticker.starts_with(series) {
                        continue;
                    }

                    // Parse event ticker
                    let parsed = match parse_kalshi_event_ticker(event_ticker) {
                        Some(p) => p,
                        None => continue,
                    };

                    // Apply horizon filter
                    if !is_within_horizon(&parsed.date, horizon_days) {
                        continue;
                    }

                    // Build slug for each market
                    for market in markets {
                        let poly_slug =
                            self.build_poly_slug(config.poly_prefix, &parsed, *market_type, market);
                        poly_slugs.push(poly_slug);
                    }
                }
            }
        }

        info!("  📋 Built {} poly slugs to prefetch", poly_slugs.len());

        // Fetch Polymarket tokens in parallel
        let poly_tokens = match prefetch::fetch_all_poly_tokens(
            self.gamma.clone(),
            &poly_slugs,
            prefetch::get_prefetch_concurrency(),
        )
        .await
        {
            Ok(tokens) => tokens,
            Err(e) => {
                warn!("Failed to fetch poly tokens: {}", e);
                std::collections::HashMap::new()
            }
        };

        let phase2_ms = phase2_start.elapsed().as_millis();
        info!(
            "  ✅ Fetched {} Polymarket tokens in {}ms",
            poly_tokens.len(),
            phase2_ms
        );

        // Now build market pairs from prefetched data
        let mut result = DiscoveryResult::default();

        for config in &configs {
            for market_type in &market_types {
                let pairs = match self.build_pairs_from_prefetch_data(
                    config,
                    *market_type,
                    &phase1_result,
                    &poly_tokens,
                    horizon_days,
                ) {
                    Ok(pairs) => pairs,
                    Err(e) => {
                        result
                            .errors
                            .push(format!("{} {}: {}", config.league_code, market_type, e));
                        continue;
                    }
                };

                let count = pairs.len();
                if count > 0 {
                    info!(
                        "  ✅ {} {}: {} pairs",
                        config.league_code, market_type, count
                    );
                }
                result.poly_matches += count;
                result.pairs.extend(pairs);
            }
        }

        result.kalshi_events_found = result.pairs.len();

        let total_ms = start.elapsed().as_millis();
        info!("🎉 Discovery complete in {}ms using prefetch", total_ms);

        result
    }

    /// Legacy full discovery without prefetch (kept for compatibility)
    async fn discover_full_legacy(&self, leagues: &[&str]) -> DiscoveryResult {
        let configs: Vec<_> = if leagues.is_empty() {
            get_league_configs()
        } else {
            leagues
                .iter()
                .filter_map(|l| get_league_config(l))
                .collect()
        };

        // Parallel discovery across all leagues
        let league_futures: Vec<_> = configs
            .iter()
            .map(|config| self.discover_league(config, None))
            .collect();

        let league_results = futures_util::future::join_all(league_futures).await;

        // Merge results
        let mut result = DiscoveryResult::default();
        for league_result in league_results {
            result.pairs.extend(league_result.pairs);
            result.poly_matches += league_result.poly_matches;
            result.errors.extend(league_result.errors);
        }
        result.kalshi_events_found = result.pairs.len();

        result
    }

    /// Build market pairs from prefetch result for a specific league/market_type
    fn build_pairs_from_prefetch_data(
        &self,
        config: &LeagueConfig,
        market_type: MarketType,
        phase1: &crate::prefetch::PrefetchResult,
        poly_tokens: &std::collections::HashMap<String, (String, String)>,
        horizon_days: u32,
    ) -> Result<Vec<MarketPair>> {
        let series = match self.get_series_for_type(config, market_type) {
            Some(s) => s,
            None => return Ok(vec![]),
        };

        let mut pairs = Vec::new();

        // Process all markets from prefetch for this series
        for (event_ticker, markets) in &phase1.kalshi_markets {
            // Check if event belongs to this series (event_ticker starts with series prefix)
            if !event_ticker.as_str().starts_with(series) {
                continue;
            }

            // Get event details
            let event = match phase1.kalshi_events.get(event_ticker.as_str()) {
                Some(e) => e,
                None => continue,
            };

            // Parse event ticker
            let parsed = match parse_kalshi_event_ticker(event_ticker) {
                Some(p) => p,
                None => continue,
            };

            // Apply horizon filter
            if !is_within_horizon(&parsed.date, horizon_days) {
                continue;
            }

            // Process each market for this event
            for market in markets {
                let poly_slug =
                    self.build_poly_slug(config.poly_prefix, &parsed, market_type, market);

                // Check if we have Polymarket tokens for this slug
                if let Some((yes_token, no_token)) = poly_tokens.get(&poly_slug) {
                    let team_suffix = extract_team_suffix(&market.ticker);
                    pairs.push(MarketPair {
                        pair_id: format!("{}-{}", poly_slug, market.ticker).into(),
                        league: config.league_code.to_string().into(),
                        market_type,
                        description: format!("{} - {}", event.title, market.title).into(),
                        kalshi_event_ticker: event.event_ticker.clone().into(),
                        kalshi_market_ticker: market.ticker.clone().into(),
                        poly_slug: poly_slug.into(),
                        poly_yes_token: yes_token.clone().into(),
                        poly_no_token: no_token.clone().into(),
                        line_value: market.floor_strike,
                        team_suffix: team_suffix.map(|s| s.into()),
                    });
                }
            }
        }

        Ok(pairs)
    }

    /// Incremental discovery - merge cached pairs with newly discovered ones
    async fn discover_incremental(
        &self,
        leagues: &[&str],
        cache: DiscoveryCache,
        effective_leagues: &[String],
        horizon_days: u32,
    ) -> DiscoveryResult {
        let configs: Vec<_> = if leagues.is_empty() {
            get_league_configs()
        } else {
            leagues
                .iter()
                .filter_map(|l| get_league_config(l))
                .collect()
        };

        // Discover with filter for known tickers
        let league_futures: Vec<_> = configs
            .iter()
            .map(|config| self.discover_league(config, Some(&cache)))
            .collect();

        let league_results = futures_util::future::join_all(league_futures).await;

        // Merge cached pairs with newly discovered ones
        let mut all_pairs = cache.pairs;
        let mut new_count = 0;

        for league_result in league_results {
            for pair in league_result.pairs {
                if !all_pairs
                    .iter()
                    .any(|p| *p.kalshi_market_ticker == *pair.kalshi_market_ticker)
                {
                    all_pairs.push(pair);
                    new_count += 1;
                }
            }
        }

        if new_count > 0 {
            info!("🆕 Found {} new market pairs", new_count);

            // Update cache
            let new_cache =
                DiscoveryCache::new(all_pairs.clone(), effective_leagues.to_vec(), horizon_days);
            if let Err(e) = Self::save_cache(&new_cache).await {
                warn!("Failed to update discovery cache: {}", e);
            } else {
                info!("💾 Updated cache with {} total pairs", all_pairs.len());
            }
        } else {
            info!(
                "✅ No new markets found, using {} cached pairs",
                all_pairs.len()
            );

            // Just update timestamp to extend TTL
            let refreshed_cache =
                DiscoveryCache::new(all_pairs.clone(), effective_leagues.to_vec(), horizon_days);
            let _ = Self::save_cache(&refreshed_cache).await;
        }

        DiscoveryResult {
            pairs: all_pairs,
            kalshi_events_found: new_count,
            poly_matches: new_count,
            poly_misses: 0,
            errors: vec![],
        }
    }

    /// Discover all market types for a single league (PARALLEL)
    /// If cache is provided, only discovers markets not already in cache
    async fn discover_league(
        &self,
        config: &LeagueConfig,
        cache: Option<&DiscoveryCache>,
    ) -> DiscoveryResult {
        info!("🔍 Discovering {} markets...", config.league_code);

        let market_types = [
            MarketType::Moneyline,
            MarketType::Spread,
            MarketType::Total,
            MarketType::Btts,
        ];

        // Parallel discovery across market types
        let type_futures: Vec<_> = market_types
            .iter()
            .filter_map(|market_type| {
                let series = self.get_series_for_type(config, *market_type)?;
                Some(self.discover_series(config, series, *market_type, cache))
            })
            .collect();

        let type_results = futures_util::future::join_all(type_futures).await;

        let mut result = DiscoveryResult::default();
        for (pairs_result, market_type) in type_results.into_iter().zip(market_types.iter()) {
            match pairs_result {
                Ok(pairs) => {
                    let count = pairs.len();
                    if count > 0 {
                        info!(
                            "  ✅ {} {}: {} pairs",
                            config.league_code, market_type, count
                        );
                    }
                    result.poly_matches += count;
                    result.pairs.extend(pairs);
                }
                Err(e) => {
                    result
                        .errors
                        .push(format!("{} {}: {}", config.league_code, market_type, e));
                }
            }
        }

        result
    }

    fn get_series_for_type(
        &self,
        config: &LeagueConfig,
        market_type: MarketType,
    ) -> Option<&'static str> {
        match market_type {
            MarketType::Moneyline => Some(config.kalshi_series_game),
            MarketType::Spread => config.kalshi_series_spread,
            MarketType::Total => config.kalshi_series_total,
            MarketType::Btts => config.kalshi_series_btts,
        }
    }

    /// Discover markets for a specific series (PARALLEL Kalshi + Gamma lookups)
    /// If cache is provided, skips markets already in cache
    async fn discover_series(
        &self,
        config: &LeagueConfig,
        series: &str,
        market_type: MarketType,
        cache: Option<&DiscoveryCache>,
    ) -> Result<Vec<MarketPair>> {
        let horizon_days = get_discovery_horizon_days();

        // Fetch Kalshi events with proper permit scope
        let events = {
            let _permit = self
                .kalshi_semaphore
                .acquire()
                .await
                .map_err(|e| anyhow::anyhow!("semaphore closed: {}", e))?;
            self.kalshi_limiter.until_ready().await;
            // HTTP call happens while permit is held
            self.kalshi.get_events(series, 50).await?
        };

        let total_events = events.len();

        // PHASE 2: Parallel market fetching
        let kalshi = self.kalshi.clone();
        let limiter = self.kalshi_limiter.clone();
        let semaphore = self.kalshi_semaphore.clone();

        // Parse events first, filtering out unparseable ones and applying horizon filter
        let mut kept = 0;
        let mut dropped_horizon = 0;
        let mut parse_failures = 0;

        let parsed_events: Vec<_> = events
            .into_iter()
            .filter_map(|event| {
                let parsed = match parse_kalshi_event_ticker(&event.event_ticker) {
                    Some(p) => p,
                    None => {
                        tracing::debug!(
                            "Could not parse event ticker {} (keeping anyway)",
                            event.event_ticker
                        );
                        parse_failures += 1;
                        return None;
                    }
                };

                // Apply horizon filter
                if !is_within_horizon(&parsed.date, horizon_days) {
                    tracing::debug!(
                        "Event {} outside horizon ({}d), skipping",
                        event.event_ticker,
                        horizon_days
                    );
                    dropped_horizon += 1;
                    return None;
                }

                kept += 1;
                Some((parsed, event))
            })
            .collect();

        if total_events > 0 {
            tracing::debug!(
                "  {} {}: events={}, kept={}, dropped_horizon={}, parse_fail={}",
                config.league_code,
                market_type,
                total_events,
                kept,
                dropped_horizon,
                parse_failures
            );
        }

        // Execute market fetches with GLOBAL concurrency limit
        let market_results: Vec<_> = stream::iter(parsed_events)
            .map(|(parsed, event)| {
                let kalshi = kalshi.clone();
                let limiter = limiter.clone();
                let semaphore = semaphore.clone();
                let event_ticker = event.event_ticker.clone();
                async move {
                    // Acquire permit and hold it during the entire HTTP call
                    let _permit = semaphore.acquire().await.ok()?;
                    limiter.until_ready().await;
                    // HTTP call happens while permit is held
                    let markets_result = kalshi.get_markets(&event_ticker).await;
                    Some((parsed, Arc::new(event), markets_result))
                }
            })
            .buffer_unordered(KALSHI_GLOBAL_CONCURRENCY * 2) // Allow some buffering, semaphore is the real limit
            .filter_map(|x| async { x })
            .collect()
            .await;

        // Collect all (event, market) pairs
        let mut event_markets = Vec::with_capacity(market_results.len() * 3);
        for (parsed, event, markets_result) in market_results {
            match markets_result {
                Ok(markets) => {
                    for market in markets {
                        // Skip if already in cache
                        if let Some(c) = cache {
                            if c.has_ticker(&market.ticker) {
                                continue;
                            }
                        }
                        event_markets.push((parsed.clone(), event.clone(), market));
                    }
                }
                Err(e) => {
                    warn!(
                        "  ⚠️ Failed to get markets for {}: {}",
                        event.event_ticker, e
                    );
                }
            }
        }

        // Parallel Gamma lookups with semaphore
        let lookup_futures: Vec<_> = event_markets
            .into_iter()
            .map(|(parsed, event, market)| {
                let poly_slug =
                    self.build_poly_slug(config.poly_prefix, &parsed, market_type, &market);

                GammaLookupTask {
                    event,
                    market,
                    poly_slug,
                    market_type,
                    league: config.league_code.to_string(),
                }
            })
            .collect();

        // Execute lookups in parallel
        let pairs: Vec<MarketPair> = stream::iter(lookup_futures)
            .map(|task| {
                let gamma = self.gamma.clone();
                let semaphore = self.gamma_semaphore.clone();
                async move {
                    let _permit = semaphore.acquire().await.ok()?;
                    match gamma.lookup_market(&task.poly_slug).await {
                        Ok(Some((yes_token, no_token))) => {
                            let team_suffix = extract_team_suffix(&task.market.ticker);
                            Some(MarketPair {
                                pair_id: format!("{}-{}", task.poly_slug, task.market.ticker)
                                    .into(),
                                league: task.league.into(),
                                market_type: task.market_type,
                                description: format!(
                                    "{} - {}",
                                    task.event.title, task.market.title
                                )
                                .into(),
                                kalshi_event_ticker: task.event.event_ticker.clone().into(),
                                kalshi_market_ticker: task.market.ticker.into(),
                                poly_slug: task.poly_slug.into(),
                                poly_yes_token: yes_token.into(),
                                poly_no_token: no_token.into(),
                                line_value: task.market.floor_strike,
                                team_suffix: team_suffix.map(|s| s.into()),
                            })
                        }
                        Ok(None) => None,
                        Err(e) => {
                            warn!("  ⚠️ Gamma lookup failed for {}: {}", task.poly_slug, e);
                            None
                        }
                    }
                }
            })
            .buffer_unordered(GAMMA_CONCURRENCY)
            .filter_map(|x| async { x })
            .collect()
            .await;

        Ok(pairs)
    }

    /// Build Polymarket slug from Kalshi event data
    fn build_poly_slug(
        &self,
        poly_prefix: &str,
        parsed: &ParsedKalshiTicker,
        market_type: MarketType,
        market: &KalshiMarket,
    ) -> String {
        // Convert Kalshi team codes to Polymarket codes using cache
        let poly_team1 = self
            .team_cache
            .kalshi_to_poly(poly_prefix, &parsed.team1)
            .unwrap_or_else(|| parsed.team1.to_lowercase());
        let poly_team2 = self
            .team_cache
            .kalshi_to_poly(poly_prefix, &parsed.team2)
            .unwrap_or_else(|| parsed.team2.to_lowercase());

        // Convert date from "25DEC27" to "2025-12-27"
        let date_str = kalshi_date_to_iso(&parsed.date);

        // Base slug: league-team1-team2-date
        let base = format!("{}-{}-{}-{}", poly_prefix, poly_team1, poly_team2, date_str);

        match market_type {
            MarketType::Moneyline => {
                if let Some(suffix) = extract_team_suffix(&market.ticker) {
                    if suffix.to_lowercase() == "tie" {
                        format!("{}-draw", base)
                    } else {
                        let poly_suffix = self
                            .team_cache
                            .kalshi_to_poly(poly_prefix, &suffix)
                            .unwrap_or_else(|| suffix.to_lowercase());
                        format!("{}-{}", base, poly_suffix)
                    }
                } else {
                    base
                }
            }
            MarketType::Spread => {
                if let Some(floor) = market.floor_strike {
                    let floor_str = format!("{:.1}", floor).replace(".", "pt");
                    format!("{}-spread-{}", base, floor_str)
                } else {
                    format!("{}-spread", base)
                }
            }
            MarketType::Total => {
                if let Some(floor) = market.floor_strike {
                    let floor_str = format!("{:.1}", floor).replace(".", "pt");
                    format!("{}-total-{}", base, floor_str)
                } else {
                    format!("{}-total", base)
                }
            }
            MarketType::Btts => {
                format!("{}-btts", base)
            }
        }
    }
}

// === Helpers ===

#[derive(Debug, Clone)]
struct ParsedKalshiTicker {
    date: String,  // "25DEC27"
    team1: String, // "CFC"
    team2: String, // "AVL"
}

/// Parse Kalshi event ticker like "KXEPLGAME-25DEC27CFCAVL" or "KXNCAAFGAME-25DEC27M-OHFRES"
fn parse_kalshi_event_ticker(ticker: &str) -> Option<ParsedKalshiTicker> {
    let parts: Vec<&str> = ticker.split('-').collect();
    if parts.len() < 2 {
        return None;
    }

    // Handle two formats:
    // 1. "KXEPLGAME-25DEC27CFCAVL" - date+teams in parts[1]
    // 2. "KXNCAAFGAME-25DEC27M-OHFRES" - date in parts[1], teams in parts[2]
    let (date, teams_part) = if parts.len() >= 3 && parts[2].len() >= 4 {
        // Format 2: 3-part ticker with separate teams section
        // parts[1] is like "25DEC27M" (date + optional suffix)
        let date_part = parts[1];
        let date = if date_part.len() >= 7 {
            date_part[..7].to_uppercase()
        } else {
            return None;
        };
        (date, parts[2])
    } else {
        // Format 1: 2-part ticker with combined date+teams
        let date_teams = parts[1];
        // Minimum: 7 (date) + 2 + 2 (min team codes) = 11
        if date_teams.len() < 11 {
            return None;
        }
        let date = date_teams[..7].to_uppercase();
        let teams = &date_teams[7..];
        (date, teams)
    };

    // Split team codes - try to find the best split point
    // Team codes range from 2-4 chars (e.g., OM, CFC, FRES)
    let (team1, team2) = split_team_codes(teams_part);

    Some(ParsedKalshiTicker { date, team1, team2 })
}

/// Split a combined team string into two team codes
/// Tries multiple split strategies based on string length
fn split_team_codes(teams: &str) -> (String, String) {
    let len = teams.len();

    // For 6 chars, could be 3+3, 2+4, or 4+2
    // For 5 chars, could be 2+3 or 3+2
    // For 4 chars, must be 2+2
    // For 7 chars, could be 3+4 or 4+3
    // For 8 chars, could be 4+4, 3+5, 5+3

    match len {
        4 => (teams[..2].to_uppercase(), teams[2..].to_uppercase()),
        5 => {
            // Prefer 2+3 (common for OM+ASM, OL+PSG)
            (teams[..2].to_uppercase(), teams[2..].to_uppercase())
        }
        6 => {
            // Check if it looks like 2+4 pattern (e.g., OHFRES = OH+FRES)
            // Common 2-letter codes: OM, OL, OH, SF, LA, NY, KC, TB, etc.
            let first_two = &teams[..2].to_uppercase();
            if is_likely_two_letter_code(first_two) {
                (first_two.clone(), teams[2..].to_uppercase())
            } else {
                // Default to 3+3
                (teams[..3].to_uppercase(), teams[3..].to_uppercase())
            }
        }
        7 => {
            // Could be 3+4 or 4+3 - prefer 3+4
            (teams[..3].to_uppercase(), teams[3..].to_uppercase())
        }
        _ if len >= 8 => {
            // 4+4 or longer
            (teams[..4].to_uppercase(), teams[4..].to_uppercase())
        }
        _ => {
            let mid = len / 2;
            (teams[..mid].to_uppercase(), teams[mid..].to_uppercase())
        }
    }
}

/// Check if a 2-letter code is a known/likely team abbreviation
fn is_likely_two_letter_code(code: &str) -> bool {
    matches!(
        code,
        // European football (Ligue 1, etc.)
        "OM" | "OL" | "FC" |
        // US sports common abbreviations
        "OH" | "SF" | "LA" | "NY" | "KC" | "TB" | "GB" | "NE" | "NO" | "LV" |
        // Generic short codes
        "BC" | "SC" | "AC" | "AS" | "US"
    )
}

/// Convert Kalshi date "25DEC27" to ISO "2025-12-27"
fn kalshi_date_to_iso(kalshi_date: &str) -> String {
    if kalshi_date.len() != 7 {
        return kalshi_date.to_string();
    }

    let year = format!("20{}", &kalshi_date[..2]);
    let month = match &kalshi_date[2..5].to_uppercase()[..] {
        "JAN" => "01",
        "FEB" => "02",
        "MAR" => "03",
        "APR" => "04",
        "MAY" => "05",
        "JUN" => "06",
        "JUL" => "07",
        "AUG" => "08",
        "SEP" => "09",
        "OCT" => "10",
        "NOV" => "11",
        "DEC" => "12",
        _ => "01",
    };
    let day = &kalshi_date[5..7];

    format!("{}-{}-{}", year, month, day)
}

/// Parse Kalshi date "25DEC27" to NaiveDate
fn parse_kalshi_date(kalshi_date: &str) -> Option<NaiveDate> {
    if kalshi_date.len() != 7 {
        return None;
    }

    let year: i32 = format!("20{}", &kalshi_date[..2]).parse().ok()?;
    let month = match &kalshi_date[2..5].to_uppercase()[..] {
        "JAN" => 1,
        "FEB" => 2,
        "MAR" => 3,
        "APR" => 4,
        "MAY" => 5,
        "JUN" => 6,
        "JUL" => 7,
        "AUG" => 8,
        "SEP" => 9,
        "OCT" => 10,
        "NOV" => 11,
        "DEC" => 12,
        _ => return None,
    };
    let day: u32 = kalshi_date[5..7].parse().ok()?;

    NaiveDate::from_ymd_opt(year, month, day)
}

/// Check if date is within discovery horizon
fn is_within_horizon(kalshi_date: &str, horizon_days: u32) -> bool {
    is_within_horizon_from(kalshi_date, horizon_days, Utc::now().date_naive())
}

/// Check if date is within discovery horizon (testable version with fixed today)
fn is_within_horizon_from(kalshi_date: &str, horizon_days: u32, today: NaiveDate) -> bool {
    let event_date = match parse_kalshi_date(kalshi_date) {
        Some(d) => d,
        None => return true, // If can't parse, include by default
    };

    let end_date = today + chrono::Duration::days(horizon_days as i64);

    // Include if: today <= event_date <= today + horizon_days
    event_date >= today && event_date <= end_date
}

/// Extract team suffix from market ticker (e.g., "KXEPLGAME-25DEC27CFCAVL-CFC" -> "CFC")
fn extract_team_suffix(ticker: &str) -> Option<String> {
    let mut splits = ticker.splitn(3, '-');
    splits.next()?; // series
    splits.next()?; // event
    splits.next().map(|s| s.to_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn test_parse_kalshi_ticker() {
        let parsed = parse_kalshi_event_ticker("KXEPLGAME-25DEC27CFCAVL").unwrap();
        assert_eq!(parsed.date, "25DEC27");
        assert_eq!(parsed.team1, "CFC");
        assert_eq!(parsed.team2, "AVL");
    }

    #[test]
    fn test_kalshi_date_to_iso() {
        assert_eq!(kalshi_date_to_iso("25DEC27"), "2025-12-27");
        assert_eq!(kalshi_date_to_iso("25JAN01"), "2025-01-01");
    }

    #[test]
    fn test_parse_kalshi_date_valid() {
        // Test valid dates
        let date = parse_kalshi_date("25DEC27").unwrap();
        assert_eq!(date.year(), 2025);
        assert_eq!(date.month(), 12);
        assert_eq!(date.day(), 27);

        let date = parse_kalshi_date("26JAN15").unwrap();
        assert_eq!(date.year(), 2026);
        assert_eq!(date.month(), 1);
        assert_eq!(date.day(), 15);

        let date = parse_kalshi_date("24FEB29").unwrap(); // Leap year
        assert_eq!(date.year(), 2024);
        assert_eq!(date.month(), 2);
        assert_eq!(date.day(), 29);
    }

    #[test]
    fn test_parse_kalshi_date_invalid() {
        // Test invalid formats
        assert!(parse_kalshi_date("").is_none());
        assert!(parse_kalshi_date("25DEC").is_none());
        assert!(parse_kalshi_date("25DECXX").is_none());
        assert!(parse_kalshi_date("INVALID").is_none());
        assert!(parse_kalshi_date("25XXX27").is_none());

        // Test invalid date values
        assert!(parse_kalshi_date("25FEB30").is_none()); // Feb 30 doesn't exist
        assert!(parse_kalshi_date("25DEC32").is_none()); // Day 32 invalid
    }

    #[test]
    fn test_is_within_horizon_from() {
        let today = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();

        // Event today - should be included
        assert!(is_within_horizon_from("26JAN02", 3, today));

        // Event tomorrow - should be included
        assert!(is_within_horizon_from("26JAN03", 3, today));

        // Event at horizon limit (today + 3 days = Jan 5) - should be included
        assert!(is_within_horizon_from("26JAN05", 3, today));

        // Event beyond horizon (Jan 6) - should be excluded
        assert!(!is_within_horizon_from("26JAN06", 3, today));

        // Event far in future - should be excluded
        assert!(!is_within_horizon_from("26FEB15", 3, today));

        // Event in past (yesterday) - should be excluded
        assert!(!is_within_horizon_from("26JAN01", 3, today));

        // Event far in past - should be excluded
        assert!(!is_within_horizon_from("25DEC27", 3, today));

        // Invalid date - should be included (conservative default)
        assert!(is_within_horizon_from("INVALID", 3, today));
        assert!(is_within_horizon_from("", 3, today));
    }

    #[test]
    fn test_is_within_horizon_zero_days() {
        let today = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();

        // With 0 day horizon, only today should be included
        assert!(is_within_horizon_from("26JAN02", 0, today));
        assert!(!is_within_horizon_from("26JAN03", 0, today));
        assert!(!is_within_horizon_from("26JAN01", 0, today));
    }

    #[test]
    fn test_normalize_leagues() {
        // Empty list
        assert_eq!(normalize_leagues(vec![]), Vec::<String>::new());

        // Single league
        assert_eq!(
            normalize_leagues(vec!["EPL".to_string()]),
            vec!["epl".to_string()]
        );

        // Multiple leagues - should be sorted
        assert_eq!(
            normalize_leagues(vec![
                "NBA".to_string(),
                "EPL".to_string(),
                "NFL".to_string()
            ]),
            vec!["epl".to_string(), "nba".to_string(), "nfl".to_string()]
        );

        // Order-insensitive
        assert_eq!(
            normalize_leagues(vec!["EPL".to_string(), "NBA".to_string()]),
            normalize_leagues(vec!["NBA".to_string(), "EPL".to_string()])
        );

        // Duplicates removed
        assert_eq!(
            normalize_leagues(vec![
                "epl".to_string(),
                "EPL".to_string(),
                "Epl".to_string()
            ]),
            vec!["epl".to_string()]
        );

        // Mixed case
        assert_eq!(
            normalize_leagues(vec!["EpL".to_string(), "NbA".to_string()]),
            vec!["epl".to_string(), "nba".to_string()]
        );
    }

    #[test]
    fn test_cache_matches_config() {
        let pairs = vec![]; // Empty for testing

        // Exact match
        let cache =
            DiscoveryCache::new(pairs.clone(), vec!["epl".to_string(), "nba".to_string()], 3);
        assert!(cache.matches_config(&vec!["epl".to_string(), "nba".to_string()], 3));

        // Order doesn't matter for leagues
        assert!(cache.matches_config(&vec!["nba".to_string(), "epl".to_string()], 3));

        // Case insensitive
        assert!(cache.matches_config(&vec!["EPL".to_string(), "NBA".to_string()], 3));

        // Different leagues - no match
        let cache2 = DiscoveryCache::new(pairs.clone(), vec!["epl".to_string()], 3);
        assert!(!cache2.matches_config(&vec!["nba".to_string()], 3));
        assert!(!cache2.matches_config(&vec!["epl".to_string(), "nba".to_string()], 3));

        // Different horizon - no match
        let cache3 = DiscoveryCache::new(pairs.clone(), vec!["epl".to_string()], 3);
        assert!(!cache3.matches_config(&vec!["epl".to_string()], 7));

        // Empty leagues (all leagues)
        let cache_all = DiscoveryCache::new(pairs.clone(), vec![], 3);
        assert!(cache_all.matches_config(&vec![], 3));
        assert!(!cache_all.matches_config(&vec!["epl".to_string()], 3));

        // Schema version mismatch
        let mut cache_v1 = DiscoveryCache::new(pairs.clone(), vec!["epl".to_string()], 3);
        cache_v1.schema_version = 1;
        assert!(!cache_v1.matches_config(&vec!["epl".to_string()], 3));
    }

    #[test]
    fn test_discovery_cache_serialization() {
        let pairs = vec![]; // Empty for testing

        // Create v2 cache
        let cache = DiscoveryCache::new(pairs, vec!["epl".to_string(), "nba".to_string()], 3);

        // Serialize
        let json = serde_json::to_string(&cache).unwrap();

        // Deserialize
        let deserialized: DiscoveryCache = serde_json::from_str(&json).unwrap();

        // Verify fields
        assert_eq!(deserialized.schema_version, 2);
        assert_eq!(
            deserialized.leagues,
            vec!["epl".to_string(), "nba".to_string()]
        );
        assert_eq!(deserialized.horizon_days, 3);
    }

    #[test]
    fn test_legacy_cache_detection() {
        // Simulate legacy v1 cache JSON (no schema_version, leagues, or horizon_days)
        let legacy_json = r#"{
            "timestamp_secs": 1234567890,
            "pairs": [],
            "known_kalshi_tickers": []
        }"#;

        // Should deserialize as legacy
        let legacy: Result<DiscoveryCacheLegacy, _> = serde_json::from_str(legacy_json);
        assert!(legacy.is_ok());

        // When deserialized as v2, fields should have defaults
        let v2_with_defaults: DiscoveryCache = serde_json::from_str(legacy_json).unwrap();
        assert_eq!(v2_with_defaults.schema_version, 0); // default for u32
        assert_eq!(v2_with_defaults.leagues, Vec::<String>::new()); // default for Vec
        assert_eq!(v2_with_defaults.horizon_days, 0); // default for u32

        // matches_config should reject due to schema_version != 2
        assert!(!v2_with_defaults.matches_config(&vec![], 3));
    }
}
