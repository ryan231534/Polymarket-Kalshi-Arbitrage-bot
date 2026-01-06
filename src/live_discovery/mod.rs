//! Live Market Discovery & Join Module
//!
//! This module provides real-time market discovery and cross-exchange matching
//! for sports markets on Kalshi and Polymarket. Unlike the slug-based approach,
//! this module discovers actual open/live events via REST APIs and joins them
//! using normalized metadata (team names + start times).
//!
//! ## Architecture
//!
//! - **kalshi_discovery**: Discovers open Kalshi events/markets by series ticker
//! - **polymarket_discovery**: Discovers active Polymarket events via Gamma API
//! - **normalize**: Canonicalizes team names and builds matchup keys
//! - **join**: Joins Kalshi and Polymarket events into matched market pairs
//! - **types**: Core data structures for discovery metadata
//!
//! ## Live Games Mode
//!
//! The `LIVE_GAMES_MODE` env var controls what "live" means:
//! - `tradeable` (legacy): Any active/open market, including futures and awards
//! - `window` (default): Games starting within a time window (tonight's slate)
//! - `in_play`: Only games currently being played
//!
//! ## Usage
//!
//! ```ignore
//! let engine = LiveDiscoveryEngine::new(kalshi_client, gamma_client);
//! let league_spec = LeagueSpec::nfl();
//! let result = engine.discover_and_join(&league_spec).await?;
//!
//! // result.matched_markets contains pairs ready for WS subscription
//! // result.unmatched_kalshi and result.unmatched_poly for debugging
//! ```

mod join;
mod kalshi_discovery;
mod normalize;
mod polymarket_discovery;
pub mod types;

pub use join::{join_markets, JoinConfig, JoinResult};
pub use kalshi_discovery::{discover_kalshi_events, KalshiDiscoveryConfig};
pub use normalize::{build_matchup_key, normalize_team_name, NormalizationConfig};
pub use polymarket_discovery::{discover_poly_events, PolyDiscoveryConfig};
pub use types::matched_markets_to_pairs;
pub use types::*;

use anyhow::Result;
use chrono::{Duration, Utc};
use std::sync::Arc;
use tracing::{info, warn};

use crate::kalshi::KalshiApiClient;
use crate::polymarket::GammaClient;

/// Live games filtering mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LiveGamesMode {
    /// Legacy: Any active/open market (includes futures and awards)
    Tradeable,
    /// Games starting within a time window (tonight's slate) - DEFAULT
    #[default]
    Window,
    /// Only games currently being played
    InPlay,
}

impl LiveGamesMode {
    /// Parse from environment variable string
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "tradeable" | "legacy" | "active" => LiveGamesMode::Tradeable,
            "in_play" | "inplay" | "live" => LiveGamesMode::InPlay,
            "window" | "tonight" | "slate" => LiveGamesMode::Window,
            _ => LiveGamesMode::Window, // Default to window mode
        }
    }
}

/// Configuration for live games time window filtering
#[derive(Debug, Clone)]
pub struct LiveGamesConfig {
    /// Filtering mode: tradeable, window, or in_play
    pub mode: LiveGamesMode,
    /// Hours to look back for games that already started (default: 6)
    pub lookback_hours: u32,
    /// Hours to look ahead for upcoming games (default: 12)
    pub lookahead_hours: u32,
    /// Minutes before game start to include (for in_play mode, default: 30)
    pub pregame_minutes: u32,
    /// Minutes after game end to include (for in_play mode, default: 60)
    pub postgame_minutes: u32,
}

impl Default for LiveGamesConfig {
    fn default() -> Self {
        Self {
            mode: LiveGamesMode::Window,
            lookback_hours: 6,
            lookahead_hours: 12,
            pregame_minutes: 30,
            postgame_minutes: 60,
        }
    }
}

impl LiveGamesConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let mode = std::env::var("LIVE_GAMES_MODE")
            .map(|v| LiveGamesMode::from_str(&v))
            .unwrap_or(LiveGamesMode::Window);

        let lookback_hours = std::env::var("LIVE_GAMES_LOOKBACK_HOURS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(6);

        let lookahead_hours = std::env::var("LIVE_GAMES_LOOKAHEAD_HOURS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(12);

        let pregame_minutes = std::env::var("LIVE_GAMES_PREGAME_MINUTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);

        let postgame_minutes = std::env::var("LIVE_GAMES_POSTGAME_MINUTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(60);

        Self {
            mode,
            lookback_hours,
            lookahead_hours,
            pregame_minutes,
            postgame_minutes,
        }
    }

    /// Check if an event is within the configured time window
    pub fn is_in_window(
        &self,
        start_time: Option<chrono::DateTime<Utc>>,
        end_time: Option<chrono::DateTime<Utc>>,
    ) -> bool {
        let now = Utc::now();

        match self.mode {
            LiveGamesMode::Tradeable => true, // Accept all tradeable markets

            LiveGamesMode::Window => {
                // Event must start within [now - lookback, now + lookahead]
                if let Some(start) = start_time {
                    let window_start = now - Duration::hours(self.lookback_hours as i64);
                    let window_end = now + Duration::hours(self.lookahead_hours as i64);
                    start >= window_start && start <= window_end
                } else {
                    false // No start time = can't determine, exclude
                }
            }

            LiveGamesMode::InPlay => {
                // Event must be "live": between (start - pregame) and (end + postgame)
                let pregame = Duration::minutes(self.pregame_minutes as i64);
                let postgame = Duration::minutes(self.postgame_minutes as i64);

                match (start_time, end_time) {
                    (Some(start), Some(end)) => {
                        let live_start = start - pregame;
                        let live_end = end + postgame;
                        now >= live_start && now <= live_end
                    }
                    (Some(start), None) => {
                        // No end time: assume 3-hour duration for typical game
                        let live_start = start - pregame;
                        let live_end = start + Duration::hours(3) + postgame;
                        now >= live_start && now <= live_end
                    }
                    _ => false, // No start time = exclude
                }
            }
        }
    }
}

/// Configuration for live discovery feature flags
#[derive(Debug, Clone)]
pub struct LiveDiscoveryConfig {
    /// Enable live join (LIVE_JOIN env var, default: true for sports)
    pub live_join_enabled: bool,
    /// Fall back to slug mapping if join yields 0 matches (FALLBACK_SLUG_MAP, default: false)
    pub fallback_slug_map: bool,
    /// Time tolerance in hours for matching events (default: 6)
    pub time_tolerance_hours: u32,
    /// Refresh interval for discovery in seconds (default: 300 = 5 min)
    pub refresh_interval_secs: u64,
    /// Live games filtering configuration
    pub live_games: LiveGamesConfig,
}

impl Default for LiveDiscoveryConfig {
    fn default() -> Self {
        Self {
            live_join_enabled: true,
            fallback_slug_map: false,
            time_tolerance_hours: 6,
            refresh_interval_secs: 300,
            live_games: LiveGamesConfig::default(),
        }
    }
}

impl LiveDiscoveryConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let live_join_enabled = std::env::var("LIVE_JOIN")
            .map(|v| v != "0" && v.to_lowercase() != "false")
            .unwrap_or(true);

        let fallback_slug_map = std::env::var("FALLBACK_SLUG_MAP")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false);

        let time_tolerance_hours = std::env::var("LIVE_JOIN_TIME_TOLERANCE_HOURS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(6);

        let refresh_interval_secs = std::env::var("LIVE_DISCOVERY_REFRESH_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(300);

        let live_games = LiveGamesConfig::from_env();

        Self {
            live_join_enabled,
            fallback_slug_map,
            time_tolerance_hours,
            refresh_interval_secs,
            live_games,
        }
    }
}

/// Main engine for live market discovery and cross-exchange joining
pub struct LiveDiscoveryEngine {
    kalshi: Arc<KalshiApiClient>,
    gamma: Arc<GammaClient>,
    config: LiveDiscoveryConfig,
}

impl LiveDiscoveryEngine {
    /// Create a new discovery engine with default configuration
    pub fn new(kalshi: Arc<KalshiApiClient>, gamma: Arc<GammaClient>) -> Self {
        Self {
            kalshi,
            gamma,
            config: LiveDiscoveryConfig::from_env(),
        }
    }

    /// Create a new discovery engine with custom configuration
    pub fn with_config(
        kalshi: Arc<KalshiApiClient>,
        gamma: Arc<GammaClient>,
        config: LiveDiscoveryConfig,
    ) -> Self {
        Self {
            kalshi,
            gamma,
            config,
        }
    }

    /// Check if live join is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.live_join_enabled
    }

    /// Discover and join markets for a given league specification
    ///
    /// Returns matched market pairs along with unmatched events for debugging
    pub async fn discover_and_join(&self, league_spec: &LeagueSpec) -> Result<DiscoveryJoinResult> {
        info!(
            event = "live_discovery_start",
            league = %league_spec.league_code,
            mode = ?self.config.live_games.mode,
            lookback_hours = self.config.live_games.lookback_hours,
            lookahead_hours = self.config.live_games.lookahead_hours,
            "Starting live discovery for {} (mode={:?})",
            league_spec.league_code,
            self.config.live_games.mode
        );

        // Step 1: Discover Kalshi events
        let kalshi_config = KalshiDiscoveryConfig {
            series_tickers: league_spec.kalshi_series_tickers.clone(),
            status_filter: "open".to_string(),
            limit_per_series: 100,
            live_games: self.config.live_games.clone(),
        };

        let kalshi_events = discover_kalshi_events(&self.kalshi, &kalshi_config).await?;

        info!(
            event = "discovery_kalshi_open_events_count",
            count = kalshi_events.len(),
            league = %league_spec.league_code,
            "Discovered {} Kalshi game events (after filtering)",
            kalshi_events.len()
        );

        // Step 2: Discover Polymarket events
        let poly_config = PolyDiscoveryConfig {
            league_tag: league_spec.polymarket_tag.clone(),
            series_slug_prefix: league_spec.polymarket_slug_prefix.clone(),
            active_only: true,
            closed_filter: false,
            live_games: self.config.live_games.clone(),
        };

        let poly_events = discover_poly_events(&self.gamma, &poly_config).await?;

        info!(
            event = "discovery_polymarket_active_events_count",
            count = poly_events.len(),
            league = %league_spec.league_code,
            "Discovered {} Polymarket active events",
            poly_events.len()
        );

        // Step 3: Join/match events
        let join_config = JoinConfig {
            time_tolerance_hours: self.config.time_tolerance_hours,
            normalization: NormalizationConfig::default(),
        };

        let join_result = join_markets(&kalshi_events, &poly_events, &join_config);

        info!(
            event = "join_matched_pairs_count",
            matched = join_result.matched.len(),
            unmatched_kalshi = join_result.unmatched_kalshi.len(),
            unmatched_poly = join_result.unmatched_poly.len(),
            league = %league_spec.league_code,
            "Joined markets: {} matched, {} unmatched Kalshi, {} unmatched Poly",
            join_result.matched.len(),
            join_result.unmatched_kalshi.len(),
            join_result.unmatched_poly.len()
        );

        // Log sample matched pairs
        for (i, matched) in join_result.matched.iter().take(3).enumerate() {
            info!(
                event = "matched_pair_sample",
                index = i,
                matchup_key = %matched.matchup_key,
                kalshi_ticker = %matched.kalshi_event_ticker,
                poly_event_id = %matched.poly_event_id,
                "Sample matched pair {}: {} (K:{} P:{})",
                i + 1,
                matched.matchup_key,
                matched.kalshi_event_ticker,
                matched.poly_event_id
            );
        }

        // Log top unmatched for debugging
        if !join_result.unmatched_kalshi.is_empty() {
            warn!(
                event = "unmatched_kalshi_events",
                count = join_result.unmatched_kalshi.len(),
                "Top {} unmatched Kalshi events:",
                join_result.unmatched_kalshi.len().min(10)
            );
            for event in join_result.unmatched_kalshi.iter().take(10) {
                warn!(
                    "  - {} | {} | {}",
                    event.event_ticker,
                    event.title,
                    event.start_time.map(|t| t.to_string()).unwrap_or_default()
                );
            }
        }

        if !join_result.unmatched_poly.is_empty() {
            warn!(
                event = "unmatched_polymarket_events",
                count = join_result.unmatched_poly.len(),
                "Top {} unmatched Polymarket events:",
                join_result.unmatched_poly.len().min(10)
            );
            for event in join_result.unmatched_poly.iter().take(10) {
                warn!(
                    "  - {} | {} | {}",
                    event.event_id,
                    event.title,
                    event.start_time.map(|t| t.to_string()).unwrap_or_default()
                );
            }
        }

        Ok(DiscoveryJoinResult {
            matched_markets: join_result.matched,
            unmatched_kalshi: join_result.unmatched_kalshi,
            unmatched_poly: join_result.unmatched_poly,
            kalshi_discovered_count: kalshi_events.len(),
            poly_discovered_count: poly_events.len(),
        })
    }

    /// Discover and join for multiple leagues
    pub async fn discover_all_leagues(
        &self,
        league_specs: &[LeagueSpec],
    ) -> Result<Vec<DiscoveryJoinResult>> {
        let mut results = Vec::with_capacity(league_specs.len());

        for spec in league_specs {
            match self.discover_and_join(spec).await {
                Ok(result) => results.push(result),
                Err(e) => {
                    warn!(
                        event = "league_discovery_error",
                        league = %spec.league_code,
                        error = %e,
                        "Failed to discover league {}: {}",
                        spec.league_code,
                        e
                    );
                }
            }
        }

        Ok(results)
    }
}

/// Result of discovery + join operation
#[derive(Debug, Clone)]
pub struct DiscoveryJoinResult {
    /// Successfully matched market pairs
    pub matched_markets: Vec<MatchedMarket>,
    /// Kalshi events that could not be matched
    pub unmatched_kalshi: Vec<KalshiEventMeta>,
    /// Polymarket events that could not be matched
    pub unmatched_poly: Vec<PolyEventMeta>,
    /// Total Kalshi events discovered
    pub kalshi_discovered_count: usize,
    /// Total Polymarket events discovered
    pub poly_discovered_count: usize,
}

impl DiscoveryJoinResult {
    /// Check if any markets were matched
    pub fn has_matches(&self) -> bool {
        !self.matched_markets.is_empty()
    }

    /// Get match rate as a percentage
    pub fn match_rate(&self) -> f64 {
        if self.kalshi_discovered_count == 0 {
            return 0.0;
        }
        (self.matched_markets.len() as f64 / self.kalshi_discovered_count as f64) * 100.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_from_env_defaults() {
        // Clear any existing env vars for clean test
        std::env::remove_var("LIVE_JOIN");
        std::env::remove_var("FALLBACK_SLUG_MAP");

        let config = LiveDiscoveryConfig::default();
        assert!(config.live_join_enabled);
        assert!(!config.fallback_slug_map);
        assert_eq!(config.time_tolerance_hours, 6);
        assert_eq!(config.refresh_interval_secs, 300);
    }

    #[test]
    fn test_discovery_join_result_match_rate() {
        let result = DiscoveryJoinResult {
            matched_markets: vec![],
            unmatched_kalshi: vec![],
            unmatched_poly: vec![],
            kalshi_discovered_count: 10,
            poly_discovered_count: 8,
        };
        assert_eq!(result.match_rate(), 0.0);

        // With 0 kalshi events, avoid division by zero
        let result_empty = DiscoveryJoinResult {
            matched_markets: vec![],
            unmatched_kalshi: vec![],
            unmatched_poly: vec![],
            kalshi_discovered_count: 0,
            poly_discovered_count: 0,
        };
        assert_eq!(result_empty.match_rate(), 0.0);
    }
}
