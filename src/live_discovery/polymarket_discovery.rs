//! Polymarket event/market discovery via Gamma REST API.
//!
//! This module handles discovering active/open Polymarket sports events
//! using the Gamma API, which provides market metadata including CLOB token IDs.
//!
//! ## Live Games Filtering
//!
//! When `LIVE_GAMES_MODE` is set to "window" or "in_play", this module filters
//! out non-game events (awards, futures) and only returns actual game markets.

use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::Deserialize;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::config::GAMMA_API_BASE;
use crate::polymarket::GammaClient;

use super::normalize::parse_teams_from_title;
use super::types::{PolyEventMeta, PolyMarketMeta};
use super::{LiveGamesConfig, LiveGamesMode};

/// Configuration for Polymarket discovery
#[derive(Debug, Clone)]
pub struct PolyDiscoveryConfig {
    /// Tag/category filter (e.g., "nfl", "ncaamb")
    pub league_tag: Option<String>,
    /// Slug prefix filter (e.g., "nfl-", "cbb-")
    pub series_slug_prefix: Option<String>,
    /// Only return active markets
    pub active_only: bool,
    /// Exclude closed markets
    pub closed_filter: bool,
    /// Live games filtering configuration
    pub live_games: LiveGamesConfig,
}

impl Default for PolyDiscoveryConfig {
    fn default() -> Self {
        Self {
            league_tag: None,
            series_slug_prefix: None,
            active_only: true,
            closed_filter: false,
            live_games: LiveGamesConfig::default(),
        }
    }
}

/// Raw Gamma API event response
#[derive(Debug, Deserialize)]
struct GammaEventResponse {
    #[allow(dead_code)]
    id: Option<String>,
    slug: Option<String>,
    title: Option<String>,
    #[serde(rename = "endDate")]
    end_date: Option<String>,
    #[serde(rename = "startTime")]
    start_time: Option<String>,
    #[serde(rename = "gameStartTime")]
    game_start_time: Option<String>,
    #[allow(dead_code)]
    active: Option<bool>,
    #[allow(dead_code)]
    closed: Option<bool>,
    #[allow(dead_code)]
    live: Option<bool>,
    markets: Option<Vec<GammaMarketInEvent>>,
}

/// Market embedded in event response
#[derive(Debug, Deserialize)]
struct GammaMarketInEvent {
    #[allow(dead_code)]
    id: Option<String>,
    slug: Option<String>,
    question: Option<String>,
    #[serde(rename = "conditionId")]
    condition_id: Option<String>,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: Option<String>,
    #[allow(dead_code)]
    outcomes: Option<String>,
    #[serde(rename = "sportsMarketType")]
    sports_market_type: Option<String>,
    #[allow(dead_code)]
    #[serde(rename = "groupItemTitle")]
    group_item_title: Option<String>,
}

/// Discover active Polymarket events for the specified configuration.
///
/// Uses Gamma API events endpoint to query sports events directly.
/// The events endpoint returns complete event objects with embedded markets.
///
/// ## Filtering Pipeline
///
/// 1. Fetch all events matching tag/slug prefix
/// 2. Filter to "game-only" events (has teams, has start time, has arb-able markets)
/// 3. Apply time window filter based on LIVE_GAMES_MODE
pub async fn discover_poly_events(
    _gamma: &Arc<GammaClient>,
    config: &PolyDiscoveryConfig,
) -> Result<Vec<PolyEventMeta>> {
    // Build query parameters for events endpoint
    let mut query_parts = Vec::new();

    if config.active_only {
        query_parts.push("active=true".to_string());
    }

    if !config.closed_filter {
        query_parts.push("closed=false".to_string());
    }

    // Add tag filter if specified - map internal tags to Polymarket's tag slugs
    if let Some(tag) = &config.league_tag {
        let poly_tag = match tag.as_str() {
            "ncaamb" | "cbb" => "ncaa-basketball",
            "ncaawb" | "cwbb" => "ncaa-basketball",
            "nfl" => "nfl",
            "nba" => "nba",
            "nhl" => "nhl",
            "mlb" => "mlb",
            "epl" => "epl",
            "mls" => "mls",
            other => other,
        };
        query_parts.push(format!("tag_slug={}", poly_tag));
    }

    // Higher limit for events since there are many games
    query_parts.push("limit=500".to_string());

    let query_string = query_parts.join("&");
    let url = format!("{}/events?{}", GAMMA_API_BASE, query_string);

    debug!("Fetching Polymarket events from: {}", url);

    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    let resp = http.get(&url).send().await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Gamma API error {}: {}", status, body);
    }

    let events: Vec<GammaEventResponse> = resp.json().await?;

    let total_fetched = events.len();
    info!("Fetched {} Polymarket events from Gamma API", total_fetched);

    let now = Utc::now();

    // Track exclusion reasons for logging
    let mut excluded_no_teams = 0;
    let mut excluded_no_start_time = 0;
    let mut excluded_no_arb_markets = 0;
    let mut excluded_time_window = 0;
    let mut excluded_past = 0;
    let mut exclusion_samples: Vec<(String, String)> = Vec::new(); // (title, reason)

    // Collect game events
    let mut game_events: Vec<PolyEventMeta> = Vec::new();

    for event in &events {
        let slug = match &event.slug {
            Some(s) => s.clone(),
            None => continue,
        };
        let title = match &event.title {
            Some(t) => t.clone(),
            None => continue,
        };

        // Filter by slug prefix if specified
        if let Some(prefix) = &config.series_slug_prefix {
            if !slug.starts_with(prefix) {
                continue;
            }
        }

        // Parse start time (required for game events)
        let start_time = event
            .game_start_time
            .as_ref()
            .or(event.start_time.as_ref())
            .and_then(|s| parse_gamma_datetime(s));

        // Parse end time for in_play mode
        let end_time = event
            .end_date
            .as_ref()
            .and_then(|s| parse_gamma_datetime(s));

        // === GAME-ONLY FILTER (when not in tradeable mode) ===
        if config.live_games.mode != LiveGamesMode::Tradeable {
            // 1. Must have parseable teams (home vs away)
            let has_teams = parse_teams_from_title(&title).is_some();
            if !has_teams {
                excluded_no_teams += 1;
                if exclusion_samples.len() < 5 {
                    exclusion_samples.push((
                        title.clone(),
                        "no teams parsed (likely award/future)".to_string(),
                    ));
                }
                continue;
            }

            // 2. Must have a start time
            if start_time.is_none() {
                excluded_no_start_time += 1;
                if exclusion_samples.len() < 5 {
                    exclusion_samples.push((title.clone(), "missing start time".to_string()));
                }
                continue;
            }

            // 3. Must have at least one arb-able market type (moneyline/spread/total/puckline)
            let has_arb_market = event
                .markets
                .as_ref()
                .map(|mks| {
                    mks.iter().any(|m| {
                        // Check sportsMarketType
                        // Note: NHL uses "puckline" instead of "spread"
                        let by_type = m
                            .sports_market_type
                            .as_ref()
                            .map(|t| {
                                matches!(
                                    t.to_lowercase().as_str(),
                                    "moneyline" | "spread" | "total" | "game" | "puckline" | "puck line"
                                )
                            })
                            .unwrap_or(false);

                        // Check slug for market type keywords
                        let by_slug = m
                            .slug
                            .as_ref()
                            .map(|s| {
                                let s_lower = s.to_lowercase();
                                s_lower.contains("spread")
                                    || s_lower.contains("total")
                                    || s_lower.contains("moneyline")
                                    || s_lower.contains("-winner")
                                    || s_lower.contains("win-")
                                    || s_lower.contains("puckline")
                                    || s_lower.contains("puck-line")
                            })
                            .unwrap_or(false);

                        by_type || by_slug
                    })
                })
                .unwrap_or(false);

            if !has_arb_market {
                excluded_no_arb_markets += 1;
                if exclusion_samples.len() < 5 {
                    exclusion_samples.push((
                        title.clone(),
                        "no arb-able market type (moneyline/spread/total/puckline)".to_string(),
                    ));
                }
                continue;
            }

            // 4. Apply time window filter
            if !config.live_games.is_in_window(start_time, end_time) {
                excluded_time_window += 1;
                if exclusion_samples.len() < 5 {
                    let reason = match start_time {
                        Some(st) => format!("outside time window (starts {})", st),
                        None => "outside time window".to_string(),
                    };
                    exclusion_samples.push((title.clone(), reason));
                }
                continue;
            }
        } else {
            // Tradeable mode: just check future end date
            let is_future = end_time.map(|dt| dt > now).unwrap_or(false);

            if !is_future {
                excluded_past += 1;
                continue;
            }
        }

        // Build PolyEventMeta
        if let Some(meta) = build_poly_event_from_gamma_event(event) {
            game_events.push(meta);
        }
    }

    // Log filtering summary
    let game_count = game_events.len();
    info!(
        "Polymarket filtering: {} total -> {} games ({} no teams, {} no start time, {} no arb markets, {} outside window, {} past)",
        total_fetched, game_count, excluded_no_teams, excluded_no_start_time,
        excluded_no_arb_markets, excluded_time_window, excluded_past
    );

    // Log sample exclusions
    if !exclusion_samples.is_empty() {
        warn!("Sample excluded Polymarket events:");
        for (title, reason) in exclusion_samples.iter().take(5) {
            warn!("  - {} | {}", title, reason);
        }
    }

    Ok(game_events)
}

/// Build PolyEventMeta from Gamma event response
fn build_poly_event_from_gamma_event(event: &GammaEventResponse) -> Option<PolyEventMeta> {
    let slug = event.slug.clone()?;
    let title = event.title.clone()?;

    // Parse start time from gameStartTime or startTime
    let start_time = event
        .game_start_time
        .as_ref()
        .or(event.start_time.as_ref())
        .and_then(|s| parse_gamma_datetime(s));

    // Parse teams from title
    let (home_team, away_team) = parse_teams_from_title(&title)
        .map(|(h, a)| (Some(h), Some(a)))
        .unwrap_or((None, None));

    // Convert embedded markets
    let markets: Vec<PolyMarketMeta> = event
        .markets
        .as_ref()
        .map(|mks| {
            mks.iter()
                .filter_map(build_poly_market_from_embedded)
                .collect()
        })
        .unwrap_or_default();

    Some(PolyEventMeta {
        event_id: slug.clone(),
        slug,
        title,
        start_time,
        home_team,
        away_team,
        category: None,
        markets,
    })
}

/// Build PolyMarketMeta from embedded market in event
fn build_poly_market_from_embedded(market: &GammaMarketInEvent) -> Option<PolyMarketMeta> {
    let slug = market.slug.clone()?;
    let question = market.question.clone()?;

    // Parse CLOB token IDs
    let (clob_yes, clob_no) = parse_clob_token_ids(market.clob_token_ids.as_deref());

    // Require both CLOB tokens
    let clob_token_id_yes = clob_yes?;
    let clob_token_id_no = clob_no?;

    // Determine market type
    // Note: NHL "puckline" is equivalent to spread for arbitrage purposes
    let market_type = market.sports_market_type.clone().map(|t| {
        let t_lower = t.to_lowercase();
        if t_lower == "puckline" || t_lower == "puck line" {
            "spread".to_string() // Normalize puckline to spread
        } else {
            t
        }
    }).unwrap_or_else(|| {
        if slug.contains("spread") || slug.contains("puckline") || slug.contains("puck-line") {
            "spread".to_string()
        } else if slug.contains("total") {
            "total".to_string()
        } else {
            "moneyline".to_string()
        }
    });

    Some(PolyMarketMeta {
        market_id: market.condition_id.clone().unwrap_or_else(|| slug.clone()),
        question,
        slug,
        clob_token_id_yes,
        clob_token_id_no,
        market_type,
        active: true,
        closed: false,
    })
}

/// Parse CLOB token IDs from JSON string array
fn parse_clob_token_ids(raw: Option<&str>) -> (Option<String>, Option<String>) {
    raw.and_then(|s| {
        let tokens: Vec<String> = serde_json::from_str(s).ok()?;
        if tokens.len() >= 2 {
            Some((Some(tokens[0].clone()), Some(tokens[1].clone())))
        } else if tokens.len() == 1 {
            Some((Some(tokens[0].clone()), None))
        } else {
            None
        }
    })
    .unwrap_or((None, None))
}

/// Parse datetime from Gamma API format
fn parse_gamma_datetime(s: &str) -> Option<DateTime<Utc>> {
    // Try ISO 8601 / RFC3339 format first
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }

    // Try PostgreSQL format: "2026-01-03 19:00:00+00"
    let trimmed = s.trim_end_matches("+00").trim_end_matches("+00:00");
    if let Ok(ndt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S") {
        return Some(ndt.and_utc());
    }

    // Try date only
    if s.len() == 10 {
        if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            if let Some(dt) = date.and_hms_opt(12, 0, 0) {
                return Some(dt.and_utc());
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_clob_token_ids() {
        let (yes, no) = parse_clob_token_ids(Some(r#"["abc123", "def456"]"#));
        assert_eq!(yes, Some("abc123".to_string()));
        assert_eq!(no, Some("def456".to_string()));
    }

    #[test]
    fn test_parse_clob_token_ids_invalid() {
        assert_eq!(parse_clob_token_ids(None), (None, None));
        assert_eq!(parse_clob_token_ids(Some("invalid")), (None, None));
        assert_eq!(parse_clob_token_ids(Some("[]")), (None, None));
    }

    #[test]
    fn test_parse_gamma_datetime() {
        let cases = vec![
            ("2026-01-05T18:30:00Z", true),
            ("2026-01-05T18:30:00.000Z", true),
            ("2026-01-05 19:00:00+00", true),
            ("2026-01-05", true),
            ("invalid", false),
        ];

        for (input, should_parse) in cases {
            let result = parse_gamma_datetime(input);
            assert_eq!(
                result.is_some(),
                should_parse,
                "Failed for input: {}",
                input
            );
        }
    }
}
