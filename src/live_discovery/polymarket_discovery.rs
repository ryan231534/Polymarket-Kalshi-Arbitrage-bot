//! Polymarket event/market discovery via Gamma REST API.
//!
//! This module handles discovering active/open Polymarket sports events
//! using the Gamma API, which provides market metadata including CLOB token IDs.

use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::Deserialize;
use std::sync::Arc;
use tracing::{debug, info};

use crate::config::GAMMA_API_BASE;
use crate::polymarket::GammaClient;

use super::normalize::parse_teams_from_title;
use super::types::{PolyEventMeta, PolyMarketMeta};

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
}

impl Default for PolyDiscoveryConfig {
    fn default() -> Self {
        Self {
            league_tag: None,
            series_slug_prefix: None,
            active_only: true,
            closed_filter: false,
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

    info!("Fetched {} Polymarket events from Gamma API", events.len());

    // Filter by slug prefix and end date
    let now = Utc::now();
    let filtered_events: Vec<&GammaEventResponse> = events
        .iter()
        .filter(|e| {
            // Must have end date in the future
            let is_future = e
                .end_date
                .as_ref()
                .and_then(|d| DateTime::parse_from_rfc3339(d).ok())
                .map(|dt| dt > now)
                .unwrap_or(false);

            if !is_future {
                return false;
            }

            // Filter by slug prefix if specified
            if let Some(prefix) = &config.series_slug_prefix {
                e.slug
                    .as_ref()
                    .map(|s| s.starts_with(prefix))
                    .unwrap_or(false)
            } else {
                true
            }
        })
        .collect();

    info!(
        "Filtered to {} events matching criteria (future end date, slug prefix)",
        filtered_events.len()
    );

    // Convert to PolyEventMeta
    let poly_events: Vec<PolyEventMeta> = filtered_events
        .into_iter()
        .filter_map(build_poly_event_from_gamma_event)
        .collect();

    Ok(poly_events)
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
    let market_type = market.sports_market_type.clone().unwrap_or_else(|| {
        if slug.contains("spread") {
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
