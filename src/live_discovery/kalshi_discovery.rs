//! Kalshi event/market discovery via REST API.
//!
//! This module handles discovering open/live Kalshi events and markets
//! for sports leagues using the Kalshi REST API.

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::kalshi::KalshiApiClient;
use crate::types::{KalshiEvent, KalshiMarket};

use super::normalize::{parse_teams_from_title, NormalizationConfig};
use super::types::{KalshiEventMeta, KalshiMarketMeta, LiveMarketType};

/// Configuration for Kalshi discovery
#[derive(Debug, Clone)]
pub struct KalshiDiscoveryConfig {
    /// Series tickers to query (e.g., ["KXNFLGAME", "KXNFLSPREAD"])
    pub series_tickers: Vec<String>,
    /// Event status filter (typically "open")
    pub status_filter: String,
    /// Maximum events per series
    pub limit_per_series: u32,
}

impl Default for KalshiDiscoveryConfig {
    fn default() -> Self {
        Self {
            series_tickers: vec![],
            status_filter: "open".to_string(),
            limit_per_series: 100,
        }
    }
}

/// Discover open Kalshi events for the specified configuration.
///
/// Queries multiple series tickers and fetches event details including
/// all markets within each event.
pub async fn discover_kalshi_events(
    client: &Arc<KalshiApiClient>,
    config: &KalshiDiscoveryConfig,
) -> Result<Vec<KalshiEventMeta>> {
    let mut all_events = Vec::new();

    for series_ticker in &config.series_tickers {
        debug!("Discovering Kalshi events for series: {}", series_ticker);

        // Fetch events for this series
        let events = match client
            .get_events(series_ticker, config.limit_per_series)
            .await
        {
            Ok(events) => events,
            Err(e) => {
                warn!(
                    "Failed to fetch Kalshi events for series {}: {}",
                    series_ticker, e
                );
                continue;
            }
        };

        info!(
            "Found {} Kalshi events for series {}",
            events.len(),
            series_ticker
        );

        // Process each event
        for event in events {
            // Fetch markets for this event
            let markets = match client.get_markets(&event.event_ticker).await {
                Ok(markets) => markets,
                Err(e) => {
                    warn!(
                        "Failed to fetch markets for event {}: {}",
                        event.event_ticker, e
                    );
                    continue;
                }
            };

            // Filter to only active markets (tradeable)
            let active_markets: Vec<_> = markets
                .into_iter()
                .filter(|m| m.status.as_deref() == Some("active"))
                .collect();

            // Skip events with no active markets
            if active_markets.is_empty() {
                debug!(
                    "Skipping event {} - no active markets",
                    event.event_ticker
                );
                continue;
            }

            // Parse event metadata
            let event_meta = parse_kalshi_event(&event, &active_markets, series_ticker);
            all_events.push(event_meta);
        }

        // Small delay between series to avoid rate limiting
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    Ok(all_events)
}

/// Parse a Kalshi event and its markets into our metadata format
fn parse_kalshi_event(
    event: &KalshiEvent,
    markets: &[KalshiMarket],
    series_ticker: &str,
) -> KalshiEventMeta {
    // Try to get start time from market's expected_expiration_time first (most accurate)
    // The expected_expiration_time is when the game ends, so we subtract ~2.5 hours for start
    let start_time = markets
        .first()
        .and_then(|m| m.expected_expiration_time.as_ref())
        .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
        .map(|dt| dt.with_timezone(&Utc) - chrono::Duration::hours(2))
        .or_else(|| parse_start_time_from_ticker(&event.event_ticker));

    // Parse team names from title
    let (home_team, away_team) = parse_teams_from_title(&event.title)
        .map(|(a, b)| (Some(b), Some(a))) // Convert to (home, away) - typically second team is home
        .unwrap_or((None, None));

    // Parse markets
    let market_metas: Vec<KalshiMarketMeta> = markets
        .iter()
        .map(|m| parse_kalshi_market(m, series_ticker))
        .collect();

    KalshiEventMeta {
        event_ticker: event.event_ticker.clone(),
        title: event.title.clone(),
        start_time,
        home_team,
        away_team,
        series_ticker: series_ticker.to_string(),
        markets: market_metas,
    }
}

/// Parse a Kalshi market into our metadata format
fn parse_kalshi_market(market: &KalshiMarket, series_ticker: &str) -> KalshiMarketMeta {
    // Determine market type from series ticker
    let market_type = if series_ticker.contains("SPREAD") {
        "spread".to_string()
    } else if series_ticker.contains("TOTAL") {
        "total".to_string()
    } else if series_ticker.contains("BTTS") {
        "btts".to_string()
    } else {
        "moneyline".to_string()
    };

    // Extract team from ticker (last component)
    let team = market.ticker.rsplit('-').next().map(|s| s.to_string());

    KalshiMarketMeta {
        ticker: market.ticker.clone(),
        title: market.title.clone(),
        market_type,
        line_value: market.floor_strike,
        team,
    }
}

/// Parse start time from Kalshi event ticker.
///
/// Kalshi tickers have format like: KXNFLGAME-26JAN01BUFDET
/// The date portion is YYMMMDD where MMM is 3-letter month
fn parse_start_time_from_ticker(ticker: &str) -> Option<DateTime<Utc>> {
    // Extract the date portion after the first dash
    let parts: Vec<&str> = ticker.split('-').collect();
    if parts.len() < 2 {
        return None;
    }

    let date_part = parts[1];
    if date_part.len() < 7 {
        return None;
    }

    // Parse YYMMMDD
    let year_str = &date_part[0..2];
    let month_str = &date_part[2..5];
    let day_str = &date_part[5..7];

    let year: i32 = year_str.parse().ok()?;
    let full_year = 2000 + year; // Assuming 2000s

    let month = match month_str.to_uppercase().as_str() {
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

    let day: u32 = day_str.parse().ok()?;

    // Create date at noon UTC (reasonable default for game time)
    NaiveDate::from_ymd_opt(full_year, month, day)
        .and_then(|date| date.and_hms_opt(12, 0, 0))
        .map(|dt| DateTime::from_naive_utc_and_offset(dt, Utc))
}

/// Parse team codes from Kalshi event ticker.
///
/// Format: KXNFLGAME-26JAN01BUFDET -> (BUF, DET)
pub fn parse_teams_from_ticker(ticker: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = ticker.split('-').collect();
    if parts.len() < 2 {
        return None;
    }

    let code_part = parts[1];
    // After date (7 chars YYMMMDD), the rest is team codes
    if code_part.len() < 13 {
        // Need at least 7 date chars + 6 for two 3-char team codes
        return None;
    }

    let teams_part = &code_part[7..];
    if teams_part.len() >= 6 {
        let team1 = teams_part[0..3].to_uppercase();
        let team2 = teams_part[3..6].to_uppercase();
        return Some((team1, team2));
    }

    None
}

/// Map 3-letter Kalshi team codes to full team names
pub fn kalshi_code_to_team_name(code: &str) -> Option<String> {
    let name = match code.to_uppercase().as_str() {
        // NFL
        "ARI" => "Arizona Cardinals",
        "ATL" => "Atlanta Falcons",
        "BAL" => "Baltimore Ravens",
        "BUF" => "Buffalo Bills",
        "CAR" => "Carolina Panthers",
        "CHI" => "Chicago Bears",
        "CIN" => "Cincinnati Bengals",
        "CLE" => "Cleveland Browns",
        "DAL" => "Dallas Cowboys",
        "DEN" => "Denver Broncos",
        "DET" => "Detroit Lions",
        "GB" | "GBP" => "Green Bay Packers",
        "HOU" => "Houston Texans",
        "IND" => "Indianapolis Colts",
        "JAX" | "JAC" => "Jacksonville Jaguars",
        "KC" | "KCC" => "Kansas City Chiefs",
        "LV" | "LVR" | "OAK" => "Las Vegas Raiders",
        "LAC" => "Los Angeles Chargers",
        "LAR" | "LA" => "Los Angeles Rams",
        "MIA" => "Miami Dolphins",
        "MIN" => "Minnesota Vikings",
        "NE" | "NEP" => "New England Patriots",
        "NO" | "NOS" => "New Orleans Saints",
        "NYG" => "New York Giants",
        "NYJ" => "New York Jets",
        "PHI" => "Philadelphia Eagles",
        "PIT" => "Pittsburgh Steelers",
        "SF" | "SFO" => "San Francisco 49ers",
        "SEA" => "Seattle Seahawks",
        "TB" | "TBB" => "Tampa Bay Buccaneers",
        "TEN" => "Tennessee Titans",
        "WAS" | "WSH" => "Washington Commanders",
        // NBA
        "BOS" => "Boston Celtics",
        "BKN" | "BRK" => "Brooklyn Nets",
        "CHA" => "Charlotte Hornets",
        "GSW" | "GS" => "Golden State Warriors",
        "LAL" => "Los Angeles Lakers",
        "MEM" => "Memphis Grizzlies",
        "MIL" => "Milwaukee Bucks",
        "NYK" => "New York Knicks",
        "OKC" => "Oklahoma City Thunder",
        "ORL" => "Orlando Magic",
        "PHX" | "PHO" => "Phoenix Suns",
        "POR" => "Portland Trail Blazers",
        "SAC" => "Sacramento Kings",
        "SAS" | "SA" => "San Antonio Spurs",
        "TOR" => "Toronto Raptors",
        "UTA" => "Utah Jazz",
        // NHL
        "ANA" => "Anaheim Ducks",
        "CGY" => "Calgary Flames",
        "COL" => "Colorado Avalanche",
        "CBJ" => "Columbus Blue Jackets",
        "EDM" => "Edmonton Oilers",
        "FLA" => "Florida Panthers",
        "MTL" => "Montreal Canadiens",
        "NJ" | "NJD" => "New Jersey Devils",
        "NYI" => "New York Islanders",
        "NYR" => "New York Rangers",
        "OTT" => "Ottawa Senators",
        "STL" => "St. Louis Blues",
        "VAN" => "Vancouver Canucks",
        "VGK" | "VEG" => "Vegas Golden Knights",
        "WPG" => "Winnipeg Jets",
        // MLB (abbreviated)
        "TEX" => "Texas Rangers",
        "SD" | "SDP" => "San Diego Padres",
        _ => return None,
    };
    Some(name.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_start_time_from_ticker() {
        let time = parse_start_time_from_ticker("KXNFLGAME-26JAN01BUFDET");
        assert!(time.is_some());
        let dt = time.unwrap();
        assert_eq!(dt.year(), 2026);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 1);
    }

    #[test]
    fn test_parse_teams_from_ticker() {
        let teams = parse_teams_from_ticker("KXNFLGAME-26JAN01BUFDET");
        assert!(teams.is_some());
        let (team1, team2) = teams.unwrap();
        assert_eq!(team1, "BUF");
        assert_eq!(team2, "DET");
    }

    #[test]
    fn test_kalshi_code_to_team_name() {
        assert_eq!(
            kalshi_code_to_team_name("BUF"),
            Some("Buffalo Bills".to_string())
        );
        assert_eq!(
            kalshi_code_to_team_name("DET"),
            Some("Detroit Lions".to_string())
        );
        assert_eq!(
            kalshi_code_to_team_name("LAL"),
            Some("Los Angeles Lakers".to_string())
        );
        assert!(kalshi_code_to_team_name("XXX").is_none());
    }

    use chrono::Datelike;

    #[test]
    fn test_parse_start_time_various_months() {
        let cases = vec![
            ("KXNBAGAME-25DEC31LALGSW", (2025, 12, 31)),
            ("KXNFLGAME-26FEB15BUFMIA", (2026, 2, 15)),
            ("KXMLBGAME-26JUL04NYYLAD", (2026, 7, 4)),
        ];

        for (ticker, expected) in cases {
            let time = parse_start_time_from_ticker(ticker);
            assert!(time.is_some(), "Failed to parse: {}", ticker);
            let dt = time.unwrap();
            assert_eq!(
                (dt.year(), dt.month(), dt.day()),
                expected,
                "Date mismatch for: {}",
                ticker
            );
        }
    }
}
