//! Kalshi event/market discovery via REST API.
//!
//! This module handles discovering open/live Kalshi events and markets
//! for sports leagues using the Kalshi REST API.
//!
//! ## Live Games Filtering
//!
//! When `LIVE_GAMES_MODE` is set to "window" or "in_play", this module filters
//! events to only include actual games within the configured time window.

use anyhow::Result;
use chrono::{DateTime, NaiveDate, Utc};
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::kalshi::KalshiApiClient;
use crate::types::{KalshiEvent, KalshiMarket};

use super::normalize::parse_teams_from_title;
use super::types::{KalshiEventMeta, KalshiMarketMeta, LiveMarketType};
use super::{LiveGamesConfig, LiveGamesMode};

/// Configuration for Kalshi discovery
#[derive(Debug, Clone)]
pub struct KalshiDiscoveryConfig {
    /// Series tickers to query (e.g., ["KXNFLGAME", "KXNFLSPREAD"])
    pub series_tickers: Vec<String>,
    /// Event status filter (typically "open")
    pub status_filter: String,
    /// Maximum events per series
    pub limit_per_series: u32,
    /// Live games filtering configuration
    pub live_games: LiveGamesConfig,
}

impl Default for KalshiDiscoveryConfig {
    fn default() -> Self {
        Self {
            series_tickers: vec![],
            status_filter: "open".to_string(),
            limit_per_series: 100,
            live_games: LiveGamesConfig::default(),
        }
    }
}

/// Discover open Kalshi events for the specified configuration.
///
/// Queries multiple series tickers and fetches event details including
/// all markets within each event.
///
/// ## Filtering Pipeline
///
/// 1. Fetch events for each series ticker
/// 2. Filter to events with active markets
/// 3. Filter to "game-only" events (can parse teams + start time)
/// 4. Apply time window filter based on LIVE_GAMES_MODE
pub async fn discover_kalshi_events(
    client: &Arc<KalshiApiClient>,
    config: &KalshiDiscoveryConfig,
) -> Result<Vec<KalshiEventMeta>> {
    let mut all_events = Vec::new();

    // Track filtering stats
    let mut total_events = 0;
    let mut after_active_filter = 0;
    let mut excluded_no_teams = 0;
    let mut excluded_no_time = 0;
    let mut excluded_time_window = 0;
    let mut exclusion_samples: Vec<(String, String)> = Vec::new();

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
        total_events += events.len();

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
                debug!("Skipping event {} - no active markets", event.event_ticker);
                continue;
            }

            after_active_filter += 1;

            // === GAME-ONLY FILTER (when not in tradeable mode) ===
            if config.live_games.mode != LiveGamesMode::Tradeable {
                // 1. Must be able to parse teams from title or ticker
                let has_teams = parse_teams_from_title(&event.title).is_some()
                    || parse_teams_from_ticker(&event.event_ticker).is_some();

                if !has_teams {
                    excluded_no_teams += 1;
                    if exclusion_samples.len() < 5 {
                        exclusion_samples
                            .push((event.event_ticker.clone(), "no teams parsed".to_string()));
                    }
                    continue;
                }

                // 2. Determine start time and end time
                // Use expected_expiration_time from first market as end time
                let end_time = active_markets
                    .first()
                    .and_then(|m| m.expected_expiration_time.as_ref())
                    .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
                    .map(|dt| dt.with_timezone(&Utc));

                // Estimate start time based on league duration
                // NBA/NHL: ~2.5 hours, NFL: ~3.5 hours, MLB: ~3 hours
                let estimated_duration = estimate_game_duration(series_ticker);
                let start_time = end_time
                    .map(|et| et - estimated_duration)
                    .or_else(|| parse_start_time_from_ticker(&event.event_ticker));

                if start_time.is_none() {
                    excluded_no_time += 1;
                    if exclusion_samples.len() < 5 {
                        exclusion_samples.push((
                            event.event_ticker.clone(),
                            "cannot determine start time".to_string(),
                        ));
                    }
                    continue;
                }

                // 3. Apply time window filter
                if !config.live_games.is_in_window(start_time, end_time) {
                    excluded_time_window += 1;
                    if exclusion_samples.len() < 5 {
                        let reason = match start_time {
                            Some(st) => format!("outside time window (starts {})", st),
                            None => "outside time window".to_string(),
                        };
                        exclusion_samples.push((event.event_ticker.clone(), reason));
                    }
                    continue;
                }
            }

            // Parse event metadata (includes start_time calculation)
            let event_meta = parse_kalshi_event(&event, &active_markets, series_ticker);
            all_events.push(event_meta);
        }

        // Small delay between series to avoid rate limiting
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    // Log filtering summary
    let final_count = all_events.len();
    info!(
        "Kalshi filtering: {} total -> {} with active markets -> {} games ({} no teams, {} no time, {} outside window)",
        total_events, after_active_filter, final_count,
        excluded_no_teams, excluded_no_time, excluded_time_window
    );

    // Log sample exclusions
    if !exclusion_samples.is_empty() {
        warn!("Sample excluded Kalshi events:");
        for (ticker, reason) in exclusion_samples.iter().take(5) {
            warn!("  - {} | {}", ticker, reason);
        }
    }

    Ok(all_events)
}

/// Estimate game duration based on league/series
fn estimate_game_duration(series_ticker: &str) -> chrono::Duration {
    let upper = series_ticker.to_uppercase();
    if upper.contains("NFL") || upper.contains("NCAAF") {
        chrono::Duration::hours(3) + chrono::Duration::minutes(30)
    } else if upper.contains("NBA") || upper.contains("NCAAMB") || upper.contains("NCAAWB") {
        chrono::Duration::hours(2) + chrono::Duration::minutes(30)
    } else if upper.contains("NHL") {
        chrono::Duration::hours(2) + chrono::Duration::minutes(45)
    } else if upper.contains("MLB") {
        chrono::Duration::hours(3)
    } else if upper.contains("EPL") || upper.contains("MLS") || upper.contains("SOCCER") {
        chrono::Duration::hours(2) // 90 min + halftime + stoppage
    } else {
        chrono::Duration::hours(3) // Default
    }
}

/// Parse a Kalshi event and its markets into our metadata format
fn parse_kalshi_event(
    event: &KalshiEvent,
    markets: &[KalshiMarket],
    series_ticker: &str,
) -> KalshiEventMeta {
    // Get end time from market's expected_expiration_time (most accurate)
    let end_time = markets
        .first()
        .and_then(|m| m.expected_expiration_time.as_ref())
        .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
        .map(|dt| dt.with_timezone(&Utc));

    // Estimate start time based on end time and league duration
    let estimated_duration = estimate_game_duration(series_ticker);
    let start_time = end_time
        .map(|et| et - estimated_duration)
        .or_else(|| parse_start_time_from_ticker(&event.event_ticker));

    // Parse team names from title first
    let (home_team, away_team) = parse_teams_from_title(&event.title)
        .map(|(a, b)| (Some(b), Some(a))) // Convert to (home, away) - typically second team is home
        .unwrap_or_else(|| {
            // Fallback: parse from ticker codes and convert to team names
            // This is especially useful for NHL where titles may be inconsistent
            if let Some((code1, code2)) = parse_teams_from_ticker(&event.event_ticker) {
                let team1 = kalshi_code_to_team_name(&code1).map(|s| s.to_lowercase());
                let team2 = kalshi_code_to_team_name(&code2).map(|s| s.to_lowercase());
                // In ticker format, first team is usually away, second is home
                (team2, team1)
            } else {
                (None, None)
            }
        });

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
        // NHL - Complete roster with standard abbreviations
        // Reference: https://en.wikipedia.org/wiki/Template:NHL_team_abbreviations
        "ANA" => "Anaheim Ducks",
        "BOS_NHL" => "Boston Bruins", // Disambiguate from Celtics if needed
        "BUF_NHL" => "Buffalo Sabres", // Disambiguate from Bills if needed
        "CGY" => "Calgary Flames",
        "CAR_NHL" => "Carolina Hurricanes", // Disambiguate from Panthers NFL
        "CHI_NHL" => "Chicago Blackhawks", // Disambiguate from Bears/Bulls
        "COL" => "Colorado Avalanche",
        "CBJ" => "Columbus Blue Jackets",
        "DAL_NHL" => "Dallas Stars", // Disambiguate from Cowboys
        "DET_NHL" => "Detroit Red Wings", // Disambiguate from Lions/Pistons
        "EDM" => "Edmonton Oilers",
        "FLA" => "Florida Panthers",
        "LAK" => "Los Angeles Kings",
        "MIN_NHL" => "Minnesota Wild", // Disambiguate from Vikings/Timberwolves
        "MTL" => "Montreal Canadiens",
        "NSH" => "Nashville Predators",
        "NJ" | "NJD" => "New Jersey Devils",
        "NYI" => "New York Islanders",
        "NYR" => "New York Rangers",
        "OTT" => "Ottawa Senators",
        "PHI_NHL" => "Philadelphia Flyers", // Disambiguate from Eagles/76ers
        "PIT_NHL" => "Pittsburgh Penguins", // Disambiguate from Steelers
        "SJS" | "SJ" => "San Jose Sharks",
        "SEA_NHL" => "Seattle Kraken", // Disambiguate from Seahawks/Mariners
        "STL" => "St. Louis Blues",
        "TBL" | "TB_NHL" => "Tampa Bay Lightning", // Disambiguate from Buccaneers
        "TOR_NHL" => "Toronto Maple Leafs", // Disambiguate from Raptors
        "UHC" | "UTA_NHL" => "Utah Mammoth", // Utah Hockey Club -> Utah Mammoth
        "VAN" => "Vancouver Canucks",
        "VGK" | "VEG" => "Vegas Golden Knights",
        "WSH_NHL" | "WAS_NHL" => "Washington Capitals", // Disambiguate from Commanders/Wizards
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

    #[test]
    fn test_kalshi_code_to_team_name_nhl() {
        // LA Kings = LAK (not LA, which is Rams)
        assert_eq!(
            kalshi_code_to_team_name("LAK"),
            Some("Los Angeles Kings".to_string())
        );

        // Tampa Bay Lightning = TBL
        assert_eq!(
            kalshi_code_to_team_name("TBL"),
            Some("Tampa Bay Lightning".to_string())
        );

        // San Jose Sharks = SJS
        assert_eq!(
            kalshi_code_to_team_name("SJS"),
            Some("San Jose Sharks".to_string())
        );
        assert_eq!(
            kalshi_code_to_team_name("SJ"),
            Some("San Jose Sharks".to_string())
        );

        // New Jersey Devils = NJD
        assert_eq!(
            kalshi_code_to_team_name("NJD"),
            Some("New Jersey Devils".to_string())
        );
        assert_eq!(
            kalshi_code_to_team_name("NJ"),
            Some("New Jersey Devils".to_string())
        );

        // Vegas Golden Knights = VGK
        assert_eq!(
            kalshi_code_to_team_name("VGK"),
            Some("Vegas Golden Knights".to_string())
        );
        assert_eq!(
            kalshi_code_to_team_name("VEG"),
            Some("Vegas Golden Knights".to_string())
        );

        // St. Louis Blues = STL
        assert_eq!(
            kalshi_code_to_team_name("STL"),
            Some("St. Louis Blues".to_string())
        );

        // Utah Mammoth = UHC (Utah Hockey Club) or UTA_NHL
        assert_eq!(
            kalshi_code_to_team_name("UHC"),
            Some("Utah Mammoth".to_string())
        );

        // NY Rangers vs NY Islanders
        assert_eq!(
            kalshi_code_to_team_name("NYR"),
            Some("New York Rangers".to_string())
        );
        assert_eq!(
            kalshi_code_to_team_name("NYI"),
            Some("New York Islanders".to_string())
        );

        // Other common NHL codes
        assert_eq!(
            kalshi_code_to_team_name("MTL"),
            Some("Montreal Canadiens".to_string())
        );
        assert_eq!(
            kalshi_code_to_team_name("ANA"),
            Some("Anaheim Ducks".to_string())
        );
        assert_eq!(
            kalshi_code_to_team_name("CGY"),
            Some("Calgary Flames".to_string())
        );
        assert_eq!(
            kalshi_code_to_team_name("EDM"),
            Some("Edmonton Oilers".to_string())
        );
        assert_eq!(
            kalshi_code_to_team_name("WPG"),
            Some("Winnipeg Jets".to_string())
        );
        assert_eq!(
            kalshi_code_to_team_name("OTT"),
            Some("Ottawa Senators".to_string())
        );
        assert_eq!(
            kalshi_code_to_team_name("VAN"),
            Some("Vancouver Canucks".to_string())
        );
        assert_eq!(
            kalshi_code_to_team_name("NSH"),
            Some("Nashville Predators".to_string())
        );
        assert_eq!(
            kalshi_code_to_team_name("CBJ"),
            Some("Columbus Blue Jackets".to_string())
        );
        assert_eq!(
            kalshi_code_to_team_name("FLA"),
            Some("Florida Panthers".to_string())
        );
        assert_eq!(
            kalshi_code_to_team_name("COL"),
            Some("Colorado Avalanche".to_string())
        );
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
