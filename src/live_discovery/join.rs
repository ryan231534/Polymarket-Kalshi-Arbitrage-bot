//! Cross-exchange event joining logic.
//!
//! This module matches Kalshi events with Polymarket events using normalized
//! metadata (team names + start times) to create matched market pairs.

use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::kalshi_discovery::kalshi_code_to_team_name;
use super::normalize::{
    build_matchup_key, normalize_team_name, times_within_tolerance, NormalizationConfig,
};
use super::types::{KalshiEventMeta, MatchedMarket, PolyEventMeta};

/// Configuration for the join operation
#[derive(Debug, Clone)]
pub struct JoinConfig {
    /// Time tolerance in hours for matching events
    pub time_tolerance_hours: u32,
    /// Normalization configuration
    pub normalization: NormalizationConfig,
}

impl Default for JoinConfig {
    fn default() -> Self {
        Self {
            time_tolerance_hours: 6,
            normalization: NormalizationConfig::default(),
        }
    }
}

/// Result of the join operation
#[derive(Debug, Clone)]
pub struct JoinResult {
    /// Successfully matched market pairs
    pub matched: Vec<MatchedMarket>,
    /// Kalshi events that could not be matched
    pub unmatched_kalshi: Vec<KalshiEventMeta>,
    /// Polymarket events that could not be matched
    pub unmatched_poly: Vec<PolyEventMeta>,
}

/// Join Kalshi and Polymarket events into matched market pairs.
///
/// The algorithm:
/// 1. Build matchup keys for all events on both sides
/// 2. Match events by key with time tolerance
/// 3. For matched events, create MatchedMarket entries
/// 4. Track unmatched events for debugging
pub fn join_markets(
    kalshi_events: &[KalshiEventMeta],
    poly_events: &[PolyEventMeta],
    config: &JoinConfig,
) -> JoinResult {
    let debug_join = env::var("LIVE_JOIN_DEBUG")
        .map(|v| v == "1")
        .unwrap_or(false);

    // Build index of Poly events by matchup key
    let mut poly_by_key: HashMap<String, Vec<&PolyEventMeta>> = HashMap::new();
    let mut poly_matched: Vec<bool> = vec![false; poly_events.len()];
    let mut poly_keys_set: HashSet<String> = HashSet::new();

    // For debug: collect detailed records
    let mut poly_debug_records: Vec<(String, String, String, String)> = Vec::new(); // (key, time, teams, slug)

    for (_idx, event) in poly_events.iter().enumerate() {
        if let Some(key) = build_event_key(event, &config.normalization) {
            poly_by_key.entry(key.clone()).or_default().push(event);
            poly_keys_set.insert(key.clone());

            if debug_join && poly_debug_records.len() < 20 {
                let time_str = event
                    .start_time
                    .map(|t| t.to_rfc3339())
                    .unwrap_or_else(|| "None".to_string());
                let teams = format!("{:?} vs {:?}", event.home_team, event.away_team);
                poly_debug_records.push((key, time_str, teams, event.slug.clone()));
            }

            debug!("Poly event indexed: {} -> {}", event.event_id, event.title);
        } else {
            debug!(
                "Could not build key for Poly event: {} (home={:?}, away={:?}, time={:?})",
                event.event_id, event.home_team, event.away_team, event.start_time
            );
        }
    }

    let mut matched = Vec::new();
    let mut kalshi_matched: Vec<bool> = vec![false; kalshi_events.len()];
    let mut kalshi_keys_set: HashSet<String> = HashSet::new();

    // For debug: collect detailed Kalshi records
    let mut kalshi_debug_records: Vec<(String, String, String, String, String)> = Vec::new(); // (key, time, teams, ticker, parse_method)

    // Try to match each Kalshi event
    for (k_idx, kalshi_event) in kalshi_events.iter().enumerate() {
        let (kalshi_key, parse_method) =
            match build_kalshi_event_key_debug(kalshi_event, &config.normalization) {
                Some((key, method)) => (key, method),
                None => {
                    debug!(
                    "Could not build key for Kalshi event: {} (home={:?}, away={:?}, time={:?})",
                    kalshi_event.event_ticker,
                    kalshi_event.home_team,
                    kalshi_event.away_team,
                    kalshi_event.start_time
                );
                    continue;
                }
            };

        kalshi_keys_set.insert(kalshi_key.clone());

        if debug_join && kalshi_debug_records.len() < 20 {
            let time_str = kalshi_event
                .start_time
                .map(|t| t.to_rfc3339())
                .unwrap_or_else(|| "None".to_string());
            let teams = format!(
                "{:?} vs {:?}",
                kalshi_event.home_team, kalshi_event.away_team
            );
            kalshi_debug_records.push((
                kalshi_key.clone(),
                time_str,
                teams,
                kalshi_event.event_ticker.clone(),
                parse_method.clone(),
            ));
        }

        // Look for matching Poly events
        if let Some(poly_candidates) = poly_by_key.get(&kalshi_key) {
            for poly_event in poly_candidates {
                // Verify time tolerance
                if let (Some(k_time), Some(p_time)) =
                    (&kalshi_event.start_time, &poly_event.start_time)
                {
                    if !times_within_tolerance(k_time, p_time, config.time_tolerance_hours) {
                        debug!(
                            "Time mismatch: Kalshi {} ({}) vs Poly {} ({})",
                            kalshi_event.event_ticker, k_time, poly_event.event_id, p_time
                        );
                        continue;
                    }
                }

                // Create matched market entry
                let matched_market = build_matched_market(
                    kalshi_event,
                    poly_event,
                    &kalshi_key,
                    &config.normalization,
                );

                matched.push(matched_market);
                kalshi_matched[k_idx] = true;

                // Mark poly event as matched
                if let Some(p_idx) = poly_events
                    .iter()
                    .position(|e| e.event_id == poly_event.event_id)
                {
                    poly_matched[p_idx] = true;
                }

                info!(
                    "Matched: {} <-> {} (key: {})",
                    kalshi_event.event_ticker, poly_event.event_id, kalshi_key
                );
                break; // One match per Kalshi event
            }
        }
    }

    // Collect unmatched events
    let unmatched_kalshi: Vec<KalshiEventMeta> = kalshi_events
        .iter()
        .enumerate()
        .filter(|(idx, _)| !kalshi_matched[*idx])
        .map(|(_, e)| e.clone())
        .collect();

    let unmatched_poly: Vec<PolyEventMeta> = poly_events
        .iter()
        .enumerate()
        .filter(|(idx, _)| !poly_matched[*idx])
        .map(|(_, e)| e.clone())
        .collect();

    // LIVE_JOIN_DEBUG output - use info! so it shows with RUST_LOG=info
    if debug_join {
        // Key overlap analysis
        let overlap_count = kalshi_keys_set.intersection(&poly_keys_set).count();

        info!(
            "[LIVE_JOIN_DEBUG] overlap_exact_key_count={}",
            overlap_count
        );
        info!(
            "[LIVE_JOIN_DEBUG] unique_kalshi_keys={}, unique_poly_keys={}",
            kalshi_keys_set.len(),
            poly_keys_set.len()
        );

        // Print first 20 Kalshi keys
        let kalshi_keys_str: Vec<String> = kalshi_debug_records
            .iter()
            .map(|(key, _, _, _, _)| key.clone())
            .collect();
        info!(
            "[LIVE_JOIN_DEBUG] First 20 Kalshi keys: {:?}",
            kalshi_keys_str
        );

        // Print first 20 Poly keys
        let poly_keys_str: Vec<String> = poly_debug_records
            .iter()
            .map(|(key, _, _, _)| key.clone())
            .collect();
        info!("[LIVE_JOIN_DEBUG] First 20 Poly keys: {:?}", poly_keys_str);

        // Print raw Poly event fields for first 10
        info!("[LIVE_JOIN_DEBUG] === First 10 Poly raw fields ===");
        for (i, (key, time, teams, slug)) in poly_debug_records.iter().take(10).enumerate() {
            info!(
                "[LIVE_JOIN_DEBUG] P[{}] slug={} | key={} | teams={} | time={}",
                i, slug, key, teams, time
            );
        }

        // Print raw Kalshi event fields for first 10
        info!("[LIVE_JOIN_DEBUG] === First 10 Kalshi raw fields ===");
        for (i, (key, time, teams, ticker, method)) in
            kalshi_debug_records.iter().take(10).enumerate()
        {
            info!(
                "[LIVE_JOIN_DEBUG] K[{}] ticker={} | key={} | teams={} | time={} | method={}",
                i, ticker, key, teams, time, method
            );
        }

        // If no overlap, find closest diagnostic
        if overlap_count == 0 && !kalshi_debug_records.is_empty() && !poly_debug_records.is_empty()
        {
            info!("[LIVE_JOIN_DEBUG] === Closest match diagnostic (overlap=0) ===");

            // Extract team tokens from first 5 Kalshi keys
            for (i, (k_key, k_time, k_teams, k_ticker, _)) in
                kalshi_debug_records.iter().take(5).enumerate()
            {
                let k_parts: Vec<&str> = k_key.split('_').collect();
                let k_team1 = k_parts.get(0).unwrap_or(&"?");
                let k_team2 = k_parts.get(2).unwrap_or(&"?"); // skip "vs"
                let k_date = k_parts.last().unwrap_or(&"?");

                info!("[LIVE_JOIN_DEBUG] Kalshi[{}]: team1='{}', team2='{}', date='{}' from ticker={}", 
                      i, k_team1, k_team2, k_date, k_ticker);

                // Find any Poly key that shares a team token
                let mut found_partial = false;
                for (p_key, p_time, _p_teams, p_slug) in poly_debug_records.iter().take(50) {
                    let p_lower = p_key.to_lowercase();
                    if p_lower.contains(k_team1) || p_lower.contains(k_team2) {
                        info!("[LIVE_JOIN_DEBUG]   PARTIAL MATCH: poly_slug={} poly_key={} poly_time={} kalshi_time={}", 
                              p_slug, p_key, p_time, k_time);
                        found_partial = true;
                        break;
                    }
                }
                if !found_partial {
                    info!("[LIVE_JOIN_DEBUG]   No partial team match found in first 50 Poly keys");
                }
            }
        }
    }

    JoinResult {
        matched,
        unmatched_kalshi,
        unmatched_poly,
    }
}

/// Build matchup key for a Kalshi event with debug info about parse method
fn build_kalshi_event_key_debug(
    event: &KalshiEventMeta,
    config: &NormalizationConfig,
) -> Option<(String, String)> {
    // First try to get teams from parsed home/away
    if let (Some(home), Some(away), Some(time)) =
        (&event.home_team, &event.away_team, &event.start_time)
    {
        return Some((
            build_matchup_key(home, away, time, config),
            "home_away_fields".to_string(),
        ));
    }

    // Try to parse team codes from ticker
    if let Some((code1, code2)) =
        super::kalshi_discovery::parse_teams_from_ticker(&event.event_ticker)
    {
        if let Some(time) = &event.start_time {
            let team1 = kalshi_code_to_team_name(&code1).unwrap_or(code1.clone());
            let team2 = kalshi_code_to_team_name(&code2).unwrap_or(code2.clone());
            return Some((
                build_matchup_key(&team1, &team2, time, config),
                format!("ticker_codes:{}_{}", code1, code2),
            ));
        }
    }

    None
}

/// Build matchup key for a Kalshi event
fn build_kalshi_event_key(event: &KalshiEventMeta, config: &NormalizationConfig) -> Option<String> {
    build_kalshi_event_key_debug(event, config).map(|(key, _)| key)
}

/// Build matchup key for a Polymarket event
fn build_event_key(event: &PolyEventMeta, config: &NormalizationConfig) -> Option<String> {
    if let (Some(home), Some(away), Some(time)) =
        (&event.home_team, &event.away_team, &event.start_time)
    {
        return Some(build_matchup_key(home, away, time, config));
    }

    // Try to parse from title
    if let Some((team_a, team_b)) = super::normalize::parse_teams_from_title(&event.title) {
        if let Some(time) = &event.start_time {
            return Some(build_matchup_key(&team_a, &team_b, time, config));
        }
    }

    None
}

/// Build a MatchedMarket from matched Kalshi and Poly events
fn build_matched_market(
    kalshi_event: &KalshiEventMeta,
    poly_event: &PolyEventMeta,
    matchup_key: &str,
    _config: &NormalizationConfig,
) -> MatchedMarket {
    // Extract all Kalshi market tickers
    let kalshi_market_tickers: Vec<String> = kalshi_event
        .markets
        .iter()
        .map(|m| m.ticker.clone())
        .collect();

    // Extract all Poly CLOB token IDs
    let poly_clob_token_ids: Vec<(String, String)> = poly_event
        .markets
        .iter()
        .filter(|m| m.active && !m.closed)
        .map(|m| (m.clob_token_id_yes.clone(), m.clob_token_id_no.clone()))
        .collect();

    // Determine league from series ticker
    let league = determine_league_from_series(&kalshi_event.series_ticker);

    // Calculate confidence based on matching quality
    let confidence = calculate_match_confidence(kalshi_event, poly_event);

    MatchedMarket {
        league,
        matchup_key: matchup_key.to_string(),
        start_time: kalshi_event
            .start_time
            .or(poly_event.start_time)
            .unwrap_or_else(chrono::Utc::now),
        kalshi_event_ticker: kalshi_event.event_ticker.clone(),
        kalshi_market_tickers,
        kalshi_event: Arc::new(kalshi_event.clone()),
        poly_event_id: poly_event.event_id.clone(),
        poly_clob_token_ids,
        poly_event: Arc::new(poly_event.clone()),
        confidence,
    }
}

/// Determine league from Kalshi series ticker
fn determine_league_from_series(series: &str) -> String {
    let upper = series.to_uppercase();
    if upper.contains("NFL") {
        "nfl".to_string()
    } else if upper.contains("NBA") {
        "nba".to_string()
    } else if upper.contains("NCAAMB") || upper.contains("CBB") {
        "ncaamb".to_string()
    } else if upper.contains("NCAAF") || upper.contains("CFB") {
        "ncaaf".to_string()
    } else if upper.contains("NHL") {
        "nhl".to_string()
    } else if upper.contains("MLB") {
        "mlb".to_string()
    } else if upper.contains("EPL") {
        "epl".to_string()
    } else if upper.contains("MLS") {
        "mls".to_string()
    } else {
        "unknown".to_string()
    }
}

/// Calculate confidence score for a match (0.0 - 1.0)
fn calculate_match_confidence(kalshi: &KalshiEventMeta, poly: &PolyEventMeta) -> f64 {
    let mut score = 0.0;
    let mut factors = 0;

    // Both have start times
    if kalshi.start_time.is_some() && poly.start_time.is_some() {
        score += 0.3;
        factors += 1;

        // Times are close
        if let (Some(kt), Some(pt)) = (&kalshi.start_time, &poly.start_time) {
            let diff_hours = (*kt - *pt).num_hours().unsigned_abs();
            if diff_hours == 0 {
                score += 0.2;
            } else if diff_hours <= 2 {
                score += 0.1;
            }
        }
    }

    // Both have team names parsed
    if kalshi.home_team.is_some() && kalshi.away_team.is_some() {
        score += 0.2;
        factors += 1;
    }
    if poly.home_team.is_some() && poly.away_team.is_some() {
        score += 0.2;
        factors += 1;
    }

    // Has markets on both sides
    if !kalshi.markets.is_empty() && !poly.markets.is_empty() {
        score += 0.1;
        factors += 1;
    }

    // Normalize
    if factors > 0 {
        (score / 1.0_f64).min(1.0)
    } else {
        0.5 // Default confidence if no factors evaluated
    }
}

/// Advanced join with fuzzy matching for harder cases.
///
/// This tries alternative matching strategies when exact key matching fails.
pub fn join_markets_fuzzy(
    kalshi_events: &[KalshiEventMeta],
    poly_events: &[PolyEventMeta],
    config: &JoinConfig,
) -> JoinResult {
    // First do exact matching
    let mut result = join_markets(kalshi_events, poly_events, config);

    // Try fuzzy matching for remaining unmatched events
    let mut additional_matches = Vec::new();
    let mut newly_matched_kalshi = Vec::new();
    let mut newly_matched_poly = Vec::new();

    for kalshi_event in &result.unmatched_kalshi {
        for poly_event in &result.unmatched_poly {
            if fuzzy_match_events(kalshi_event, poly_event, config) {
                let matched = build_matched_market(
                    kalshi_event,
                    poly_event,
                    &format!("fuzzy_{}", kalshi_event.event_ticker),
                    &config.normalization,
                );

                info!(
                    "Fuzzy matched: {} <-> {}",
                    kalshi_event.event_ticker, poly_event.event_id
                );

                additional_matches.push(matched);
                newly_matched_kalshi.push(kalshi_event.event_ticker.clone());
                newly_matched_poly.push(poly_event.event_id.clone());
                break;
            }
        }
    }

    // Update result
    result.matched.extend(additional_matches);
    result
        .unmatched_kalshi
        .retain(|e| !newly_matched_kalshi.contains(&e.event_ticker));
    result
        .unmatched_poly
        .retain(|e| !newly_matched_poly.contains(&e.event_id));

    result
}

/// Check if two events fuzzy-match based on title similarity
fn fuzzy_match_events(kalshi: &KalshiEventMeta, poly: &PolyEventMeta, config: &JoinConfig) -> bool {
    // Check time tolerance first
    if let (Some(kt), Some(pt)) = (&kalshi.start_time, &poly.start_time) {
        if !times_within_tolerance(kt, pt, config.time_tolerance_hours) {
            return false;
        }
    } else {
        // If either is missing time, don't fuzzy match
        return false;
    }

    // Normalize both titles
    let k_title = kalshi.title.to_lowercase();
    let p_title = poly.title.to_lowercase();

    // Extract significant words from each title
    let k_words: Vec<&str> = k_title
        .split_whitespace()
        .filter(|w| w.len() > 2)
        .filter(|w| !is_common_word(w))
        .collect();

    let p_words: Vec<&str> = p_title
        .split_whitespace()
        .filter(|w| w.len() > 2)
        .filter(|w| !is_common_word(w))
        .collect();

    // Count matching words
    let matching: usize = k_words.iter().filter(|w| p_words.contains(w)).count();

    // Require at least 2 matching significant words
    matching >= 2
}

/// Check if a word is a common/stop word
fn is_common_word(word: &str) -> bool {
    matches!(
        word.to_lowercase().as_str(),
        "the" | "vs" | "at" | "and" | "or" | "will" | "to" | "in" | "on" | "win" | "beat"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn make_kalshi_event(
        ticker: &str,
        title: &str,
        home: Option<&str>,
        away: Option<&str>,
        time: Option<chrono::DateTime<Utc>>,
    ) -> KalshiEventMeta {
        KalshiEventMeta {
            event_ticker: ticker.to_string(),
            title: title.to_string(),
            start_time: time,
            home_team: home.map(|s| s.to_string()),
            away_team: away.map(|s| s.to_string()),
            series_ticker: "KXNFLGAME".to_string(),
            markets: vec![],
        }
    }

    fn make_poly_event(
        id: &str,
        title: &str,
        home: Option<&str>,
        away: Option<&str>,
        time: Option<chrono::DateTime<Utc>>,
    ) -> PolyEventMeta {
        PolyEventMeta {
            event_id: id.to_string(),
            slug: id.to_string(),
            title: title.to_string(),
            start_time: time,
            home_team: home.map(|s| s.to_string()),
            away_team: away.map(|s| s.to_string()),
            category: Some("nfl".to_string()),
            markets: vec![],
        }
    }

    #[test]
    fn test_join_exact_match() {
        let time = Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap();

        let kalshi_events = vec![make_kalshi_event(
            "KXNFLGAME-26JAN05BUFDET",
            "Buffalo Bills vs Detroit Lions",
            Some("Detroit Lions"),
            Some("Buffalo Bills"),
            Some(time),
        )];

        let poly_events = vec![make_poly_event(
            "nfl-buf-det-0105",
            "Buffalo Bills vs Detroit Lions",
            Some("Detroit Lions"),
            Some("Buffalo Bills"),
            Some(time),
        )];

        let config = JoinConfig::default();
        let result = join_markets(&kalshi_events, &poly_events, &config);

        assert_eq!(result.matched.len(), 1);
        assert!(result.unmatched_kalshi.is_empty());
        assert!(result.unmatched_poly.is_empty());
    }

    #[test]
    fn test_join_with_time_tolerance() {
        let k_time = Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap();
        let p_time = Utc.with_ymd_and_hms(2026, 1, 5, 20, 0, 0).unwrap(); // 2 hours later

        let kalshi_events = vec![make_kalshi_event(
            "KXNFLGAME-26JAN05BUFDET",
            "Buffalo Bills vs Detroit Lions",
            Some("Detroit Lions"),
            Some("Buffalo Bills"),
            Some(k_time),
        )];

        let poly_events = vec![make_poly_event(
            "nfl-buf-det-0105",
            "Buffalo Bills vs Detroit Lions",
            Some("Detroit Lions"),
            Some("Buffalo Bills"),
            Some(p_time),
        )];

        let config = JoinConfig {
            time_tolerance_hours: 6,
            ..Default::default()
        };

        let result = join_markets(&kalshi_events, &poly_events, &config);
        assert_eq!(result.matched.len(), 1, "Should match with 6h tolerance");

        // Try with tight tolerance
        let strict_config = JoinConfig {
            time_tolerance_hours: 1,
            ..Default::default()
        };

        let strict_result = join_markets(&kalshi_events, &poly_events, &strict_config);
        assert_eq!(
            strict_result.matched.len(),
            0,
            "Should not match with 1h tolerance"
        );
    }

    #[test]
    fn test_join_no_match_different_teams() {
        let time = Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap();

        let kalshi_events = vec![make_kalshi_event(
            "KXNFLGAME-26JAN05BUFDET",
            "Buffalo Bills vs Detroit Lions",
            Some("Detroit Lions"),
            Some("Buffalo Bills"),
            Some(time),
        )];

        let poly_events = vec![make_poly_event(
            "nfl-kc-den-0105",
            "Kansas City Chiefs vs Denver Broncos",
            Some("Denver Broncos"),
            Some("Kansas City Chiefs"),
            Some(time),
        )];

        let config = JoinConfig::default();
        let result = join_markets(&kalshi_events, &poly_events, &config);

        assert!(result.matched.is_empty());
        assert_eq!(result.unmatched_kalshi.len(), 1);
        assert_eq!(result.unmatched_poly.len(), 1);
    }

    #[test]
    fn test_join_symmetric_team_order() {
        let time = Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap();

        // Kalshi has home/away in one order
        let kalshi_events = vec![make_kalshi_event(
            "KXNFLGAME-26JAN05BUFDET",
            "Buffalo Bills @ Detroit Lions",
            Some("Detroit Lions"),
            Some("Buffalo Bills"),
            Some(time),
        )];

        // Poly has teams in opposite order
        let poly_events = vec![make_poly_event(
            "nfl-det-buf-0105",
            "Detroit Lions vs Buffalo Bills",
            Some("Buffalo Bills"), // Note: swapped
            Some("Detroit Lions"),
            Some(time),
        )];

        let config = JoinConfig::default();
        let result = join_markets(&kalshi_events, &poly_events, &config);

        assert_eq!(
            result.matched.len(),
            1,
            "Should match regardless of team order"
        );
    }

    #[test]
    fn test_determine_league_from_series() {
        assert_eq!(determine_league_from_series("KXNFLGAME"), "nfl");
        assert_eq!(determine_league_from_series("KXNBASPREAD"), "nba");
        assert_eq!(determine_league_from_series("KXEPLTOTAL"), "epl");
        assert_eq!(determine_league_from_series("UNKNOWN"), "unknown");
    }

    #[test]
    fn test_calculate_match_confidence() {
        let time = Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap();

        let kalshi = make_kalshi_event(
            "KXNFLGAME-26JAN05BUFDET",
            "Buffalo Bills vs Detroit Lions",
            Some("Detroit Lions"),
            Some("Buffalo Bills"),
            Some(time),
        );

        let poly = make_poly_event(
            "nfl-buf-det-0105",
            "Buffalo Bills vs Detroit Lions",
            Some("Detroit Lions"),
            Some("Buffalo Bills"),
            Some(time),
        );

        let confidence = calculate_match_confidence(&kalshi, &poly);
        assert!(
            confidence > 0.5,
            "Well-matched events should have high confidence"
        );
    }
}
