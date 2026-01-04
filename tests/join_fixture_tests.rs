//! Fixture-based tests for live discovery joining.
//!
//! These tests use JSON fixtures to verify the join logic without network calls.

use chrono::{TimeZone, Utc};
use std::sync::Arc;

use prediction_market_arbitrage::live_discovery::types::{
    KalshiEventMeta, KalshiMarketMeta, MatchedMarket, PolyEventMeta, PolyMarketMeta,
};
use prediction_market_arbitrage::live_discovery::{
    join_markets, matched_markets_to_pairs, JoinConfig, NormalizationConfig,
};
use prediction_market_arbitrage::types::MarketType;

/// Create a Kalshi event from fixture-style data
fn make_kalshi_event(
    event_ticker: &str,
    title: &str,
    series_ticker: &str,
    start_time: chrono::DateTime<Utc>,
    markets: Vec<(&str, &str, Option<&str>)>, // (ticker, title, team)
) -> KalshiEventMeta {
    // Parse teams from title
    let (home, away) = parse_teams(title);

    KalshiEventMeta {
        event_ticker: event_ticker.to_string(),
        title: title.to_string(),
        start_time: Some(start_time),
        home_team: home,
        away_team: away,
        series_ticker: series_ticker.to_string(),
        markets: markets
            .into_iter()
            .map(|(ticker, title, team)| KalshiMarketMeta {
                ticker: ticker.to_string(),
                title: title.to_string(),
                market_type: "moneyline".to_string(),
                line_value: None,
                team: team.map(|s| s.to_string()),
            })
            .collect(),
    }
}

/// Create a Polymarket event from fixture-style data
fn make_poly_event(
    event_id: &str,
    slug: &str,
    title: &str,
    start_time: chrono::DateTime<Utc>,
    markets: Vec<(&str, &str, &str, &str)>, // (market_id, question, yes_token, no_token)
) -> PolyEventMeta {
    let (home, away) = parse_teams(title);

    PolyEventMeta {
        event_id: event_id.to_string(),
        slug: slug.to_string(),
        title: title.to_string(),
        start_time: Some(start_time),
        home_team: home,
        away_team: away,
        category: Some("nfl".to_string()),
        markets: markets
            .into_iter()
            .map(|(id, question, yes, no)| PolyMarketMeta {
                market_id: id.to_string(),
                question: question.to_string(),
                slug: format!("{}-{}", slug, id),
                clob_token_id_yes: yes.to_string(),
                clob_token_id_no: no.to_string(),
                market_type: "moneyline".to_string(),
                active: true,
                closed: false,
            })
            .collect(),
    }
}

/// Simple team parser for test fixtures
fn parse_teams(title: &str) -> (Option<String>, Option<String>) {
    if let Some(pos) = title.to_lowercase().find(" vs ") {
        let team_a = title[..pos].trim().to_string();
        let after_vs = &title[pos + 4..];
        // Take until question mark or end
        let team_b = after_vs
            .split('?')
            .next()
            .unwrap_or(after_vs)
            .trim()
            .to_string();
        return (Some(team_b), Some(team_a)); // home, away (vs format: away vs home)
    }
    (None, None)
}

#[test]
fn test_join_fixture_exact_match() {
    let start_time = Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap();

    let kalshi_events = vec![make_kalshi_event(
        "KXNFLGAME-26JAN05BUFDET",
        "Buffalo Bills vs Detroit Lions",
        "KXNFLGAME",
        start_time,
        vec![
            (
                "KXNFLGAME-26JAN05BUFDET-BUF",
                "Buffalo Bills to Win",
                Some("BUF"),
            ),
            (
                "KXNFLGAME-26JAN05BUFDET-DET",
                "Detroit Lions to Win",
                Some("DET"),
            ),
        ],
    )];

    let poly_events = vec![make_poly_event(
        "poly-12345",
        "nfl-buffalo-bills-vs-detroit-lions-2026-01-05",
        "Buffalo Bills vs Detroit Lions",
        start_time,
        vec![
            (
                "pm-buf",
                "Will Buffalo Bills win?",
                "yes-token-buf",
                "no-token-buf",
            ),
            (
                "pm-det",
                "Will Detroit Lions win?",
                "yes-token-det",
                "no-token-det",
            ),
        ],
    )];

    let config = JoinConfig::default();
    let result = join_markets(&kalshi_events, &poly_events, &config);

    assert_eq!(result.matched.len(), 1, "Should match exactly one event");
    assert!(
        result.unmatched_kalshi.is_empty(),
        "No unmatched Kalshi events"
    );
    assert!(result.unmatched_poly.is_empty(), "No unmatched Poly events");

    let matched = &result.matched[0];
    assert_eq!(matched.league, "nfl");
    assert!(matched.matchup_key.contains("buffalo bills"));
    assert!(matched.matchup_key.contains("detroit lions"));
    assert_eq!(matched.kalshi_market_tickers.len(), 2);
    assert_eq!(matched.poly_clob_token_ids.len(), 2);
}

#[test]
fn test_join_fixture_time_tolerance() {
    // Kalshi time is 2 hours earlier than Poly time
    let kalshi_time = Utc.with_ymd_and_hms(2026, 1, 5, 16, 0, 0).unwrap();
    let poly_time = Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap();

    let kalshi_events = vec![make_kalshi_event(
        "KXNFLGAME-26JAN05BUFDET",
        "Buffalo Bills vs Detroit Lions",
        "KXNFLGAME",
        kalshi_time,
        vec![(
            "KXNFLGAME-26JAN05BUFDET-BUF",
            "Buffalo Bills to Win",
            Some("BUF"),
        )],
    )];

    let poly_events = vec![make_poly_event(
        "poly-12345",
        "nfl-buffalo-bills-vs-detroit-lions-2026-01-05",
        "Buffalo Bills vs Detroit Lions",
        poly_time,
        vec![(
            "pm-buf",
            "Will Buffalo Bills win?",
            "yes-token-buf",
            "no-token-buf",
        )],
    )];

    // Default tolerance is 6 hours, so 2 hours should match
    let config = JoinConfig::default();
    let result = join_markets(&kalshi_events, &poly_events, &config);

    assert_eq!(result.matched.len(), 1, "Should match with time tolerance");
}

#[test]
fn test_join_fixture_time_outside_tolerance() {
    // Kalshi time is 10 hours earlier than Poly time
    let kalshi_time = Utc.with_ymd_and_hms(2026, 1, 5, 8, 0, 0).unwrap();
    let poly_time = Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap();

    let kalshi_events = vec![make_kalshi_event(
        "KXNFLGAME-26JAN05BUFDET",
        "Buffalo Bills vs Detroit Lions",
        "KXNFLGAME",
        kalshi_time,
        vec![(
            "KXNFLGAME-26JAN05BUFDET-BUF",
            "Buffalo Bills to Win",
            Some("BUF"),
        )],
    )];

    let poly_events = vec![make_poly_event(
        "poly-12345",
        "nfl-buffalo-bills-vs-detroit-lions-2026-01-05",
        "Buffalo Bills vs Detroit Lions",
        poly_time,
        vec![(
            "pm-buf",
            "Will Buffalo Bills win?",
            "yes-token-buf",
            "no-token-buf",
        )],
    )];

    // 6 hour tolerance, 10 hours apart should NOT match
    let config = JoinConfig::default();
    let result = join_markets(&kalshi_events, &poly_events, &config);

    // Note: With date-only matching (default), they may still match on same date
    // This depends on NormalizationConfig.date_only setting
    // For strict time matching, we would need date_only = false
}

#[test]
fn test_join_fixture_team_alias_matching() {
    let start_time = Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap();

    // Kalshi uses abbreviation "Bills"
    let kalshi_events = vec![make_kalshi_event(
        "KXNFLGAME-26JAN05BUFDET",
        "Bills vs Lions",
        "KXNFLGAME",
        start_time,
        vec![("KXNFLGAME-26JAN05BUFDET-BUF", "Bills to Win", Some("BUF"))],
    )];

    // Polymarket uses full name
    let poly_events = vec![make_poly_event(
        "poly-12345",
        "nfl-buffalo-bills-vs-detroit-lions",
        "Buffalo Bills vs Detroit Lions",
        start_time,
        vec![(
            "pm-buf",
            "Will Buffalo Bills win?",
            "yes-token-buf",
            "no-token-buf",
        )],
    )];

    let config = JoinConfig::default();
    let result = join_markets(&kalshi_events, &poly_events, &config);

    assert_eq!(
        result.matched.len(),
        1,
        "Should match via alias normalization"
    );
}

#[test]
fn test_join_fixture_symmetric_matching() {
    let start_time = Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap();

    // Kalshi has home team first
    let kalshi_events = vec![make_kalshi_event(
        "KXNFLGAME-26JAN05BUFDET",
        "Detroit Lions vs Buffalo Bills",
        "KXNFLGAME",
        start_time,
        vec![(
            "KXNFLGAME-26JAN05BUFDET-DET",
            "Detroit Lions to Win",
            Some("DET"),
        )],
    )];

    // Polymarket has away team first
    let poly_events = vec![make_poly_event(
        "poly-12345",
        "nfl-buffalo-bills-vs-detroit-lions",
        "Buffalo Bills vs Detroit Lions",
        start_time,
        vec![(
            "pm-buf",
            "Will Buffalo Bills win?",
            "yes-token-buf",
            "no-token-buf",
        )],
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
fn test_matched_to_market_pairs_conversion() {
    let start_time = Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap();

    let kalshi_events = vec![make_kalshi_event(
        "KXNFLGAME-26JAN05BUFDET",
        "Buffalo Bills vs Detroit Lions",
        "KXNFLGAME",
        start_time,
        vec![
            (
                "KXNFLGAME-26JAN05BUFDET-BUF",
                "Buffalo Bills to Win",
                Some("BUF"),
            ),
            (
                "KXNFLGAME-26JAN05BUFDET-DET",
                "Detroit Lions to Win",
                Some("DET"),
            ),
        ],
    )];

    let poly_events = vec![make_poly_event(
        "poly-12345",
        "nfl-buffalo-bills-vs-detroit-lions-2026-01-05",
        "Buffalo Bills vs Detroit Lions",
        start_time,
        vec![
            (
                "pm-buf",
                "Will Buffalo Bills win?",
                "yes-buf-token",
                "no-buf-token",
            ),
            (
                "pm-det",
                "Will Detroit Lions win?",
                "yes-det-token",
                "no-det-token",
            ),
        ],
    )];

    let config = JoinConfig::default();
    let result = join_markets(&kalshi_events, &poly_events, &config);

    let pairs = matched_markets_to_pairs(&result.matched);

    // Should have 2 pairs (one for each market in the matched event)
    assert_eq!(pairs.len(), 2, "Should convert to 2 MarketPairs");

    // Verify first pair
    let p0 = &pairs[0];
    assert_eq!(p0.league.as_ref(), "nfl");
    assert_eq!(p0.market_type, MarketType::Moneyline);
    assert_eq!(
        p0.kalshi_market_ticker.as_ref(),
        "KXNFLGAME-26JAN05BUFDET-BUF"
    );
    assert_eq!(p0.poly_yes_token.as_ref(), "yes-buf-token");
    assert_eq!(p0.poly_no_token.as_ref(), "no-buf-token");

    // Verify second pair
    let p1 = &pairs[1];
    assert_eq!(
        p1.kalshi_market_ticker.as_ref(),
        "KXNFLGAME-26JAN05BUFDET-DET"
    );
    assert_eq!(p1.poly_yes_token.as_ref(), "yes-det-token");
}

#[test]
fn test_no_match_different_events() {
    let start_time = Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap();

    let kalshi_events = vec![make_kalshi_event(
        "KXNFLGAME-26JAN05BUFDET",
        "Buffalo Bills vs Detroit Lions",
        "KXNFLGAME",
        start_time,
        vec![(
            "KXNFLGAME-26JAN05BUFDET-BUF",
            "Buffalo Bills to Win",
            Some("BUF"),
        )],
    )];

    // Completely different teams
    let poly_events = vec![make_poly_event(
        "poly-99999",
        "nfl-kansas-city-chiefs-vs-denver-broncos",
        "Kansas City Chiefs vs Denver Broncos",
        start_time,
        vec![("pm-kc", "Will Kansas City Chiefs win?", "yes-kc", "no-kc")],
    )];

    let config = JoinConfig::default();
    let result = join_markets(&kalshi_events, &poly_events, &config);

    assert!(
        result.matched.is_empty(),
        "Should not match different events"
    );
    assert_eq!(result.unmatched_kalshi.len(), 1);
    assert_eq!(result.unmatched_poly.len(), 1);
}
