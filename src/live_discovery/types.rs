//! Core types for live market discovery and cross-exchange joining.
//!
//! These structs capture metadata from both exchanges in a normalized format
//! that enables reliable matching based on event details rather than slugs.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Specification for a sports league's market configuration
#[derive(Debug, Clone)]
pub struct LeagueSpec {
    /// Human-readable league code (e.g., "nfl", "nba", "epl")
    pub league_code: String,
    /// Kalshi series tickers for this league (e.g., ["KXNFLGAME", "KXNFLSPREAD"])
    pub kalshi_series_tickers: Vec<String>,
    /// Polymarket tag/category for filtering (e.g., "nfl", "football")
    pub polymarket_tag: Option<String>,
    /// Polymarket slug prefix for matching (e.g., "nfl-")
    pub polymarket_slug_prefix: Option<String>,
    /// Whether this is a sports league (enables live discovery)
    pub is_sports: bool,
}

impl LeagueSpec {
    /// Create NFL league specification
    pub fn nfl() -> Self {
        Self {
            league_code: "nfl".to_string(),
            kalshi_series_tickers: vec![
                "KXNFLGAME".to_string(),
                "KXNFLSPREAD".to_string(),
                "KXNFLTOTAL".to_string(),
            ],
            polymarket_tag: Some("nfl".to_string()),
            polymarket_slug_prefix: Some("nfl-".to_string()),
            is_sports: true,
        }
    }

    /// Create NBA league specification
    pub fn nba() -> Self {
        Self {
            league_code: "nba".to_string(),
            kalshi_series_tickers: vec![
                "KXNBAGAME".to_string(),
                "KXNBASPREAD".to_string(),
                "KXNBATOTAL".to_string(),
            ],
            polymarket_tag: Some("nba".to_string()),
            polymarket_slug_prefix: Some("nba-".to_string()),
            is_sports: true,
        }
    }

    /// Create NCAA Men's Basketball league specification
    pub fn ncaamb() -> Self {
        Self {
            league_code: "ncaamb".to_string(),
            kalshi_series_tickers: vec![
                "KXNCAAMBGAME".to_string(),
                "KXNCAAMBSPREAD".to_string(),
                "KXNCAAMBTOTAL".to_string(),
            ],
            polymarket_tag: Some("ncaamb".to_string()),
            // Polymarket uses "cbb-" prefix for college basketball
            polymarket_slug_prefix: Some("cbb-".to_string()),
            is_sports: true,
        }
    }

    /// Create NCAA Women's Basketball league specification
    pub fn ncaawb() -> Self {
        Self {
            league_code: "ncaawb".to_string(),
            kalshi_series_tickers: vec!["KXNCAAWBGAME".to_string()],
            polymarket_tag: Some("ncaawb".to_string()),
            // Polymarket uses "cwbb-" prefix for women's college basketball
            polymarket_slug_prefix: Some("cwbb-".to_string()),
            is_sports: true,
        }
    }

    /// Create NCAA Football league specification
    pub fn ncaaf() -> Self {
        Self {
            league_code: "ncaaf".to_string(),
            kalshi_series_tickers: vec![
                "KXNCAAFGAME".to_string(),
                "KXNCAAFSPREAD".to_string(),
                "KXNCAAFTOTAL".to_string(),
            ],
            polymarket_tag: Some("cfb".to_string()),
            polymarket_slug_prefix: Some("cfb-".to_string()),
            is_sports: true,
        }
    }

    /// Create NHL league specification
    pub fn nhl() -> Self {
        Self {
            league_code: "nhl".to_string(),
            kalshi_series_tickers: vec![
                "KXNHLGAME".to_string(),
                "KXNHLSPREAD".to_string(),
                "KXNHLTOTAL".to_string(),
            ],
            polymarket_tag: Some("nhl".to_string()),
            polymarket_slug_prefix: Some("nhl-".to_string()),
            is_sports: true,
        }
    }

    /// Create MLB league specification
    pub fn mlb() -> Self {
        Self {
            league_code: "mlb".to_string(),
            kalshi_series_tickers: vec![
                "KXMLBGAME".to_string(),
                "KXMLBSPREAD".to_string(),
                "KXMLBTOTAL".to_string(),
            ],
            polymarket_tag: Some("mlb".to_string()),
            polymarket_slug_prefix: Some("mlb-".to_string()),
            is_sports: true,
        }
    }

    /// Create EPL (English Premier League) specification
    pub fn epl() -> Self {
        Self {
            league_code: "epl".to_string(),
            kalshi_series_tickers: vec![
                "KXEPLGAME".to_string(),
                "KXEPLSPREAD".to_string(),
                "KXEPLTOTAL".to_string(),
                "KXEPLBTTS".to_string(),
            ],
            polymarket_tag: Some("epl".to_string()),
            polymarket_slug_prefix: Some("epl-".to_string()),
            is_sports: true,
        }
    }

    /// Create MLS league specification
    pub fn mls() -> Self {
        Self {
            league_code: "mls".to_string(),
            kalshi_series_tickers: vec!["KXMLSGAME".to_string()],
            polymarket_tag: Some("mls".to_string()),
            polymarket_slug_prefix: Some("mls-".to_string()),
            is_sports: true,
        }
    }

    /// Get all supported sports league specs
    pub fn all_sports() -> Vec<Self> {
        vec![
            Self::nfl(),
            Self::nba(),
            Self::ncaamb(),
            Self::ncaawb(),
            Self::ncaaf(),
            Self::nhl(),
            Self::mlb(),
            Self::epl(),
            Self::mls(),
        ]
    }

    /// Get league spec by code
    pub fn from_code(code: &str) -> Option<Self> {
        match code.to_lowercase().as_str() {
            "nfl" => Some(Self::nfl()),
            "nba" => Some(Self::nba()),
            "ncaamb" | "ncaab" | "cbb" => Some(Self::ncaamb()),
            "ncaawb" | "cwbb" => Some(Self::ncaawb()),
            "ncaaf" | "cfb" => Some(Self::ncaaf()),
            "nhl" => Some(Self::nhl()),
            "mlb" => Some(Self::mlb()),
            "epl" => Some(Self::epl()),
            "mls" => Some(Self::mls()),
            _ => None,
        }
    }

    /// Check if a given league code represents a sports league
    pub fn is_sports_league(code: &str) -> bool {
        Self::from_code(code).map(|s| s.is_sports).unwrap_or(false)
    }
}

/// Metadata for a Kalshi event discovered via REST API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiEventMeta {
    /// Kalshi event ticker (e.g., "KXNFLGAME-26JAN01BUFDET")
    pub event_ticker: String,
    /// Human-readable event title
    pub title: String,
    /// Event start time (if available)
    pub start_time: Option<DateTime<Utc>>,
    /// Home team name (parsed from title/ticker)
    pub home_team: Option<String>,
    /// Away team name (parsed from title/ticker)
    pub away_team: Option<String>,
    /// Series ticker this event belongs to
    pub series_ticker: String,
    /// Individual markets within this event
    pub markets: Vec<KalshiMarketMeta>,
}

impl KalshiEventMeta {
    /// Get the normalized matchup key for this event
    pub fn matchup_key(&self, norm_config: &super::NormalizationConfig) -> Option<String> {
        match (&self.home_team, &self.away_team, &self.start_time) {
            (Some(home), Some(away), Some(time)) => Some(super::normalize::build_matchup_key(
                home,
                away,
                time,
                norm_config,
            )),
            _ => None,
        }
    }
}

/// Metadata for a specific Kalshi market within an event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiMarketMeta {
    /// Market ticker (e.g., "KXNFLGAME-26JAN01BUFDET-BUF")
    pub ticker: String,
    /// Market title/subtitle
    pub title: String,
    /// Market type indicator (moneyline, spread, total)
    pub market_type: String,
    /// Line value for spread/total markets
    pub line_value: Option<f64>,
    /// Team this market is for (if applicable)
    pub team: Option<String>,
}

/// Metadata for a Polymarket event discovered via Gamma API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolyEventMeta {
    /// Polymarket event ID
    pub event_id: String,
    /// Event slug (for reference, not used for matching)
    pub slug: String,
    /// Human-readable event title/question
    pub title: String,
    /// Event start time (if available)
    pub start_time: Option<DateTime<Utc>>,
    /// Home team name (parsed from title)
    pub home_team: Option<String>,
    /// Away team name (parsed from title)
    pub away_team: Option<String>,
    /// Category/tag
    pub category: Option<String>,
    /// Individual markets within this event
    pub markets: Vec<PolyMarketMeta>,
}

impl PolyEventMeta {
    /// Get the normalized matchup key for this event
    pub fn matchup_key(&self, norm_config: &super::NormalizationConfig) -> Option<String> {
        match (&self.home_team, &self.away_team, &self.start_time) {
            (Some(home), Some(away), Some(time)) => Some(super::normalize::build_matchup_key(
                home,
                away,
                time,
                norm_config,
            )),
            _ => None,
        }
    }
}

/// Metadata for a specific Polymarket market within an event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolyMarketMeta {
    /// Polymarket market ID/condition ID
    pub market_id: String,
    /// Market question/title
    pub question: String,
    /// Market slug
    pub slug: String,
    /// YES token CLOB ID (for WS subscription)
    pub clob_token_id_yes: String,
    /// NO token CLOB ID (for WS subscription)
    pub clob_token_id_no: String,
    /// Market type indicator
    pub market_type: String,
    /// Whether market is active
    pub active: bool,
    /// Whether market is closed
    pub closed: bool,
}

/// A successfully matched market pair across exchanges
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchedMarket {
    /// League this market belongs to
    pub league: String,
    /// Normalized matchup key used for joining
    pub matchup_key: String,
    /// Event start time
    pub start_time: DateTime<Utc>,

    // Kalshi side
    /// Kalshi event ticker
    pub kalshi_event_ticker: String,
    /// Kalshi market ticker(s) for this matchup
    pub kalshi_market_tickers: Vec<String>,
    /// Full Kalshi event metadata (for reference)
    pub kalshi_event: Arc<KalshiEventMeta>,

    // Polymarket side
    /// Polymarket event ID
    pub poly_event_id: String,
    /// Polymarket CLOB token IDs (YES/NO) for WS subscription
    pub poly_clob_token_ids: Vec<(String, String)>, // Vec of (yes_token, no_token)
    /// Full Polymarket event metadata (for reference)
    pub poly_event: Arc<PolyEventMeta>,

    /// Match confidence score (0.0 - 1.0)
    pub confidence: f64,
}

impl MatchedMarket {
    /// Get all Kalshi tickers that need WS subscription
    pub fn kalshi_ws_tickers(&self) -> Vec<&str> {
        self.kalshi_market_tickers
            .iter()
            .map(|s| s.as_str())
            .collect()
    }

    /// Get all Polymarket token IDs that need WS subscription
    pub fn poly_ws_tokens(&self) -> Vec<&str> {
        self.poly_clob_token_ids
            .iter()
            .flat_map(|(yes, no)| vec![yes.as_str(), no.as_str()])
            .collect()
    }

    /// Get human-readable description
    pub fn description(&self) -> String {
        format!(
            "{} - {} ({})",
            self.matchup_key,
            self.league,
            self.start_time.format("%Y-%m-%d %H:%M UTC")
        )
    }
}

/// Market type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LiveMarketType {
    /// Moneyline/outright winner
    Moneyline,
    /// Point spread
    Spread,
    /// Over/under total
    Total,
    /// Both teams to score (soccer)
    Btts,
    /// Unknown/other
    Unknown,
}

impl LiveMarketType {
    /// Parse market type from string indicators
    pub fn from_str(s: &str) -> Self {
        let lower = s.to_lowercase();
        if lower.contains("spread") || lower.contains("handicap") {
            Self::Spread
        } else if lower.contains("total") || lower.contains("over") || lower.contains("under") {
            Self::Total
        } else if lower.contains("btts") || lower.contains("both teams") {
            Self::Btts
        } else if lower.contains("win")
            || lower.contains("moneyline")
            || lower.contains("ml")
            || lower.contains("game")
        {
            Self::Moneyline
        } else {
            Self::Unknown
        }
    }
}

impl std::fmt::Display for LiveMarketType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Moneyline => write!(f, "moneyline"),
            Self::Spread => write!(f, "spread"),
            Self::Total => write!(f, "total"),
            Self::Btts => write!(f, "btts"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

// =============================================================================
// Conversion to existing MarketPair format
// =============================================================================

use crate::types::{MarketPair, MarketType};

impl MatchedMarket {
    /// Convert this MatchedMarket into zero or more MarketPairs compatible with
    /// the existing WS subscription and arbitrage detection pipeline.
    ///
    /// Each Kalshi market ticker is paired with a corresponding Polymarket token pair.
    /// If counts mismatch, we pair what we can (zip semantics).
    pub fn to_market_pairs(&self) -> Vec<MarketPair> {
        let mut pairs = Vec::new();

        // Determine market type from Kalshi market metadata or event
        let market_type = self.infer_market_type();

        // Zip Kalshi tickers with Poly tokens
        // If there's a mismatch in counts, zip pairs what we can
        let ticker_count = self.kalshi_market_tickers.len();
        let token_count = self.poly_clob_token_ids.len();

        let pair_count = ticker_count.min(token_count);

        for i in 0..pair_count {
            let kalshi_ticker = &self.kalshi_market_tickers[i];
            let (poly_yes, poly_no) = &self.poly_clob_token_ids[i];

            // Generate a stable pair_id
            let pair_id = format!("{}-{}", self.matchup_key, kalshi_ticker);

            // Extract team suffix from ticker if applicable (e.g., "-BUF" from "KXNFLGAME-26JAN01BUFDET-BUF")
            let team_suffix = extract_team_suffix_from_ticker(kalshi_ticker);

            // Extract line value from Kalshi market metadata if this is a spread/total
            let line_value = self.extract_line_value(i);

            // Construct a poly_slug for reference (not used for matching, just for logging/debugging)
            // We use the Polymarket event slug if available
            let poly_slug = self.poly_event.slug.clone();

            pairs.push(MarketPair {
                pair_id: pair_id.into(),
                league: self.league.clone().into(),
                market_type,
                description: self.description().into(),
                kalshi_event_ticker: self.kalshi_event_ticker.clone().into(),
                kalshi_market_ticker: kalshi_ticker.clone().into(),
                poly_slug: poly_slug.into(),
                poly_yes_token: poly_yes.clone().into(),
                poly_no_token: poly_no.clone().into(),
                line_value,
                team_suffix: team_suffix.map(|s| s.into()),
            });
        }

        pairs
    }

    /// Infer the MarketType from the Kalshi ticker patterns
    fn infer_market_type(&self) -> MarketType {
        // Check series ticker first
        let series = &self.kalshi_event.series_ticker;
        if series.contains("SPREAD") {
            return MarketType::Spread;
        }
        if series.contains("TOTAL") {
            return MarketType::Total;
        }
        if series.contains("BTTS") {
            return MarketType::Btts;
        }
        if series.contains("GAME") || series.contains("ML") {
            return MarketType::Moneyline;
        }

        // Check individual market titles
        for market in &self.kalshi_event.markets {
            let lower = market.market_type.to_lowercase();
            if lower.contains("spread") {
                return MarketType::Spread;
            }
            if lower.contains("total") || lower.contains("over") {
                return MarketType::Total;
            }
            if lower.contains("btts") {
                return MarketType::Btts;
            }
        }

        // Default to moneyline
        MarketType::Moneyline
    }

    /// Extract line value for spread/total markets
    fn extract_line_value(&self, market_index: usize) -> Option<f64> {
        self.kalshi_event
            .markets
            .get(market_index)
            .and_then(|m| m.line_value)
    }
}

/// Extract team suffix from a Kalshi market ticker (e.g., "-BUF" from "KXNFLGAME-26JAN01BUFDET-BUF")
fn extract_team_suffix_from_ticker(ticker: &str) -> Option<String> {
    let parts: Vec<&str> = ticker.split('-').collect();
    if parts.len() >= 3 {
        let last = parts.last()?;
        // Team suffixes are typically 2-4 letter codes
        if last.len() >= 2 && last.len() <= 4 && last.chars().all(|c| c.is_ascii_alphabetic()) {
            return Some(last.to_string());
        }
    }
    None
}

/// Convert a slice of MatchedMarkets into MarketPairs for the WS subscription pipeline
pub fn matched_markets_to_pairs(matched: &[MatchedMarket]) -> Vec<MarketPair> {
    matched.iter().flat_map(|m| m.to_market_pairs()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_league_spec_from_code() {
        assert!(LeagueSpec::from_code("nfl").is_some());
        assert!(LeagueSpec::from_code("NFL").is_some());
        assert!(LeagueSpec::from_code("nba").is_some());
        assert!(LeagueSpec::from_code("ncaamb").is_some());
        assert!(LeagueSpec::from_code("ncaab").is_some()); // alias
        assert!(LeagueSpec::from_code("unknown").is_none());
    }

    #[test]
    fn test_league_spec_is_sports() {
        assert!(LeagueSpec::is_sports_league("nfl"));
        assert!(LeagueSpec::is_sports_league("nba"));
        assert!(!LeagueSpec::is_sports_league("politics"));
    }

    #[test]
    fn test_live_market_type_parsing() {
        assert_eq!(LiveMarketType::from_str("spread"), LiveMarketType::Spread);
        assert_eq!(LiveMarketType::from_str("SPREAD"), LiveMarketType::Spread);
        assert_eq!(LiveMarketType::from_str("total"), LiveMarketType::Total);
        assert_eq!(
            LiveMarketType::from_str("over/under"),
            LiveMarketType::Total
        );
        assert_eq!(
            LiveMarketType::from_str("moneyline"),
            LiveMarketType::Moneyline
        );
        assert_eq!(LiveMarketType::from_str("win"), LiveMarketType::Moneyline);
        assert_eq!(LiveMarketType::from_str("btts"), LiveMarketType::Btts);
        assert_eq!(LiveMarketType::from_str("random"), LiveMarketType::Unknown);
    }

    #[test]
    fn test_all_sports_leagues() {
        let all = LeagueSpec::all_sports();
        assert!(all.len() >= 5);
        assert!(all.iter().all(|s| s.is_sports));
    }

    #[test]
    fn test_extract_team_suffix_from_ticker() {
        assert_eq!(
            extract_team_suffix_from_ticker("KXNFLGAME-26JAN01BUFDET-BUF"),
            Some("BUF".to_string())
        );
        assert_eq!(
            extract_team_suffix_from_ticker("KXNBAGAME-26JAN01LAKBOS-LAK"),
            Some("LAK".to_string())
        );
        // No suffix for simple tickers
        assert_eq!(
            extract_team_suffix_from_ticker("KXNFLGAME-26JAN01BUFDET"),
            None
        );
        // Too long suffix
        assert_eq!(
            extract_team_suffix_from_ticker("KXNFLGAME-26JAN01-TOOLONG"),
            None
        );
    }

    #[test]
    fn test_matched_market_to_market_pairs() {
        use chrono::TimeZone;

        // Create test MatchedMarket
        let kalshi_event = Arc::new(KalshiEventMeta {
            event_ticker: "KXNFLGAME-26JAN05BUFDET".to_string(),
            title: "Buffalo Bills vs Detroit Lions".to_string(),
            start_time: Some(Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap()),
            home_team: Some("Detroit Lions".to_string()),
            away_team: Some("Buffalo Bills".to_string()),
            series_ticker: "KXNFLGAME".to_string(),
            markets: vec![
                KalshiMarketMeta {
                    ticker: "KXNFLGAME-26JAN05BUFDET-BUF".to_string(),
                    title: "Buffalo Bills to win".to_string(),
                    market_type: "moneyline".to_string(),
                    line_value: None,
                    team: Some("BUF".to_string()),
                },
                KalshiMarketMeta {
                    ticker: "KXNFLGAME-26JAN05BUFDET-DET".to_string(),
                    title: "Detroit Lions to win".to_string(),
                    market_type: "moneyline".to_string(),
                    line_value: None,
                    team: Some("DET".to_string()),
                },
            ],
        });

        let poly_event = Arc::new(PolyEventMeta {
            event_id: "poly-12345".to_string(),
            slug: "nfl-buffalo-bills-vs-detroit-lions-2026-01-05".to_string(),
            title: "Will Buffalo Bills beat Detroit Lions?".to_string(),
            start_time: Some(Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap()),
            home_team: Some("Detroit Lions".to_string()),
            away_team: Some("Buffalo Bills".to_string()),
            category: Some("nfl".to_string()),
            markets: vec![
                PolyMarketMeta {
                    market_id: "pm-buf".to_string(),
                    question: "Will Buffalo Bills win?".to_string(),
                    slug: "nfl-buffalo-bills-vs-detroit-lions-2026-01-05-bills-win".to_string(),
                    clob_token_id_yes: "yes-token-buf-12345".to_string(),
                    clob_token_id_no: "no-token-buf-12345".to_string(),
                    market_type: "moneyline".to_string(),
                    active: true,
                    closed: false,
                },
                PolyMarketMeta {
                    market_id: "pm-det".to_string(),
                    question: "Will Detroit Lions win?".to_string(),
                    slug: "nfl-buffalo-bills-vs-detroit-lions-2026-01-05-lions-win".to_string(),
                    clob_token_id_yes: "yes-token-det-12345".to_string(),
                    clob_token_id_no: "no-token-det-12345".to_string(),
                    market_type: "moneyline".to_string(),
                    active: true,
                    closed: false,
                },
            ],
        });

        let matched = MatchedMarket {
            league: "nfl".to_string(),
            matchup_key: "2026-01-05|bills|lions".to_string(),
            start_time: Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap(),
            kalshi_event_ticker: "KXNFLGAME-26JAN05BUFDET".to_string(),
            kalshi_market_tickers: vec![
                "KXNFLGAME-26JAN05BUFDET-BUF".to_string(),
                "KXNFLGAME-26JAN05BUFDET-DET".to_string(),
            ],
            kalshi_event: kalshi_event.clone(),
            poly_event_id: "poly-12345".to_string(),
            poly_clob_token_ids: vec![
                (
                    "yes-token-buf-12345".to_string(),
                    "no-token-buf-12345".to_string(),
                ),
                (
                    "yes-token-det-12345".to_string(),
                    "no-token-det-12345".to_string(),
                ),
            ],
            poly_event: poly_event.clone(),
            confidence: 1.0,
        };

        // Convert to MarketPairs
        let pairs = matched.to_market_pairs();

        // Should produce 2 pairs (one for each market)
        assert_eq!(pairs.len(), 2);

        // Check first pair
        let p0 = &pairs[0];
        assert_eq!(p0.league.as_ref(), "nfl");
        assert_eq!(
            p0.kalshi_market_ticker.as_ref(),
            "KXNFLGAME-26JAN05BUFDET-BUF"
        );
        assert_eq!(p0.poly_yes_token.as_ref(), "yes-token-buf-12345");
        assert_eq!(p0.poly_no_token.as_ref(), "no-token-buf-12345");
        assert_eq!(p0.team_suffix.as_ref().map(|s| s.as_ref()), Some("BUF"));
        assert_eq!(p0.market_type, MarketType::Moneyline);

        // Check second pair
        let p1 = &pairs[1];
        assert_eq!(
            p1.kalshi_market_ticker.as_ref(),
            "KXNFLGAME-26JAN05BUFDET-DET"
        );
        assert_eq!(p1.poly_yes_token.as_ref(), "yes-token-det-12345");
        assert_eq!(p1.team_suffix.as_ref().map(|s| s.as_ref()), Some("DET"));
    }

    #[test]
    fn test_matched_markets_to_pairs_empty() {
        let pairs = matched_markets_to_pairs(&[]);
        assert!(pairs.is_empty());
    }

    #[test]
    fn test_infer_market_type_from_series() {
        use chrono::TimeZone;

        // Create events with different series tickers
        let make_event = |series: &str| -> KalshiEventMeta {
            KalshiEventMeta {
                event_ticker: format!("{}-26JAN05TEST", series),
                title: "Test Event".to_string(),
                start_time: Some(Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap()),
                home_team: Some("Team A".to_string()),
                away_team: Some("Team B".to_string()),
                series_ticker: series.to_string(),
                markets: vec![],
            }
        };

        let poly_event = Arc::new(PolyEventMeta {
            event_id: "test".to_string(),
            slug: "test-event".to_string(),
            title: "Test".to_string(),
            start_time: Some(Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap()),
            home_team: None,
            away_team: None,
            category: None,
            markets: vec![],
        });

        let make_matched = |series: &str| -> MatchedMarket {
            MatchedMarket {
                league: "nfl".to_string(),
                matchup_key: "test".to_string(),
                start_time: Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap(),
                kalshi_event_ticker: format!("{}-26JAN05TEST", series),
                kalshi_market_tickers: vec!["TEST".to_string()],
                kalshi_event: Arc::new(make_event(series)),
                poly_event_id: "test".to_string(),
                poly_clob_token_ids: vec![("yes".to_string(), "no".to_string())],
                poly_event: poly_event.clone(),
                confidence: 1.0,
            }
        };

        // Test different series patterns
        assert_eq!(
            make_matched("KXNFLGAME").infer_market_type(),
            MarketType::Moneyline
        );
        assert_eq!(
            make_matched("KXNFLSPREAD").infer_market_type(),
            MarketType::Spread
        );
        assert_eq!(
            make_matched("KXNFLTOTAL").infer_market_type(),
            MarketType::Total
        );
        assert_eq!(
            make_matched("KXEPLBTTS").infer_market_type(),
            MarketType::Btts
        );
    }
}
