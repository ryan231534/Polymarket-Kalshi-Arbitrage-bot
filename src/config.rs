//! System configuration and league mapping definitions.
//!
//! This module contains all configuration constants, league mappings, and
//! environment variable parsing for the trading system.

/// Kalshi WebSocket URL
pub const KALSHI_WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";

/// Kalshi REST API base URL
pub const KALSHI_API_BASE: &str = "https://api.elections.kalshi.com/trade-api/v2";

/// Polymarket WebSocket URL
pub const POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// Gamma API base URL (Polymarket market data)
pub const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";

/// DEPRECATED: Use profit_threshold_cents() instead
/// Arb threshold: alert when total cost < this (e.g., 0.995 = 0.5% profit)
///
/// This constant is kept for backward compatibility with existing code/configs.
/// New code should use `profit_threshold_cents()` for consistent threshold handling.
#[deprecated(note = "Use profit_threshold_cents() for consistent threshold handling")]
#[allow(dead_code)]
pub const ARB_THRESHOLD: f64 = 0.995;

/// Default profit threshold in cents (100 cents = $1.00 = break-even)
/// Set lower values for more aggressive arbitrage (e.g., 99 = 1% profit minimum)
const DEFAULT_PROFIT_THRESHOLD_CENTS: u16 = 100;

/// Default minimum profit in cents per contract (must be positive to execute)
const DEFAULT_MIN_PROFIT_CENTS: u16 = 1;

/// Default slippage allowance in cents per leg (includes tick rounding worst-case)
/// TODO: Polymarket tick sizes can be < 1 cent and would require Decimal/bps pricing for full fidelity.
const DEFAULT_SLIPPAGE_CENTS_PER_LEG: u16 = 1;

/// Default Polymarket maker fee in basis points (0 = no fee)
const DEFAULT_POLY_MAKER_FEE_BPS: u16 = 0;

/// Default Polymarket taker fee in basis points (0 = no fee)
const DEFAULT_POLY_TAKER_FEE_BPS: u16 = 0;

// === Mismatch / Unwind Configuration Defaults ===

/// Maximum mismatch contracts before triggering severe mode
const DEFAULT_MAX_MISMATCH_CONTRACTS: u32 = 1;

/// Maximum ladder steps for unwind
const DEFAULT_UNWIND_MAX_STEPS: u32 = 5;

/// Price step size in cents for ladder
const DEFAULT_UNWIND_STEP_CENTS: u16 = 1;

/// Backoff between unwind steps in milliseconds
const DEFAULT_UNWIND_BACKOFF_MS: u64 = 25;

/// Panic price for buys (worst case to guarantee fill)
const DEFAULT_UNWIND_PANIC_PRICE_BUY_CENTS: u16 = 99;

/// Panic price for sells (worst case to guarantee fill)
const DEFAULT_UNWIND_PANIC_PRICE_SELL_CENTS: u16 = 1;

/// Multiplier for emergency unwind (severe mismatch)
const DEFAULT_UNWIND_EMERGENCY_MULTIPLIER: u32 = 3;

/// Polymarket ping interval (seconds) - keep connection alive
pub const POLY_PING_INTERVAL_SECS: u64 = 30;

/// Kalshi API rate limit delay (milliseconds between requests)
/// Kalshi limit: 20 req/sec = 50ms minimum. We use 60ms for safety margin.
pub const KALSHI_API_DELAY_MS: u64 = 60;

/// WebSocket reconnect delay (seconds)
pub const WS_RECONNECT_DELAY_SECS: u64 = 5;

/// Which leagues to monitor (empty slice = all)
pub const ENABLED_LEAGUES: &[&str] = &[];

/// Price logging enabled (set PRICE_LOGGING=1 to enable)
#[allow(dead_code)]
pub fn price_logging_enabled() -> bool {
    static CACHED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("PRICE_LOGGING")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false)
    })
}

/// Get enabled leagues from LEAGUES env var (comma-separated).
/// If unset/empty, returns empty Vec meaning "all leagues" (same as ENABLED_LEAGUES = &[]).
/// Example: LEAGUES="nfl,nba,epl"
pub fn enabled_leagues_from_env() -> Vec<String> {
    std::env::var("LEAGUES")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.split(',').map(|l| l.trim().to_string()).collect())
        .unwrap_or_default()
}

/// Get the canonical profit threshold in cents.
///
/// This is the SINGLE SOURCE OF TRUTH for the profit threshold used throughout
/// the system for:
/// - Arbitrage opportunity detection (check_arbs)
/// - Execution decisions (process)
/// - Logging (startup, heartbeat)
///
/// The threshold represents the maximum total cost (in cents) for an arbitrage
/// to be considered profitable:
/// - 100 cents = break-even (buy YES + NO for exactly $1.00)
/// - 99 cents = 1% minimum profit (buy YES + NO for $0.99)
/// - 95 cents = 5% minimum profit (buy YES + NO for $0.95)
///
/// Configuration:
/// - Reads from PROFIT_THRESHOLD_CENTS env var (integer cents: 1-100)
/// - Falls back to ARB_THRESHOLD env var if set (legacy float: 0.01-1.00)
/// - Uses DEFAULT_PROFIT_THRESHOLD_CENTS (100) if neither is set
///
/// The value is cached after first call for performance and consistency.
///
/// # Examples
/// ```
/// // Get threshold (default 100 cents)
/// let threshold = profit_threshold_cents();
///
/// // With env var:
/// // PROFIT_THRESHOLD_CENTS=99 → 99 cents (1% profit)
/// // PROFIT_THRESHOLD_CENTS=95 → 95 cents (5% profit)
/// ```
pub fn profit_threshold_cents() -> u16 {
    use std::sync::OnceLock;
    use tracing::warn;

    static CACHED: OnceLock<u16> = OnceLock::new();
    *CACHED.get_or_init(|| {
        // Try PROFIT_THRESHOLD_CENTS first (preferred, integer cents)
        if let Ok(val_str) = std::env::var("PROFIT_THRESHOLD_CENTS") {
            if let Ok(cents) = val_str.parse::<u16>() {
                if cents > 0 && cents <= 100 {
                    return cents;
                } else {
                    warn!(
                        "Invalid PROFIT_THRESHOLD_CENTS={} (must be 1-100), using default {}",
                        cents, DEFAULT_PROFIT_THRESHOLD_CENTS
                    );
                }
            } else {
                warn!(
                    "Failed to parse PROFIT_THRESHOLD_CENTS='{}', using default {}",
                    val_str, DEFAULT_PROFIT_THRESHOLD_CENTS
                );
            }
        }

        // Fallback to legacy ARB_THRESHOLD if set (float 0.01-1.00)
        #[allow(deprecated)]
        if let Ok(val_str) = std::env::var("ARB_THRESHOLD") {
            if let Ok(threshold_f64) = val_str.parse::<f64>() {
                if threshold_f64 > 0.0 && threshold_f64 <= 1.0 {
                    let cents = (threshold_f64 * 100.0).round() as u16;
                    if cents > 0 && cents <= 100 {
                        return cents;
                    }
                }
                warn!(
                    "Invalid ARB_THRESHOLD={} (must be 0.01-1.00), using default {}",
                    threshold_f64, DEFAULT_PROFIT_THRESHOLD_CENTS
                );
            }
        }

        // Default value
        DEFAULT_PROFIT_THRESHOLD_CENTS
    })
}

/// Kalshi fee role (taker or maker)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KalshiRole {
    #[default]
    Taker,
    Maker,
}

/// Polymarket fee role (taker or maker)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PolyRole {
    #[default]
    Taker,
    Maker,
}

/// Get execution threshold in cents (max all-in cost per contract to execute).
///
/// Priority:
/// 1. EXECUTION_THRESHOLD_CENTS env var (if explicitly set)
/// 2. PROFIT_THRESHOLD_CENTS env var (backward compat - same semantics)
/// 3. ARB_THRESHOLD legacy fallback
/// 4. Default (100 cents)
///
/// The value is cached after first call.
pub fn execution_threshold_cents() -> u16 {
    use std::sync::OnceLock;
    use tracing::warn;

    static CACHED: OnceLock<u16> = OnceLock::new();
    *CACHED.get_or_init(|| {
        // Try EXECUTION_THRESHOLD_CENTS first (new preferred)
        if let Ok(val_str) = std::env::var("EXECUTION_THRESHOLD_CENTS") {
            if let Ok(cents) = val_str.parse::<u16>() {
                if cents > 0 && cents <= 100 {
                    return cents;
                } else {
                    warn!(
                        "Invalid EXECUTION_THRESHOLD_CENTS={} (must be 1-100), using PROFIT_THRESHOLD_CENTS fallback",
                        cents
                    );
                }
            } else {
                warn!(
                    "Failed to parse EXECUTION_THRESHOLD_CENTS='{}', using PROFIT_THRESHOLD_CENTS fallback",
                    val_str
                );
            }
        }
        // Fall back to profit_threshold_cents() which handles PROFIT_THRESHOLD_CENTS and ARB_THRESHOLD
        profit_threshold_cents()
    })
}

/// Get minimum profit in cents per contract required to execute.
///
/// Reads from MIN_PROFIT_CENTS env var, defaults to 1.
/// The value is cached after first call.
pub fn min_profit_cents() -> u16 {
    use std::sync::OnceLock;
    use tracing::warn;

    static CACHED: OnceLock<u16> = OnceLock::new();
    *CACHED.get_or_init(|| {
        if let Ok(val_str) = std::env::var("MIN_PROFIT_CENTS") {
            if let Ok(cents) = val_str.parse::<u16>() {
                return cents; // Allow 0 to disable the check
            } else {
                warn!(
                    "Failed to parse MIN_PROFIT_CENTS='{}', using default {}",
                    val_str, DEFAULT_MIN_PROFIT_CENTS
                );
            }
        }
        DEFAULT_MIN_PROFIT_CENTS
    })
}

/// Get slippage allowance in cents per leg.
///
/// This includes worst-case tick/rounding effects.
/// Reads from SLIPPAGE_CENTS_PER_LEG env var, defaults to 1.
/// The value is cached after first call.
pub fn slippage_cents_per_leg() -> u16 {
    use std::sync::OnceLock;
    use tracing::warn;

    static CACHED: OnceLock<u16> = OnceLock::new();
    *CACHED.get_or_init(|| {
        if let Ok(val_str) = std::env::var("SLIPPAGE_CENTS_PER_LEG") {
            if let Ok(cents) = val_str.parse::<u16>() {
                return cents;
            } else {
                warn!(
                    "Failed to parse SLIPPAGE_CENTS_PER_LEG='{}', using default {}",
                    val_str, DEFAULT_SLIPPAGE_CENTS_PER_LEG
                );
            }
        }
        DEFAULT_SLIPPAGE_CENTS_PER_LEG
    })
}

/// Get Polymarket maker fee in basis points.
///
/// Reads from POLY_MAKER_FEE_BPS env var, defaults to 0.
/// The value is cached after first call.
pub fn poly_maker_fee_bps() -> u16 {
    use std::sync::OnceLock;
    use tracing::warn;

    static CACHED: OnceLock<u16> = OnceLock::new();
    *CACHED.get_or_init(|| {
        if let Ok(val_str) = std::env::var("POLY_MAKER_FEE_BPS") {
            if let Ok(bps) = val_str.parse::<u16>() {
                return bps;
            } else {
                warn!(
                    "Failed to parse POLY_MAKER_FEE_BPS='{}', using default {}",
                    val_str, DEFAULT_POLY_MAKER_FEE_BPS
                );
            }
        }
        DEFAULT_POLY_MAKER_FEE_BPS
    })
}

/// Get Polymarket taker fee in basis points.
///
/// Reads from POLY_TAKER_FEE_BPS env var, defaults to 0.
/// The value is cached after first call.
pub fn poly_taker_fee_bps() -> u16 {
    use std::sync::OnceLock;
    use tracing::warn;

    static CACHED: OnceLock<u16> = OnceLock::new();
    *CACHED.get_or_init(|| {
        if let Ok(val_str) = std::env::var("POLY_TAKER_FEE_BPS") {
            if let Ok(bps) = val_str.parse::<u16>() {
                return bps;
            } else {
                warn!(
                    "Failed to parse POLY_TAKER_FEE_BPS='{}', using default {}",
                    val_str, DEFAULT_POLY_TAKER_FEE_BPS
                );
            }
        }
        DEFAULT_POLY_TAKER_FEE_BPS
    })
}

/// Get Kalshi fee role (taker or maker).
///
/// Reads from KALSHI_FEE_ROLE env var ("taker" or "maker"), defaults to taker.
/// The value is cached after first call.
pub fn kalshi_fee_role() -> KalshiRole {
    use std::sync::OnceLock;
    use tracing::warn;

    static CACHED: OnceLock<KalshiRole> = OnceLock::new();
    *CACHED.get_or_init(|| {
        if let Ok(val_str) = std::env::var("KALSHI_FEE_ROLE") {
            match val_str.to_lowercase().as_str() {
                "maker" => return KalshiRole::Maker,
                "taker" => return KalshiRole::Taker,
                _ => {
                    warn!(
                        "Invalid KALSHI_FEE_ROLE='{}' (must be 'taker' or 'maker'), using default 'taker'",
                        val_str
                    );
                }
            }
        }
        KalshiRole::Taker
    })
}

// === Mismatch / Unwind Configuration Functions ===

/// Get maximum mismatch contracts before triggering severe mode.
///
/// Reads from MAX_MISMATCH_CONTRACTS env var, defaults to 1.
pub fn max_mismatch_contracts() -> u32 {
    use std::sync::OnceLock;
    use tracing::warn;

    static CACHED: OnceLock<u32> = OnceLock::new();
    *CACHED.get_or_init(|| {
        if let Ok(val_str) = std::env::var("MAX_MISMATCH_CONTRACTS") {
            if let Ok(val) = val_str.parse::<u32>() {
                return val;
            } else {
                warn!(
                    "Failed to parse MAX_MISMATCH_CONTRACTS='{}', using default {}",
                    val_str, DEFAULT_MAX_MISMATCH_CONTRACTS
                );
            }
        }
        DEFAULT_MAX_MISMATCH_CONTRACTS
    })
}

/// Get maximum ladder steps for unwind.
///
/// Reads from UNWIND_MAX_STEPS env var, defaults to 5.
pub fn unwind_max_steps() -> u32 {
    use std::sync::OnceLock;
    use tracing::warn;

    static CACHED: OnceLock<u32> = OnceLock::new();
    *CACHED.get_or_init(|| {
        if let Ok(val_str) = std::env::var("UNWIND_MAX_STEPS") {
            if let Ok(val) = val_str.parse::<u32>() {
                return val;
            } else {
                warn!(
                    "Failed to parse UNWIND_MAX_STEPS='{}', using default {}",
                    val_str, DEFAULT_UNWIND_MAX_STEPS
                );
            }
        }
        DEFAULT_UNWIND_MAX_STEPS
    })
}

/// Get price step size in cents for ladder.
///
/// Reads from UNWIND_STEP_CENTS env var, defaults to 1.
pub fn unwind_step_cents() -> u16 {
    use std::sync::OnceLock;
    use tracing::warn;

    static CACHED: OnceLock<u16> = OnceLock::new();
    *CACHED.get_or_init(|| {
        if let Ok(val_str) = std::env::var("UNWIND_STEP_CENTS") {
            if let Ok(val) = val_str.parse::<u16>() {
                return val;
            } else {
                warn!(
                    "Failed to parse UNWIND_STEP_CENTS='{}', using default {}",
                    val_str, DEFAULT_UNWIND_STEP_CENTS
                );
            }
        }
        DEFAULT_UNWIND_STEP_CENTS
    })
}

/// Get backoff between unwind steps in milliseconds.
///
/// Reads from UNWIND_BACKOFF_MS env var, defaults to 25.
pub fn unwind_backoff_ms() -> u64 {
    use std::sync::OnceLock;
    use tracing::warn;

    static CACHED: OnceLock<u64> = OnceLock::new();
    *CACHED.get_or_init(|| {
        if let Ok(val_str) = std::env::var("UNWIND_BACKOFF_MS") {
            if let Ok(val) = val_str.parse::<u64>() {
                return val;
            } else {
                warn!(
                    "Failed to parse UNWIND_BACKOFF_MS='{}', using default {}",
                    val_str, DEFAULT_UNWIND_BACKOFF_MS
                );
            }
        }
        DEFAULT_UNWIND_BACKOFF_MS
    })
}

/// Get panic price for buys (worst case to guarantee fill).
///
/// Reads from UNWIND_PANIC_PRICE_BUY_CENTS env var, defaults to 99.
pub fn unwind_panic_price_buy_cents() -> u16 {
    use std::sync::OnceLock;
    use tracing::warn;

    static CACHED: OnceLock<u16> = OnceLock::new();
    *CACHED.get_or_init(|| {
        if let Ok(val_str) = std::env::var("UNWIND_PANIC_PRICE_BUY_CENTS") {
            if let Ok(val) = val_str.parse::<u16>() {
                if val >= 1 && val <= 99 {
                    return val;
                }
            }
            warn!(
                "Invalid UNWIND_PANIC_PRICE_BUY_CENTS='{}', using default {}",
                val_str, DEFAULT_UNWIND_PANIC_PRICE_BUY_CENTS
            );
        }
        DEFAULT_UNWIND_PANIC_PRICE_BUY_CENTS
    })
}

/// Get panic price for sells (worst case to guarantee fill).
///
/// Reads from UNWIND_PANIC_PRICE_SELL_CENTS env var, defaults to 1.
pub fn unwind_panic_price_sell_cents() -> u16 {
    use std::sync::OnceLock;
    use tracing::warn;

    static CACHED: OnceLock<u16> = OnceLock::new();
    *CACHED.get_or_init(|| {
        if let Ok(val_str) = std::env::var("UNWIND_PANIC_PRICE_SELL_CENTS") {
            if let Ok(val) = val_str.parse::<u16>() {
                if val >= 1 && val <= 99 {
                    return val;
                }
            }
            warn!(
                "Invalid UNWIND_PANIC_PRICE_SELL_CENTS='{}', using default {}",
                val_str, DEFAULT_UNWIND_PANIC_PRICE_SELL_CENTS
            );
        }
        DEFAULT_UNWIND_PANIC_PRICE_SELL_CENTS
    })
}

/// Get multiplier for emergency unwind (severe mismatch).
///
/// Reads from UNWIND_EMERGENCY_MULTIPLIER env var, defaults to 3.
pub fn unwind_emergency_multiplier() -> u32 {
    use std::sync::OnceLock;
    use tracing::warn;

    static CACHED: OnceLock<u32> = OnceLock::new();
    *CACHED.get_or_init(|| {
        if let Ok(val_str) = std::env::var("UNWIND_EMERGENCY_MULTIPLIER") {
            if let Ok(val) = val_str.parse::<u32>() {
                if val >= 1 {
                    return val;
                }
            }
            warn!(
                "Invalid UNWIND_EMERGENCY_MULTIPLIER='{}', using default {}",
                val_str, DEFAULT_UNWIND_EMERGENCY_MULTIPLIER
            );
        }
        DEFAULT_UNWIND_EMERGENCY_MULTIPLIER
    })
}

/// Format the threshold for display in logs.
///
/// Returns a string like "100¢" or "99¢" for consistent logging.
pub fn format_threshold_cents(cents: u16) -> String {
    format!("{}¢", cents)
}

/// Calculate the profit percentage from a threshold in cents.
///
/// # Examples
/// - 100 cents → 0.0% profit (break-even)
/// - 99 cents → 1.0% profit
/// - 95 cents → 5.0% profit
pub fn threshold_profit_percent(cents: u16) -> f64 {
    ((100 - cents) as f64 / 100.0) * 100.0
}

/// League configuration for market discovery
#[derive(Debug, Clone)]
pub struct LeagueConfig {
    pub league_code: &'static str,
    pub poly_prefix: &'static str,
    pub kalshi_series_game: &'static str,
    pub kalshi_series_spread: Option<&'static str>,
    pub kalshi_series_total: Option<&'static str>,
    pub kalshi_series_btts: Option<&'static str>,
}

/// Get all supported leagues with their configurations
pub fn get_league_configs() -> Vec<LeagueConfig> {
    vec![
        // Major European leagues (full market types)
        LeagueConfig {
            league_code: "epl",
            poly_prefix: "epl",
            kalshi_series_game: "KXEPLGAME",
            kalshi_series_spread: Some("KXEPLSPREAD"),
            kalshi_series_total: Some("KXEPLTOTAL"),
            kalshi_series_btts: Some("KXEPLBTTS"),
        },
        LeagueConfig {
            league_code: "bundesliga",
            poly_prefix: "bun",
            kalshi_series_game: "KXBUNDESLIGAGAME",
            kalshi_series_spread: Some("KXBUNDESLIGASPREAD"),
            kalshi_series_total: Some("KXBUNDESLIGATOTAL"),
            kalshi_series_btts: Some("KXBUNDESLIGABTTS"),
        },
        LeagueConfig {
            league_code: "laliga",
            poly_prefix: "lal",
            kalshi_series_game: "KXLALIGAGAME",
            kalshi_series_spread: Some("KXLALIGASPREAD"),
            kalshi_series_total: Some("KXLALIGATOTAL"),
            kalshi_series_btts: Some("KXLALIGABTTS"),
        },
        LeagueConfig {
            league_code: "seriea",
            poly_prefix: "sea",
            kalshi_series_game: "KXSERIEAGAME",
            kalshi_series_spread: Some("KXSERIEASPREAD"),
            kalshi_series_total: Some("KXSERIEATOTAL"),
            kalshi_series_btts: Some("KXSERIEABTTS"),
        },
        LeagueConfig {
            league_code: "ligue1",
            poly_prefix: "fl1",
            kalshi_series_game: "KXLIGUE1GAME",
            kalshi_series_spread: Some("KXLIGUE1SPREAD"),
            kalshi_series_total: Some("KXLIGUE1TOTAL"),
            kalshi_series_btts: Some("KXLIGUE1BTTS"),
        },
        LeagueConfig {
            league_code: "ucl",
            poly_prefix: "ucl",
            kalshi_series_game: "KXUCLGAME",
            kalshi_series_spread: Some("KXUCLSPREAD"),
            kalshi_series_total: Some("KXUCLTOTAL"),
            kalshi_series_btts: Some("KXUCLBTTS"),
        },
        // Secondary European leagues (moneyline only)
        LeagueConfig {
            league_code: "uel",
            poly_prefix: "uel",
            kalshi_series_game: "KXUELGAME",
            kalshi_series_spread: None,
            kalshi_series_total: None,
            kalshi_series_btts: None,
        },
        LeagueConfig {
            league_code: "eflc",
            poly_prefix: "elc",
            kalshi_series_game: "KXEFLCHAMPIONSHIPGAME",
            kalshi_series_spread: None,
            kalshi_series_total: None,
            kalshi_series_btts: None,
        },
        // US Sports
        LeagueConfig {
            league_code: "nba",
            poly_prefix: "nba",
            kalshi_series_game: "KXNBAGAME",
            kalshi_series_spread: Some("KXNBASPREAD"),
            kalshi_series_total: Some("KXNBATOTAL"),
            kalshi_series_btts: None,
        },
        LeagueConfig {
            league_code: "nfl",
            poly_prefix: "nfl",
            kalshi_series_game: "KXNFLGAME",
            kalshi_series_spread: Some("KXNFLSPREAD"),
            kalshi_series_total: Some("KXNFLTOTAL"),
            kalshi_series_btts: None,
        },
        LeagueConfig {
            league_code: "nhl",
            poly_prefix: "nhl",
            kalshi_series_game: "KXNHLGAME",
            kalshi_series_spread: Some("KXNHLSPREAD"),
            kalshi_series_total: Some("KXNHLTOTAL"),
            kalshi_series_btts: None,
        },
        LeagueConfig {
            league_code: "mlb",
            poly_prefix: "mlb",
            kalshi_series_game: "KXMLBGAME",
            kalshi_series_spread: Some("KXMLBSPREAD"),
            kalshi_series_total: Some("KXMLBTOTAL"),
            kalshi_series_btts: None,
        },
        LeagueConfig {
            league_code: "mls",
            poly_prefix: "mls",
            kalshi_series_game: "KXMLSGAME",
            kalshi_series_spread: None,
            kalshi_series_total: None,
            kalshi_series_btts: None,
        },
        LeagueConfig {
            league_code: "ncaaf",
            poly_prefix: "cfb",
            kalshi_series_game: "KXNCAAFGAME",
            kalshi_series_spread: Some("KXNCAAFSPREAD"),
            kalshi_series_total: Some("KXNCAAFTOTAL"),
            kalshi_series_btts: None,
        },
    ]
}

/// Get config for a specific league
pub fn get_league_config(league: &str) -> Option<LeagueConfig> {
    get_league_configs()
        .into_iter()
        .find(|c| c.league_code == league || c.poly_prefix == league)
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_threshold_cents() {
        assert_eq!(format_threshold_cents(100), "100¢");
        assert_eq!(format_threshold_cents(99), "99¢");
        assert_eq!(format_threshold_cents(95), "95¢");
        assert_eq!(format_threshold_cents(1), "1¢");
    }

    #[test]
    fn test_threshold_profit_percent() {
        // 100 cents = break-even = 0% profit
        assert!((threshold_profit_percent(100) - 0.0).abs() < 0.01);

        // 99 cents = 1% profit
        assert!((threshold_profit_percent(99) - 1.0).abs() < 0.01);

        // 95 cents = 5% profit
        assert!((threshold_profit_percent(95) - 5.0).abs() < 0.01);

        // 90 cents = 10% profit
        assert!((threshold_profit_percent(90) - 10.0).abs() < 0.01);

        // 50 cents = 50% profit
        assert!((threshold_profit_percent(50) - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_profit_threshold_cents_default() {
        // This test cannot reliably set env vars due to caching,
        // but we can verify the default constant
        assert_eq!(DEFAULT_PROFIT_THRESHOLD_CENTS, 100);
    }
}
