//! P&L (Profit & Loss) tracking system with event ledger and persistence.
//!
//! This module provides:
//! - Event-sourced P&L tracking (append-only JSONL ledger)
//! - Per-venue, per-market, and total P&L aggregation
//! - Weighted-average cost basis for positions
//! - Periodic snapshots for fast recovery on restart
//!
//! All monetary values use integer cents (i64) for consistency with the codebase.
//! No floating-point math for money calculations.

#![allow(dead_code)] // Methods for future use (settlements, marks, etc.)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Lightweight P&L summary for logging (no full snapshot allocation)
#[derive(Debug, Clone, Copy)]
pub struct PnlSummary {
    pub open_positions: usize,
    pub realized_cents: i64,
    pub unrealized_cents: i64,
    pub fees_cents: i64,
}
use tracing::{info, warn};

// =============================================================================
// ENUMS
// =============================================================================

/// Trading venue identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Venue {
    Kalshi,
    Polymarket,
}

impl std::fmt::Display for Venue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Venue::Kalshi => write!(f, "kalshi"),
            Venue::Polymarket => write!(f, "polymarket"),
        }
    }
}

impl Venue {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "kalshi" => Some(Venue::Kalshi),
            "polymarket" | "poly" => Some(Venue::Polymarket),
            _ => None,
        }
    }
}

/// Order/position side
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Side::Buy => write!(f, "buy"),
            Side::Sell => write!(f, "sell"),
        }
    }
}

/// Contract side (YES or NO)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContractSide {
    Yes,
    No,
}

impl std::fmt::Display for ContractSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContractSide::Yes => write!(f, "yes"),
            ContractSide::No => write!(f, "no"),
        }
    }
}

impl ContractSide {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "yes" => Some(ContractSide::Yes),
            "no" => Some(ContractSide::No),
            _ => None,
        }
    }
}

// =============================================================================
// P&L EVENTS (append-only ledger)
// =============================================================================

/// A P&L event that is written to the ledger.
/// All monetary values in integer cents.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PnLEvent {
    /// Order submitted (not yet filled)
    OrderSubmitted {
        ts_ms: u64,
        venue: Venue,
        market_key: String,
        order_id: String,
        side: Side,
        contract_side: ContractSide,
        qty: i64,
        limit_price_cents: i64,
    },

    /// Order filled (partial or full)
    OrderFill {
        ts_ms: u64,
        venue: Venue,
        market_key: String,
        order_id: String,
        side: Side,
        contract_side: ContractSide,
        qty: i64,
        price_cents: i64,
        fee_cents: i64,
    },

    /// Order cancelled (unfilled portion)
    OrderCancelled {
        ts_ms: u64,
        venue: Venue,
        market_key: String,
        order_id: String,
    },

    /// Mark-to-market update
    Mark {
        ts_ms: u64,
        venue: Venue,
        market_key: String,
        contract_side: ContractSide,
        mid_price_cents: i64,
    },

    /// Market settlement
    Settlement {
        ts_ms: u64,
        venue: Venue,
        market_key: String,
        outcome: ContractSide,
        payout_cents_per_contract: i64,
    },
}

impl PnLEvent {
    pub fn ts_ms(&self) -> u64 {
        match self {
            PnLEvent::OrderSubmitted { ts_ms, .. } => *ts_ms,
            PnLEvent::OrderFill { ts_ms, .. } => *ts_ms,
            PnLEvent::OrderCancelled { ts_ms, .. } => *ts_ms,
            PnLEvent::Mark { ts_ms, .. } => *ts_ms,
            PnLEvent::Settlement { ts_ms, .. } => *ts_ms,
        }
    }

    pub fn venue(&self) -> Venue {
        match self {
            PnLEvent::OrderSubmitted { venue, .. } => *venue,
            PnLEvent::OrderFill { venue, .. } => *venue,
            PnLEvent::OrderCancelled { venue, .. } => *venue,
            PnLEvent::Mark { venue, .. } => *venue,
            PnLEvent::Settlement { venue, .. } => *venue,
        }
    }

    pub fn market_key(&self) -> &str {
        match self {
            PnLEvent::OrderSubmitted { market_key, .. } => market_key,
            PnLEvent::OrderFill { market_key, .. } => market_key,
            PnLEvent::OrderCancelled { market_key, .. } => market_key,
            PnLEvent::Mark { market_key, .. } => market_key,
            PnLEvent::Settlement { market_key, .. } => market_key,
        }
    }
}

// =============================================================================
// POSITION TRACKING (weighted-average cost)
// =============================================================================

/// Position key: (Venue, MarketKey, ContractSide)
pub type PositionKey = (Venue, String, ContractSide);

/// A position using weighted-average cost basis.
///
/// All values in integer cents except qty (contracts).
/// Positive qty = long, negative qty = short.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Position {
    /// Number of contracts (positive = long, negative = short)
    pub qty: i64,

    /// Weighted-average cost per contract in cents
    /// For buys: total_cost / qty
    /// Updated on each fill using weighted average formula
    pub avg_cost_cents: i64,

    /// Cumulative realized P&L in cents
    pub realized_pnl_cents: i64,

    /// Cumulative fees paid in cents
    pub fees_paid_cents: i64,

    /// Last mark price (mid) for unrealized P&L calculation
    pub last_mark_cents: Option<i64>,
}

impl Position {
    /// Apply a fill to this position.
    ///
    /// For buys: increases qty, updates avg cost
    /// For sells: decreases qty, realizes P&L
    ///
    /// Returns the realized P&L from this fill (0 if buy, actual if sell).
    pub fn apply_fill(&mut self, side: Side, qty: i64, price_cents: i64, fee_cents: i64) -> i64 {
        self.fees_paid_cents += fee_cents;
        let qty = qty.abs(); // Ensure positive

        match side {
            Side::Buy => {
                // Buying increases position
                // New avg = (old_qty * old_avg + new_qty * new_price) / (old_qty + new_qty)
                if self.qty >= 0 {
                    // Adding to long position
                    let old_total = self.qty * self.avg_cost_cents;
                    let new_total = qty * price_cents;
                    let combined_qty = self.qty + qty;
                    if combined_qty > 0 {
                        self.avg_cost_cents = (old_total + new_total) / combined_qty;
                    }
                    self.qty = combined_qty;
                    0 // No realized P&L on buy
                } else {
                    // Closing short position (buying to cover)
                    let close_qty = qty.min((-self.qty) as i64);
                    let realized = close_qty * (self.avg_cost_cents - price_cents);
                    self.realized_pnl_cents += realized;
                    self.qty += qty;
                    if self.qty > 0 {
                        // Flipped to long
                        self.avg_cost_cents = price_cents;
                    }
                    realized
                }
            }
            Side::Sell => {
                // Selling decreases position
                if self.qty > 0 {
                    // Closing long position
                    let close_qty = qty.min(self.qty);
                    let realized = close_qty * (price_cents - self.avg_cost_cents);
                    self.realized_pnl_cents += realized;
                    self.qty -= qty;
                    if self.qty < 0 {
                        // Flipped to short
                        self.avg_cost_cents = price_cents;
                    }
                    realized
                } else {
                    // Adding to short position
                    let old_total = (-self.qty) * self.avg_cost_cents;
                    let new_total = qty * price_cents;
                    let combined_qty = (-self.qty) + qty;
                    if combined_qty > 0 {
                        self.avg_cost_cents = (old_total + new_total) / combined_qty;
                    }
                    self.qty -= qty;
                    0 // No realized P&L on short entry
                }
            }
        }
    }

    /// Calculate unrealized P&L based on last mark
    pub fn unrealized_pnl_cents(&self) -> i64 {
        match self.last_mark_cents {
            Some(mark) => {
                if self.qty > 0 {
                    // Long: profit if mark > avg_cost
                    self.qty * (mark - self.avg_cost_cents)
                } else if self.qty < 0 {
                    // Short: profit if mark < avg_cost
                    (-self.qty) * (self.avg_cost_cents - mark)
                } else {
                    0
                }
            }
            None => 0,
        }
    }

    /// Update mark price
    pub fn update_mark(&mut self, mark_cents: i64) {
        self.last_mark_cents = Some(mark_cents);
    }

    /// Apply settlement payout.
    /// payout_cents_per_contract is typically 100 (winning side) or 0 (losing side).
    pub fn apply_settlement(&mut self, payout_cents_per_contract: i64) -> i64 {
        if self.qty == 0 {
            return 0;
        }

        let payout = self.qty.abs() * payout_cents_per_contract;
        let cost = self.qty.abs() * self.avg_cost_cents;
        let realized = if self.qty > 0 {
            payout - cost
        } else {
            cost - payout // Short position
        };

        self.realized_pnl_cents += realized;
        self.qty = 0;
        self.avg_cost_cents = 0;
        self.last_mark_cents = None;
        realized
    }
}

// =============================================================================
// P&L SNAPSHOT
// =============================================================================

/// Per-venue P&L totals
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VenuePnL {
    pub venue: String,
    pub realized_pnl_cents: i64,
    pub unrealized_pnl_cents: i64,
    pub fees_paid_cents: i64,
    pub net_pnl_cents: i64,
    pub open_positions: usize,
}

/// Per-market P&L totals
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MarketPnL {
    pub market_key: String,
    pub venue: String,
    pub realized_pnl_cents: i64,
    pub unrealized_pnl_cents: i64,
    pub fees_paid_cents: i64,
    pub net_pnl_cents: i64,
    pub qty_yes: i64,
    pub qty_no: i64,
}

/// Complete P&L snapshot for persistence
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PnLSnapshot {
    /// Snapshot timestamp (ms since epoch)
    pub ts_ms: u64,

    /// Total realized P&L (all venues, all markets)
    pub total_realized_pnl_cents: i64,

    /// Total unrealized P&L
    pub total_unrealized_pnl_cents: i64,

    /// Total fees paid
    pub total_fees_paid_cents: i64,

    /// Net P&L = realized + unrealized - fees (already deducted in realized)
    pub total_net_pnl_cents: i64,

    /// Per-venue breakdown
    pub by_venue: Vec<VenuePnL>,

    /// Per-market breakdown
    pub by_market: Vec<MarketPnL>,

    /// All positions (for recovery)
    pub positions: HashMap<String, Position>,

    /// Last event timestamp processed
    pub last_event_ts_ms: u64,
}

// =============================================================================
// P&L TRACKER
// =============================================================================

/// Main P&L tracking state
pub struct PnLTracker {
    /// All positions keyed by "venue:market_key:contract_side"
    positions: HashMap<String, Position>,

    /// Event ledger file (JSONL)
    events_path: PathBuf,

    /// Snapshot file
    snapshot_path: PathBuf,

    /// Event file handle (kept open for appending)
    events_file: Option<File>,

    /// Last snapshot timestamp
    last_snapshot_ts_ms: AtomicU64,

    /// Events since last snapshot
    events_since_snapshot: AtomicU64,

    /// DRY_RUN mode flag
    dry_run: bool,
}

impl PnLTracker {
    /// Create a new tracker with the given data directory.
    ///
    /// If `data_dir` doesn't exist, it will be created.
    /// If snapshot exists, it will be loaded.
    pub fn new(data_dir: &str, dry_run: bool) -> Self {
        let data_path = PathBuf::from(data_dir);

        // Create directory if needed
        if !data_path.exists() {
            if let Err(e) = fs::create_dir_all(&data_path) {
                warn!("[PNL] Failed to create data directory: {}", e);
            }
        }

        let events_path = data_path.join("pnl_events.jsonl");
        let snapshot_path = data_path.join("pnl_snapshot.json");

        let mut tracker = Self {
            positions: HashMap::new(),
            events_path,
            snapshot_path,
            events_file: None,
            last_snapshot_ts_ms: AtomicU64::new(0),
            events_since_snapshot: AtomicU64::new(0),
            dry_run,
        };

        // Load existing snapshot if present
        tracker.load_snapshot();

        // Open events file for appending
        tracker.open_events_file();

        tracker
    }

    /// Generate position key from components
    pub fn position_key(venue: Venue, market_key: &str, contract_side: ContractSide) -> String {
        format!("{}:{}:{}", venue, market_key, contract_side)
    }

    /// Get current timestamp in milliseconds
    pub fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    // =========================================================================
    // EVENT HANDLERS
    // =========================================================================

    /// Record an order submission
    pub fn on_order_submitted(
        &mut self,
        venue: Venue,
        market_key: &str,
        order_id: &str,
        side: Side,
        contract_side: ContractSide,
        qty: i64,
        limit_price_cents: i64,
    ) {
        let event = PnLEvent::OrderSubmitted {
            ts_ms: Self::now_ms(),
            venue,
            market_key: market_key.to_string(),
            order_id: order_id.to_string(),
            side,
            contract_side,
            qty,
            limit_price_cents,
        };
        self.write_event(&event);
    }

    /// Record a fill and update position
    pub fn on_fill(
        &mut self,
        venue: Venue,
        market_key: &str,
        order_id: &str,
        side: Side,
        contract_side: ContractSide,
        qty: i64,
        price_cents: i64,
        fee_cents: i64,
    ) -> i64 {
        let event = PnLEvent::OrderFill {
            ts_ms: Self::now_ms(),
            venue,
            market_key: market_key.to_string(),
            order_id: order_id.to_string(),
            side,
            contract_side,
            qty,
            price_cents,
            fee_cents,
        };
        self.write_event(&event);

        // Update position
        let key = Self::position_key(venue, market_key, contract_side);
        let position = self.positions.entry(key).or_default();
        position.apply_fill(side, qty, price_cents, fee_cents)
    }

    /// Record order cancellation
    pub fn on_order_cancelled(&mut self, venue: Venue, market_key: &str, order_id: &str) {
        let event = PnLEvent::OrderCancelled {
            ts_ms: Self::now_ms(),
            venue,
            market_key: market_key.to_string(),
            order_id: order_id.to_string(),
        };
        self.write_event(&event);
    }

    /// Update mark price for a position
    pub fn on_mark(
        &mut self,
        venue: Venue,
        market_key: &str,
        contract_side: ContractSide,
        mid_price_cents: i64,
    ) {
        let event = PnLEvent::Mark {
            ts_ms: Self::now_ms(),
            venue,
            market_key: market_key.to_string(),
            contract_side,
            mid_price_cents,
        };
        self.write_event(&event);

        let key = Self::position_key(venue, market_key, contract_side);
        if let Some(position) = self.positions.get_mut(&key) {
            position.update_mark(mid_price_cents);
        }
    }

    /// Record settlement
    pub fn on_settlement(
        &mut self,
        venue: Venue,
        market_key: &str,
        outcome: ContractSide,
        payout_cents_per_contract: i64,
    ) -> i64 {
        let event = PnLEvent::Settlement {
            ts_ms: Self::now_ms(),
            venue,
            market_key: market_key.to_string(),
            outcome,
            payout_cents_per_contract,
        };
        self.write_event(&event);

        // Settle both YES and NO positions for this market
        let mut total_realized = 0i64;

        // Winning side gets payout
        let yes_key = Self::position_key(venue, market_key, ContractSide::Yes);
        let no_key = Self::position_key(venue, market_key, ContractSide::No);

        let yes_payout = if outcome == ContractSide::Yes {
            payout_cents_per_contract
        } else {
            0
        };
        let no_payout = if outcome == ContractSide::No {
            payout_cents_per_contract
        } else {
            0
        };

        if let Some(pos) = self.positions.get_mut(&yes_key) {
            total_realized += pos.apply_settlement(yes_payout);
        }
        if let Some(pos) = self.positions.get_mut(&no_key) {
            total_realized += pos.apply_settlement(no_payout);
        }

        total_realized
    }

    // =========================================================================
    // SNAPSHOTS
    // =========================================================================

    /// Generate current P&L snapshot
    pub fn snapshot(&self) -> PnLSnapshot {
        let ts_ms = Self::now_ms();

        let mut total_realized = 0i64;
        let mut total_unrealized = 0i64;
        let mut total_fees = 0i64;

        let mut venue_stats: HashMap<Venue, (i64, i64, i64, usize)> = HashMap::new();
        let mut market_stats: HashMap<(Venue, String), MarketPnL> = HashMap::new();

        for (key, pos) in &self.positions {
            // Parse key: "venue:market_key:contract_side"
            let parts: Vec<&str> = key.splitn(3, ':').collect();
            if parts.len() < 3 {
                continue;
            }

            let venue = match Venue::from_str(parts[0]) {
                Some(v) => v,
                None => continue,
            };

            // market_key might contain colons, so rejoin remaining parts
            let rest = &key[parts[0].len() + 1..];
            let contract_side_str = if rest.ends_with(":yes") {
                "yes"
            } else if rest.ends_with(":no") {
                "no"
            } else {
                continue;
            };
            let market_key = &rest[..rest.len() - contract_side_str.len() - 1];

            let realized = pos.realized_pnl_cents;
            let unrealized = pos.unrealized_pnl_cents();
            let fees = pos.fees_paid_cents;

            total_realized += realized;
            total_unrealized += unrealized;
            total_fees += fees;

            // Venue stats
            let venue_entry = venue_stats.entry(venue).or_insert((0, 0, 0, 0));
            venue_entry.0 += realized;
            venue_entry.1 += unrealized;
            venue_entry.2 += fees;
            if pos.qty != 0 {
                venue_entry.3 += 1;
            }

            // Market stats
            let market_entry = market_stats
                .entry((venue, market_key.to_string()))
                .or_insert_with(|| MarketPnL {
                    market_key: market_key.to_string(),
                    venue: venue.to_string(),
                    ..Default::default()
                });
            market_entry.realized_pnl_cents += realized;
            market_entry.unrealized_pnl_cents += unrealized;
            market_entry.fees_paid_cents += fees;

            if contract_side_str == "yes" {
                market_entry.qty_yes = pos.qty;
            } else {
                market_entry.qty_no = pos.qty;
            }
        }

        // Build venue breakdown
        let by_venue: Vec<VenuePnL> = venue_stats
            .into_iter()
            .map(|(venue, (realized, unrealized, fees, open))| VenuePnL {
                venue: venue.to_string(),
                realized_pnl_cents: realized,
                unrealized_pnl_cents: unrealized,
                fees_paid_cents: fees,
                net_pnl_cents: realized + unrealized, // fees already in realized
                open_positions: open,
            })
            .collect();

        // Build market breakdown
        let by_market: Vec<MarketPnL> = market_stats
            .into_values()
            .map(|mut m| {
                m.net_pnl_cents = m.realized_pnl_cents + m.unrealized_pnl_cents;
                m
            })
            .collect();

        // Serialize positions for recovery
        let positions_map: HashMap<String, Position> = self.positions.clone();

        PnLSnapshot {
            ts_ms,
            total_realized_pnl_cents: total_realized,
            total_unrealized_pnl_cents: total_unrealized,
            total_fees_paid_cents: total_fees,
            total_net_pnl_cents: total_realized + total_unrealized,
            by_venue,
            by_market,
            positions: positions_map,
            last_event_ts_ms: self.last_snapshot_ts_ms.load(Ordering::Relaxed),
        }
    }

    /// Save snapshot to disk (atomic write via temp file + rename)
    pub fn save_snapshot(&self) -> bool {
        let snapshot = self.snapshot();

        let tmp_path = self.snapshot_path.with_extension("json.tmp");

        match serde_json::to_string_pretty(&snapshot) {
            Ok(json) => {
                if let Err(e) = fs::write(&tmp_path, &json) {
                    warn!("[PNL] Failed to write snapshot tmp: {}", e);
                    return false;
                }
                if let Err(e) = fs::rename(&tmp_path, &self.snapshot_path) {
                    warn!("[PNL] Failed to rename snapshot: {}", e);
                    return false;
                }
                self.last_snapshot_ts_ms
                    .store(snapshot.ts_ms, Ordering::Relaxed);
                self.events_since_snapshot.store(0, Ordering::Relaxed);
                true
            }
            Err(e) => {
                warn!("[PNL] Failed to serialize snapshot: {}", e);
                false
            }
        }
    }

    /// Load snapshot from disk
    fn load_snapshot(&mut self) {
        if !self.snapshot_path.exists() {
            info!("[PNL] No snapshot file found, starting fresh");
            return;
        }

        match fs::read_to_string(&self.snapshot_path) {
            Ok(json) => match serde_json::from_str::<PnLSnapshot>(&json) {
                Ok(snapshot) => {
                    self.positions = snapshot.positions;
                    self.last_snapshot_ts_ms
                        .store(snapshot.ts_ms, Ordering::Relaxed);
                    info!(
                        "[PNL] Loaded snapshot: {} positions, realized={}¢, unrealized={}¢, net={}¢",
                        self.positions.len(),
                        snapshot.total_realized_pnl_cents,
                        snapshot.total_unrealized_pnl_cents,
                        snapshot.total_net_pnl_cents
                    );
                }
                Err(e) => {
                    warn!("[PNL] Failed to parse snapshot: {}", e);
                }
            },
            Err(e) => {
                warn!("[PNL] Failed to read snapshot: {}", e);
            }
        }
    }

    // =========================================================================
    // EVENT PERSISTENCE
    // =========================================================================

    /// Open events file for appending
    fn open_events_file(&mut self) {
        match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.events_path)
        {
            Ok(file) => {
                self.events_file = Some(file);
            }
            Err(e) => {
                warn!("[PNL] Failed to open events file: {}", e);
            }
        }
    }

    /// Write event to ledger
    fn write_event(&mut self, event: &PnLEvent) {
        self.events_since_snapshot.fetch_add(1, Ordering::Relaxed);

        if let Some(ref mut file) = self.events_file {
            match serde_json::to_string(event) {
                Ok(json) => {
                    if let Err(e) = writeln!(file, "{}", json) {
                        warn!("[PNL] Failed to write event: {}", e);
                    }
                }
                Err(e) => {
                    warn!("[PNL] Failed to serialize event: {}", e);
                }
            }
        }
    }

    /// Replay events from JSONL file (for recovery without snapshot)
    #[allow(dead_code)]
    pub fn replay_events(&mut self) -> usize {
        if !self.events_path.exists() {
            return 0;
        }

        let file = match File::open(&self.events_path) {
            Ok(f) => f,
            Err(e) => {
                warn!("[PNL] Failed to open events for replay: {}", e);
                return 0;
            }
        };

        let reader = BufReader::new(file);
        let mut count = 0;

        for line in reader.lines() {
            let line = match line {
                Ok(l) => l,
                Err(_) => continue,
            };

            let event: PnLEvent = match serde_json::from_str(&line) {
                Ok(e) => e,
                Err(_) => continue,
            };

            // Apply event to positions
            match event {
                PnLEvent::OrderFill {
                    venue,
                    market_key,
                    side,
                    contract_side,
                    qty,
                    price_cents,
                    fee_cents,
                    ..
                } => {
                    let key = Self::position_key(venue, &market_key, contract_side);
                    let position = self.positions.entry(key).or_default();
                    position.apply_fill(side, qty, price_cents, fee_cents);
                }
                PnLEvent::Mark {
                    venue,
                    market_key,
                    contract_side,
                    mid_price_cents,
                    ..
                } => {
                    let key = Self::position_key(venue, &market_key, contract_side);
                    if let Some(position) = self.positions.get_mut(&key) {
                        position.update_mark(mid_price_cents);
                    }
                }
                PnLEvent::Settlement {
                    venue,
                    market_key,
                    outcome,
                    payout_cents_per_contract,
                    ..
                } => {
                    let yes_key = Self::position_key(venue, &market_key, ContractSide::Yes);
                    let no_key = Self::position_key(venue, &market_key, ContractSide::No);

                    let yes_payout = if outcome == ContractSide::Yes {
                        payout_cents_per_contract
                    } else {
                        0
                    };
                    let no_payout = if outcome == ContractSide::No {
                        payout_cents_per_contract
                    } else {
                        0
                    };

                    if let Some(pos) = self.positions.get_mut(&yes_key) {
                        pos.apply_settlement(yes_payout);
                    }
                    if let Some(pos) = self.positions.get_mut(&no_key) {
                        pos.apply_settlement(no_payout);
                    }
                }
                _ => {} // OrderSubmitted and OrderCancelled don't affect positions
            }

            count += 1;
        }

        info!("[PNL] Replayed {} events", count);
        count
    }

    // =========================================================================
    // GETTERS
    // =========================================================================

    /// Get events since last snapshot
    pub fn events_since_snapshot(&self) -> u64 {
        self.events_since_snapshot.load(Ordering::Relaxed)
    }

    /// Check if snapshot should be saved (based on time or event count)
    pub fn should_save_snapshot(&self, flush_secs: u64) -> bool {
        let last_ts = self.last_snapshot_ts_ms.load(Ordering::Relaxed);
        let now = Self::now_ms();
        let elapsed_secs = (now - last_ts) / 1000;

        elapsed_secs >= flush_secs || self.events_since_snapshot() > 100
    }

    /// Get position by key
    pub fn get_position(
        &self,
        venue: Venue,
        market_key: &str,
        contract_side: ContractSide,
    ) -> Option<&Position> {
        let key = Self::position_key(venue, market_key, contract_side);
        self.positions.get(&key)
    }

    /// Get all positions
    pub fn positions(&self) -> &HashMap<String, Position> {
        &self.positions
    }

    /// Is this tracker in dry run mode?
    pub fn is_dry_run(&self) -> bool {
        self.dry_run
    }

    /// Format snapshot summary for heartbeat log
    /// Get a lightweight P&L summary for logging (no allocation of full snapshot)
    pub fn summary(&self) -> PnlSummary {
        let mut realized_cents = 0i64;
        let mut unrealized_cents = 0i64;
        let mut fees_cents = 0i64;
        let mut open_positions = 0usize;

        for pos in self.positions.values() {
            realized_cents += pos.realized_pnl_cents;
            unrealized_cents += pos.unrealized_pnl_cents();
            fees_cents += pos.fees_paid_cents;
            if pos.qty != 0 {
                open_positions += 1;
            }
        }

        PnlSummary {
            open_positions,
            realized_cents,
            unrealized_cents,
            fees_cents,
        }
    }

    /// Format snapshot summary for heartbeat log
    pub fn format_summary(&self) -> String {
        let snap = self.snapshot();
        format!(
            "realized={}¢ unrealized={}¢ fees={}¢ net={}¢",
            snap.total_realized_pnl_cents,
            snap.total_unrealized_pnl_cents,
            snap.total_fees_paid_cents,
            snap.total_net_pnl_cents
        )
    }

    /// Format per-venue summary
    pub fn format_venue_summary(&self) -> String {
        let snap = self.snapshot();
        snap.by_venue
            .iter()
            .map(|v| format!("{}:{}¢", v.venue, v.net_pnl_cents))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_position_buy_then_sell_realized_pnl() {
        let mut pos = Position::default();

        // Buy 10 contracts at 40¢, fee 2¢
        let realized = pos.apply_fill(Side::Buy, 10, 40, 2);
        assert_eq!(realized, 0); // No realized P&L on buy
        assert_eq!(pos.qty, 10);
        assert_eq!(pos.avg_cost_cents, 40);
        assert_eq!(pos.fees_paid_cents, 2);

        // Sell 10 contracts at 60¢, fee 2¢
        let realized = pos.apply_fill(Side::Sell, 10, 60, 2);
        // Realized = 10 * (60 - 40) = 200¢
        assert_eq!(realized, 200);
        assert_eq!(pos.qty, 0);
        assert_eq!(pos.realized_pnl_cents, 200);
        assert_eq!(pos.fees_paid_cents, 4);
    }

    #[test]
    fn test_position_buy_then_mark_unrealized() {
        let mut pos = Position::default();

        // Buy 10 contracts at 40¢
        pos.apply_fill(Side::Buy, 10, 40, 0);
        assert_eq!(pos.qty, 10);
        assert_eq!(pos.avg_cost_cents, 40);

        // No mark yet
        assert_eq!(pos.unrealized_pnl_cents(), 0);

        // Mark at 55¢
        pos.update_mark(55);
        // Unrealized = 10 * (55 - 40) = 150¢
        assert_eq!(pos.unrealized_pnl_cents(), 150);

        // Mark at 35¢ (loss)
        pos.update_mark(35);
        // Unrealized = 10 * (35 - 40) = -50¢
        assert_eq!(pos.unrealized_pnl_cents(), -50);
    }

    #[test]
    fn test_partial_fills_weighted_avg() {
        let mut pos = Position::default();

        // Buy 10 contracts at 40¢
        pos.apply_fill(Side::Buy, 10, 40, 0);
        assert_eq!(pos.qty, 10);
        assert_eq!(pos.avg_cost_cents, 40);

        // Buy 10 more at 50¢
        pos.apply_fill(Side::Buy, 10, 50, 0);
        // New avg = (10*40 + 10*50) / 20 = 900/20 = 45¢
        assert_eq!(pos.qty, 20);
        assert_eq!(pos.avg_cost_cents, 45);

        // Buy 20 more at 60¢
        pos.apply_fill(Side::Buy, 20, 60, 0);
        // New avg = (20*45 + 20*60) / 40 = 2100/40 = 52.5 → 52¢ (integer division)
        assert_eq!(pos.qty, 40);
        assert_eq!(pos.avg_cost_cents, 52); // 2100/40 = 52
    }

    #[test]
    fn test_settlement() {
        let mut pos = Position::default();

        // Buy 10 YES contracts at 40¢
        pos.apply_fill(Side::Buy, 10, 40, 1);
        assert_eq!(pos.qty, 10);
        assert_eq!(pos.fees_paid_cents, 1);

        // Settlement: YES wins, payout 100¢/contract
        let realized = pos.apply_settlement(100);
        // Payout = 10 * 100 = 1000¢
        // Cost = 10 * 40 = 400¢
        // Realized = 1000 - 400 = 600¢
        assert_eq!(realized, 600);
        assert_eq!(pos.realized_pnl_cents, 600);
        assert_eq!(pos.qty, 0);
    }

    #[test]
    fn test_settlement_loser() {
        let mut pos = Position::default();

        // Buy 10 YES contracts at 40¢
        pos.apply_fill(Side::Buy, 10, 40, 0);

        // Settlement: NO wins (YES payout = 0)
        let realized = pos.apply_settlement(0);
        // Payout = 0
        // Cost = 400¢
        // Realized = 0 - 400 = -400¢
        assert_eq!(realized, -400);
        assert_eq!(pos.realized_pnl_cents, -400);
    }

    #[test]
    fn test_snapshot_persistence_roundtrip() {
        let dir = tempdir().unwrap();
        let dir_str = dir.path().to_str().unwrap();

        // Create tracker and add some fills
        let mut tracker = PnLTracker::new(dir_str, true);

        tracker.on_fill(
            Venue::Kalshi,
            "epl:test:moneyline",
            "order1",
            Side::Buy,
            ContractSide::Yes,
            10,
            40,
            2,
        );

        tracker.on_fill(
            Venue::Polymarket,
            "epl:test:moneyline",
            "order2",
            Side::Buy,
            ContractSide::No,
            10,
            55,
            0,
        );

        // Save snapshot
        assert!(tracker.save_snapshot());

        // Create new tracker (should load snapshot)
        let tracker2 = PnLTracker::new(dir_str, true);

        let snap = tracker2.snapshot();
        assert_eq!(snap.total_fees_paid_cents, 2);

        // Check positions recovered
        let kalshi_pos =
            tracker2.get_position(Venue::Kalshi, "epl:test:moneyline", ContractSide::Yes);
        assert!(kalshi_pos.is_some());
        assert_eq!(kalshi_pos.unwrap().qty, 10);
        assert_eq!(kalshi_pos.unwrap().avg_cost_cents, 40);
    }

    #[test]
    fn test_jsonl_event_format() {
        let event = PnLEvent::OrderFill {
            ts_ms: 1704153600000,
            venue: Venue::Kalshi,
            market_key: "epl:test:moneyline".to_string(),
            order_id: "order123".to_string(),
            side: Side::Buy,
            contract_side: ContractSide::Yes,
            qty: 10,
            price_cents: 45,
            fee_cents: 2,
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"type\":\"order_fill\""));
        assert!(json.contains("\"venue\":\"kalshi\""));
        assert!(json.contains("\"price_cents\":45"));
        assert!(json.contains("\"fee_cents\":2"));

        // Verify roundtrip
        let parsed: PnLEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.ts_ms(), 1704153600000);
    }

    #[test]
    fn test_venue_from_str() {
        assert_eq!(Venue::from_str("kalshi"), Some(Venue::Kalshi));
        assert_eq!(Venue::from_str("KALSHI"), Some(Venue::Kalshi));
        assert_eq!(Venue::from_str("polymarket"), Some(Venue::Polymarket));
        assert_eq!(Venue::from_str("poly"), Some(Venue::Polymarket));
        assert_eq!(Venue::from_str("invalid"), None);
    }

    #[test]
    fn test_position_key_format() {
        let key =
            PnLTracker::position_key(Venue::Kalshi, "epl:ars-mun:moneyline", ContractSide::Yes);
        assert_eq!(key, "kalshi:epl:ars-mun:moneyline:yes");
    }
}
