//! Prediction Market Arbitrage Trading System
//!
//! A high-performance, production-ready arbitrage trading system for cross-platform
//! prediction markets with real-time price monitoring and execution.

pub mod cache;
pub mod circuit_breaker;
pub mod config;
pub mod cost;
pub mod discovery;
pub mod execution;
#[cfg(test)]
mod execution_tests;
pub mod fees;
pub mod kalshi;
pub mod logging;
pub mod mismatch;
pub mod pnl;
pub mod polymarket;
pub mod polymarket_clob;
pub mod position_tracker;
pub mod prefetch;
pub mod retry;
pub mod risk;
pub mod types;
