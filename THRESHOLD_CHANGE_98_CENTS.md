# Arbitrage Threshold Change: 100¢ → 98¢ (Go-Live Setting)

## Summary

Changed the default arbitrage opportunity threshold from **100¢ (break-even)** to **98¢ (2% minimum profit)** across the entire system. This affects both detection (hot path scanner) and execution gate.

## What Changed

### Default Threshold: 100¢ → 98¢

**Before:**
- Default: 100 cents = break-even ($1.00 total cost)
- Any profit > 0 was executable

**After:**
- Default: 98 cents = 2% minimum profit ($0.98 total cost)
- Only arbs with ≥2% profit margin are executable

### Files Modified

1. **[src/config.rs](src/config.rs)** (3 changes)
   - Updated `DEFAULT_PROFIT_THRESHOLD_CENTS` constant: `100` → `98`
   - Updated documentation to reflect 98¢ as the default
   - Added new test: `test_default_threshold_is_98_cents()`

## Configuration Behavior (Unchanged)

The threshold resolution priority remains the same:

1. **EXECUTION_THRESHOLD_CENTS** env var (if set) - overrides everything
2. **PROFIT_THRESHOLD_CENTS** env var (if set) - preferred integer format
3. **ARB_THRESHOLD** env var (if set) - legacy float format (e.g., 0.98)
4. **DEFAULT** - Now 98 cents (was 100)

### Examples

```bash
# Use default (98¢ = 2% minimum profit)
./prediction-market-arbitrage

# Override to 95¢ (5% minimum profit) - more conservative
PROFIT_THRESHOLD_CENTS=95 ./prediction-market-arbitrage

# Override to 99¢ (1% minimum profit) - more aggressive
PROFIT_THRESHOLD_CENTS=99 ./prediction-market-arbitrage

# Legacy format still works (0.98 = 98¢)
ARB_THRESHOLD=0.98 ./prediction-market-arbitrage
```

## System-Wide Impact

### ✅ Detection (Hot Path)
- `check_arbs(threshold_cents)` uses the threshold passed from main
- Main initializes `threshold_cents = profit_threshold_cents()` (now returns 98)
- Both Kalshi and Polymarket scanners receive the same threshold
- **Location**: [src/types.rs:228](src/types.rs#L228) `check_arbs()` function

### ✅ Execution Gate
- Uses `execution_threshold_cents()` which falls back to `profit_threshold_cents()`
- Unified cost model compares all-in cost against threshold
- **Location**: [src/execution.rs:217](src/execution.rs#L217) execution gate logic

### ✅ Logging
- Startup log shows: `threshold_cents: 98`
- Human-readable: `Profit threshold: <98¢ (2.0% minimum profit)`
- Heartbeat includes threshold value
- **Location**: [src/main.rs:107-115](src/main.rs#L107-L115)

## Testing

### New Test
```rust
#[test]
fn test_default_threshold_is_98_cents() {
    // Verify go-live default: 98 cents = 2% minimum profit margin
    assert_eq!(DEFAULT_PROFIT_THRESHOLD_CENTS, 98, 
               "Default threshold must be 98¢ for go-live");
    
    // Verify conversion to profit percentage
    let profit_pct = threshold_profit_percent(98);
    assert!((profit_pct - 2.0).abs() < 0.01, 
            "98¢ threshold should be ~2% profit");
}
```

### Test Results
```bash
$ cargo test --lib -- threshold
running 6 tests
test config::tests::test_default_threshold_is_98_cents ... ok
test config::tests::test_format_threshold_cents ... ok
test config::tests::test_profit_threshold_cents_default ... ok
test config::tests::test_threshold_profit_percent ... ok
test cost::tests::test_analyze_all_in_fails_threshold ... ok
test risk::tests::test_higher_min_qty_threshold ... ok

test result: ok. 6 passed; 0 failed
```

**Full test suite**: ✅ All 179 tests passing

## Verification

### Startup Log Output
```
INFO  🚀 Prediction Market Arbitrage System v2.0
    in arb_bot with threshold_cents: 98
    
INFO     Profit threshold: <98¢ (2.0% minimum profit)
```

### Code Flow

1. **Main Initialization** ([src/main.rs:83](src/main.rs#L83))
   ```rust
   let threshold_cents = profit_threshold_cents(); // Returns 98
   ```

2. **Detection Scanner** ([src/polymarket.rs:358](src/polymarket.rs#L358), [src/kalshi.rs:555](src/kalshi.rs#L555))
   ```rust
   let arb_mask = market.check_arbs(threshold_cents); // Uses 98
   ```

3. **Execution Gate** ([src/execution.rs:217](src/execution.rs#L217))
   ```rust
   let exec_threshold = execution_threshold_cents(); // Falls back to 98
   let passes_threshold = cost_per_contract <= exec_threshold as u32;
   ```

## Rationale

### Why 98¢ (2% minimum)?

1. **Risk Buffer**: Provides cushion against:
   - Slippage (price movement between detection and execution)
   - Fee estimation errors (cached vs actual fees)
   - Latency-induced price changes

2. **Go-Live Safety**: Conservative threshold reduces execution risk while system proves itself in production

3. **Still Competitive**: 2% profit margin is attractive while maintaining safety

4. **Easy Override**: Can adjust via env var without code changes:
   ```bash
   PROFIT_THRESHOLD_CENTS=99  # More aggressive (1% profit)
   PROFIT_THRESHOLD_CENTS=95  # More conservative (5% profit)
   ```

## Backward Compatibility

✅ **Fully backward compatible**
- All existing env var overrides still work (PROFIT_THRESHOLD_CENTS, ARB_THRESHOLD, EXECUTION_THRESHOLD_CENTS)
- Legacy float format (ARB_THRESHOLD=0.98) still supported
- Existing tests unchanged (they explicitly pass threshold values)
- Only the default value changed when no env vars are set

## Related Documentation

- [FEE_MODEL_UNIFICATION.md](FEE_MODEL_UNIFICATION.md) - Fee calculation architecture
- [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md) - System overview
- [src/config.rs](src/config.rs) - Configuration module with threshold functions

## Validation Checklist

- ✅ Default constant changed to 98
- ✅ Documentation updated
- ✅ New test added and passing
- ✅ All existing tests pass (179/179)
- ✅ Detection uses threshold correctly
- ✅ Execution gate uses threshold correctly
- ✅ Logs display "98¢" correctly
- ✅ Env var overrides still work
- ✅ Code formatted (cargo fmt)
- ✅ Binary builds successfully
- ✅ Startup logs verified

## Command Reference

```bash
# Run threshold tests
cargo test --lib -- threshold

# Run all tests
cargo test --lib

# Build and verify logs
cargo build --release
./target/release/prediction-market-arbitrage  # Check logs show 98¢

# Override threshold for testing
PROFIT_THRESHOLD_CENTS=99 ./target/release/prediction-market-arbitrage
```

---

**Change Date**: January 3, 2026  
**Version**: 2.0  
**Status**: ✅ Complete and Validated
