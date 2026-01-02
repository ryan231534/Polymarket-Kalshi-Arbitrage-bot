# Discovery Cache Config Scoping Fix

## Problem
The discovery cache was not scoped to the current `LEAGUES` and `DISCOVERY_HORIZON_DAYS` configuration, causing runs with different configs to incorrectly load cached pairs from other leagues/horizons.

**Example bug:** Running with `LEAGUES="epl"` would load cache containing NBA/NFL pairs from a previous run.

## Solution
Added configuration metadata to the cache file to ensure cache is only used when it matches the current discovery config.

### Changes Made

#### 1. Enhanced Cache Structure (Schema v2)
Added metadata fields to `DiscoveryCache` struct in [src/discovery.rs](src/discovery.rs):
- `schema_version: u32` - Set to 2 (v1 had no versioning)
- `leagues: Vec<String>` - Normalized (lowercase, sorted) list of leagues; empty = all
- `horizon_days: u32` - Discovery horizon in days

#### 2. Backward Compatible Cache Loading
- Attempts to load as v2 cache first
- If v2 load succeeds, validates config match (schema, leagues, horizon)
- Falls back to detecting v1 (legacy) format - treated as "config unknown" and rejected
- Logs detailed reason when cache is rejected (config mismatch, legacy format, etc.)

#### 3. Cache Validation
Cache is used ONLY if:
- File exists and is parseable
- `schema_version == 2`
- `leagues` matches current normalized leagues (order-insensitive)
- `horizon_days` matches current horizon
- Cache age < TTL (existing rule, 2 hours)

If any condition fails, cache is rejected and fresh discovery runs.

#### 4. Manual Cache Bypass
Added `DISCOVERY_CACHE_BYPASS` environment variable:
```bash
DISCOVERY_CACHE_BYPASS=1 cargo run --release
```
Skips cache load entirely, always runs fresh discovery.

#### 5. League Normalization
Helper function `normalize_leagues()` ensures consistent comparison:
- Converts to lowercase
- Sorts alphabetically
- Removes duplicates
- Empty list = "all leagues"

#### 6. Atomic Cache Writes
Updated `save_cache()` to use atomic write pattern:
- Write to `.discovery_cache.json.tmp`
- Atomic rename to `.discovery_cache.json`
- Prevents corruption from interrupted writes

#### 7. Comprehensive Tests
Added unit tests in [src/discovery.rs](src/discovery.rs):
- `test_normalize_leagues()` - League list normalization
- `test_cache_matches_config()` - Config matching logic
- `test_discovery_cache_serialization()` - v2 serialization
- `test_legacy_cache_detection()` - v1 format detection

All tests pass: `cargo test --lib discovery`

### Usage Examples

#### Standard operation (cache enabled):
```bash
# First run - creates cache
LEAGUES="epl,nba" DISCOVERY_HORIZON_DAYS=3 cargo run --release

# Second run - uses cache (same config)
LEAGUES="epl,nba" DISCOVERY_HORIZON_DAYS=3 cargo run --release
# Output: "📂 Loaded X pairs from cache (age: Ys)"

# Different league - rejects cache, runs fresh discovery
LEAGUES="nfl" DISCOVERY_HORIZON_DAYS=3 cargo run --release
# Output: "⚠️ Cache config mismatch (will run fresh discovery)"
```

#### Cache bypass:
```bash
DISCOVERY_CACHE_BYPASS=1 cargo run --release
# Output: "🔄 DISCOVERY_CACHE_BYPASS=1, skipping cache load"
```

### Cache File Format

**Schema v2 (current):**
```json
{
  "schema_version": 2,
  "timestamp_secs": 1704240000,
  "pairs": [...],
  "known_kalshi_tickers": [...],
  "leagues": ["epl", "nba"],
  "horizon_days": 3
}
```

**Schema v1 (legacy, rejected):**
```json
{
  "timestamp_secs": 1704240000,
  "pairs": [...],
  "known_kalshi_tickers": [...]
}
```

### Logging

The implementation provides clear logging:

**Cache match:**
```
📂 Cache config match: schema_version=2, leagues=["epl", "nba"], horizon_days=3
📂 Loaded 42 pairs from cache (age: 1200s)
```

**Config mismatch:**
```
⚠️  Cache config mismatch (will run fresh discovery):
   Cache: schema_version=2, leagues=["epl"], horizon_days=3
   Current: schema_version=2, leagues=["nba"], horizon_days=3
📂 No valid cache, doing full discovery...
```

**Legacy cache:**
```
⚠️  Legacy cache (v1) detected - config unknown, running fresh discovery
```

**Cache bypass:**
```
🔄 DISCOVERY_CACHE_BYPASS=1, skipping cache load
```

### Quality Gates Passed
✅ `cargo fmt` - Code formatted  
✅ `cargo test --lib` - All 50 tests pass (including 7 new cache tests)  
✅ `cargo build --release` - Clean release build  
✅ Backward compatible - Detects and handles legacy v1 caches safely  

### Migration Path
No manual migration needed:
- Existing v1 caches are detected and rejected (with informative log)
- First run with new code creates v2 cache
- Subsequent runs use v2 cache with config validation

### Files Modified
- [src/discovery.rs](src/discovery.rs) - All cache logic and tests

### Related Environment Variables
- `LEAGUES` - Comma-separated league codes (e.g., "epl,nba,nfl"), empty = all
- `DISCOVERY_HORIZON_DAYS` - Days to look ahead (default: 3)
- `DISCOVERY_CACHE_BYPASS` - Set to 1 to skip cache (default: 0)
