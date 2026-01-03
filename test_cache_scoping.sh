#!/bin/bash
# Test script to verify cache scoping fix
set -e

echo "========================================="
echo "Cache Scoping Test Script"
echo "========================================="
echo ""

# Clean any existing cache
rm -f .discovery_cache.json

echo "Test 1: Create cache for EPL league"
echo "-----------------------------------"
LEAGUES="epl" DISCOVERY_HORIZON_DAYS=3 cargo run --release -- &
PID=$!
sleep 10
kill $PID || true
wait $PID 2>/dev/null || true

if [ -f .discovery_cache.json ]; then
    echo "✓ Cache file created"
    echo "Cache content:"
    cat .discovery_cache.json | jq '.schema_version, .leagues, .horizon_days'
else
    echo "✗ Cache file not created"
fi
echo ""

echo "Test 2: Try to use cache with different league (should reject)"
echo "--------------------------------------------------------------"
LEAGUES="nba" DISCOVERY_HORIZON_DAYS=3 cargo run --release -- &
PID=$!
sleep 10
kill $PID || true
wait $PID 2>/dev/null || true
echo ""

echo "Test 3: Try to use cache with different horizon (should reject)"
echo "---------------------------------------------------------------"
LEAGUES="epl" DISCOVERY_HORIZON_DAYS=7 cargo run --release -- &
PID=$!
sleep 10
kill $PID || true
wait $PID 2>/dev/null || true
echo ""

echo "Test 4: Cache bypass test"
echo "-------------------------"
LEAGUES="epl" DISCOVERY_HORIZON_DAYS=3 DISCOVERY_CACHE_BYPASS=1 cargo run --release -- &
PID=$!
sleep 10
kill $PID || true
wait $PID 2>/dev/null || true
echo ""

echo "========================================="
echo "Tests complete! Check logs above for:"
echo "  - 'Cache config match' messages"
echo "  - 'Cache config mismatch' warnings"
echo "  - 'DISCOVERY_CACHE_BYPASS' messages"
echo "========================================="
