#!/bin/bash
# Test structured logging with JSON format

set -e

echo "=== Testing Structured Logging ==="
echo

# Clean up old logs
rm -rf /tmp/arb_logs_test
mkdir -p /tmp/arb_logs_test

echo "1. Building release binary..."
cargo build --release --quiet

echo "2. Running bot with JSON logging for 5 seconds..."
LOG_DIR=/tmp/arb_logs_test \
LOG_FORMAT=json \
RUST_LOG=info \
DRY_RUN=1 \
ENABLED_LEAGUES=nba \
timeout 5s ./target/release/prediction-market-arbitrage 2>&1 | head -30 || true

echo
echo "3. Checking log file..."
LOG_FILE=$(ls -t /tmp/arb_logs_test/arb_bot.log.* | head -1)
if [ -f "$LOG_FILE" ]; then
    echo "✅ Log file created: $LOG_FILE"
    echo "   Size: $(wc -l < "$LOG_FILE") lines"
    echo
    
    echo "4. Validating JSON format (first 3 lines)..."
    head -3 "$LOG_FILE" | jq -c '{timestamp, level, message}' && echo "✅ Valid JSON" || echo "❌ Invalid JSON"
    echo
    
    echo "5. Checking for run_id in span context..."
    if grep -q '"run_id"' "$LOG_FILE"; then
        RUN_ID=$(grep -m1 '"run_id"' "$LOG_FILE" | jq -r '.span.run_id // .run_id' | head -1)
        echo "✅ run_id found: $RUN_ID"
    else
        echo "❌ No run_id found"
    fi
    echo
    
    echo "6. Searching for structured events..."
    echo "   Checking for event fields..."
    
    # Count events
    EVENT_COUNT=$(grep -o '"event":"[^"]*"' "$LOG_FILE" 2>/dev/null | wc -l || echo "0")
    echo "   Found $EVENT_COUNT structured events"
    
    if [ "$EVENT_COUNT" -gt 0 ]; then
        echo "   Event types:"
        grep -o '"event":"[^"]*"' "$LOG_FILE" | sort | uniq -c || true
    fi
    
else
    echo "❌ No log file found"
    exit 1
fi

echo
echo "=== Test Complete ==="
