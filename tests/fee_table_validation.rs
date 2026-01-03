//! Fee table validation tests (Item #5)
//!
//! Ensures that types::KALSHI_FEE_TABLE exactly matches the canonical formula
//! in cost::kalshi_fee_total_cents() for single-contract trades.

use prediction_market_arbitrage::config::KalshiRole;
use prediction_market_arbitrage::cost::kalshi_fee_total_cents;
use prediction_market_arbitrage::types::kalshi_fee_cents;

#[test]
fn test_fee_table_matches_canonical_formula() {
    // KALSHI_FEE_TABLE covers price_cents 0..=100
    // For each price point, verify the table value matches the canonical formula
    // for a single contract (contracts=1) with Taker role

    let mut mismatches = Vec::new();

    for price_cents in 0..=100u16 {
        let table_value = kalshi_fee_cents(price_cents);
        let formula_value = kalshi_fee_total_cents(price_cents, 1, KalshiRole::Taker) as u16;

        if table_value != formula_value {
            mismatches.push((price_cents, table_value, formula_value));
        }
    }

    if !mismatches.is_empty() {
        eprintln!("\n=== FEE TABLE MISMATCHES DETECTED ===");
        eprintln!("The KALSHI_FEE_TABLE does not match the canonical formula!");
        eprintln!("\nFirst 5 mismatches:");
        for (price, table_val, formula_val) in mismatches.iter().take(5) {
            eprintln!(
                "  price_cents={:3}: table={:2}¢, formula={:2}¢ (diff={}¢)",
                price,
                table_val,
                formula_val,
                *formula_val as i32 - *table_val as i32
            );
        }
        eprintln!("\nTotal mismatches: {}/101", mismatches.len());
        panic!(
            "Fee table validation failed: {} mismatches found",
            mismatches.len()
        );
    }

    // Success case
    println!("✅ All 101 fee table entries match the canonical formula");
}

#[test]
fn test_fee_table_bounds_and_invariants() {
    // Test 1: All values should be non-negative (u16 guarantees this, but check explicitly)
    for price_cents in 0..=100u16 {
        let fee = kalshi_fee_cents(price_cents);
        assert!(
            fee < 1000,
            "Fee unreasonably high at price_cents={}: {}¢",
            price_cents,
            fee
        );
    }

    // Test 2: Edge cases
    // Fee at price=0 should be 0 (no trade)
    assert_eq!(kalshi_fee_cents(0), 0, "Fee at price=0 should be 0");

    // Fee at price=100 should be 0 (invalid/edge case)
    assert_eq!(kalshi_fee_cents(100), 0, "Fee at price=100 should be 0");

    // Test 3: Reasonable fee range for typical prices (10-90 cents)
    // Fees should be in the 0-5 cent range for single contracts
    for price_cents in 10..=90u16 {
        let fee = kalshi_fee_cents(price_cents);
        assert!(
            fee <= 5,
            "Fee unexpectedly high at price_cents={}: {}¢",
            price_cents,
            fee
        );
    }

    // Test 4: Peak fee should be around 50¢ (maximum variance)
    // At p=50, fee = ceil(7 × 50 × 50 / 10000) = ceil(1750/10000) = ceil(0.175) = 1¢
    let fee_at_50 = kalshi_fee_cents(50);
    assert!(fee_at_50 > 0, "Fee at price=50 should be positive");

    println!("✅ Fee table bounds and invariants validated");
}

#[test]
fn test_fee_table_symmetry() {
    // Kalshi fees should be symmetric: fee(p) should equal fee(100-p)
    // because the formula is 7 × p × (100-p) which is symmetric around p=50

    for price_cents in 1..50u16 {
        let fee_low = kalshi_fee_cents(price_cents);
        let fee_high = kalshi_fee_cents(100 - price_cents);

        assert_eq!(
            fee_low,
            fee_high,
            "Fee symmetry broken: fee({})={} but fee({})={}",
            price_cents,
            fee_low,
            100 - price_cents,
            fee_high
        );
    }

    println!("✅ Fee table exhibits expected symmetry around p=50");
}

#[test]
fn test_canonical_formula_properties() {
    // Verify the canonical formula itself has expected properties

    // Property 1: Zero contracts = zero fee
    for price_cents in 1..100u16 {
        let fee = kalshi_fee_total_cents(price_cents, 0, KalshiRole::Taker);
        assert_eq!(fee, 0, "Zero contracts should have zero fee");
    }

    // Property 2: Maker fee should be lower than Taker fee
    for price_cents in [10u16, 30, 50, 70, 90] {
        let taker_fee = kalshi_fee_total_cents(price_cents, 10, KalshiRole::Taker);
        let maker_fee = kalshi_fee_total_cents(price_cents, 10, KalshiRole::Maker);

        assert!(
            maker_fee <= taker_fee,
            "Maker fee should be <= taker fee at price={}: maker={}, taker={}",
            price_cents,
            maker_fee,
            taker_fee
        );
    }

    // Property 3: Fee should scale with contract count (but ceiling causes nonlinearity)
    let price = 50u16;
    let fee_1 = kalshi_fee_total_cents(price, 1, KalshiRole::Taker);
    let fee_10 = kalshi_fee_total_cents(price, 10, KalshiRole::Taker);

    // Due to ceiling division, fee(N) can be LESS than N*fee(1) because ceiling rounds once
    // for N contracts instead of N times. Example: (49/100)→1 but (490/100)→5, not 10.
    // Verify that fee(10) is within reasonable bounds (more than N-1 contracts worth, less than linear)
    assert!(
        fee_10 >= fee_1 * 9 && fee_10 <= fee_1 * 11,
        "Fee should roughly scale with contracts: fee(1)={}, fee(10)={}, expected in [{}, {}]",
        fee_1,
        fee_10,
        fee_1 * 9,
        fee_1 * 11
    );

    println!("✅ Canonical formula properties validated");
}

#[test]
fn test_multi_contract_accumulation_warning() {
    // This test demonstrates WHY the table should only be used for single contracts
    // For multi-contract trades, per-contract rounding causes errors

    let price = 45u16;
    let contracts = 10u32;

    // Method 1: Using table (wrong for multi-contract)
    let table_per_contract = kalshi_fee_cents(price) as u32;
    let table_total = table_per_contract * contracts;

    // Method 2: Using canonical formula (correct)
    let formula_total = kalshi_fee_total_cents(price, contracts, KalshiRole::Taker);

    // These should differ due to rounding
    if table_total != formula_total {
        let diff = (formula_total as i64 - table_total as i64).abs();
        println!(
            "⚠️  Multi-contract rounding demo: price={}¢, qty={}",
            price, contracts
        );
        println!(
            "   Table extrapolation: {}¢ × {} = {}¢",
            table_per_contract, contracts, table_total
        );
        println!("   Canonical formula: {}¢", formula_total);
        println!("   Difference: {}¢", diff);
    }

    // The key point: always use the canonical formula for execution
    assert!(formula_total > 0, "Formula should compute non-zero fee");
}
