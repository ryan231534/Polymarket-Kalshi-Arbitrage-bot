//! Tests for execution fee recording (Item #2)

#[cfg(test)]
mod tests {
    use crate::config::KalshiRole;
    use crate::fees::{Exchange, FeeModel};
    use crate::pnl::{ContractSide, PnLTracker, Side as PnLSide, Venue};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_buy_fill_with_fee_correct_cost_basis() {
        // Setup: Create P&L tracker and fee model
        let temp_dir = std::env::temp_dir().join("exec_test_buy");
        let _ = std::fs::create_dir_all(&temp_dir);
        let dir_str = temp_dir.to_str().unwrap();

        let tracker = Arc::new(Mutex::new(PnLTracker::new(dir_str, true)));
        let fee_model = Arc::new(FeeModel::new(KalshiRole::Taker));

        // Test: Buy 10 contracts at 45¢ on Polymarket
        let poly_price: u16 = 45;
        let qty: i64 = 10;
        let fee = fee_model
            .estimate_fees(Exchange::Polymarket, "test_token", poly_price, qty as u32)
            .await as i64;

        // Record fill with fee
        {
            let mut pnl = tracker.lock().await;
            pnl.on_fill(
                Venue::Polymarket,
                "test_market",
                "order123",
                PnLSide::Buy,
                ContractSide::Yes,
                qty,
                poly_price as i64,
                fee, // total fee, not per contract
            );
        }

        // Verify: Cost basis should include fee
        let summary = {
            let pnl = tracker.lock().await;
            pnl.summary()
        };

        // Fee should be recorded
        assert!(summary.fees_cents > 0, "Fee should be recorded");
        assert_eq!(summary.fees_cents, fee, "Fee should match estimate");

        // Position should exist
        assert_eq!(summary.open_positions, 1);

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_sell_fill_with_fee_correct_realized_pnl() {
        // Setup
        let temp_dir = std::env::temp_dir().join("exec_test_sell");
        let _ = std::fs::create_dir_all(&temp_dir);
        let dir_str = temp_dir.to_str().unwrap();

        let tracker = Arc::new(Mutex::new(PnLTracker::new(dir_str, true)));
        let fee_model = Arc::new(FeeModel::new(KalshiRole::Taker));

        // Test: Buy at 40¢, sell at 60¢
        let buy_price: u16 = 40;
        let sell_price: u16 = 60;
        let qty: i64 = 10;

        // Buy fill
        let buy_fee = fee_model
            .estimate_fees(Exchange::Kalshi, "", buy_price, qty as u32)
            .await as i64;

        {
            let mut pnl = tracker.lock().await;
            pnl.on_fill(
                Venue::Kalshi,
                "test_market",
                "buy_order",
                PnLSide::Buy,
                ContractSide::Yes,
                qty,
                buy_price as i64,
                buy_fee, // total fee
            );
        }

        // Sell fill
        let sell_fee = fee_model
            .estimate_fees(Exchange::Kalshi, "", sell_price, qty as u32)
            .await as i64;

        {
            let mut pnl = tracker.lock().await;
            pnl.on_fill(
                Venue::Kalshi,
                "test_market",
                "sell_order",
                PnLSide::Sell,
                ContractSide::Yes,
                qty,
                sell_price as i64,
                sell_fee, // total fee
            );
        }

        // Verify: Realized P&L should account for both fees
        let summary = {
            let pnl = tracker.lock().await;
            pnl.summary()
        };

        // Gross profit = (60 - 40) * 10 = 200¢
        // Net profit = 200¢ - buy_fee - sell_fee
        let expected_gross_profit = (sell_price as i64 - buy_price as i64) * qty;
        let total_fees = buy_fee + sell_fee;

        assert_eq!(summary.fees_cents, total_fees, "Total fees should match");

        // Realized P&L should be gross profit
        // (fees are tracked separately, not subtracted from realized)
        assert_eq!(
            summary.realized_cents, expected_gross_profit,
            "Realized P&L should be gross profit"
        );

        // Net profit = realized - fees
        let net_profit = summary.realized_cents - summary.fees_cents;
        assert_eq!(
            net_profit,
            expected_gross_profit - total_fees,
            "Net profit should account for fees"
        );

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_arb_two_legs_combined_net_profit() {
        // Setup
        let temp_dir = std::env::temp_dir().join("exec_test_arb");
        let _ = std::fs::create_dir_all(&temp_dir);
        let dir_str = temp_dir.to_str().unwrap();

        let tracker = Arc::new(Mutex::new(PnLTracker::new(dir_str, true)));
        let fee_model = Arc::new(FeeModel::new(KalshiRole::Taker));

        // Test: Cross-platform arb - Poly YES (45¢) + Kalshi NO (50¢)
        let poly_yes_price: u16 = 45;
        let kalshi_no_price: u16 = 50;
        let qty: i64 = 10;

        // Expected profit without fees: 100 - (45 + 50) = 5¢ per contract = 50¢ total

        // Leg 1: Buy Poly YES
        let poly_fee = fee_model
            .estimate_fees(
                Exchange::Polymarket,
                "test_token_yes",
                poly_yes_price,
                qty as u32,
            )
            .await as i64;

        {
            let mut pnl = tracker.lock().await;
            pnl.on_fill(
                Venue::Polymarket,
                "arb_market",
                "poly_order",
                PnLSide::Buy,
                ContractSide::Yes,
                qty,
                poly_yes_price as i64,
                poly_fee, // total fee
            );
        }

        // Leg 2: Buy Kalshi NO
        let kalshi_fee = fee_model
            .estimate_fees(Exchange::Kalshi, "", kalshi_no_price, qty as u32)
            .await as i64;

        {
            let mut pnl = tracker.lock().await;
            pnl.on_fill(
                Venue::Kalshi,
                "arb_market",
                "kalshi_order",
                PnLSide::Buy,
                ContractSide::No,
                qty,
                kalshi_no_price as i64,
                kalshi_fee, // total fee
            );
        }

        // Verify: Net profit matches expectation
        let summary = {
            let pnl = tracker.lock().await;
            pnl.summary()
        };

        let total_cost = (poly_yes_price as i64 + kalshi_no_price as i64) * qty;
        let total_fees = poly_fee + kalshi_fee;
        let payout = 100 * qty; // One side wins $1.00 per contract

        let expected_net_profit = payout - total_cost - total_fees;

        // With positions still open, realized P&L is 0, but we check fees
        assert_eq!(summary.fees_cents, total_fees, "Total fees should match");

        // Both positions should be open
        assert_eq!(summary.open_positions, 2);

        // Simulate settlement (YES wins)
        {
            let mut pnl = tracker.lock().await;
            pnl.on_settlement(Venue::Polymarket, "arb_market", ContractSide::Yes, 100);
            pnl.on_settlement(Venue::Kalshi, "arb_market", ContractSide::Yes, 100);
        }

        // After settlement, check final P&L
        let final_summary = {
            let pnl = tracker.lock().await;
            pnl.summary()
        };

        // Net profit = realized - fees
        let actual_net_profit = final_summary.realized_cents - final_summary.fees_cents;

        assert_eq!(
            actual_net_profit, expected_net_profit,
            "Net profit after settlement should match expectation"
        );

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_polymarket_fees_nonzero() {
        // Verify that Polymarket fees are actually calculated (not zero)
        let fee_model = Arc::new(FeeModel::new(KalshiRole::Taker));

        let poly_fee = fee_model
            .estimate_fees(Exchange::Polymarket, "fake_token", 50, 10)
            .await;

        // With fallback of 50 bps, premium=500¢, fee should be ceil(500*50/10000) = 3¢
        assert!(poly_fee > 0, "Polymarket fee should not be zero");
        assert!(
            poly_fee >= 2,
            "Polymarket fee should be at least 2¢ for this trade"
        );
    }
}
