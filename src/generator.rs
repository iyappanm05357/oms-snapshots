use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::order::{OrderType, Side, TimeInForce};
use crate::ws::{submit_and_broadcast, AppState};


pub async fn run(state: Arc<AppState>) {
    let mut mid_price: f64 = 100.0;
    let mut rng_state: u64 = 0x9E3779B97F4A7C15;

    loop {
        let interval_ms = state.stream_interval_ms.load(Ordering::Relaxed).max(20);
        tokio::time::sleep(Duration::from_millis(interval_ms)).await;

        if !state.streaming.load(Ordering::Relaxed) {
            continue;
        }

        
        let mut next_f64 = || -> f64 {
            rng_state ^= rng_state << 13;
            rng_state ^= rng_state >> 7;
            rng_state ^= rng_state << 17;
            (rng_state >> 11) as f64 / (1u64 << 53) as f64
        };

  
        mid_price += (next_f64() - 0.5) * 0.4;
        mid_price = mid_price.clamp(50.0, 500.0);

        let side = if next_f64() < 0.5 { Side::Buy } else { Side::Sell };
        let is_market = next_f64() < 0.15;
        let order_type = if is_market { OrderType::Market } else { OrderType::Limit };

        let tif = if is_market {
            TimeInForce::IOC
        } else {
            match (next_f64() * 10.0) as u32 {
                0..=6 => TimeInForce::GTC,
                7..=8 => TimeInForce::IOC,
                _ => TimeInForce::FOK,
            }
        };

   
        let offset = next_f64() * 3.0 - 1.0; 
        let price = match side {
            Side::Buy => mid_price - offset,
            Side::Sell => mid_price + offset,
        };
        let price = (price * 100.0).round() / 100.0;

        let qty = (1.0 + next_f64() * 9.0 * 100.0).round() / 100.0;

        submit_and_broadcast(&state, side, order_type, tif, price.max(0.01), qty).await;
    }
}
