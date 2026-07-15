use chrono::Utc;

use crate::engine::Engine;
use crate::order::{Order, OrderType, Side, TimeInForce};


pub fn seed(engine: &mut Engine) {
    let now = Utc::now().timestamp_millis();
    let start = now - 5 * 60 * 1000;
    let step = (now - start) / 24;

    let script: [(Side, OrderType, TimeInForce, f64, f64); 24] = [
        (Side::Sell, OrderType::Limit, TimeInForce::GTC, 101.00, 5.0),
        (Side::Sell, OrderType::Limit, TimeInForce::GTC, 101.50, 8.0),
        (Side::Sell, OrderType::Limit, TimeInForce::GTC, 102.00, 3.0),
        (Side::Buy, OrderType::Limit, TimeInForce::GTC, 99.00, 4.0),
        (Side::Buy, OrderType::Limit, TimeInForce::GTC, 98.50, 6.0),
        (Side::Buy, OrderType::Limit, TimeInForce::GTC, 98.00, 10.0),
        (Side::Sell, OrderType::Limit, TimeInForce::GTC, 100.50, 2.0),
        (Side::Buy, OrderType::Limit, TimeInForce::GTC, 99.50, 3.0),
        (Side::Buy, OrderType::Limit, TimeInForce::IOC, 100.60, 1.5),
        (Side::Sell, OrderType::Limit, TimeInForce::GTC, 103.00, 7.0),
        (Side::Buy, OrderType::Limit, TimeInForce::GTC, 97.50, 5.0),
        (Side::Sell, OrderType::Limit, TimeInForce::GTC, 101.20, 4.0),
        (Side::Buy, OrderType::Market, TimeInForce::IOC, 0.0, 6.0),
        (Side::Sell, OrderType::Limit, TimeInForce::GTC, 100.80, 3.5),
        (Side::Buy, OrderType::Limit, TimeInForce::GTC, 99.20, 2.5),
        (Side::Sell, OrderType::Limit, TimeInForce::GTC, 101.80, 6.0),
        (Side::Buy, OrderType::Limit, TimeInForce::GTC, 98.80, 4.5),
        (Side::Sell, OrderType::Market, TimeInForce::IOC, 0.0, 5.0),
        (Side::Buy, OrderType::Limit, TimeInForce::GTC, 99.70, 3.0),
        (Side::Sell, OrderType::Limit, TimeInForce::GTC, 102.50, 2.0),
        (Side::Buy, OrderType::Limit, TimeInForce::FOK, 101.00, 8.0),
        (Side::Sell, OrderType::Limit, TimeInForce::GTC, 100.20, 4.0),
        (Side::Buy, OrderType::Limit, TimeInForce::GTC, 97.00, 9.0),
        (Side::Sell, OrderType::Limit, TimeInForce::GTC, 103.50, 3.0),
    ];
    
    for (i, (side, order_type, tif, price, qty)) in script.into_iter().enumerate() {
        let id = engine.next_id();
        let order = Order {
            id,
            side,
            order_type,
            tif,
            price,
            qty,
            remaining_qty: qty,
            timestamp_ms: start + step * i as i64,
        };
        engine.submit(order);
    }
}
