use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    Limit,
    Market,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeInForce {
    GTC,
    IOC,
    FOK,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: u64,
    pub side: Side,
    pub order_type: OrderType,
    pub tif: TimeInForce,
    pub price: f64,
    pub qty: f64,
    pub remaining_qty: f64,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub id: u64,
    pub maker_order_id: u64,
    pub taker_order_id: u64,
    pub price: f64,
    pub qty: f64,
    pub timestamp_ms: i64,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum EngineEvent {
    Submit { order: Order },
    Cancel { order_id: u64, timestamp_ms: i64 },
}

impl EngineEvent {
    pub fn timestamp_ms(&self) -> i64 {
        match self {
            EngineEvent::Submit { order } => order.timestamp_ms,
            EngineEvent::Cancel { timestamp_ms, .. } => *timestamp_ms,
        }
    }
}
