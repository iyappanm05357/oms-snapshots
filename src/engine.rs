use serde::{Deserialize, Serialize};

use crate::order::{EngineEvent, Order, Trade};
use crate::orderbook::{BookSnapshot, OrderBook, SubmitResult};


pub struct Engine {
    pub book: OrderBook,
    pub journal: Vec<EngineEvent>,
    pub trade_log: Vec<Trade>,
    next_order_id: u64,
}

impl Engine {
    pub fn new() -> Self {
        Self {
            book: OrderBook::new(),
            journal: Vec::new(),
            trade_log: Vec::new(),
            next_order_id: 0,
        }
    }

    pub fn next_id(&mut self) -> u64 {
        self.next_order_id += 1;
        self.next_order_id
    }

    pub fn submit(&mut self, order: Order) -> SubmitResult {
        self.journal.push(EngineEvent::Submit {
            order: order.clone(),
        });
        let result = self.book.submit(order);
        self.trade_log.extend(result.trades.clone());
        result
    }

    pub fn cancel(&mut self, order_id: u64, timestamp_ms: i64) -> bool {
        self.journal.push(EngineEvent::Cancel {
            order_id,
            timestamp_ms,
        });
        self.book.cancel(order_id)
    }

    pub fn snapshot(&self) -> BookSnapshot {
        self.book.snapshot(25)
    }
}

// Replay

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationReport {
    pub valid: bool,
    pub errors: Vec<String>,
    pub events_replayed: usize,
    pub events_skipped: usize,
    pub trades_produced: usize,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub total_bid_qty: f64,
    pub total_ask_qty: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayResult {
    pub target_time_ms: i64,
    pub book: BookSnapshot,
    pub trades: Vec<Trade>,
    pub validation: ValidationReport,
}


pub fn replay_to(journal: &[EngineEvent], target_time_ms: i64) -> ReplayResult {
    let mut book = OrderBook::new();
    let mut trades = Vec::new();
    let mut replayed = 0usize;
    let mut skipped = 0usize;

   
    let mut ordered: Vec<&EngineEvent> = journal.iter().collect();
    ordered.sort_by_key(|e| e.timestamp_ms());

    for event in ordered {
        if event.timestamp_ms() > target_time_ms {
            skipped += 1;
            continue;
        }
        match event {
            EngineEvent::Submit { order } => {
                let result = book.submit(order.clone());
                trades.extend(result.trades);
            }
            EngineEvent::Cancel { order_id, .. } => {
                book.cancel(*order_id);
            }
        }
        replayed += 1;
    }

    let validation = validate(&book, &trades, replayed, skipped);
    let snapshot = book.snapshot(25);

    ReplayResult {
        target_time_ms,
        book: snapshot,
        trades,
        validation,
    }
}


fn validate(
    book: &OrderBook,
    trades: &[Trade],
    replayed: usize,
    skipped: usize,
) -> ValidationReport {
    let mut errors = Vec::new();

    let best_bid = book.best_bid();
    let best_ask = book.best_ask();

    if let (Some(b), Some(a)) = (best_bid, best_ask) {
        if b >= a {
            errors.push(format!(
                "crossed book: best_bid {b} >= best_ask {a}"
            ));
        }
    }

    let snapshot = book.snapshot(usize::MAX);
    for level in snapshot.bids.iter().chain(snapshot.asks.iter()) {
        if level.qty <= 0.0 {
            errors.push(format!(
                "non-positive quantity {} at price {}",
                level.qty, level.price
            ));
        }
        if level.order_count == 0 {
            errors.push(format!("empty price level present at {}", level.price));
        }
    }

  
    let mut last: Option<f64> = None;
    for level in &snapshot.bids {
        if let Some(l) = last {
            if level.price >= l {
                errors.push("bid levels not strictly descending".into());
            }
        }
        last = Some(level.price);
    }
    let mut last: Option<f64> = None;
    for level in &snapshot.asks {
        if let Some(l) = last {
            if level.price <= l {
                errors.push("ask levels not strictly ascending".into());
            }
        }
        last = Some(level.price);
    }

    let total_bid_qty: f64 = snapshot.bids.iter().map(|l| l.qty).sum();
    let total_ask_qty: f64 = snapshot.asks.iter().map(|l| l.qty).sum();

    ValidationReport {
        valid: errors.is_empty(),
        errors,
        events_replayed: replayed,
        events_skipped: skipped,
        trades_produced: trades.len(),
        best_bid,
        best_ask,
        total_bid_qty,
        total_ask_qty,
    }
}
