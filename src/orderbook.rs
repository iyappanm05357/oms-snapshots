use std::collections::{BTreeMap, HashMap, VecDeque};

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::order::{Order, OrderType, Side, TimeInForce, Trade};

type Px = OrderedFloat<f64>;


#[derive(Default)]
pub struct OrderBook {
    pub bids: BTreeMap<Px, VecDeque<Order>>,
    pub asks: BTreeMap<Px, VecDeque<Order>>,
    index: HashMap<u64, (Side, Px)>,
    next_trade_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitResult {
    pub trades: Vec<Trade>,
    pub accepted: bool,
    pub rejection_reason: Option<String>,
    pub resting_qty: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: f64,
    pub qty: f64,
    pub order_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookSnapshot {
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn best_bid(&self) -> Option<f64> {
        self.bids.keys().next_back().map(|p| p.0)
    }

    pub fn best_ask(&self) -> Option<f64> {
        self.asks.keys().next().map(|p| p.0)
    }

   
    pub fn submit(&mut self, mut order: Order) -> SubmitResult {
        let mut trades = Vec::new();

    
        if order.tif == TimeInForce::FOK {
            let available = self.available_liquidity(order.side, order.order_type, order.price);
            if available < order.qty {
                return SubmitResult {
                    trades,
                    accepted: false,
                    rejection_reason: Some(
                        "FOK order could not be fully filled at submission time".into(),
                    ),
                    resting_qty: 0.0,
                };
            }
        }

        self.match_order(&mut order, &mut trades);

        let resting_qty = order.remaining_qty;

       
        let should_rest = order.order_type == OrderType::Limit
            && order.tif == TimeInForce::GTC
            && resting_qty > 1e-9;

        if should_rest {
            self.insert_resting(order);
        }

        SubmitResult {
            trades,
            accepted: true,
            rejection_reason: None,
            resting_qty: if should_rest { resting_qty } else { 0.0 },
        }
    }

    fn available_liquidity(&self, side: Side, order_type: OrderType, limit_price: f64) -> f64 {
        let mut total = 0.0;
        match side {
            Side::Buy => {
                for (px, q) in self.asks.iter() {
                    if order_type == OrderType::Limit && px.0 > limit_price {
                        break;
                    }
                    total += q.iter().map(|o| o.remaining_qty).sum::<f64>();
                }
            }
            Side::Sell => {
                for (px, q) in self.bids.iter().rev() {
                    if order_type == OrderType::Limit && px.0 < limit_price {
                        break;
                    }
                    total += q.iter().map(|o| o.remaining_qty).sum::<f64>();
                }
            }
        }
        total
    }

    fn match_order(&mut self, taker: &mut Order, trades: &mut Vec<Trade>) {
        match taker.side {
            Side::Buy => self.match_against_asks(taker, trades),
            Side::Sell => self.match_against_bids(taker, trades),
        }
    }

    fn match_against_asks(&mut self, taker: &mut Order, trades: &mut Vec<Trade>) {
        while taker.remaining_qty > 1e-9 {
            let Some((&best_px, _)) = self.asks.iter().next() else {
                break;
            };
            if taker.order_type == OrderType::Limit && best_px.0 > taker.price {
                break;
            }
            let level = self.asks.get_mut(&best_px).unwrap();
            let filled_ids =
                Self::drain_level(level, taker, best_px.0, trades, &mut self.next_trade_id);
            for id in filled_ids {
                self.index.remove(&id);
            }
            if level.is_empty() {
                self.asks.remove(&best_px);
            }
        }
    }

    fn match_against_bids(&mut self, taker: &mut Order, trades: &mut Vec<Trade>) {
        while taker.remaining_qty > 1e-9 {
            let Some((&best_px, _)) = self.bids.iter().next_back() else {
                break;
            };
            if taker.order_type == OrderType::Limit && best_px.0 < taker.price {
                break;
            }
            let level = self.bids.get_mut(&best_px).unwrap();
            let filled_ids =
                Self::drain_level(level, taker, best_px.0, trades, &mut self.next_trade_id);
            for id in filled_ids {
                self.index.remove(&id);
            }
            if level.is_empty() {
                self.bids.remove(&best_px);
            }
        }
    }

   
    fn drain_level(
        level: &mut VecDeque<Order>,
        taker: &mut Order,
        trade_price: f64,
        trades: &mut Vec<Trade>,
        next_trade_id: &mut u64,
    ) -> Vec<u64> {
        let mut filled_ids = Vec::new();
        while taker.remaining_qty > 1e-9 {
            let Some(maker) = level.front_mut() else {
                break;
            };
            let fill_qty = taker.remaining_qty.min(maker.remaining_qty);
            maker.remaining_qty -= fill_qty;
            taker.remaining_qty -= fill_qty;

            *next_trade_id += 1;
            trades.push(Trade {
                id: *next_trade_id,
                maker_order_id: maker.id,
                taker_order_id: taker.id,
                price: trade_price,
                qty: fill_qty,
                timestamp_ms: taker.timestamp_ms,
            });

            if maker.remaining_qty <= 1e-9 {
                filled_ids.push(maker.id);
                level.pop_front();
            }
        }
        filled_ids
    }

    fn insert_resting(&mut self, order: Order) {
        let px = OrderedFloat(order.price);
        self.index.insert(order.id, (order.side, px));
        let book = match order.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        book.entry(px).or_default().push_back(order);
    }

  
    pub fn cancel(&mut self, order_id: u64) -> bool {
        let Some((side, px)) = self.index.remove(&order_id) else {
            return false;
        };
        let book = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if let Some(level) = book.get_mut(&px) {
            level.retain(|o| o.id != order_id);
            if level.is_empty() {
                book.remove(&px);
            }
            true
        } else {
            false
        }
    }

    pub fn snapshot(&self, depth: usize) -> BookSnapshot {
        let bids = self
            .bids
            .iter()
            .rev()
            .take(depth)
            .map(|(px, q)| BookLevel {
                price: px.0,
                qty: q.iter().map(|o| o.remaining_qty).sum(),
                order_count: q.len(),
            })
            .collect();
        let asks = self
            .asks
            .iter()
            .take(depth)
            .map(|(px, q)| BookLevel {
                price: px.0,
                qty: q.iter().map(|o| o.remaining_qty).sum(),
                order_count: q.len(),
            })
            .collect();
        BookSnapshot { bids, asks }
    }
}
