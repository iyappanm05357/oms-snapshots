use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use singularity::common::instrument_id::InstrumentId;
use singularity::common::price::{Price, Qty};
use singularity::common::{order_id::OrderId, seq_num::SeqNum};
use singularity::me_ingress_model::*;
use singularity::now;

use switchboard::{
    AeronConfig, AeronExclusivePublisher, AeronPublisherConfig, BusySpinConfig, Publication,
    Publisher, WaitStrategy,
};

use crate::order::{Order, OrderType, Side, TimeInForce};

pub type BoxedPublisher = AeronExclusivePublisher<Box<dyn FnMut()>>;

static SEQ: AtomicU64 = AtomicU64::new(500_000);
fn next_seq() -> u64 {
    SEQ.fetch_add(1, Ordering::Relaxed)
}


pub const PRICE_SCALE: f64 = 100.0;
pub const QTY_SCALE: f64 = 100.0;

fn to_ticks(value: f64, scale: f64) -> u64 {
    (value * scale).round().max(0.0) as u64
}


fn to_side(side: Side) -> OrderSide {
    match side {
        Side::Buy => OrderSide::Bid,
        Side::Sell => OrderSide::Ask,
    }
}

fn to_kind(order_type: OrderType) -> OrderKind {
    match order_type {
        OrderType::Limit => OrderKind::Limit,
        OrderType::Market => OrderKind::Market,
    }
}

fn to_tif(tif: TimeInForce) -> OrderTif {
    match tif {
        TimeInForce::GTC => OrderTif::Gtc,
        TimeInForce::IOC => OrderTif::Ioc,
        TimeInForce::FOK => OrderTif::Fok,
    }
}


pub fn to_ingress_model(order: &Order, account_id: u64, instrument_id: u64) -> IngressModel {
    let seq = next_seq();
    IngressModel::Order(OrderIngressModel {
        order_id: OrderId::new(order.id.try_into().unwrap()),
        account_id: account_id.try_into().unwrap(),
        price: Price::new(to_ticks(order.price, PRICE_SCALE).try_into().unwrap()),
        qty: Qty::new(to_ticks(order.qty, QTY_SCALE).try_into().unwrap()),
        seq: SeqNum::new(seq.try_into().unwrap()),
        order_type: to_kind(order.order_type),
        flags: 0,
        side: to_side(order.side),
        tif: to_tif(order.tif),
        instrument_id: InstrumentId::new(instrument_id.try_into().unwrap()),
        expiry: now!(),
    })
}


pub fn make_publisher(channel: &str, stream_id: i32) -> BoxedPublisher {
    let pub_config = AeronPublisherConfig {
        aeron: AeronConfig {
            channel: channel.into(),
            stream_id,
        },
        wait_strategy: WaitStrategy::BusySpin(BusySpinConfig { spin_count: 10 }),
        not_connected_timeout: Duration::from_secs(5),
    };
    AeronExclusivePublisher::new(pub_config).expect("failed to create aeron publisher")
}


pub fn publish_order(
    publisher: &mut BoxedPublisher,
    order: &Order,
    account_id: u64,
    instrument_id: u64,
) -> Result<(), String> {
    let model = to_ingress_model(order, account_id, instrument_id);
    let bytes = wincode::serialize(&model).map_err(|e| format!("serialize failed: {e}"))?;

    let mut retries: u64 = 0;
    loop {
        match publisher.publish(&bytes) {
            Ok(_) => return Ok(()),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("BackPressured")
                    || msg.contains("NotConnected")
                    || msg.contains("AdminAction")
                    || msg.contains("Closed")
                {
                    retries += 1;
                    let wait_us = match retries {
                        1..=10 => 10,
                        11..=50 => 50,
                        _ => 200,
                    };
                    std::thread::sleep(Duration::from_micros(wait_us));

                    if retries % 100 == 0 {
                        eprintln!(
                            "[publisher] order #{} back-pressure retries={retries} err={msg}",
                            order.id
                        );
                    }
                    if retries > 1_000 {
                        return Err(format!(
                            "gave up publishing order #{} after 1000 retries, last error: {msg}",
                            order.id
                        ));
                    }
                } else {
                    return Err(format!("publish error for order #{}: {msg}", order.id));
                }
            }
        }
    }
}


pub fn publish_batch(
    publisher: &mut BoxedPublisher,
    orders: &[Order],
    account_id: u64,
    instrument_id: u64,
) -> usize {
    orders
        .iter()
        .filter(|o| publish_order(publisher, o, account_id, instrument_id).is_ok())
        .count()
}