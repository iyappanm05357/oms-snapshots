use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use chrono::Utc;
use futures::stream::{SplitSink, StreamExt};
use futures::SinkExt;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::order::{Order, OrderType, Side, TimeInForce};
use crate::publisher::{self, BoxedPublisher};

pub const AERON_CHANNEL: &str = "aeron:udp?endpoint=localhost:40123";
pub const AERON_STREAM_ID: i32 = 1001;
pub const DEFAULT_ACCOUNT_ID: u64 = 1;
pub const DEFAULT_INSTRUMENT_ID: u64 = 1;



pub struct AeronState {
    pub tx: broadcast::Sender<String>,
    cmd_tx: std_mpsc::Sender<PublishCmd>,
    pub streaming: AtomicBool,
    pub stream_interval_ms: AtomicU64,
    pub total_sent: AtomicU64,
    pub total_errors: AtomicU64,
    next_order_id: AtomicU64,
}

impl AeronState {
  
    pub fn new() -> Arc<Self> {
        let (tx, _rx) = broadcast::channel(1024);
        let (cmd_tx, cmd_rx) = std_mpsc::channel::<PublishCmd>();

        let state = Arc::new(Self {
            tx,
            cmd_tx,
            streaming: AtomicBool::new(false),
            stream_interval_ms: AtomicU64::new(500),
            total_sent: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            next_order_id: AtomicU64::new(0),
        });

        let worker_state = state.clone();
        std::thread::spawn(move || publisher_thread(worker_state, cmd_rx));

        state
    }

    fn next_id(&self) -> u64 {
        self.next_order_id.fetch_add(1, Ordering::Relaxed) + 1
    }
}


enum PublishCmd {
    Submit(Order),
    Prefill(PrefillKind),
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum PrefillKind {
    Dense,
    Sparse,
    HighSpread,
    LowSpread,
}


fn publisher_thread(state: Arc<AeronState>, cmd_rx: std_mpsc::Receiver<PublishCmd>) {
    let mut aeron_publisher: BoxedPublisher =
        publisher::make_publisher(AERON_CHANNEL, AERON_STREAM_ID);

    let mut mid_price: f64 = 100.0;
    let mut rng_state: u64 = 0xD1B54A32D192ED03;
    let mut last_tick = std::time::Instant::now();

    loop {
        match cmd_rx.recv_timeout(Duration::from_millis(20)) {
            Ok(PublishCmd::Submit(order)) => {
                send_one(&state, &mut aeron_publisher, order);
            }
            Ok(PublishCmd::Prefill(kind)) => {
                run_prefill_once(&state, &mut aeron_publisher, kind);
            }
            Err(std_mpsc::RecvTimeoutError::Timeout) => {}
            Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
        }

        if state.streaming.load(Ordering::Relaxed) {
            let interval = state.stream_interval_ms.load(Ordering::Relaxed).max(20);
            if last_tick.elapsed() >= Duration::from_millis(interval) {
                last_tick = std::time::Instant::now();
                let order = random_order(&state, &mut mid_price, &mut rng_state);
                send_one(&state, &mut aeron_publisher, order);
            }
        }
    }
}

fn send_one(state: &Arc<AeronState>, aeron_publisher: &mut BoxedPublisher, order: Order) {
    let result = publisher::publish_order(
        aeron_publisher,
        &order,
        DEFAULT_ACCOUNT_ID,
        DEFAULT_INSTRUMENT_ID,
    );

    match &result {
        Ok(()) => {
            state.total_sent.fetch_add(1, Ordering::Relaxed);
        }
        Err(_) => {
            state.total_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    broadcast(
        state,
        &ServerMsg::PublishAck {
            order_id: order.id,
            side: order.side,
            order_type: order.order_type,
            tif: order.tif,
            price: order.price,
            qty: order.qty,
            ok: result.is_ok(),
            error: result.err(),
            total_sent: state.total_sent.load(Ordering::Relaxed),
            total_errors: state.total_errors.load(Ordering::Relaxed),
        },
    );
}

fn random_order(state: &Arc<AeronState>, mid_price: &mut f64, rng_state: &mut u64) -> Order {
    let mut next_f64 = || -> f64 {
        *rng_state ^= *rng_state << 13;
        *rng_state ^= *rng_state >> 7;
        *rng_state ^= *rng_state << 17;
        (*rng_state >> 11) as f64 / (1u64 << 53) as f64
    };

    *mid_price += (next_f64() - 0.5) * 0.4;
    *mid_price = mid_price.clamp(50.0, 500.0);

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
        Side::Buy => *mid_price - offset,
        Side::Sell => *mid_price + offset,
    };
    let price = ((price.max(0.01)) * 100.0).round() / 100.0;
    let qty = (1.0 + next_f64() * 9.0 * 100.0).round() / 100.0;

    build_order(state, side, order_type, tif, price, qty)
}

fn build_order(
    state: &Arc<AeronState>,
    side: Side,
    order_type: OrderType,
    tif: TimeInForce,
    price: f64,
    qty: f64,
) -> Order {
    Order {
        id: state.next_id(),
        side,
        order_type,
        tif,
        price,
        qty,
        remaining_qty: qty,
        timestamp_ms: Utc::now().timestamp_millis(),
    }
}



fn run_prefill_once(state: &Arc<AeronState>, aeron_publisher: &mut BoxedPublisher, kind: PrefillKind) {
    let mid = 100.0_f64;
    let orders: Vec<Order> = match kind {
        PrefillKind::Dense => prefill_levels(state, mid, 0.10, 200, |i| 10 + ((200 - i + 1) % 20)),
        PrefillKind::Sparse => prefill_levels(state, mid, 2.00, 50, |i| (i % 3) + 1),
        PrefillKind::HighSpread => {
            prefill_levels_gapped(state, mid, 5.00, 1.00, 100, |i| (i % 4) + 2)
        }
        PrefillKind::LowSpread => prefill_levels(state, mid, 0.01, 200, |i| 15 + ((200 - i + 1) % 15)),
    };

    let total = orders.len();
    let mut sent = 0usize;
    for order in orders {
        let order_id = order.id;
        let order_side = order.side;
        let order_type = order.order_type;
        let order_tif = order.tif;
        let order_price = order.price;
        let order_qty = order.qty;

        let result = publisher::publish_order(
            aeron_publisher,
            &order,
            DEFAULT_ACCOUNT_ID,
            DEFAULT_INSTRUMENT_ID,
        );
        if result.is_ok() {
            sent += 1;
            state.total_sent.fetch_add(1, Ordering::Relaxed);
        } else {
            state.total_errors.fetch_add(1, Ordering::Relaxed);
        }

        broadcast(
            state,
            &ServerMsg::PublishAck {
                order_id,
                side: order_side,
                order_type,
                tif: order_tif,
                price: order_price,
                qty: order_qty,
                ok: result.is_ok(),
                error: result.err(),
                total_sent: state.total_sent.load(Ordering::Relaxed),
                total_errors: state.total_errors.load(Ordering::Relaxed),
            },
        );
    }

    broadcast(state, &ServerMsg::PrefillResult { kind, sent, total });
}

fn prefill_levels(
    state: &Arc<AeronState>,
    mid: f64,
    tick: f64,
    levels: u64,
    qty_fn: impl Fn(u64) -> u64,
) -> Vec<Order> {
    let mut out = Vec::with_capacity((levels * 2) as usize);
    for i in 1..=levels {
        let qty = qty_fn(i) as f64;
        out.push(build_order(state, Side::Buy, OrderType::Limit, TimeInForce::GTC, mid - i as f64 * tick, qty));
        out.push(build_order(state, Side::Sell, OrderType::Limit, TimeInForce::GTC, mid + i as f64 * tick, qty));
    }
    out
}

fn prefill_levels_gapped(
    state: &Arc<AeronState>,
    mid: f64,
    base_gap: f64,
    tick: f64,
    levels: u64,
    qty_fn: impl Fn(u64) -> u64,
) -> Vec<Order> {
    let mut out = Vec::with_capacity((levels * 2) as usize);
    for i in 1..=levels {
        let qty = qty_fn(i) as f64;
        let gap = base_gap + (i - 1) as f64 * tick;
        out.push(build_order(state, Side::Buy, OrderType::Limit, TimeInForce::GTC, mid - gap, qty));
        out.push(build_order(state, Side::Sell, OrderType::Limit, TimeInForce::GTC, mid + gap, qty));
    }
    out
}

// Wire protocol

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum ClientMsg {
    #[serde(rename = "submit_order")]
    SubmitOrder {
        side: Side,
        order_type: OrderType,
        tif: TimeInForce,
        #[serde(default)]
        price: f64,
        qty: f64,
    },
    #[serde(rename = "run_prefill")]
    RunPrefill { kind: PrefillKind },
    #[serde(rename = "start_stream")]
    StartStream {
        #[serde(default = "default_interval")]
        interval_ms: u64,
    },
    #[serde(rename = "stop_stream")]
    StopStream,
    #[serde(rename = "get_state")]
    GetState,
}

fn default_interval() -> u64 {
    500
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
enum ServerMsg {
    #[serde(rename = "publish_ack")]
    PublishAck {
        order_id: u64,
        side: Side,
        order_type: OrderType,
        tif: TimeInForce,
        price: f64,
        qty: f64,
        ok: bool,
        error: Option<String>,
        total_sent: u64,
        total_errors: u64,
    },
    #[serde(rename = "prefill_result")]
    PrefillResult {
        kind: PrefillKind,
        sent: usize,
        total: usize,
    },
    #[serde(rename = "stream_status")]
    StreamStatus { streaming: bool, interval_ms: u64 },
    #[serde(rename = "stats")]
    Stats {
        total_sent: u64,
        total_errors: u64,
        streaming: bool,
        interval_ms: u64,
    },
    #[serde(rename = "error")]
    Error { message: String },
}

fn broadcast(state: &Arc<AeronState>, msg: &ServerMsg) {
    if let Ok(json) = serde_json::to_string(msg) {
        let _ = state.tx.send(json);
    }
}

// Axum WebSocket handler

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AeronState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AeronState>) {
    let (mut sender, mut receiver) = socket.split();
    let mut rx = state.tx.subscribe();

    send_stats(&mut sender, &state).await;

    let mut send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if sender.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    let state_for_recv = state.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if let Message::Text(text) = msg {
                handle_message(&text, &state_for_recv).await;
            }
        }
    });

    tokio::select! {
        _ = &mut send_task => recv_task.abort(),
        _ = &mut recv_task => send_task.abort(),
    }
}

async fn send_stats(sender: &mut SplitSink<WebSocket, Message>, state: &Arc<AeronState>) {
    let msg = ServerMsg::Stats {
        total_sent: state.total_sent.load(Ordering::Relaxed),
        total_errors: state.total_errors.load(Ordering::Relaxed),
        streaming: state.streaming.load(Ordering::Relaxed),
        interval_ms: state.stream_interval_ms.load(Ordering::Relaxed),
    };
    if let Ok(json) = serde_json::to_string(&msg) {
        let _ = sender.send(Message::Text(json)).await;
    }
}

async fn handle_message(text: &str, state: &Arc<AeronState>) {
    let parsed: Result<ClientMsg, _> = serde_json::from_str(text);
    let msg = match parsed {
        Ok(m) => m,
        Err(e) => {
            broadcast(state, &ServerMsg::Error { message: format!("bad message: {e}") });
            return;
        }
    };

    match msg {
        ClientMsg::SubmitOrder { side, order_type, tif, price, qty } => {
            let order = build_order(state, side, order_type, tif, price, qty);
            if state.cmd_tx.send(PublishCmd::Submit(order)).is_err() {
                broadcast(state, &ServerMsg::Error { message: "publisher thread is not running".into() });
            }
        }
        ClientMsg::RunPrefill { kind } => {
            if state.cmd_tx.send(PublishCmd::Prefill(kind)).is_err() {
                broadcast(state, &ServerMsg::Error { message: "publisher thread is not running".into() });
            }
        }
        ClientMsg::StartStream { interval_ms } => {
            state.stream_interval_ms.store(interval_ms.max(20), Ordering::Relaxed);
            state.streaming.store(true, Ordering::Relaxed);
            broadcast(state, &ServerMsg::StreamStatus {
                streaming: true,
                interval_ms: state.stream_interval_ms.load(Ordering::Relaxed),
            });
        }
        ClientMsg::StopStream => {
            state.streaming.store(false, Ordering::Relaxed);
            broadcast(state, &ServerMsg::StreamStatus {
                streaming: false,
                interval_ms: state.stream_interval_ms.load(Ordering::Relaxed),
            });
        }
        ClientMsg::GetState => {
            broadcast(state, &ServerMsg::Stats {
                total_sent: state.total_sent.load(Ordering::Relaxed),
                total_errors: state.total_errors.load(Ordering::Relaxed),
                streaming: state.streaming.load(Ordering::Relaxed),
                interval_ms: state.stream_interval_ms.load(Ordering::Relaxed),
            });
        }
    }
}
