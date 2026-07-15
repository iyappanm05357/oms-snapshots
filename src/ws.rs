use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use chrono::Utc;
use futures::stream::{SplitSink, StreamExt};
use futures::SinkExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, Mutex};

use crate::engine::{replay_to, Engine};
use crate::order::{Order, OrderType, Side, TimeInForce};

pub struct AppState {
    pub engine: Mutex<Engine>,
    pub tx: broadcast::Sender<String>,
    /// Whether the background continuous-order generator is currently
    /// emitting orders. Toggled by "start_stream" / "stop_stream".
    pub streaming: AtomicBool,
    /// Delay between generated orders, in milliseconds. The generator
    /// task re-reads this every iteration, so changing it takes effect
    /// on the next tick without restarting the task.
    pub stream_interval_ms: AtomicU64,
}

impl AppState {
    pub fn new() -> Arc<Self> {
        let (tx, _rx) = broadcast::channel(1024);
        Arc::new(Self {
            engine: Mutex::new(Engine::new()),
            tx,
            streaming: AtomicBool::new(false),
            stream_interval_ms: AtomicU64::new(500),
        })
    }
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
    #[serde(rename = "cancel_order")]
    CancelOrder { order_id: u64 },
    #[serde(rename = "replay")]
    Replay {
        /// Epoch milliseconds to replay up to and including.
        target_time_ms: i64,
    },
    #[serde(rename = "get_state")]
    GetState,
    #[serde(rename = "seed_demo")]
    SeedDemo,
    #[serde(rename = "reset")]
    Reset,
    #[serde(rename = "start_stream")]
    StartStream {
        #[serde(default = "default_interval")]
        interval_ms: u64,
    },
    #[serde(rename = "stop_stream")]
    StopStream,
}

fn default_interval() -> u64 {
    500
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
enum ServerMsg<'a> {
    #[serde(rename = "book_snapshot")]
    BookSnapshot {
        book: &'a crate::orderbook::BookSnapshot,
    },
    #[serde(rename = "order_ack")]
    OrderAck {
        order_id: u64,
        accepted: bool,
        rejection_reason: Option<String>,
        trades: Vec<crate::order::Trade>,
        resting_qty: f64,
    },
    #[serde(rename = "cancel_ack")]
    CancelAck { order_id: u64, found: bool },
    #[serde(rename = "replay_result")]
    ReplayResult {
        result: &'a crate::engine::ReplayResult,
    },
    #[serde(rename = "journal_info")]
    JournalInfo {
        count: usize,
        earliest_ms: Option<i64>,
        latest_ms: Option<i64>,
        server_now_ms: i64,
    },
    #[serde(rename = "error")]
    Error { message: String },
    #[serde(rename = "stream_status")]
    StreamStatus { streaming: bool, interval_ms: u64 },
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let mut rx = state.tx.subscribe();

    // Send current state immediately on connect.
    {
        let engine = state.engine.lock().await;
        let snap = engine.snapshot();
        let _ = send_json(&mut sender, &ServerMsg::BookSnapshot { book: &snap }).await;
    }

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

async fn handle_message(text: &str, state: &Arc<AppState>) {
    let parsed: Result<ClientMsg, _> = serde_json::from_str(text);
    let msg = match parsed {
        Ok(m) => m,
        Err(e) => {
            broadcast_one(state, &ServerMsg::Error {
                message: format!("bad message: {e}"),
            });
            return;
        }
    };

    match msg {
        ClientMsg::SubmitOrder {
            side,
            order_type,
            tif,
            price,
            qty,
        } => {
            submit_and_broadcast(state, side, order_type, tif, price, qty).await;
        }
        ClientMsg::CancelOrder { order_id } => {
            let mut engine = state.engine.lock().await;
            let found = engine.cancel(order_id, Utc::now().timestamp_millis());
            let snap = engine.snapshot();
            drop(engine);

            broadcast_all(state, &ServerMsg::CancelAck { order_id, found });
            broadcast_all(state, &ServerMsg::BookSnapshot { book: &snap });
        }
        ClientMsg::Replay { target_time_ms } => {
            let engine = state.engine.lock().await;
            let result = replay_to(&engine.journal, target_time_ms);
            drop(engine);
            broadcast_one(state, &ServerMsg::ReplayResult { result: &result });
        }
        ClientMsg::GetState => {
            let engine = state.engine.lock().await;
            let snap = engine.snapshot();
            let count = engine.journal.len();
            let earliest_ms = engine.journal.iter().map(|e| e.timestamp_ms()).min();
            let latest_ms = engine.journal.iter().map(|e| e.timestamp_ms()).max();
            drop(engine);
            broadcast_one(state, &ServerMsg::BookSnapshot { book: &snap });
            broadcast_one(state, &ServerMsg::JournalInfo {
                count,
                earliest_ms,
                latest_ms,
                server_now_ms: Utc::now().timestamp_millis(),
            });
            broadcast_one(state, &ServerMsg::StreamStatus {
                streaming: state.streaming.load(Ordering::Relaxed),
                interval_ms: state.stream_interval_ms.load(Ordering::Relaxed),
            });
        }
        ClientMsg::SeedDemo => {
            let mut engine = state.engine.lock().await;
            crate::demo::seed(&mut engine);
            let snap = engine.snapshot();
            let count = engine.journal.len();
            let earliest_ms = engine.journal.iter().map(|e| e.timestamp_ms()).min();
            let latest_ms = engine.journal.iter().map(|e| e.timestamp_ms()).max();
            drop(engine);
            broadcast_all(state, &ServerMsg::BookSnapshot { book: &snap });
            broadcast_all(state, &ServerMsg::JournalInfo {
                count,
                earliest_ms,
                latest_ms,
                server_now_ms: Utc::now().timestamp_millis(),
            });
        }
        ClientMsg::Reset => {
            let mut engine = state.engine.lock().await;
            *engine = Engine::new();
            let snap = engine.snapshot();
            drop(engine);
            broadcast_all(state, &ServerMsg::BookSnapshot { book: &snap });
        }
        ClientMsg::StartStream { interval_ms } => {
            state.stream_interval_ms.store(interval_ms.max(50), Ordering::Relaxed);
            state.streaming.store(true, Ordering::Relaxed);
            broadcast_all(state, &ServerMsg::StreamStatus {
                streaming: true,
                interval_ms: state.stream_interval_ms.load(Ordering::Relaxed),
            });
        }
        ClientMsg::StopStream => {
            state.streaming.store(false, Ordering::Relaxed);
            broadcast_all(state, &ServerMsg::StreamStatus {
                streaming: false,
                interval_ms: state.stream_interval_ms.load(Ordering::Relaxed),
            });
        }
    }
}

/// Submits one order against the engine and broadcasts the result to
/// every connected client. Shared by the WebSocket message handler and
/// the continuous background order generator, so both paths produce
/// identical journal entries and identical live updates.
pub async fn submit_and_broadcast(
    state: &Arc<AppState>,
    side: Side,
    order_type: OrderType,
    tif: TimeInForce,
    price: f64,
    qty: f64,
) -> u64 {
    let mut engine = state.engine.lock().await;
    let id = engine.next_id();
    let order = Order {
        id,
        side,
        order_type,
        tif,
        price,
        qty,
        remaining_qty: qty,
        timestamp_ms: Utc::now().timestamp_millis(),
    };
    let result = engine.submit(order);
    let snap = engine.snapshot();
    drop(engine);

    broadcast_all(state, &ServerMsg::OrderAck {
        order_id: id,
        accepted: result.accepted,
        rejection_reason: result.rejection_reason,
        trades: result.trades,
        resting_qty: result.resting_qty,
    });
    broadcast_all(state, &ServerMsg::BookSnapshot { book: &snap });
    id
}

fn broadcast_all(state: &Arc<AppState>, msg: &ServerMsg) {
    if let Ok(json) = serde_json::to_string(msg) {
        let _ = state.tx.send(json);
    }
}

// Same as broadcast_all today (single shared channel); kept as a distinct
// name to make call sites express intent (reply-only vs fan-out) even
// though both currently go through the same broadcast channel.
fn broadcast_one(state: &Arc<AppState>, msg: &ServerMsg) {
    broadcast_all(state, msg)
}

async fn send_json(
    sender: &mut SplitSink<WebSocket, Message>,
    msg: &ServerMsg<'_>,
) -> Result<(), axum::Error> {
    if let Ok(json) = serde_json::to_string(msg) {
        sender.send(Message::Text(json)).await?;
    }
    Ok(())
}