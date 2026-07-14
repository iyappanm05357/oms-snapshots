# Order Matching Engine — Rust + Replay/Validation + WebSocket UI

## Run it

```
cd matching-engine
cargo run
```

Then open http://localhost:8080 — the Rust server serves the frontend directly
(no separate frontend server / build step needed).

## What's inside

- **src/order.rs** — `Order`, `Trade`, and `EngineEvent` (the journal entry
  type: `Submit` or `Cancel`, each carrying a timestamp).
- **src/orderbook.rs** — The book itself: `BTreeMap<price, VecDeque<Order>>`
  for bids and asks, price-time priority matching, Limit/Market orders,
  GTC/IOC/FOK time-in-force.
- **src/engine.rs** — Wraps the book with an append-only `journal: Vec<EngineEvent>`.
  This is the key piece for replay: `replay_to(journal, target_time_ms)` takes
  the full event history, replays only the events with `timestamp <= target`
  into a **brand-new** OrderBook, and returns the reconstructed state plus a
  `ValidationReport`.
- **src/ws.rs** — Axum WebSocket endpoint (`/ws`) and the JSON message protocol.
- **src/demo.rs** — Seeds 24 scripted orders spread across the last 5 minutes
  so there's something to actually scrub through on the replay slider.
- **frontend/** — plain HTML/CSS/JS, no build tooling. Live order book,
  order entry, cancel, trade tape, and a replay panel with both a
  datetime picker and a slider bound to the journal's time range.

## Replay / validation, specifically

Every order submission and cancellation is journaled with its timestamp as
it happens. "Replay to a specific time" means: take the journal, keep only
events with `timestamp_ms <= target`, and re-run them in timestamp order
against a fresh book. This is a full deterministic re-derivation, not a diff
off the live book, so what you see is a genuine reconstruction of "what did
the book look like at time T."

The validation step checks structural invariants on the replayed result:
- book not crossed (`best_bid < best_ask`)
- no non-positive-quantity or empty price levels
- bid levels strictly descending / ask levels strictly ascending
- reports events replayed vs. skipped, trades produced, best bid/ask, total
  resting quantity on each side

If any invariant is violated the response includes `valid: false` and a list
of specific errors — rendered directly in the "Replay Result" panel.

## WebSocket protocol

Client → server (JSON, `type` tag):
```
{"type":"submit_order","side":"Buy","order_type":"Limit","tif":"GTC","price":100.5,"qty":10}
{"type":"cancel_order","order_id":5}
{"type":"replay","target_time_ms":1737000000000}
{"type":"get_state"}
{"type":"seed_demo"}
{"type":"reset"}
```

Server → client:
```
{"type":"book_snapshot","book":{"bids":[...],"asks":[...]}}
{"type":"order_ack","order_id":..,"accepted":..,"rejection_reason":..,"trades":[...],"resting_qty":..}
{"type":"cancel_ack","order_id":..,"found":..}
{"type":"journal_info","count":..,"earliest_ms":..,"latest_ms":..,"server_now_ms":..}
{"type":"replay_result","result":{"target_time_ms":..,"book":..,"trades":[...],"validation":{...}}}
{"type":"error","message":".."}
```

## Design notes

- Price levels use `BTreeMap` (not a radix tree or dense array) — sparse
  order books make BTreeMap the stronger practical default.
- Price/qty are `f64` for simplicity in this demo. For a production book
  you'd want fixed-point/integer ticks to avoid float comparison edge
  cases — `OrderedFloat` here is a stopgap, not a substitute for that.
- FOK does a liquidity pre-check before touching the book, so a rejected
  FOK order never partially mutates state.
- Built and smoke-tested in this environment: `cargo build` is clean
  (Rust 1.75), and the full WebSocket round trip — seed → submit → replay
  → validate — was exercised end to end with a scripted client and
  confirmed working.
