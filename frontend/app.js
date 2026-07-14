const WS_URL = (location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/ws";

let ws;
let journalEarliest = null;
let journalLatest = null;

function connect() {
  ws = new WebSocket(WS_URL);

  ws.onopen = () => {
    setStatus(true);
    send({ type: "get_state" });
  };
  ws.onclose = () => {
    setStatus(false);
    setTimeout(connect, 1500);
  };
  ws.onerror = () => ws.close();

  ws.onmessage = (evt) => {
    const msg = JSON.parse(evt.data);
    handleServerMsg(msg);
  };
}

function setStatus(connected) {
  const el = document.getElementById("connStatus");
  el.textContent = connected ? "connected" : "reconnecting…";
  el.className = "status " + (connected ? "connected" : "disconnected");
}

function send(obj) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

// Server -> client message handling

function handleServerMsg(msg) {
  switch (msg.type) {
    case "book_snapshot":
      renderBook("asksBody", "bidsBody", "spread", msg.book);
      break;
    case "order_ack":
      logLine(
        msg.accepted
          ? `order #${msg.order_id} accepted — ${msg.trades.length} trade(s), resting ${msg.resting_qty}`
          : `order #${msg.order_id} REJECTED — ${msg.rejection_reason}`
      );
      msg.trades.forEach((t) => addTradeRow("tradeTape", t));
      break;
    case "cancel_ack":
      logLine(`cancel #${msg.order_id} — ${msg.found ? "removed" : "not found"}`);
      break;
    case "journal_info":
      journalEarliest = msg.earliest_ms;
      journalLatest = msg.latest_ms;
      renderJournalInfo(msg);
      break;
    case "replay_result":
      renderReplay(msg.result);
      break;
    case "stream_status":
      renderStreamStatus(msg);
      break;
    case "error":
      logLine("ERROR: " + msg.message);
      break;
  }
}

function renderStreamStatus(msg) {
  const tag = document.getElementById("streamTag");
  tag.classList.toggle("hidden", !msg.streaming);
  tag.className = "tag" + (msg.streaming ? " streaming" : " hidden");
  document.getElementById("streamInterval").value = msg.interval_ms;
}

// Rendering

function renderBook(asksId, bidsId, spreadId, book) {
  const asksBody = document.getElementById(asksId);
  const bidsBody = document.getElementById(bidsId);
  asksBody.innerHTML = "";
  bidsBody.innerHTML = "";

  // Asks displayed best (lowest) at the bottom, closest to the spread.
  [...book.asks].reverse().forEach((lvl) => {
    asksBody.insertAdjacentHTML(
      "beforeend",
      `<tr><td class="px">${lvl.price.toFixed(2)}</td><td>${lvl.qty.toFixed(2)}</td><td>${lvl.order_count}</td></tr>`
    );
  });
  book.bids.forEach((lvl) => {
    bidsBody.insertAdjacentHTML(
      "beforeend",
      `<tr><td class="px">${lvl.price.toFixed(2)}</td><td>${lvl.qty.toFixed(2)}</td><td>${lvl.order_count}</td></tr>`
    );
  });

  const spreadEl = document.getElementById(spreadId);
  if (book.asks.length && book.bids.length) {
    const spread = book.asks[0].price - book.bids[0].price;
    spreadEl.textContent = `spread: ${spread.toFixed(2)}`;
  } else {
    spreadEl.textContent = "spread: —";
  }
}

function addTradeRow(tbodyId, t) {
  const tbody = document.getElementById(tbodyId);
  const time = new Date(t.timestamp_ms).toLocaleTimeString();
  tbody.insertAdjacentHTML(
    "afterbegin",
    `<tr><td>${t.id}</td><td>${t.price.toFixed(2)}</td><td>${t.qty.toFixed(2)}</td><td>${t.maker_order_id}</td><td>${t.taker_order_id}</td><td>${time}</td></tr>`
  );
}

function renderJournalInfo(msg) {
  const el = document.getElementById("journalInfo");
  if (msg.count === 0) {
    el.textContent = "journal is empty — seed demo data or submit some orders";
  } else {
    const earliest = new Date(msg.earliest_ms).toLocaleString();
    const latest = new Date(msg.latest_ms).toLocaleString();
    el.textContent = `${msg.count} events journaled, spanning ${earliest} → ${latest}`;
  }

  // Wire the slider/datetime picker range to the journal's actual span.
  if (msg.earliest_ms != null && msg.latest_ms != null) {
    const slider = document.getElementById("replaySlider");
    slider.min = msg.earliest_ms;
    slider.max = msg.latest_ms;
    slider.value = msg.latest_ms;
    document.getElementById("replayDatetime").value = toLocalInputValue(msg.latest_ms);
  }
}

function renderReplay(result) {
  document.getElementById("replayTag").classList.remove("hidden");
  renderBook("replayAsksBody", "replayBidsBody", "replaySpread", result.book);

  const tbody = document.getElementById("replayTradeTape");
  tbody.innerHTML = "";
  result.trades.forEach((t) => addTradeRow("replayTradeTape", t));

  const v = result.validation;
  const box = document.getElementById("validationBox");
  box.className = "validation " + (v.valid ? "ok" : "bad");
  const targetStr = new Date(result.target_time_ms).toLocaleString();
  let html = `<div><b>${v.valid ? "VALID" : "INVALID"}</b> — replayed to ${targetStr}</div>`;
  html += `<div class="stats">events replayed: ${v.events_replayed}, skipped (after target): ${v.events_skipped}, trades produced: ${v.trades_produced}</div>`;
  html += `<div class="stats">best bid: ${v.best_bid != null ? v.best_bid.toFixed(2) : "—"} · best ask: ${v.best_ask != null ? v.best_ask.toFixed(2) : "—"}</div>`;
  html += `<div class="stats">total bid qty: ${v.total_bid_qty.toFixed(2)} · total ask qty: ${v.total_ask_qty.toFixed(2)}</div>`;
  if (v.errors.length) {
    html += "<ul>" + v.errors.map((e) => `<li>${e}</li>`).join("") + "</ul>";
  }
  box.innerHTML = html;

  document.getElementById("replayMeta").textContent =
    `Reconstructed book as of ${targetStr} from ${v.events_replayed} journaled event(s).`;
}

function logLine(text) {
  const el = document.getElementById("eventLog");
  const time = new Date().toLocaleTimeString();
  el.insertAdjacentHTML("afterbegin", `<div class="log-line"><b>${time}</b> — ${text}</div>`);
  while (el.children.length > 200) el.removeChild(el.lastChild);
}

function toLocalInputValue(ms) {
  const d = new Date(ms);
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

// UI wiring

document.getElementById("orderType").addEventListener("change", (e) => {
  document.getElementById("priceLabel").style.display = e.target.value === "Market" ? "none" : "flex";
});

document.getElementById("orderForm").addEventListener("submit", (e) => {
  e.preventDefault();
  send({
    type: "submit_order",
    side: document.getElementById("side").value,
    order_type: document.getElementById("orderType").value,
    tif: document.getElementById("tif").value,
    price: parseFloat(document.getElementById("price").value) || 0,
    qty: parseFloat(document.getElementById("qty").value),
  });
});

document.getElementById("cancelForm").addEventListener("submit", (e) => {
  e.preventDefault();
  const id = parseInt(document.getElementById("cancelId").value, 10);
  if (!Number.isNaN(id)) send({ type: "cancel_order", order_id: id });
});

document.getElementById("seedBtn").addEventListener("click", () => {
  send({ type: "seed_demo" });
  logLine("seeded 5 minutes of demo history");
});

document.getElementById("resetBtn").addEventListener("click", () => {
  if (confirm("Reset the engine? This clears the live book and the journal.")) {
    send({ type: "reset" });
    logLine("engine reset");
  }
});

document.getElementById("getStateBtn").addEventListener("click", () => send({ type: "get_state" }));

document.getElementById("startStreamBtn").addEventListener("click", () => {
  const interval = parseInt(document.getElementById("streamInterval").value, 10) || 500;
  send({ type: "start_stream", interval_ms: interval });
  logLine(`started continuous order stream (every ${interval}ms)`);
});

document.getElementById("stopStreamBtn").addEventListener("click", () => {
  send({ type: "stop_stream" });
  logLine("stopped continuous order stream");
});

document.getElementById("replayBtn").addEventListener("click", () => {
  const dtStr = document.getElementById("replayDatetime").value;
  if (!dtStr) return;
  const targetMs = new Date(dtStr).getTime();
  send({ type: "replay", target_time_ms: targetMs });
});

document.getElementById("replayNowBtn").addEventListener("click", () => {
  send({ type: "replay", target_time_ms: Date.now() });
});

document.getElementById("replaySlider").addEventListener("input", (e) => {
  const ms = parseInt(e.target.value, 10);
  document.getElementById("replayDatetime").value = toLocalInputValue(ms);
});

document.getElementById("replaySlider").addEventListener("change", (e) => {
  const ms = parseInt(e.target.value, 10);
  send({ type: "replay", target_time_ms: ms });
});

connect();