// NymBroker dashboard. Plain ES module, no dependencies: everything is built with the DOM API and textContent,
// never innerHTML, so message contents shown on the page cannot inject markup.

const SVG = "http://www.w3.org/2000/svg";
const reducedMotion = matchMedia("(prefers-reduced-motion: reduce)").matches;

const state = {
  overview: null,        // GET api/overview
  live: null,            // the latest snapshot
  points: new Map(),     // second (unix) -> metric point
  depth: new Map(),      // endpoint -> recent queue depths, kept while the page is open
  healthLog: [],         // health status changes seen while the page is open
  view: null             // { update(), dispose() } of the current view
};

// ── DOM helpers ───────────────────────────────────────────────────────────────────────────────────────

function h(tag, props, ...children) {
  const el = document.createElement(tag);
  setProps(el, props);
  append(el, children);
  return el;
}

function s(tag, props, ...children) {
  const el = document.createElementNS(SVG, tag);
  setProps(el, props);
  append(el, children);
  return el;
}

function setProps(el, props) {
  for (const [key, value] of Object.entries(props ?? {})) {
    if (value === null || value === undefined || value === false) continue;
    if (key === "class") el.setAttribute("class", value);
    else if (key.startsWith("on")) el.addEventListener(key.slice(2), value);
    else el.setAttribute(key, value === true ? "" : String(value));
  }
}

function append(el, children) {
  for (const child of children.flat(Infinity)) {
    if (child === null || child === undefined || child === false) continue;
    el.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
}

/** Appends children to a container, skipping null and false (the native append would write "null"). */
function mount(el, ...children) {
  append(el, children);
}

function clear(el) {
  while (el.firstChild) el.firstChild.remove();
  return el;
}

// ── Formatting ────────────────────────────────────────────────────────────────────────────────────────

const intFormat = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });
const oneDecimal = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 });
const timeFormat = new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "medium" });

function fmtCount(n) {
  return n === null || n === undefined ? "—" : intFormat.format(n);
}

function fmtRate(n) {
  if (!n) return "0";
  if (n >= 10000) return oneDecimal.format(n / 1000) + "k";
  if (n >= 100) return intFormat.format(n);
  return oneDecimal.format(n);
}

function fmtMs(ms) {
  if (ms === null || ms === undefined) return "—";
  if (ms < 1) return oneDecimal.format(ms * 1000) + " µs";
  if (ms < 1000) return oneDecimal.format(ms) + " ms";
  return oneDecimal.format(ms / 1000) + " s";
}

function fmtSeconds(seconds) {
  if (seconds === null || seconds === undefined) return "—";
  if (seconds < 1) return intFormat.format(seconds * 1000) + " ms";
  if (seconds < 120) return oneDecimal.format(seconds) + " s";
  if (seconds < 7200) return intFormat.format(seconds / 60) + " min";
  return oneDecimal.format(seconds / 3600) + " h";
}

function fmtRelative(iso) {
  if (!iso) return "—";
  const diff = (Date.parse(iso) - Date.now()) / 1000;
  const abs = Math.abs(diff);
  const text = abs < 1 ? "now" : fmtSeconds(abs);
  if (text === "now") return text;
  return diff > 0 ? "in " + text : text + " ago";
}

function fmtTime(iso) {
  return iso ? timeFormat.format(new Date(iso)) : "—";
}

/** "00:00:01.2345678" (a .NET TimeSpan) to milliseconds. */
function spanMs(span) {
  if (!span) return null;
  const match = /^(?:(\d+)\.)?(\d+):(\d+):(\d+(?:\.\d+)?)$/.exec(span);
  if (!match) return null;
  const [, days, hours, minutes, secs] = match;
  return (((Number(days ?? 0) * 24 + Number(hours)) * 60 + Number(minutes)) * 60 + Number(secs)) * 1000;
}

function healthClass(status) {
  return status === "Healthy" ? "ok" : status === "Degraded" ? "warn" : status === "Unhealthy" ? "bad" : "";
}

/** A <time> whose text the clock keeps up to date ("3 min ago"). */
function relTime(iso) {
  return h("time", { class: "reltime", datetime: iso ?? "", title: fmtTime(iso) }, fmtRelative(iso));
}

setInterval(() => {
  for (const el of document.querySelectorAll("time.reltime")) el.textContent = fmtRelative(el.getAttribute("datetime"));
}, 1000);

/** Counts a number up or down to its new value instead of jumping. */
function tween(el, value, format) {
  const from = el._value ?? 0;
  el._value = value;
  if (reducedMotion || value === null || value === undefined || !Number.isFinite(from) || from === value) {
    el.textContent = format(value);
    return;
  }
  const start = performance.now();
  const step = now => {
    if (el._value !== value) return;
    const t = Math.min(1, (now - start) / 450);
    const eased = 1 - Math.pow(1 - t, 3);
    el.textContent = format(from + (value - from) * eased);
    if (t < 1) requestAnimationFrame(step);
  };
  requestAnimationFrame(step);
}

// ── Data ──────────────────────────────────────────────────────────────────────────────────────────────

async function getJson(url) {
  const response = await fetch(url, { headers: { Accept: "application/json" } });
  const body = await response.json().catch(() => null);
  if (!response.ok) throw new Error(body?.error ?? `${response.status} ${response.statusText}`);
  return body;
}

function addPoints(points) {
  for (const point of points ?? []) state.points.set(point.t, point);
  const newest = Math.max(...state.points.keys());
  const oldest = newest - (state.overview?.historySeconds ?? 900);
  for (const t of state.points.keys()) if (t <= oldest) state.points.delete(t);
}

/** The last `seconds` seconds of history, one entry per second (zeros where nothing happened). */
function series(seconds, pick) {
  const points = [...state.points.values()];
  const newest = points.length ? Math.max(...points.map(p => p.t)) : Math.floor(Date.now() / 1000);
  const values = [];
  for (let t = newest - seconds + 1; t <= newest; t++) {
    const point = state.points.get(t);
    values.push(point ? pick(point) ?? 0 : 0);
  }
  return values;
}

function applyLive(live) {
  state.live = live;
  addPoints(live.points);
  for (const endpoint of live.endpoints) {
    if (!endpoint.counts) continue;
    const list = state.depth.get(endpoint.name) ?? [];
    list.push(endpoint.counts.pending + endpoint.counts.inProgress);
    if (list.length > 180) list.shift();
    state.depth.set(endpoint.name, list);
  }
  recordHealth(live.health);
  renderStatus();
  state.view?.update?.();
}

function recordHealth(health) {
  if (!health) return;
  const last = state.healthLog[state.healthLog.length - 1];
  if (last?.status === health.report.status) return;
  state.healthLog.push({ status: health.report.status, at: health.checkedAt, message: health.report.message });
}

function liveEndpoint(name) {
  return state.live?.endpoints.find(e => e.name === name);
}

function endpointHealth(name) {
  return state.live?.health?.report.endpoints.find(e => e.name === name);
}

function connect() {
  const indicator = document.getElementById("live");
  const label = indicator.querySelector("span");
  const source = new EventSource("api/live");
  source.addEventListener("snapshot", event => {
    indicator.className = "live on";
    label.textContent = "Live";
    applyLive(JSON.parse(event.data));
  });
  source.addEventListener("error", () => {
    indicator.className = "live off";
    label.textContent = "Reconnecting";
  });
}

// ── Chrome: title, status, theme, tabs ────────────────────────────────────────────────────────────────

function renderStatus() {
  const pill = document.getElementById("broker-status");
  const live = state.live;
  if (!live) return;
  const status = live.health?.report.status;
  pill.className = "pill " + (live.started ? healthClass(status) : "bad");
  pill.textContent = !live.started ? "Stopped" : status ? `Running · ${status}` : "Running";
}

function setupTheme() {
  const root = document.documentElement;
  const stored = (() => { try { return localStorage.getItem("nymbroker-theme"); } catch { return null; } })();
  const system = matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark";
  root.dataset.theme = stored ?? system;
  document.getElementById("theme").addEventListener("click", () => {
    root.dataset.theme = root.dataset.theme === "light" ? "dark" : "light";
    try { localStorage.setItem("nymbroker-theme", root.dataset.theme); } catch { /* private mode: not remembered */ }
  });
}

function markTab(name) {
  for (const tab of document.querySelectorAll(".tabs a")) tab.classList.toggle("active", tab.dataset.tab === name);
}

// ── Small charts ──────────────────────────────────────────────────────────────────────────────────────

/** A shared gradient for sparkline areas; defined once in a hidden SVG. */
function defineGradients() {
  const defs = s("svg", { width: 0, height: 0, "aria-hidden": "true", class: "defs" },
    s("defs", null,
      gradient("spark-fill", "var(--accent)", 0.28),
      gradient("chart-fill", "var(--accent)", 0.32)));
  defs.style.position = "absolute";
  document.body.append(defs);
}

function gradient(id, color, opacity) {
  return s("linearGradient", { id, x1: 0, y1: 0, x2: 0, y2: 1 },
    s("stop", { offset: "0%", "stop-color": color, "stop-opacity": opacity }),
    s("stop", { offset: "100%", "stop-color": color, "stop-opacity": 0 }));
}

function sparkline(values, variant) {
  const width = 300, height = 54;
  const max = Math.max(1e-9, ...values);
  const step = values.length > 1 ? width / (values.length - 1) : width;
  const coords = values.map((v, i) => [i * step, height - 4 - (v / max) * (height - 10)]);
  const line = coords.map(([x, y], i) => `${i ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`).join("");
  const svg = s("svg", { class: "spark " + (variant ?? ""), viewBox: `0 0 ${width} ${height}`, preserveAspectRatio: "none", "aria-hidden": "true" });
  if (values.length > 1) {
    svg.append(s("path", { class: "area", d: `${line}L${width},${height}L0,${height}Z` }), s("path", { class: "line", d: line }));
  }
  return svg;
}

function depthBar(counts) {
  const total = counts.pending + counts.inProgress + counts.deferred + counts.failed;
  const bar = h("div", { class: "depth-bar", title: `${fmtCount(counts.pending)} pending, ${fmtCount(counts.inProgress)} in progress, ${fmtCount(counts.deferred)} deferred, ${fmtCount(counts.failed)} failed` });
  for (const [cls, value] of [["pending", counts.pending], ["inprogress", counts.inProgress], ["deferred", counts.deferred], ["failed", counts.failed]]) {
    if (!value) continue;
    const span = h("span", { class: cls });
    span.style.width = `${(value / total) * 100}%`;
    bar.append(span);
  }
  return bar;
}

// ── Overview ──────────────────────────────────────────────────────────────────────────────────────────

function overviewView(root) {
  markTab("overview");
  const tiles = {
    throughput: tile("Throughput", "msg/s"),
    latency: tile("Processing p99", ""),
    age: tile("Oldest message", ""),
    failures: tile("Failures · history", "")
  };
  const flow = h("div", { class: "card-body" });
  const chart = throughputChart();
  const queues = h("div", { class: "card-body" });
  const config = h("div", { class: "card-body" });

  mount(root, 
    h("div", { class: "tiles" }, Object.values(tiles).map(t => t.el)),
    h("section", { class: "card" },
      h("div", { class: "card-head" }, h("h2", null, "Message flow"), h("span", { class: "spacer" }), legend()),
      flow),
    h("div", { class: "grid-2" },
      h("section", { class: "card" }, h("div", { class: "card-head" }, h("h2", null, "Traffic"), h("span", { class: "spacer" }), chart.legend), h("div", { class: "card-body" }, chart.el)),
      h("section", { class: "card" }, h("div", { class: "card-head" }, h("h2", null, "Queues")), queues)),
    h("section", { class: "card" }, h("div", { class: "card-head" }, h("h2", null, "Broker settings")), config));

  const map = flowMap(state.overview.broker);
  flow.append(map.el);
  config.append(settingsList(state.overview.broker));

  const update = () => {
    const rates = state.live.rates;
    tween(tiles.throughput.value, rates.received, fmtRate);
    tiles.throughput.sub.textContent = `${fmtRate(rates.consumed)}/s consumed · ${fmtRate(rates.routed)}/s routed`;
    tiles.throughput.setSpark(series(300, p => p.received));

    tiles.latency.value.textContent = rates.p99Ms === undefined || rates.p99Ms === null ? "idle" : fmtMs(rates.p99Ms);
    tiles.latency.sub.textContent = rates.p50Ms === undefined || rates.p50Ms === null ? "No messages in the last 5 s" : `p50 ${fmtMs(rates.p50Ms)} · last 5 s`;
    tiles.latency.setSpark(series(300, p => p.p99Ms), "teal");

    tiles.age.value.textContent = rates.maxAgeSeconds === undefined || rates.maxAgeSeconds === null ? "idle" : fmtSeconds(rates.maxAgeSeconds);
    tiles.age.sub.textContent = "Time from posting to processing";
    tiles.age.setSpark(series(300, p => p.maxAgeSeconds), "teal");

    const failed = series(state.overview.historySeconds, p => p.failed + p.deadLettered);
    const total = failed.reduce((a, b) => a + b, 0);
    tween(tiles.failures.value, total, fmtCount);
    tiles.failures.sub.textContent = `${fmtRate(rates.failed)}/s now · ${fmtRate(rates.retries)}/s retries`;
    tiles.failures.el.classList.toggle("alert", series(60, p => p.failed + p.deadLettered).some(v => v > 0));
    tiles.failures.setSpark(series(300, p => p.failed + p.deadLettered), "bad");

    map.update();
    chart.update();
    renderQueues(queues);
  };
  update();
  return { update, dispose: chart.dispose };
}

function tile(label, unit) {
  const value = h("strong", null, "—");
  const sub = h("div", { class: "tile-sub" });
  const spark = h("div");
  const el = h("section", { class: "card tile" },
    h("div", { class: "tile-label" }, label),
    h("div", { class: "tile-value" }, value, unit ? h("span", null, unit) : null),
    h("div", null, sub, spark));
  return { el, value, sub, setSpark: (values, variant) => clear(spark).append(sparkline(values, variant)) };
}

function legend() {
  return h("div", { class: "legend" },
    h("span", { class: "in" }, h("i"), "received"),
    h("span", { class: "route" }, h("i"), "route"),
    h("span", { class: "topic" }, h("i"), "topic"),
    h("span", { class: "tap" }, h("i"), "wire tap"),
    h("span", { class: "dead" }, h("i"), "dead letter"));
}

function renderQueues(container) {
  clear(container);
  const inspectable = state.live.endpoints.filter(e => e.counts || e.countError);
  if (!inspectable.length) {
    container.append(h("div", { class: "empty" }, "No inspectable endpoints. SQLite, PostgreSQL, SQL Server and MySQL queues can be counted."));
    return;
  }
  const rows = inspectable.map(e => h("tr", { class: "link", onclick: () => go(`#/endpoints/${encodeURIComponent(e.name)}`) },
    h("td", null, e.name),
    e.counts
      ? [h("td", { class: "num" }, fmtCount(e.counts.active)), h("td", null, depthBar(e.counts))]
      : h("td", { class: "error", colspan: 2, title: e.countError }, e.countError)));
  container.append(h("div", { class: "table-wrap" }, h("table", null,
    h("thead", null, h("tr", null, h("th", null, "Endpoint"), h("th", { class: "num" }, "Active"), h("th", null, "Depth"))),
    h("tbody", null, rows))));
}

function settingsList(broker) {
  const f = broker.features;
  const items = [
    ["Dead-letter endpoint", f.deadLetterEndpoint ?? "— (transports dead-letter natively)"],
    ["Wire taps", f.wireTaps.length ? f.wireTaps.join(", ") : "—"],
    ["Duplicate check", f.idempotencyStore ?? "off"],
    ["Expire messages after", f.maxMessageAge ?? "never"],
    ["Split compression", f.compressor],
    ["Filters", fmtCount(f.filters)],
    ["Consumers", fmtCount(broker.consumers.length)],
    ["Scheduled actions", fmtCount(broker.scheduledActions.length)]
  ];
  return h("dl", { class: "kv" }, items.map(([k, v]) => [h("dt", null, k), h("dd", null, v)]));
}

// ── Flow map ──────────────────────────────────────────────────────────────────────────────────────────

function flowMap(broker) {
  const nodeW = 210, nodeH = 54, gap = 14, hubW = 230, colGap = 120;
  const left = broker.endpoints.filter(e => e.listens);
  const right = broker.endpoints.filter(e => !e.listens);
  const rows = Math.max(left.length, right.length, 2);
  const height = 40 + rows * (nodeH + gap);
  const width = nodeW * 2 + hubW + colGap * 2;
  const hub = { x: nodeW + colGap, y: Math.max(30, height / 2 - 50), w: hubW, h: 100 };
  const position = new Map();
  left.forEach((e, i) => position.set(e.name, { x: 0, y: 30 + i * (nodeH + gap), side: "left" }));
  right.forEach((e, i) => position.set(e.name, { x: width - nodeW, y: 30 + i * (nodeH + gap), side: "right" }));

  const svg = s("svg", { class: "flow", viewBox: `0 0 ${width} ${height}`, role: "img", "aria-label": "Message flow between endpoints" });
  svg.append(
    s("text", { class: "column-label", x: 0, y: 14 }, "RECEIVING"),
    s("text", { class: "column-label", x: width - nodeW, y: 14 }, "SENDING ONLY"));

  const edges = s("g");
  const nodes = s("g");
  svg.append(edges, nodes);

  const edgeRefs = [];
  for (const e of left) {
    const p = position.get(e.name);
    const path = curve(p.x + nodeW, p.y + nodeH / 2, hub.x, hub.y + hub.h / 2);
    const el = s("path", { class: "edge in", d: path }, s("title", null, `${e.name} → broker`));
    edges.append(el);
    edgeRefs.push({ el, rate: () => liveEndpoint(e.name)?.rates.received ?? 0 });
  }
  for (const target of outbound(broker)) {
    const p = position.get(target.name);
    if (!p) continue;
    const toLeft = p.side === "left";
    const path = toLeft
      ? loop(hub.x + hub.w / 2, hub.y + hub.h, p.x + nodeW, p.y + nodeH / 2 + 8)
      : curve(hub.x + hub.w, hub.y + hub.h / 2, p.x, p.y + nodeH / 2);
    const el = s("path", { class: `edge ${target.kind}`, d: path }, s("title", null, `broker → ${target.name} (${target.label})`));
    edges.append(el);
    edgeRefs.push({ el, rate: target.rate });
  }

  nodes.append(hubNode(hub, broker));
  const nodeRefs = broker.endpoints.map(e => endpointNode(e, position.get(e.name), nodeW, nodeH));
  nodes.append(...nodeRefs.map(n => n.el));

  const update = () => {
    for (const ref of edgeRefs) {
      const rate = ref.rate();
      ref.el.classList.toggle("flowing", rate > 0);
      ref.el.style.setProperty("--speed", `${Math.max(0.35, 2.2 - Math.log10(1 + rate) * 0.55).toFixed(2)}s`);
    }
    for (const ref of nodeRefs) ref.update();
  };
  return { el: svg, update };
}

/** Every edge out of the broker, one per destination and kind. */
function outbound(broker) {
  const rates = () => state.live?.rates ?? {};
  const seen = new Set();
  const targets = [];
  const add = (name, kind, label, rate) => {
    const key = kind + "|" + name;
    if (!name || seen.has(key)) return;
    seen.add(key);
    targets.push({ name, kind, label, rate });
  };
  for (const r of broker.routes) add(r.destination, "route", `route for ${r.messageType}`, () => rates().routed ?? 0);
  for (const t of broker.topics) for (const name of t.subscriberEndpoints) add(name, "topic", `topic ${t.name}`, () => rates().routed ?? 0);
  for (const name of broker.features.wireTaps) add(name, "tap", "wire tap", () => rates().received ?? 0);
  add(broker.features.deadLetterEndpoint, "dead", "dead letter", () => rates().deadLettered ?? 0);
  return targets;
}

function curve(x1, y1, x2, y2) {
  const mid = (x1 + x2) / 2;
  return `M${x1},${y1} C${mid},${y1} ${mid},${y2} ${x2},${y2}`;
}

function loop(x1, y1, x2, y2) {
  return `M${x1},${y1} C${x1},${y1 + 60} ${x2 + 70},${y2} ${x2},${y2}`;
}

function hubNode(hub, broker) {
  const detail = `${broker.consumers.length} consumers · ${broker.topics.length} topics · ${broker.routes.length} routes`;
  return s("g", { class: "hub" },
    s("rect", { x: hub.x, y: hub.y, width: hub.w, height: hub.h, rx: 16 }),
    s("text", { class: "title", x: hub.x + hub.w / 2, y: hub.y + 44 }, "NymBroker"),
    s("text", { class: "detail", x: hub.x + hub.w / 2, y: hub.y + 66 }, detail));
}

function endpointNode(endpoint, p, w, hgt) {
  const health = s("circle", { class: "health", cx: p.x + 16, cy: p.y + 19, r: 4.5 });
  const depth = s("text", { class: "depth", x: p.x + w - 12, y: p.y + 40 });
  const badge = s("g", { class: "badge hidden" }, s("circle", { cx: p.x + w - 4, cy: p.y + 4, r: 10 }), s("text", { x: p.x + w - 4, y: p.y + 7.5 }));
  const tooltip = s("title");
  const g = s("g", { class: "node" + (endpoint.inspectable ? " link" : "") },
    tooltip,
    s("rect", { class: "box", x: p.x, y: p.y, width: w, height: hgt, rx: 11 }),
    health,
    s("text", { class: "name", x: p.x + 28, y: p.y + 23 }, truncate(endpoint.name, 22)),
    s("text", { class: "kind", x: p.x + 28, y: p.y + 40 }, `${endpoint.kind} · ${modeText(endpoint.mode)}`),
    depth,
    badge);
  if (endpoint.inspectable) g.addEventListener("click", () => go(`#/endpoints/${encodeURIComponent(endpoint.name)}`));

  const update = () => {
    const status = endpointHealth(endpoint.name);
    g.setAttribute("class", `node ${healthClass(status?.status)}${endpoint.inspectable ? " link" : ""}`);
    const live = liveEndpoint(endpoint.name);
    const counts = live?.counts;
    depth.textContent = counts ? `${fmtCount(counts.active)} active` : live?.rates.received ? `${fmtRate(live.rates.received)}/s` : "";
    const failed = counts?.failed ?? 0;
    badge.classList.toggle("hidden", failed === 0);
    badge.lastChild.textContent = failed > 99 ? "99+" : String(failed);
    tooltip.textContent = `${endpoint.name} (${endpoint.kind})\n${status ? `${status.status}${status.message ? ": " + status.message : ""}` : "Health not checked yet"}`;
  };
  return { el: g, update };
}

function modeText(mode) {
  return mode === "ReadWrite" ? "read/write" : mode === "ReadOnly" ? "read only" : "write only";
}

function truncate(text, max) {
  return text.length > max ? text.slice(0, max - 1) + "…" : text;
}

// ── Traffic chart ─────────────────────────────────────────────────────────────────────────────────────

function throughputChart() {
  const wrap = h("div", { class: "chart-wrap" });
  const tooltip = h("div", { class: "tooltip hidden" });
  let svg = null;
  let hover = null;

  const draw = () => {
    const seconds = state.overview.historySeconds;
    const received = series(seconds, p => p.received);
    const consumed = series(seconds, p => p.consumed);
    const failures = series(seconds, p => p.failed + p.deadLettered);
    const width = Math.max(320, wrap.clientWidth || 600), height = 230, pad = { l: 40, r: 8, t: 10, b: 22 };
    const plotW = width - pad.l - pad.r, plotH = height - pad.t - pad.b;
    const max = niceMax(Math.max(1, ...received, ...consumed, ...failures));
    const x = i => pad.l + (i / Math.max(1, seconds - 1)) * plotW;
    const y = v => pad.t + plotH - (v / max) * plotH;

    const next = s("svg", { class: "chart", viewBox: `0 0 ${width} ${height}`, "aria-label": "Messages per second" });
    for (let i = 0; i <= 4; i++) {
      const v = (max / 4) * i;
      next.append(s("line", { class: "grid", x1: pad.l, x2: width - pad.r, y1: y(v), y2: y(v) }),
        s("text", { class: "axis", x: pad.l - 6, y: y(v) + 4, "text-anchor": "end" }, fmtRate(v)));
    }
    for (const minutes of [15, 10, 5, 0]) {
      const i = seconds - 1 - minutes * 60;
      if (i < 0) continue;
      next.append(s("text", { class: "axis", x: x(i), y: height - 6, "text-anchor": minutes ? "middle" : "end" }, minutes ? `−${minutes} min` : "now"));
    }
    const barW = Math.max(1, plotW / seconds);
    failures.forEach((v, i) => { if (v > 0) next.append(s("rect", { class: "failure", x: x(i) - barW / 2, y: y(v), width: barW, height: pad.t + plotH - y(v) })); });
    const line = values => values.map((v, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join("");
    next.append(s("path", { class: "received", d: `${line(received)}L${x(seconds - 1)},${y(0)}L${x(0)},${y(0)}Z` }),
      s("path", { class: "consumed", d: line(consumed) }));
    const cursor = s("line", { class: "cursor hidden", y1: pad.t, y2: pad.t + plotH });
    next.append(cursor);

    next.addEventListener("mousemove", event => {
      const box = next.getBoundingClientRect();
      const px = ((event.clientX - box.left) / box.width) * width;
      const i = Math.round(((px - pad.l) / plotW) * (seconds - 1));
      if (i < 0 || i >= seconds) return hideTip();
      hover = { i, received: received[i], consumed: consumed[i], failures: failures[i], ago: seconds - 1 - i };
      cursor.setAttribute("x1", x(i));
      cursor.setAttribute("x2", x(i));
      cursor.classList.remove("hidden");
      showTip(event.clientX - wrap.getBoundingClientRect().left);
    });
    next.addEventListener("mouseleave", () => { cursor.classList.add("hidden"); hideTip(); });

    if (svg) svg.replaceWith(next); else wrap.append(next);
    svg = next;
  };

  const showTip = left => {
    clear(tooltip).append(
      h("b", null, hover.ago ? `${fmtSeconds(hover.ago)} ago` : "now"),
      h("div", { class: "row" }, h("span", { class: "muted" }, "received"), h("span", { class: "num" }, fmtCount(hover.received))),
      h("div", { class: "row" }, h("span", { class: "muted" }, "consumed"), h("span", { class: "num" }, fmtCount(hover.consumed))),
      h("div", { class: "row" }, h("span", { class: "muted" }, "failed"), h("span", { class: "num" }, fmtCount(hover.failures))));
    tooltip.classList.remove("hidden");
    tooltip.style.left = `${Math.min(left + 12, wrap.clientWidth - tooltip.offsetWidth - 4)}px`;
  };
  const hideTip = () => tooltip.classList.add("hidden");

  wrap.append(tooltip);
  const observer = new ResizeObserver(() => draw());
  observer.observe(wrap);
  const legendEl = h("div", { class: "legend" },
    h("span", { class: "route" }, h("i"), "received/s"),
    h("span", { class: "in" }, h("i"), "consumed/s"),
    h("span", { class: "dead" }, h("i"), "failed/s"));
  return { el: wrap, legend: legendEl, update: draw, dispose: () => observer.disconnect() };
}

function niceMax(value) {
  const exponent = Math.pow(10, Math.floor(Math.log10(value)));
  for (const m of [1, 2, 2.5, 5, 10]) if (value <= m * exponent) return m * exponent;
  return 10 * exponent;
}

// ── Endpoints ─────────────────────────────────────────────────────────────────────────────────────────

function endpointsView(root) {
  markTab("endpoints");
  const body = h("tbody");
  mount(root, 
    h("h1", { class: "page-title" }, "Endpoints", h("span", { class: "muted" }, `${state.overview.broker.endpoints.length} registered`)),
    h("section", { class: "card" }, h("div", { class: "table-wrap" }, h("table", null,
      h("thead", null, h("tr", null,
        h("th", null, "Endpoint"), h("th", null, "Health"), h("th", { class: "num" }, "In/s"), h("th", { class: "num" }, "Failed/s"),
        h("th", { class: "num" }, "Pending"), h("th", { class: "num" }, "In progress"), h("th", { class: "num" }, "Deferred"),
        h("th", { class: "num" }, "Failed"), h("th", null, "Depth (since opened)"))),
      body))));

  const update = () => {
    clear(body).append(...state.overview.broker.endpoints.map(endpointRow));
  };
  update();
  return { update };
}

function endpointRow(endpoint) {
  const live = liveEndpoint(endpoint.name);
  const health = endpointHealth(endpoint.name);
  const counts = live?.counts;
  const notCounted = endpoint.inspectable
    ? h("td", { class: "error", colspan: 4, title: live?.countError ?? "" }, live?.countError ?? "Counting…")
    : h("td", { class: "faint", colspan: 4, title: "This transport cannot list or count its messages without receiving them." }, "not inspectable");
  const row = h("tr", { class: endpoint.inspectable ? "link" : null },
    h("td", null, h("strong", null, endpoint.name), h("span", { class: "sub" }, `${endpoint.kind} · ${modeText(endpoint.mode)}${endpoint.isCritical ? "" : " · non-critical"}`)),
    h("td", { title: health?.message ?? "" }, h("span", { class: "dot " + healthClass(health?.status) }), health?.status ?? "—"),
    h("td", { class: "num" }, live ? fmtRate(live.rates.received) : "—"),
    h("td", { class: "num" }, live ? fmtRate(live.rates.failed + live.rates.deadLettered) : "—"),
    counts
      ? [h("td", { class: "num" }, fmtCount(counts.pending)), h("td", { class: "num" }, fmtCount(counts.inProgress)),
         h("td", { class: "num" }, fmtCount(counts.deferred)), h("td", { class: "num" + (counts.failed ? " error" : "") }, fmtCount(counts.failed))]
      : notCounted,
    h("td", null, state.depth.has(endpoint.name) ? sparkline(state.depth.get(endpoint.name)) : ""));
  if (endpoint.inspectable) row.addEventListener("click", () => go(`#/endpoints/${encodeURIComponent(endpoint.name)}`));
  return row;
}

// ── Queue of one endpoint ─────────────────────────────────────────────────────────────────────────────

const states = [
  ["Pending", "pending"], ["InProgress", "inProgress"], ["Deferred", "deferred"], ["Failed", "failed"]
];

function queueView(root, name) {
  markTab("endpoints");
  const endpoint = state.overview.broker.endpoints.find(e => e.name === name);
  if (!endpoint) {
    mount(root, errorState("No such endpoint", `There is no endpoint named “${name}”.`));
    return {};
  }
  let current = "Failed";
  let cursor = null;
  let failedSince = null;
  const stateButtons = new Map();
  const tableBody = h("tbody");
  const more = h("button", { class: "button hidden", type: "button", onclick: () => load(false) }, "Load more");
  const since = h("input", { type: "datetime-local", "aria-label": "Failed since", onchange: e => { failedSince = e.target.value ? new Date(e.target.value).toISOString() : null; load(true); } });
  const sinceLabel = h("label", { class: "muted" }, "Failed since ", since);
  const completed = h("strong", null, "—");

  mount(root, 
    h("a", { class: "back", href: "#/endpoints" }, "← Endpoints"),
    h("h1", { class: "page-title" }, endpoint.name, h("span", { class: "muted" }, `${endpoint.kind} queue`)),
    h("div", { class: "chips" }, endpointChips(endpoint)),
    endpoint.inspectable ? null : h("div", { class: "notice" }, "This transport cannot list its messages without receiving them, so there is nothing to show here."),
    h("div", { class: "states" },
      states.map(([key]) => {
        const value = h("strong", null, "—");
        const button = h("button", { class: "state" + (key === "Failed" ? " failed" : ""), type: "button", onclick: () => { current = key; load(true); } },
          h("small", null, key === "InProgress" ? "In progress" : key), value);
        stateButtons.set(key, { button, value });
        return button;
      }),
      h("div", { class: "state static" }, h("small", null, "Completed"), completed)),
    state.overview.showPayloads ? null : h("div", { class: "notice" }, "Payloads are hidden. They are shown in Development, or with ShowPayloads = true (and RedactPayload to mask personal data)."),
    h("section", { class: "card" },
      h("div", { class: "card-head" }, h("h2", null, "Messages"), h("span", { class: "spacer" }), sinceLabel,
        h("button", { class: "button", type: "button", onclick: () => load(true) }, "Refresh")),
      h("div", { class: "card-body" },
        h("div", { class: "table-wrap" }, h("table", null,
          h("thead", null, h("tr", null, h("th", null, "Message"), h("th", null, "Created"), h("th", { class: "num" }, "Attempts"), h("th", null, "Due / failed"), h("th", null, "Last error"))),
          tableBody)),
        h("div", { class: "toolbar" }, more))));

  async function load(reset) {
    if (reset) { cursor = null; clear(tableBody); }
    for (const [key, ref] of stateButtons) ref.button.classList.toggle("active", key === current);
    sinceLabel.classList.toggle("hidden", current !== "Failed");
    more.disabled = true;
    const params = new URLSearchParams({ state: current, limit: "50" });
    if (cursor) params.set("cursor", cursor);
    if (failedSince && current === "Failed") params.set("failedSince", failedSince);
    try {
      const page = await getJson(`api/endpoints/${encodeURIComponent(name)}/messages?${params}`);
      cursor = page.nextCursor ?? null;
      if (reset && !page.messages.length) tableBody.append(h("tr", null, h("td", { class: "empty", colspan: 5 }, `No ${current === "InProgress" ? "in-progress" : current.toLowerCase()} messages.`)));
      tableBody.append(...page.messages.map(m => messageRow(m, endpoint)));
      more.classList.toggle("hidden", !cursor);
    } catch (error) {
      tableBody.append(h("tr", null, h("td", { class: "error", colspan: 5 }, error.message)));
    } finally {
      more.disabled = false;
    }
  }

  const update = () => {
    const counts = liveEndpoint(name)?.counts;
    for (const [key, field] of states) {
      const ref = stateButtons.get(key);
      if (counts) tween(ref.value, counts[field], fmtCount);
    }
    if (counts) tween(completed, counts.completed, fmtCount);
  };
  update();
  if (endpoint.inspectable) load(true);
  return { update };
}

function endpointChips(endpoint) {
  const health = endpointHealth(endpoint.name);
  return [
    h("span", { class: "chip" }, h("span", { class: "dot " + healthClass(health?.status) }), health?.status ?? "health unknown"),
    h("span", { class: "chip" }, modeText(endpoint.mode)),
    endpoint.listens ? h("span", { class: "chip accent" }, "receiving") : null,
    endpoint.usesNativeDeadLetter ? h("span", { class: "chip" }, "native dead-letter") : null,
    endpoint.canDefer ? h("span", { class: "chip" }, "defers in transport") : null,
    endpoint.isCritical ? null : h("span", { class: "chip" }, "non-critical"),
    endpoint.hasInputTransformer ? h("span", { class: "chip" }, "input transformer") : null,
    endpoint.hasOutputTransformer ? h("span", { class: "chip" }, "output transformer") : null
  ];
}

function messageRow(message, endpoint) {
  const env = message.envelope;
  const due = message.state === "Failed" ? message.failedAt : message.deliverAt;
  const row = h("tr", { class: "link", onclick: () => openMessage(message, endpoint) },
    h("td", null, h("span", { class: "mono" }, truncate(env?.messageType ?? "(not an envelope)", 40)), h("span", { class: "sub mono" }, truncate(message.id, 36))),
    h("td", null, relTime(message.createdAt)),
    h("td", { class: "num" }, fmtCount(message.attempts)),
    h("td", null, due ? relTime(due) : "—"),
    h("td", { class: "error", title: message.lastError ?? "" }, message.lastError ?? ""));
  return row;
}

function openMessage(message, endpoint) {
  const env = message.envelope;
  const close = () => { backdrop.remove(); drawer.remove(); document.removeEventListener("keydown", onKey); };
  const onKey = e => { if (e.key === "Escape") close(); };
  const backdrop = h("div", { class: "drawer-backdrop", onclick: close });
  const sections = [
    section("Queue", kv([
      ["Endpoint", endpoint.name], ["Row id", message.id], ["State", message.state], ["Attempts", fmtCount(message.attempts)],
      ["Created", fmtTime(message.createdAt)], ["Deliver at", message.deliverAt ? fmtTime(message.deliverAt) : null],
      ["Failed at", message.failedAt ? fmtTime(message.failedAt) : null], ["Size", `${fmtCount(message.size)} bytes`]])),
    env ? section("Envelope", kv([
      ["Message type", env.messageType], ["Message id", env.id], ["Correlation id", env.correlationId], ["Created", env.created],
      ["From", env.from], ["To", env.to], ["Trace parent", env.traceParent]])) : null,
    env?.deadLetter ? section("Dead letter", kv([
      ["Reason", env.deadLetter.reason], ["Description", env.deadLetter.description], ["Exception", env.deadLetter.exceptionType],
      ["Source endpoint", env.deadLetter.sourceEndpoint], ["Dead-lettered at", env.deadLetter.deadLetteredAt],
      ["Delivery count", env.deadLetter.deliveryCount]])) : null,
    message.lastError ? section("Last error", h("pre", { class: "code error" }, message.lastError)) : null,
    section("Payload", payloadBlock(message))
  ];
  const drawer = h("aside", { class: "drawer", role: "dialog", "aria-label": "Message details" },
    h("header", null, h("h3", { class: "mono" }, env?.messageType ?? message.id),
      h("button", { class: "icon-button", type: "button", "aria-label": "Close", onclick: close },
        s("svg", { viewBox: "0 0 24 24" }, s("path", { d: "M6 6l12 12M18 6L6 18", stroke: "currentColor", "stroke-width": 2, fill: "none" })))),
    h("div", { class: "content" }, sections));
  document.addEventListener("keydown", onKey);
  document.body.append(backdrop, drawer);
  drawer.querySelector("button").focus();
}

function section(title, ...content) {
  return h("section", null, h("h4", null, title), content);
}

function kv(pairs) {
  return h("dl", { class: "kv" }, pairs.filter(([, v]) => v !== null && v !== undefined && v !== "").map(([k, v]) => [h("dt", null, k), h("dd", { class: "mono" }, String(v))]));
}

function payloadBlock(message) {
  if (message.payload === null || message.payload === undefined) {
    return h("div", { class: "notice" }, state.overview.showPayloads ? "No payload." : "Hidden by the dashboard's ShowPayloads setting.");
  }
  let text = message.payload;
  try { text = JSON.stringify(JSON.parse(message.payload), null, 2); } catch { /* not JSON: shown as it is */ }
  const pre = h("pre", { class: "code" }, highlight(text));
  const copy = h("button", { class: "button", type: "button", onclick: async () => {
    try { await navigator.clipboard.writeText(text); copy.textContent = "Copied"; } catch { copy.textContent = "Copy failed"; }
    setTimeout(() => { copy.textContent = "Copy"; }, 1500);
  } }, "Copy");
  return [pre, h("div", { class: "toolbar" }, copy, message.payloadTruncated ? h("span", { class: "muted" }, `Cut off at ${fmtCount(new TextEncoder().encode(message.payload).length)} bytes.`) : null)];
}

/** JSON syntax colours, as spans with text nodes. */
function highlight(text) {
  const tokens = /("(?:\\.|[^"\\])*")(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g;
  const parts = [];
  let last = 0;
  for (const match of text.matchAll(tokens)) {
    if (match.index > last) parts.push(text.slice(last, match.index));
    const cls = match[1] ? (match[2] ? "k" : "s") : match[3] ? "b" : "n";
    parts.push(h("span", { class: cls }, match[1] ?? match[0]));
    if (match[2]) parts.push(match[2]);
    last = match.index + match[0].length;
  }
  parts.push(text.slice(last));
  return parts;
}

// ── Topology ──────────────────────────────────────────────────────────────────────────────────────────

function topologyView(root) {
  markTab("topology");
  const b = state.overview.broker;
  mount(root, 
    h("h1", { class: "page-title" }, "Topology", h("span", { class: "muted" }, "Where each message goes")),
    card("Routes", b.routes.length ? table(["Message type", "Condition", "Destination"], b.routes.map(r => [
      h("span", { class: "mono" }, r.messageType),
      h("div", { class: "chips" },
        r.from ? h("span", { class: "chip accent" }, `from ${r.from}`) : null,
        r.notFrom ? h("span", { class: "chip" }, `not from ${r.notFrom}`) : null,
        r.condition ? h("span", { class: "chip" }, r.condition) : null,
        !r.from && !r.notFrom && !r.condition ? h("span", { class: "faint" }, "always") : null),
      h("strong", null, r.destination)])) : empty("No routes.")),
    card("Topics", b.topics.length ? table(["Topic", "Message type", "Endpoints", "Subscribers"], b.topics.map(t => [
      h("strong", null, t.name, t.condition ? h("span", { class: "sub" }, t.condition) : null),
      h("span", { class: "mono" }, t.messageType),
      h("div", { class: "chips" }, t.subscriberEndpoints.map(e => h("span", { class: "chip topic" }, e))),
      h("div", { class: "chips" }, t.subscribers.map(e => h("span", { class: "chip" }, e)))])) : empty("No topics.")),
    card("Consumers", b.consumers.length ? table(["Message type", "Consumer", "On failure"], b.consumers.map(c => [
      h("span", { class: "mono" }, c.messageType), h("strong", null, c.consumer),
      c.retryOnFailure ? h("span", { class: "muted" }, "retry") : h("span", { class: "chip bad" }, "dead-letter at once")])) : empty("No consumers.")),
    card("Scheduled actions", b.scheduledActions.length ? table(["Action", "Schedule", "Next run", "Missed runs"], b.scheduledActions.map(a => [
      a.name ? h("strong", null, a.name) : h("span", { class: "faint" }, "unnamed"),
      h("span", { class: "mono" }, a.schedule),
      a.nextRun ? relTime(a.nextRun) : h("span", { class: "faint" }, "counts from start"),
      a.missedRuns ?? h("span", { class: "faint" }, "—")])) : empty("No scheduled actions.")),
    card("Broker settings", h("div", { class: "card-body" }, settingsList(b))));
  return {};
}

function card(title, content) {
  return h("section", { class: "card" }, h("div", { class: "card-head" }, h("h2", null, title)), content);
}

function table(headers, rows) {
  return h("div", { class: "card-body" }, h("div", { class: "table-wrap" }, h("table", null,
    h("thead", null, h("tr", null, headers.map(x => h("th", null, x)))),
    h("tbody", null, rows.map(cells => h("tr", null, cells.map(c => h("td", null, c))))))));
}

function empty(text) {
  return h("div", { class: "empty" }, text);
}

// ── Health ────────────────────────────────────────────────────────────────────────────────────────────

function healthView(root) {
  markTab("health");
  const banner = h("section", { class: "card banner" });
  const grid = h("div", { class: "health-grid" });
  const timeline = h("ul", { class: "timeline" });
  const button = h("button", { class: "button", type: "button", onclick: async () => {
    button.disabled = true;
    try { const health = await getJson("api/health"); state.live = { ...state.live, health }; recordHealth(health); update(); }
    catch (error) { banner.append(h("span", { class: "error" }, error.message)); }
    finally { button.disabled = false; }
  } }, "Check now");

  mount(root, h("h1", { class: "page-title" }, "Health"), banner, grid,
    h("section", { class: "card" }, h("div", { class: "card-head" }, h("h2", null, "Changes while this page is open")), h("div", { class: "card-body" }, timeline)));

  const update = () => {
    const health = state.live?.health;
    clear(banner);
    if (!health) {
      banner.append(h("span", { class: "big" }, "Checking…"), h("span", { class: "spacer" }), button);
      return;
    }
    const report = health.report;
    banner.className = "card banner " + healthClass(report.status);
    banner.append(h("span", { class: "big" }, report.status),
      h("span", { class: "muted" }, report.brokerStarted ? "Broker running" : "Broker not started", report.message ? ` · ${report.message}` : ""),
      h("span", { class: "spacer" }),
      h("span", { class: "muted" }, "Checked ", relTime(health.checkedAt), ` in ${fmtMs(spanMs(report.duration))}`), button);
    clear(grid).append(...report.endpoints.map(e => h("section", { class: "card health-card" },
      h("div", { class: "top" }, h("strong", null, e.name), h("span", { class: "pill " + healthClass(e.status) }, e.status)),
      h("p", null, e.message ?? "No message."),
      h("p", null, `${modeText(e.mode)} · ${fmtMs(spanMs(e.duration))}${e.isCritical ? "" : " · non-critical"}`))));
    clear(timeline).append(...[...state.healthLog].reverse().map(entry => h("li", null,
      h("span", { class: "pill " + healthClass(entry.status) }, entry.status), relTime(entry.at), entry.message ? h("span", { class: "muted" }, entry.message) : null)));
  };
  update();
  return { update };
}

// ── Routing ───────────────────────────────────────────────────────────────────────────────────────────

function errorState(title, text) {
  return h("section", { class: "card error-state" }, h("h2", null, title), h("p", { class: "muted" }, text));
}

function go(hash) {
  location.hash = hash;
}

function render() {
  state.view?.dispose?.();
  const root = clear(document.getElementById("view"));
  const route = location.hash.replace(/^#\/?/, "").split("/").map(decodeURIComponent);
  if (route[0] === "endpoints" && route[1]) state.view = queueView(root, route[1]);
  else if (route[0] === "endpoints") state.view = endpointsView(root);
  else if (route[0] === "topology") state.view = topologyView(root);
  else if (route[0] === "health") state.view = healthView(root);
  else state.view = overviewView(root);
}

async function start() {
  setupTheme();
  defineGradients();
  try {
    state.overview = await getJson("api/overview");
  } catch (error) {
    clear(document.getElementById("view")).append(errorState("The dashboard could not load", error.message));
    return;
  }
  document.getElementById("title").textContent = state.overview.title;
  document.getElementById("version").textContent = state.overview.version ? `v${state.overview.version}` : "";
  document.title = state.overview.title;
  addPoints(state.overview.history);
  state.live = state.overview.live;
  addPoints(state.live.points);
  recordHealth(state.live.health);
  renderStatus();
  render();
  addEventListener("hashchange", render);
  connect();
}

start();
