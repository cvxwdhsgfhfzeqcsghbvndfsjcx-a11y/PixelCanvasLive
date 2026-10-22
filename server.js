const express = require("express");
const http = require("http");
const crypto = require("crypto");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const PORT = process.env.PORT || 10000;
const TOKEN = process.env.STREAM_TOKEN;

if (!TOKEN) throw new Error("Set STREAM_TOKEN in Render Environment");

let queue = [];
let active = null;
let frame = null;
let sequence = 0;
const clients = new Map();

function authorized(req) {
  const h = req.headers.authorization || "";
  const token = h.startsWith("Bearer ") ? h.slice(7) : "";
  return token.length === TOKEN.length &&
    crypto.timingSafeEqual(Buffer.from(token), Buffer.from(TOKEN));
}
function status() {
  return {
    active: active ? active.id : null,
    queue: queue.map(x => ({ id: x.id, name: x.name })),
    sequence
  };
}
function promote() {
  if (!active && queue.length) {
    active = queue.shift();
    frame = null;
    console.log("Now streaming:", active.name);
  }
}
app.use(express.static("public"));
app.get("/health", (_, res) => res.json({ ok: true, active: !!active }));
app.get("/queue", (_, res) => res.json(status()));
app.get("/next", (req, res) => {
  if (!authorized(req)) return res.status(401).json({ error: "Unauthorized" });
  const after = Number(req.query.after || 0);
  if (!frame || sequence <= after) return res.status(204).end();
  res.json({ sequence, frame });
});
app.post("/stop", (req, res) => {
  if (!authorized(req)) return res.status(401).json({ error: "Unauthorized" });
  if (active) clients.get(active.id)?.close();
  active = null;
  frame = null;
  promote();
  res.json(status());
});

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, "http://localhost");
  const token = url.searchParams.get("token") || "";
  if (token.length !== TOKEN.length ||
      !crypto.timingSafeEqual(Buffer.from(token), Buffer.from(TOKEN))) {
    ws.close(1008, "Unauthorized");
    return;
  }
  const id = crypto.randomUUID();
  const name = (url.searchParams.get("name") || "Device").slice(0, 40);
  const client = { id, name, ws };
  clients.set(id, ws);
  queue.push(client);
  promote();

  function sendState() {
    if (ws.readyState === WebSocket.OPEN)
      ws.send(JSON.stringify({ type: "state", ...status(), yourId: id }));
  }
  sendState();
  const ticker = setInterval(sendState, 1000);

  ws.on("message", raw => {
    if (!active || active.id !== id) return;
    if (raw.length > 250000) return;
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.type !== "frame" || typeof msg.data !== "string") return;
      if (!/^data:image\/jpeg;base64,[A-Za-z0-9+/]+=*$/.test(msg.data)) return;
      if (msg.data.length > 240000) return;
      frame = msg.data;
      sequence++;
    } catch {}
  });
  ws.on("close", () => {
    clearInterval(ticker);
    clients.delete(id);
    queue = queue.filter(x => x.id !== id);
    if (active && active.id === id) {
      active = null;
      frame = null;
      promote();
    }
  });
});
server.listen(PORT, "0.0.0.0", () => console.log("PixelCanvas Live listening on", PORT));
