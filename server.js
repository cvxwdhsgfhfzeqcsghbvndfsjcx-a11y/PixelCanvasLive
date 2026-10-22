const express = require("express");
const http = require("http");
const crypto = require("crypto");
const WebSocket = require("ws");
const sharp = require("sharp");

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
let conversionId = 0;
const clients = new Map();

function sameToken(a, b) {
  const aa = Buffer.from(a);
  const bb = Buffer.from(b);
  return aa.length === bb.length && crypto.timingSafeEqual(aa, bb);
}

function authorized(req) {
  const h = req.headers.authorization || "";
  return h.startsWith("Bearer ") && sameToken(h.slice(7), TOKEN);
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
  if (!authorized(req)) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const after = Number(req.query.after || 0);

  if (!frame || sequence <= after) {
    return res.status(204).end();
  }

  res.json({ sequence, frame });
});

app.post("/stop", (req, res) => {
  if (!authorized(req)) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  if (active) clients.get(active.id)?.close();

  active = null;
  frame = null;
  promote();
  res.json(status());
});

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, "http://localhost");
  const token = url.searchParams.get("token") || "";

  if (!sameToken(token, TOKEN)) {
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
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: "state", ...status(), yourId: id }));
    }
  }

  sendState();
  const ticker = setInterval(sendState, 1000);

  ws.on("message", async raw => {
    if (!active || active.id !== id) return;
    if (raw.length > 250000) return;

    try {
      const msg = JSON.parse(raw.toString());

      if (msg.type !== "frame" || typeof msg.data !== "string") return;
      if (msg.data.length > 240000) return;

      const match = msg.data.match(/^data:image\/jpeg;base64,([A-Za-z0-9+/]+={0,2})$/);
      if (!match) return;

      const thisConversion = ++conversionId;
      const jpeg = Buffer.from(match[1], "base64");

      const { data, info } = await sharp(jpeg)
        .resize(300, 300, { fit: "fill" })
        .flatten({ background: "#000000" })
        .removeAlpha()
        .raw()
        .toBuffer({ resolveWithObject: true });

      if (info.width !== 300 || info.height !== 300 || info.channels !== 3) return;
      if (thisConversion !== conversionId || !active || active.id !== id) return;

      // 300 x 300 pixels, six hexadecimal RGB characters per pixel.
      frame = data.toString("hex").toUpperCase();
      sequence++;
    } catch (error) {
      console.error("Frame conversion failed:", error.message);
    }
  });

  ws.on("close", () => {
    clearInterval(ticker);
    clients.delete(id);
    queue = queue.filter(x => x.id !== id);

    if (active && active.id === id) {
      conversionId++;
      active = null;
      frame = null;
      promote();
    }
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log("PixelCanvas Live listening on", PORT);
});
