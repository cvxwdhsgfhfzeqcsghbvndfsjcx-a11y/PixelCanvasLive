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

if (!TOKEN) {
  throw new Error("Set STREAM_TOKEN in Render Environment");
}

let queue = [];
let active = null;
let frame = null;
let sequence = 0;
const clients = new Map();

function safeEqual(a, b) {
  const x = Buffer.from(a);
  const y = Buffer.from(b);
  return x.length === y.length && crypto.timingSafeEqual(x, y);
}

function authorized(req) {
  const header = req.headers.authorization || "";
  const token = header.startsWith("Bearer ")
    ? header.slice(7)
    : "";
  return safeEqual(token, TOKEN);
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

app.get("/health", (_, res) => {
  res.json({ ok: true, active: !!active });
});

app.get("/queue", (_, res) => {
  res.json(status());
});

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

  if (active) {
    clients.get(active.id)?.close();
  }

  active = null;
  frame = null;
  promote();
  res.json(status());
});

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, "http://localhost");
  const token = url.searchParams.get("token") || "";

  if (!safeEqual(token, TOKEN)) {
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
      ws.send(JSON.stringify({
        type: "state",
        ...status(),
        yourId: id
      }));
    }
  }

  sendState();
  const ticker = setInterval(sendState, 1000);

  let converting = false;

  ws.on("message", async raw => {
    if (!active || active.id !== id || converting) return;
    if (raw.length > 250000) return;

    let msg;

    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.type !== "frame" || typeof msg.data !== "string") return;

    const match = msg.data.match(
      /^data:image\/jpeg;base64,([A-Za-z0-9+/]+={0,2})$/
    );

    if (!match || msg.data.length > 240000) return;

    converting = true;

    try {
      const jpeg = Buffer.from(match[1], "base64");

      // Convert the incoming JPEG to exactly 300x300 RGB pixels.
      const rgb = await sharp(jpeg, {
        limitInputPixels: 4000000
      })
        .resize(300, 300, { fit: "fill" })
        .flatten({ background: "#000000" })
        .removeAlpha()
        .raw()
        .toBuffer();

      if (rgb.length !== 300 * 300 * 3) {
        throw new Error("Unexpected RGB buffer size: " + rgb.length);
      }

      // Each pixel becomes RRGGBB: 90,000 pixels = 540,000 characters.
      const hex = rgb.toString("hex").toUpperCase();

      if (active && active.id === id) {
        frame = hex;
        sequence++;
      }
    } catch (err) {
      console.error("Frame conversion failed:", err.message);
    } finally {
      converting = false;
    }
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

  ws.on("error", err => {
    console.error("WebSocket error:", err.message);
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log("PixelCanvas Live listening on", PORT);
});
