
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

const ROBLOX_API_KEY = process.env.ROBLOX_API_KEY;
const UNIVERSE_ID = process.env.ROBLOX_UNIVERSE_ID || "10772251344";
const DATASTORE_NAME = "PixelCanvasVIPCodes";

if (!TOKEN) {
    throw new Error("Set STREAM_TOKEN in Render Environment");
}

if (!ROBLOX_API_KEY) {
    throw new Error("Set ROBLOX_API_KEY in Render Environment");
}

app.set("trust proxy", 1);
app.use(express.json({ limit: "2kb" }));
app.use(express.static("public"));

let queue = [];
let active = null;
let frame = null;
let sequence = 0;

const clients = new Map();

let converting = false;
let pendingFrame = null;

const failedAttempts = new Map();
const redemptionLocks = new Set();

const MAX_FAILED_ATTEMPTS = 3;
const COOLDOWN_MS = 5 * 60 * 1000;

--------------------------------------------------
-- SECURITY
--------------------------------------------------

function safeEqual(a, b) {
    const x = Buffer.from(String(a));
    const y = Buffer.from(String(b));

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
        queue: queue.map(client => ({
            id: client.id,
            name: client.name
        })),
        sequence
    };
}

--------------------------------------------------
-- QUEUE
--------------------------------------------------

function promote() {
    if (!active && queue.length) {
        active = queue.shift();
        frame = null;
        pendingFrame = null;

        console.log("Now streaming:", active.name);
    }
}

function promoteVip(client) {
    if (active && active.id === client.id) {
        client.isVip = true;
        return;
    }

    const index = queue.findIndex(item => item.id === client.id);

    if (index === -1) {
        return;
    }

    queue.splice(index, 1);
    client.isVip = true;

    // Insert after any VIPs already waiting.
    const firstRegular = queue.findIndex(item => !item.isVip);

    if (firstRegular === -1) {
        queue.push(client);
    } else {
        queue.splice(firstRegular, 0, client);
    }

    promote();

    console.log("VIP queue priority granted:", client.name);
}

--------------------------------------------------
-- ROBLOX DATASTORE
--------------------------------------------------

function datastoreEntryUrl(code) {
    const datastore = encodeURIComponent(DATASTORE_NAME);
    const entry = encodeURIComponent("CODE_" + code);

    return (
        "https://apis.roblox.com/cloud/v2/universes/" +
        encodeURIComponent(UNIVERSE_ID) +
        "/data-stores/" +
        datastore +
        "/entries/" +
        entry
    );
}

async function getRobloxCode(code) {
    const response = await fetch(datastoreEntryUrl(code), {
        method: "GET",
        headers: {
            "x-api-key": ROBLOX_API_KEY
        }
    });

    if (response.status === 404) {
        return null;
    }

    if (!response.ok) {
        const details = await response.text();
        throw new Error(
            "Roblox lookup failed: " +
            response.status +
            " " +
            details
        );
    }

    const result = await response.json();
    return result.value || null;
}

async function markCodeRedeemed(code, value) {
    const updatedValue = {
        ...value,
        redeemed: true,
        redeemedAt: Date.now()
    };

    const response = await fetch(datastoreEntryUrl(code), {
        method: "PATCH",
        headers: {
            "x-api-key": ROBLOX_API_KEY,
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            value: updatedValue
        })
    });

    if (!response.ok) {
        const details = await response.text();
        throw new Error(
            "Roblox update failed: " +
            response.status +
            " " +
            details
        );
    }

    return true;
}

--------------------------------------------------
-- FAILED ATTEMPTS
--------------------------------------------------

function getAttemptState(ip) {
    let state = failedAttempts.get(ip);

    if (!state) {
        state = {
            attempts: 0,
            blockedUntil: 0
        };

        failedAttempts.set(ip, state);
    }

    return state;
}

function recordFailure(ip) {
    const state = getAttemptState(ip);

    state.attempts++;

    if (state.attempts >= MAX_FAILED_ATTEMPTS) {
        state.attempts = 0;
        state.blockedUntil = Date.now() + COOLDOWN_MS;

        return {
            blocked: true,
            remainingMs: COOLDOWN_MS
        };
    }

    return {
        blocked: false,
        attemptsLeft: MAX_FAILED_ATTEMPTS - state.attempts
    };
}

function clearFailures(ip) {
    failedAttempts.delete(ip);
}

--------------------------------------------------
-- VIP REDEMPTION
--------------------------------------------------

app.post("/redeem", async (req, res) => {
    const ip = req.ip || req.socket.remoteAddress || "unknown";
    const state = getAttemptState(ip);

    if (Date.now() < state.blockedUntil) {
        const remaining = Math.ceil(
            (state.blockedUntil - Date.now()) / 1000
        );

        return res.status(429).json({
            success: false,
            error: "Too many failed attempts. Try again in " +
                remaining +
                " seconds."
        });
    }

    const code = String(req.body?.code || "").trim();
    const clientId = String(req.body?.clientId || "");

    if (!/^\d{6}$/.test(code)) {
        const failure = recordFailure(ip);

        return res.status(failure.blocked ? 429 : 400).json({
            success: false,
            error: failure.blocked
                ? "Too many failed attempts. Wait five minutes."
                : "Enter a valid six-digit code. Attempts left: " +
                    failure.attemptsLeft
        });
    }

    const client = clients.get(clientId);

    if (!client || client.ws.readyState !== WebSocket.OPEN) {
        return res.status(400).json({
            success: false,
            error: "Join the queue before redeeming your code."
        });
    }

    if (client.isVip || (active && active.id === client.id)) {
        return res.json({
            success: true,
            message: "You already have priority access."
        });
    }

    if (redemptionLocks.has(code)) {
        return res.status(409).json({
            success: false,
            error: "This code is currently being checked. Try again."
        });
    }

    redemptionLocks.add(code);

    try {
        const value = await getRobloxCode(code);

        if (
            !value ||
            value.redeemed === true ||
            !Number.isFinite(Number(value.userId))
        ) {
            const failure = recordFailure(ip);

            return res.status(failure.blocked ? 429 : 400).json({
                success: false,
                error: failure.blocked
                    ? "Too many failed attempts. Wait five minutes."
                    : "Invalid or already-used code. Attempts left: " +
                        failure.attemptsLeft
            });
        }

        // Save redemption before granting priority.
        await markCodeRedeemed(code, value);

        clearFailures(ip);
        promoteVip(client);

        return res.json({
            success: true,
            message: "Priority Pass accepted! You have moved ahead of regular users."
        });

    } catch (error) {
        console.error("[VIP redemption]", error);

        return res.status(503).json({
            success: false,
            error: "VIP verification is temporarily unavailable. Try again later."
        });

    } finally {
        redemptionLocks.delete(code);
    }
});

--------------------------------------------------
-- NORMAL ROUTES
--------------------------------------------------

app.get("/health", (_, res) => {
    res.json({
        ok: true,
        active: !!active,
        sequence
    });
});

app.get("/queue", (_, res) => {
    res.json(status());
});

app.get("/next", (req, res) => {
    if (!authorized(req)) {
        return res.status(401).json({
            error: "Unauthorized"
        });
    }

    const after = Number(req.query.after || 0);

    if (!frame || sequence <= after) {
        return res.status(204).end();
    }

    res.json({
        sequence,
        frame
    });
});

app.post("/stop", (req, res) => {
    if (!authorized(req)) {
        return res.status(401).json({
            error: "Unauthorized"
        });
    }

    if (active) {
        clients.get(active.id)?.ws.close();
    }

    active = null;
    frame = null;
    pendingFrame = null;

    promote();

    res.json(status());
});

--------------------------------------------------
-- IMAGE PROCESSING
--------------------------------------------------

async function processPendingFrames() {
    if (converting) return;

    converting = true;

    try {
        while (pendingFrame) {
            const item = pendingFrame;
            pendingFrame = null;

            if (!active || active.id !== item.id) continue;
            if (item.ws.readyState !== WebSocket.OPEN) continue;

            try {
                const jpeg = Buffer.from(item.base64, "base64");

                const rgb = await sharp(jpeg, {
                    limitInputPixels: 4000000
                })
                    .resize(300, 300, { fit: "fill" })
                    .flatten({ background: "#000000" })
                    .removeAlpha()
                    .raw()
                    .toBuffer();

                if (rgb.length !== 300 * 300 * 3) {
                    throw new Error("Unexpected RGB buffer size");
                }

                const hex = rgb.toString("hex").toUpperCase();

                if (active && active.id === item.id) {
                    frame = hex;
                    sequence++;

                    if (sequence % 10 === 0) {
                        console.log("Converted frames:", sequence);
                    }
                }

            } catch (err) {
                console.error("Frame conversion failed:", err.message);
            }
        }

    } finally {
        converting = false;

        if (pendingFrame) {
            processPendingFrames();
        }
    }
}

--------------------------------------------------
-- WEBSOCKET QUEUE
--------------------------------------------------

wss.on("connection", (ws, req) => {
    const url = new URL(req.url, "http://localhost");
    const token = url.searchParams.get("token") || "";

    if (!safeEqual(token, TOKEN)) {
        ws.close(1008, "Unauthorized");
        return;
    }

    const id = crypto.randomUUID();
    const name = (
        url.searchParams.get("name") || "Device"
    ).slice(0, 40);

    const client = {
        id,
        name,
        ws,
        isVip: false
    };

    clients.set(id, client);
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

    ws.on("message", raw => {
        if (!active || active.id !== id) return;
        if (raw.length > 250000) return;

        let msg;

        try {
            msg = JSON.parse(raw.toString());
        } catch {
            return;
        }

        if (msg.type !== "frame" || typeof msg.data !== "string") {
            return;
        }

        const match = msg.data.match(
            /^data:image\/jpeg;base64,([A-Za-z0-9+/]+={0,2})$/
        );

        if (!match || msg.data.length > 240000) return;

        pendingFrame = {
            id,
            ws,
            base64: match[1]
        };

        processPendingFrames();
    });

    ws.on("close", () => {
        clearInterval(ticker);

        clients.delete(id);
        queue = queue.filter(item => item.id !== id);

        if (pendingFrame && pendingFrame.id === id) {
            pendingFrame = null;
        }

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

--------------------------------------------------
-- START SERVER
--------------------------------------------------

server.listen(PORT, "0.0.0.0", () => {
    console.log("PixelCanvas Live listening on", PORT);
});
