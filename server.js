// ══════════════════════════════════════════════════
//  Folio — Backend Server
//  Node.js + ws
//  Deploy: Render / Railway (free tier)
// ══════════════════════════════════════════════════

const http    = require('http');
const WebSocket = require('ws');
const crypto  = require('crypto');

const PORT = process.env.PORT || 8080;

// In-memory store — messages survive until server restarts
// For true persistence: uncomment MongoDB section at bottom
const rooms    = {};   // roomId → Set<ws>
const history  = {};   // roomId → Message[]
const MAX_HIST = 150;  // messages kept per room

// ── HTTP ────────────────────────────────────────────
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.url === '/health') { res.writeHead(200); res.end('OK'); return; }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Folio server OK');
});

// ── WebSocket ───────────────────────────────────────
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  ws.isAlive  = true;
  ws.room     = null;
  ws.username = 'Reader';
  ws.id       = crypto.randomBytes(4).toString('hex');

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      case 'join': {
        const room = clean(msg.room || '').substring(0, 40);
        const name = clean(msg.username || 'Reader').substring(0, 20);

        ws.room = room;
        ws.username = name;

        if (!rooms[room])   rooms[room]   = new Set();
        if (!history[room]) history[room] = [];
        rooms[room].add(ws);

        console.log(`[JOIN] ${name} → ${room} (${rooms[room].size} in room)`);

        // Send history to new user
        history[room].forEach(m => safe(ws, m));

        // Broadcast online count
        broadcastCount(room);

        // Announce to others
        broadcast(room, { type: 'system', text: `${name} joined` }, ws);
        break;
      }

      case 'message': {
        if (!ws.room) return;

        const payload = {
          type:     'message',
          username: ws.username,
          msgType:  sanitizeMsgType(msg.msgType),
          text:     clean(msg.text || '').substring(0, 500),
          url:      sanitizeUrl(msg.url || ''),
          filename: clean(msg.filename || '').substring(0, 100),
          page:     typeof msg.page === 'number' ? msg.page : null,
          ts:       Date.now(),
        };

        // Store
        history[ws.room].push(payload);
        if (history[ws.room].length > MAX_HIST) history[ws.room].shift();

        // Broadcast to everyone in room (including sender)
        broadcast(ws.room, payload);

        const preview = payload.msgType === 'text'
          ? payload.text.substring(0, 40)
          : `[${payload.msgType}]`;
        console.log(`[MSG] ${ws.username} in ${ws.room}: ${preview}`);
        break;
      }
    }
  });

  ws.on('close', () => {
    if (!ws.room || !rooms[ws.room]) return;
    rooms[ws.room].delete(ws);
    broadcastCount(ws.room);
    broadcast(ws.room, { type: 'system', text: `${ws.username} left` });
    if (rooms[ws.room].size === 0) delete rooms[ws.room];
    console.log(`[LEAVE] ${ws.username} ← ${ws.room}`);
  });

  ws.on('error', err => console.error(`[ERR] ${err.message}`));
});

// ── Heartbeat (keeps connections alive through proxies) ──
const hb = setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 25_000);
wss.on('close', () => clearInterval(hb));

// ── Helpers ─────────────────────────────────────────────
function broadcast(room, payload, exclude = null) {
  if (!rooms[room]) return;
  const data = JSON.stringify(payload);
  rooms[room].forEach(c => {
    if (c === exclude) return;
    if (c.readyState === WebSocket.OPEN) c.send(data);
  });
}

function broadcastCount(room) {
  const count = rooms[room]?.size || 0;
  broadcast(room, { type: 'online', count });
}

function safe(ws, payload) {
  if (ws.readyState === WebSocket.OPEN)
    ws.send(JSON.stringify({ ...payload, type: 'history' }));
}

function clean(s) {
  return String(s).replace(/[<>"'`]/g, '').trim();
}

function sanitizeMsgType(t) {
  return ['text','image','video','audio','document'].includes(t) ? t : 'text';
}

function sanitizeUrl(u) {
  // Only allow https URLs (Cloudinary etc.)
  if (!u) return '';
  try {
    const parsed = new URL(u);
    return parsed.protocol === 'https:' ? u : '';
  } catch { return ''; }
}

// ── Start ────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`Folio server running on port ${PORT}`);
});


// ════════════════════════════════════════════════════════
//  OPTIONAL — MongoDB persistent storage
//  1. npm install mongoose
//  2. Set MONGO_URI in Render environment variables
//  3. Uncomment below and remove in-memory history above
// ════════════════════════════════════════════════════════
/*
const mongoose = require('mongoose');
mongoose.connect(process.env.MONGO_URI);

const MsgSchema = new mongoose.Schema({
  room:     { type: String, index: true },
  username: String,
  msgType:  String,
  text:     String,
  url:      String,
  filename: String,
  page:     Number,
  ts:       { type: Date, default: Date.now },
});
const Msg = mongoose.model('Message', MsgSchema);

// In 'join' case, replace history send with:
//   const hist = await Msg.find({room}).sort({ts:1}).limit(100);
//   hist.forEach(m => safe(ws, m));

// In 'message' case, after building payload:
//   await Msg.create(payload);
*/
