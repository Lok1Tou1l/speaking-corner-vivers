const http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto");
const { WebSocketServer } = require("ws");
let Database;
try {
  const { DatabaseSync } = require("node:sqlite");
  Database = class extends DatabaseSync {
    pragma(sql) {
      return this.exec(`PRAGMA ${sql};`);
    }
  };
} catch {
  Database = require("better-sqlite3");
}

const PORT = process.env.PORT || 3000;
const db = new Database(process.env.DB_PATH || path.join(__dirname, "speaking.db"));
db.pragma("journal_mode = WAL");
db.exec("CREATE TABLE IF NOT EXISTS rooms(code TEXT PRIMARY KEY, host_key TEXT NOT NULL, state TEXT NOT NULL, updated INTEGER NOT NULL)");
db.prepare("DELETE FROM rooms WHERE updated < ?").run(Date.now() - 7 * 864e5); // drop rooms idle for 7 days
const qGet = db.prepare("SELECT * FROM rooms WHERE code=?");
const qPut = db.prepare("INSERT INTO rooms(code,host_key,state,updated) VALUES(?,?,?,?) ON CONFLICT(code) DO UPDATE SET state=excluded.state, updated=excluded.updated");

const rooms = new Map();
const ri = n => crypto.randomInt(n);
const shuffle = a => { a = a.slice(); for (let i = a.length - 1; i > 0; i--) { const j = ri(i + 1); [a[i], a[j]] = [a[j], a[i]]; } return a; };
const fresh = () => ({ people: [], queue: [], round: 0, topic: "", introSec: 30, speakSec: 120, phase: "setup", current: null, running: false, endAt: 0, left: 0, ev: { n: 0, kind: "" } });

function getRoom(code) {
  let r = rooms.get(code); if (r) return r;
  const row = qGet.get(code); if (!row) return null;
  r = { code, hostKey: row.host_key, state: JSON.parse(row.state), clients: new Set() };
  rooms.set(code, r); return r;
}
const persist = r => qPut.run(r.code, r.hostKey, JSON.stringify(r.state), Date.now());
const broadcast = r => { const msg = JSON.stringify({ t: "state", state: r.state, now: Date.now() }); for (const c of r.clients) if (c.readyState === 1) c.send(msg); };
const newCode = () => { const A = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; for (;;) { let c = ""; for (let i = 0; i < 5; i++) c += A[ri(A.length)]; if (!rooms.has(c) && !qGet.get(c)) return c; } };

// ---- turn logic (server is the single source of truth) ----
const clamp = (v, lo, hi, d) => (Number.isFinite(+v) && +v > 0 ? Math.min(hi, Math.max(lo, Math.round(+v))) : d);
const startT = (s, sec) => { s.left = sec * 1000; s.endAt = Date.now() + s.left; s.running = true; };
const ev = (s, k) => { s.ev = { n: s.ev.n + 1, kind: k }; };
function draw(s) {
  if (!s.people.length) { s.phase = "setup"; s.running = false; s.current = null; return; }
  if (!s.queue.length) { s.round++; s.queue = shuffle(s.people); } // everyone spoke -> new shuffled round
  s.current = s.queue.pop(); s.phase = "speaking"; startT(s, s.speakSec); ev(s, "start");
}
const ACTIONS = {
  add(s, m) { const n = String(m.name || "").trim().slice(0, 30); if (!n || s.people.includes(n) || s.people.length >= 40) return; s.people.push(n); s.queue.splice(ri(s.queue.length + 1), 0, n); },
  rm(s, m) { s.people = s.people.filter(p => p !== m.name); s.queue = s.queue.filter(p => p !== m.name); if (s.phase === "speaking" && s.current === m.name) draw(s); },
  start(s, m) {
    const t = String(m.topic || "").trim().slice(0, 140); if (!s.people.length || !t) return;
    s.topic = t; s.introSec = clamp(m.introSec, 5, 600, 30); s.speakSec = clamp(m.speakSec, 10, 1800, 120);
    s.queue = shuffle(s.people); s.round = 1; s.current = null; s.phase = "intro"; startT(s, s.introSec); ev(s, "start");
  },
  toggle(s) { if (s.phase === "setup") return; if (s.running) { s.left = Math.max(0, s.endAt - Date.now()); s.running = false; } else if (s.left > 0) { s.endAt = Date.now() + s.left; s.running = true; } },
  reset(s) { if (s.phase !== "setup") { s.left = (s.phase === "intro" ? s.introSec : s.speakSec) * 1000; s.running = false; } },
  restart(s) { if (s.phase === "speaking") { startT(s, s.speakSec); ev(s, "start"); } },
  draw(s) { if (s.phase !== "setup") draw(s); },
  skip(s) { if (s.phase === "speaking") { if (s.queue.length) s.queue.unshift(s.current); draw(s); } }, // skipped person goes to back of queue
  redraw(s) { if (s.phase === "speaking") { s.queue = shuffle(s.queue.concat(s.current)); draw(s); } },
  end(s) { const p = s.people; Object.assign(s, fresh(), { people: p, queue: shuffle(p) }); },
};

// ---- server-side timer: advances turns even if the host's tab sleeps ----
setInterval(() => {
  for (const r of rooms.values()) {
    const s = r.state;
    if (s.running && Date.now() >= s.endAt) {
      s.running = false; s.left = 0;
      if (s.phase === "speaking") draw(s); else ev(s, "end");
      persist(r); broadcast(r);
    }
  }
}, 250);

// ---- HTTP + WebSocket ----
const getHtml = () => {
  const p = fs.existsSync(path.join(__dirname, "public", "index.html"))
    ? path.join(__dirname, "public", "index.html")
    : path.join(__dirname, "index.html");
  return fs.readFileSync(p);
};
const server = http.createServer((q, res) => {
  if (q.url === "/healthz") return res.end("ok");
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(getHtml());
});
const wss = new WebSocketServer({ server, maxPayload: 4096 });
const send = (ws, o) => ws.readyState === 1 && ws.send(JSON.stringify(o));

wss.on("connection", ws => {
  ws.alive = true; ws.on("pong", () => (ws.alive = true));
  ws.on("message", raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === "create" || m.t === "join") {
      if (ws.room) ws.room.clients.delete(ws);
      let r, isHost, key;
      if (m.t === "create") {
        key = crypto.randomUUID(); r = { code: newCode(), hostKey: key, state: fresh(), clients: new Set() };
        rooms.set(r.code, r); persist(r); isHost = true;
      } else {
        r = getRoom(String(m.code || "").toUpperCase().slice(0, 5));
        if (!r) return send(ws, { t: "err", msg: "Room not found" });
        isHost = !!m.hostKey && m.hostKey === r.hostKey;
      }
      ws.room = r; ws.isHost = isHost; r.clients.add(ws);
      send(ws, { t: "joined", code: r.code, isHost, hostKey: key });
      send(ws, { t: "state", state: r.state, now: Date.now() });
    } else if (m.t === "act" && ws.room && ws.isHost && ACTIONS[m.a]) {
      ACTIONS[m.a](ws.room.state, m); persist(ws.room); broadcast(ws.room);
    }
  });
  ws.on("close", () => {
    const r = ws.room; if (!r) return;
    r.clients.delete(ws);
    if (!r.clients.size && !r.state.running) rooms.delete(r.code); // reload from SQLite on next join
  });
});
setInterval(() => { for (const c of wss.clients) { if (!c.alive) { c.terminate(); continue; } c.alive = false; c.ping(); } }, 30000);

server.listen(PORT, () => console.log("Speaking Corner on http://localhost:" + PORT));
