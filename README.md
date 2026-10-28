# Speaking Corner

Real-time speaking sessions for English clubs. Node + WebSockets + SQLite, single-file Tailwind frontend.

## Run
    npm install
    npm start          # http://localhost:3000

Env vars: `PORT` (default 3000), `DB_PATH` (default ./speaking.db).

## Use
1. Open the site → **Create a room**. You are the host (a host key is stored in your browser).
2. Open the shared link or enter the 5-letter code on the projector/other devices — they get a read-only live display.
3. Add names, choose a topic, set timers, **Start**. The server runs the clock and auto-advances turns.

## Deploy
Any Node host (Render, Railway, Fly, a VPS). Put it behind HTTPS (the client switches to `wss://` automatically)
and mount a persistent disk for `DB_PATH`. Rooms idle for 7 days are deleted.
