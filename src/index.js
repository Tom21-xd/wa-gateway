const express = require('express');
const cors = require('cors');
const body = require('body-parser');
const qrcode = require('qrcode-terminal');
const swaggerUi = require('swagger-ui-express');
const fs = require('fs');
const path = require('path');

const makeWASocket = require('@whiskeysockets/baileys').default;
const {
  useMultiFileAuthState,
  makeCacheableSignalKeyStore,
  Browsers,
  DisconnectReason
} = require('@whiskeysockets/baileys');
const { Boom } = require('@hapi/boom');

process.on('unhandledRejection', () => {});
process.on('uncaughtException', () => {});
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

async function httpFetch(url, init) {
  if (typeof globalThis.fetch === 'function') return globalThis.fetch(url, init);
  const { default: nodeFetch } = await import('node-fetch');
  return nodeFetch(url, init);
}

const app = express();
app.use(cors());
app.use(body.json({ limit: '2mb' }));

const PORT = process.env.PORT || 4000;
const N8N_INCOMING_WEBHOOK =
  process.env.N8N_INCOMING_WEBHOOK ||
  'https://n8n.srv957249.hstgr.cloud/webhook/webhook/wa-in';

const QR_TTL_MS = 60_000;
const WATCHDOG_INTERVAL_MS = 5_000;

const openapi = JSON.parse(
  fs.readFileSync(path.join(__dirname, 'openapi.json'), 'utf8')
);
app.use('/docs', swaggerUi.serve, swaggerUi.setup(openapi, { explorer: true }));

const sessions = new Map();

function upsertSession(sessionId, patch) {
  const prev = sessions.get(sessionId) || {};
  const next = { ...prev, ...patch, lastUpdate: Date.now() };
  sessions.set(sessionId, next);
  return next;
}

async function startSession(sessionId) {
  const existing = sessions.get(sessionId);
  if (existing?.sock) return existing;

  const { state, saveCreds } = await useMultiFileAuthState(`./auth/${sessionId}`);

  const browserInfo =
    Browsers && typeof Browsers.appropriate === 'function'
      ? Browsers.appropriate('Chrome')
      : ['Chrome', 'Linux', '110.0.0'];

  const sock = makeWASocket({
    browser: browserInfo,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys),
    },
    syncFullHistory: false,
  });

  upsertSession(sessionId, {
    sock,
    qr: null,
    qrAt: null,
    status: 'connecting',
    startedAt: Date.now(),
  });

  sock.ev.on('creds.update', saveCreds);

  if (existing?.qrTimer) {
    try { clearInterval(existing.qrTimer); } catch (_) {}
  }

  const qrTimer = setInterval(() => {
    const s = sessions.get(sessionId);
    if (!s) { try { clearInterval(qrTimer); } catch (_) {} return; }

    const stillWaiting = s.status !== 'active';
    const now = Date.now();

    const qrExpired =
      (s.qrAt && (now - s.qrAt) > QR_TTL_MS) ||
      (!s.qrAt && s.startedAt && (now - s.startedAt) > QR_TTL_MS);

    if (stillWaiting && qrExpired) {
      try { s.qr = null; } catch (_) {}
      try { s.qrAt = null; } catch (_) {}
      try { s.sock?.ws?.close?.(); } catch (_) {}
    }
  }, WATCHDOG_INTERVAL_MS);

  upsertSession(sessionId, { qrTimer });

  sock.ev.on('connection.update', async ({ connection, lastDisconnect, qr }) => {
    if (qr) {
      upsertSession(sessionId, { qr, qrAt: Date.now() });
      try { qrcode.generate(qr, { small: true }); } catch (_) {}
    }

    if (connection === 'close') {
      const statusCode = lastDisconnect?.error instanceof Boom
        ? (lastDisconnect.error.output?.statusCode || 0)
        : 0;

      const shouldRestart = statusCode !== DisconnectReason.loggedOut;
      if (shouldRestart) {
        upsertSession(sessionId, { status: 'connecting' });
        await wait(1500);
        try {
          await startSession(sessionId);
        } catch (_) {
          upsertSession(sessionId, { status: 'inactive', qr: null, sock: null });
        }
      } else {
        upsertSession(sessionId, { status: 'inactive', qr: null, sock: null });
      }
    }

    if (connection === 'open') {
      const s = upsertSession(sessionId, { status: 'active', qr: null, qrAt: null });
      try { clearInterval(s.qrTimer); } catch (_) {}
      upsertSession(sessionId, { qrTimer: null });
    }
  });

  sock.ev.on('messages.upsert', async ({ messages }) => {
    const m = messages?.[0];
    if (!m || m.key.fromMe || !N8N_INCOMING_WEBHOOK) return;
    const text =
      m.message?.conversation ||
      m.message?.extendedTextMessage?.text ||
      m.message?.imageMessage?.caption ||
      m.message?.videoMessage?.caption ||
      '';
    try {
      await httpFetch(N8N_INCOMING_WEBHOOK, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sessionId, from: m.key.remoteJid, text, raw: m }),
      });
    } catch (_) {}
  });

  return sessions.get(sessionId);
}

app.post('/sessions/:id/start', async (req, res) => {
  try {
    const id = req.params.id;
    const s = await startSession(id);
    return res.json({
      sessionId: id,
      status: s.status || 'connecting',
      qr: s.qr || null,
      startedAt: s.startedAt || null,
      lastUpdate: s.lastUpdate || Date.now(),
    });
  } catch {
    return res.status(500).json({ error: 'start_failed' });
  }
});

app.get('/sessions/:id/status', (req, res) => {
  const s = sessions.get(req.params.id);
  if (!s) return res.status(404).json({ error: 'session_not_found' });
  return res.json({
    sessionId: req.params.id,
    status: s.status || 'inactive',
    hasQR: !!s.qr,
    lastUpdate: s.lastUpdate || null,
    startedAt: s.startedAt || null,
  });
});

app.get('/sessions/:id/qr', (req, res) => {
  const s = sessions.get(req.params.id);
  if (!s) return res.status(404).json({ error: 'session_not_found' });
  return res.json({ sessionId: req.params.id, qr: s.qr || null, qrAt: s.qrAt || null });
});

app.get('/sessions', (req, res) => {
  const all = [];
  for (const [id, s] of sessions.entries()) {
    all.push({
      sessionId: id,
      status: s.status || 'inactive',
      hasQR: !!s.qr,
      startedAt: s.startedAt || null,
      lastUpdate: s.lastUpdate || null,
    });
  }
  res.json(all);
});

async function forceQR(sessionId) {
  let s = sessions.get(sessionId);
  if (!s) {
    s = await startSession(sessionId);
    return sessions.get(sessionId);
  }

  try { s.qr = null; } catch (_) {}
  try { s.qrAt = null; } catch (_) {}
  try { s.sock?.ws?.close?.(); } catch (_) {}
  upsertSession(sessionId, { sock: null, status: 'connecting' });

  await startSession(sessionId);

  return sessions.get(sessionId);
}

app.post('/sessions/:id/qr/refresh', async (req, res) => {
  const id = req.params.id;
  try {
    const s = await forceQR(id);
    return res.json({
      ok: true,
      sessionId: id,
      status: s?.status || 'connecting',
      // Puede que el QR aún no haya llegado en este instante;
      // el front debe seguir leyendo GET /sessions/:id/qr
      qr: s?.qr || null,
      qrAt: s?.qrAt || null,
      lastUpdate: s?.lastUpdate || null,
    });
  } catch (e) {
    return res.status(500).json({ error: 'qr_refresh_failed' });
  }
});


app.delete('/sessions/:id', async (req, res) => {
  const id = req.params.id;
  const s = sessions.get(id);
  if (!s) return res.status(404).json({ error: 'session_not_found' });
  try {
    if (s.sock) {
      await s.sock.logout().catch(() => {});
      try { s.sock.end?.(); } catch (_) {}
      try { s.sock.ws?.close?.(); } catch (_) {}
    }
    try { clearInterval(s.qrTimer); } catch (_) {}
  } finally {
    sessions.delete(id);
  }
  res.json({ ok: true, sessionId: id });
});

app.post('/messages', async (req, res) => {
  try {
    const { sessionId, to, text } = req.body;
    const s = sessions.get(sessionId);
    if (!s || !s.sock) return res.status(404).json({ error: 'session_not_found_or_inactive' });
    if (s.status !== 'active') return res.status(409).json({ error: 'session_not_active' });
    const jid = to.includes('@') ? to : `${to}@s.whatsapp.net`;
    const r = await s.sock.sendMessage(jid, { text });
    res.json({ ok: true, response: r });
  } catch {
    res.status(500).json({ error: 'send_failed' });
  }
});

app.get('/health', (req, res) => res.json({ ok: true }));

app.listen(PORT, () => {
  console.log(`🚀 WA Gateway escuchando en http://localhost:${PORT}`);
  console.log(`📑 Docs disponibles en http://localhost:${PORT}/docs`);
});
