// index.js
/* ========= Imports ========= */
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
  DisconnectReason,
  fetchLatestBaileysVersion
} = require('@whiskeysockets/baileys');
const { Boom } = require('@hapi/boom');

/* ========= Safety: no-crash ========= */
process.on('unhandledRejection', (reason) => {
  console.error('🧨 UnhandledRejection:', reason);
});
process.on('uncaughtException', (err) => {
  console.error('🧨 UncaughtException:', err);
});

/* ========= Utils ========= */
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

async function httpFetch(url, init) {
  if (typeof globalThis.fetch === 'function') return globalThis.fetch(url, init);
  const { default: nodeFetch } = await import('node-fetch');
  return nodeFetch(url, init);
}

/* ========= App setup ========= */
const app = express();
app.use(cors());
app.use(body.json({ limit: '2mb' }));

const PORT = process.env.PORT || 4000;
const N8N_INCOMING_WEBHOOK =
  process.env.N8N_INCOMING_WEBHOOK ||
  'https://n8n.srv957249.hstgr.cloud/webhook-test/d450b539-4fd4-4e95-a43a-68805849b7aa';

const QR_TTL_MS = 60_000;
const WATCHDOG_INTERVAL_MS = 5_000;

/* ========= Swagger (OpenAPI) ========= */
let openapi = null;
try {
  openapi = JSON.parse(fs.readFileSync(path.join(__dirname, 'openapi.json'), 'utf8'));
  app.use('/docs', swaggerUi.serve, swaggerUi.setup(openapi, { explorer: true }));
} catch (e) {
  console.warn('⚠️ No se encontró openapi.json; se omite /docs');
}

/* ========= In-memory sessions ========= */
const sessions = new Map();

function upsertSession(sessionId, patch) {
  const prev = sessions.get(sessionId) || {};
  const next = { ...prev, ...patch, lastUpdate: Date.now() };
  sessions.set(sessionId, next);
  return next;
}

/* ========= Core: start session ========= */
async function startSession(sessionId) {
  const existing = sessions.get(sessionId);

  // Si hay sock pero está muerto, no retornes
  const isAlive =
    !!existing?.sock?.ws && existing.sock.ws.readyState === 1; // 1 = OPEN
  if (isAlive) return existing;

  const { state, saveCreds } = await useMultiFileAuthState(path.join('./auth', sessionId));

  const browserInfo =
    Browsers && typeof Browsers.appropriate === 'function'
      ? Browsers.appropriate('Chrome')
      : ['Chrome', 'Linux', '110.0.0'];

  // Asegura versión compatible
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    browser: browserInfo,
    printQRInTerminal: true, // imprime QR en consola
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

  // Limpia timer viejo si existía
  if (existing?.qrTimer) {
    try { clearInterval(existing.qrTimer); } catch (_) {}
  }

  // Watchdog que invalida/rota QR si no se escanea en TTL
  const qrTimer = setInterval(() => {
    const s = sessions.get(sessionId);
    if (!s) { try { clearInterval(qrTimer); } catch (_) {} return; }

    const stillWaiting = s.status !== 'active';
    const now = Date.now();

    const qrExpired =
      (s.qrAt && (now - s.qrAt) > QR_TTL_MS) ||
      (!s.qrAt && s.startedAt && (now - s.startedAt) > QR_TTL_MS);

    if (stillWaiting && qrExpired) {
      console.log(`[${sessionId}] ⏲️ QR expirado; reiniciando socket para forzar nuevo QR`);
      try { s.qr = null; } catch (_) {}
      try { s.qrAt = null; } catch (_) {}
      try { s.sock?.ws?.close?.(); } catch (_) {}
    }
  }, WATCHDOG_INTERVAL_MS);

  upsertSession(sessionId, { qrTimer });

  sock.ev.on('connection.update', async ({ connection, lastDisconnect, qr }) => {
    if (qr) {
      console.log(`[${sessionId}] 🔐 QR recibido a las ${new Date().toISOString()}`);
      upsertSession(sessionId, { qr, qrAt: Date.now() });
      try { qrcode.generate(qr, { small: true }); } catch (_) {}
    }

    if (connection) console.log(`[${sessionId}] 📡 Estado conexión: ${connection}`);

    if (connection === 'close') {
      const statusCode = lastDisconnect?.error instanceof Boom
        ? (lastDisconnect.error.output?.statusCode || 0)
        : 0;

      console.log(`[${sessionId}] 🔌 Conexión cerrada. Código: ${statusCode}`);

      const shouldRestart = statusCode !== DisconnectReason.loggedOut;
      if (shouldRestart) {
        upsertSession(sessionId, { status: 'connecting' });
        await wait(1500);
        try {
          await startSession(sessionId);
        } catch (err) {
          console.error(`[${sessionId}] ❌ Falló reinicio:`, err?.message || err);
          upsertSession(sessionId, { status: 'inactive', qr: null, sock: null });
        }
      } else {
        upsertSession(sessionId, { status: 'inactive', qr: null, sock: null });
      }
    }

    if (connection === 'open') {
      console.log(`[${sessionId}] ✅ Sesión activa`);
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
    } catch (e) {
      console.error(`[${sessionId}] ⚠️ Error enviando a n8n:`, e?.message || e);
    }
  });

  return sessions.get(sessionId);
}

/* ========= Helpers ========= */
async function forceQR(sessionId) {
  let s = sessions.get(sessionId);
  if (!s) {
    s = await startSession(sessionId);
    return sessions.get(sessionId);
  }

  console.log(`[${sessionId}] ♻️ Forzando refresco de QR`);
  try { s.qr = null; } catch (_) {}
  try { s.qrAt = null; } catch (_) {}
  try { s.sock?.ws?.close?.(); } catch (_) {}
  upsertSession(sessionId, { sock: null, status: 'connecting' });

  await startSession(sessionId);

  return sessions.get(sessionId);
}

/* ========= Routes ========= */
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
  } catch (e) {
    console.error('start_failed:', e?.message || e);
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

app.post('/sessions/:id/qr/refresh', async (req, res) => {
  const id = req.params.id;
  try {
    const s = await forceQR(id);
    return res.json({
      ok: true,
      sessionId: id,
      status: s?.status || 'connecting',
      // el QR puede tardar unos ms en llegar vía event; sigue consultando GET /sessions/:id/qr
      qr: s?.qr || null,
      qrAt: s?.qrAt || null,
      lastUpdate: s?.lastUpdate || null,
    });
  } catch (e) {
    console.error('qr_refresh_failed:', e?.message || e);
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

/* ========= Hard reset: borra credenciales para forzar nuevo QR ========= */
app.post('/sessions/:id/reset', async (req, res) => {
  const id = req.params.id;
  const dir = path.join(__dirname, 'auth', id);
  try {
    const s = sessions.get(id);
    if (s?.sock) {
      await s.sock.logout().catch(() => {});
      try { s.sock.end?.(); } catch (_) {}
      try { s.sock.ws?.close?.(); } catch (_) {}
    }
    try { clearInterval(s?.qrTimer); } catch (_) {}
    sessions.delete(id);

    if (fs.existsSync(dir)) fs.rmSync(dir, { recursive: true, force: true });

    const ns = await startSession(id);
    return res.json({ ok: true, sessionId: id, status: ns?.status || 'connecting' });
  } catch (e) {
    console.error('reset_failed:', e?.message || e);
    return res.status(500).json({ error: 'reset_failed' });
  }
});

/* ========= Send text ========= */
app.post('/messages', async (req, res) => {
  try {
    const { sessionId, to, text } = req.body;
    const s = sessions.get(sessionId);
    if (!s || !s.sock) return res.status(404).json({ error: 'session_not_found_or_inactive' });
    if (s.status !== 'active') return res.status(409).json({ error: 'session_not_active' });
    const jid = to.includes('@') ? to : `${to}@s.whatsapp.net`;
    const r = await s.sock.sendMessage(jid, { text });
    res.json({ ok: true, response: r });
  } catch (e) {
    console.error('send_failed:', e?.message || e);
    res.status(500).json({ error: 'send_failed' });
  }
});

/* ========= Health ========= */
app.get('/health', (req, res) => res.json({ ok: true }));

/* ========= Listen ========= */
app.listen(PORT, () => {
  console.log(`🚀 WA Gateway escuchando en http://localhost:${PORT}`);
  if (openapi) console.log(`📑 Docs disponibles en http://localhost:${PORT}/docs`);
});
