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
  const method = (init && init.method) || 'GET';
  console.log(`🌐 httpFetch -> ${method} ${url}`);
  if (typeof globalThis.fetch === 'function') return globalThis.fetch(url, init);
  const { default: nodeFetch } = await import('node-fetch');
  return nodeFetch(url, init);
}

/* ========= App setup ========= */
const app = express();
app.use(cors());
app.use(body.json({ limit: '2mb' }));

// 🔎 Middleware de tracing por request
app.use((req, res, next) => {
  const rid = (Math.random() + 1).toString(36).slice(2, 8);
  req.rid = rid;
  const start = Date.now();

  // Evitar logs gigantes: truncamos body
  const shortBody =
    req.method !== 'GET' && req.body
      ? JSON.stringify(req.body).slice(0, 600)
      : '';

  console.log(`[${rid}] ➡️ ${req.method} ${req.originalUrl} body=${shortBody || '<empty>'}`);

  res.on('finish', () => {
    const ms = Date.now() - start;
    console.log(
      `[${rid}] ⬅️ ${res.statusCode} ${req.method} ${req.originalUrl} (${ms}ms)`
    );
  });

  next();
});

const PORT = process.env.PORT || 4000;
const N8N_INCOMING_WEBHOOK =
  process.env.N8N_INCOMING_WEBHOOK ||
  'https://n8n.srv957249.hstgr.cloud/webhook/webhook/wa-in';

const QR_TTL_MS = 60_000;
const WATCHDOG_INTERVAL_MS = 5_000;
const AUTH_ROOT = path.resolve(__dirname, 'auth');
const AUTO_PURGE_ON_LOGOUT = process.env.AUTO_PURGE_ON_LOGOUT === '1';

/* ========= Swagger (OpenAPI) ========= */
let openapi = null;
try {
  openapi = JSON.parse(fs.readFileSync(path.join(__dirname, 'openapi.json'), 'utf8'));
  app.use('/docs', swaggerUi.serve, swaggerUi.setup(openapi, { explorer: true }));
  console.log('📑 Swagger habilitado en /docs');
} catch (e) {
  console.warn('⚠️ No se encontró openapi.json; se omite /docs');
}

/* ========= In-memory sessions ========= */
const sessions = new Map();

function upsertSession(sessionId, patch) {
  const prev = sessions.get(sessionId) || {};
  const next = { ...prev, ...patch, lastUpdate: Date.now() };
  sessions.set(sessionId, next);
  const keysPatched = Object.keys(patch);
  console.log(`[${sessionId}] 🧩 upsertSession -> patched: ${keysPatched.join(', ') || 'none'}`);
  return next;
}

/* ========= Core: start session ========= */
async function startSession(sessionId) {
  console.log(`[${sessionId}] ▶️ startSession() llamado`);
  const existing = sessions.get(sessionId);

  const isAlive =
    !!existing?.sock?.ws && existing.sock.ws.readyState === 1; // 1 = OPEN
  if (isAlive) {
    console.log(`[${sessionId}] ♻️ startSession() reutiliza socket vivo`);
    return existing;
  }

  const { state, saveCreds } = await useMultiFileAuthState(path.join(AUTH_ROOT, sessionId));
  console.log(`[${sessionId}] 🔑 Credenciales cargadas (multi-file)`);

  const browserInfo =
    Browsers && typeof Browsers.appropriate === 'function'
      ? Browsers.appropriate('Chrome')
      : ['Chrome', 'Linux', '110.0.0'];

  const { version } = await fetchLatestBaileysVersion();
  console.log(`[${sessionId}] 📦 Baileys version negociada: ${version?.join?.('.') || version}`);

  const sock = makeWASocket({
    version,
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

  sock.ev.on('creds.update', (...args) => {
    console.log(`[${sessionId}] 💾 creds.update`);
    saveCreds(...args);
  });

  const existingTimer = existing?.qrTimer;
  if (existingTimer) {
    try { clearInterval(existingTimer); } catch (_) {}
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

      if (statusCode === DisconnectReason.loggedOut && AUTO_PURGE_ON_LOGOUT) {
        const dir = path.join(AUTH_ROOT, sessionId);
        try {
          if (fs.existsSync(dir)) fs.rmSync(dir, { recursive: true, force: true });
          console.log(`[${sessionId}] 🧹 Purga automática de credenciales por 401`);
        } catch {}
      }

      const shouldRestart = statusCode !== DisconnectReason.loggedOut;
      if (shouldRestart) {
        upsertSession(sessionId, { status: 'connecting' });
        await wait(1500);
        try {
          console.log(`[${sessionId}] 🔁 Reintentando startSession() tras cierre`);
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
    console.log(
      `[${sessionId}] ✉️ inbound from=${m.key.remoteJid} type=${Object.keys(m.message || {}).join(',')}`
    );
    try {
      await httpFetch(N8N_INCOMING_WEBHOOK, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sessionId, from: m.key.remoteJid, text, raw: m }),
      });
      console.log(`[${sessionId}] 🔁 reenviado a n8n (len=${text?.length || 0})`);
    } catch (e) {
      console.error(`[${sessionId}] ⚠️ Error enviando a n8n:`, e?.message || e);
    }
  });

  console.log(`[${sessionId}] ◀️ startSession() listo (status=${sessions.get(sessionId)?.status})`);
  return sessions.get(sessionId);
}

/* ========= Helpers ========= */
async function forceQR(sessionId) {
  console.log(`[${sessionId}] ♻️ forceQR() llamado`);
  let s = sessions.get(sessionId);
  if (!s) {
    console.log(`[${sessionId}] forceQR() -> no había sesión, iniciando`);
    s = await startSession(sessionId);
    return sessions.get(sessionId);
  }

  console.log(`[${sessionId}] forceQR() -> refrescando QR`);
  try { s.qr = null; } catch (_) {}
  try { s.qrAt = null; } catch (_) {}
  try { s.sock?.ws?.close?.(); } catch (_) {}
  upsertSession(sessionId, { sock: null, status: 'connecting' });

  await startSession(sessionId);

  console.log(`[${sessionId}] forceQR() -> completado`);
  return sessions.get(sessionId);
}

/* ========= Routes ========= */
app.post('/sessions/:id/start', async (req, res) => {
  console.log(`[${req.rid}] [ROUTE] POST /sessions/:id/start id=${req.params.id}`);
  try {
    const id = req.params.id;
    const s = await startSession(id);
    console.log(`[${req.rid}] [ROUTE] start -> status=${s.status}`);
    return res.json({
      sessionId: id,
      status: s.status || 'connecting',
      qr: s.qr || null,
      startedAt: s.startedAt || null,
      lastUpdate: s.lastUpdate || Date.now(),
    });
  } catch (e) {
    console.error(`[${req.rid}] start_failed:`, e?.message || e);
    return res.status(500).json({ error: 'start_failed' });
  }
});

app.get('/sessions/:id/status', (req, res) => {
  console.log(`[${req.rid}] [ROUTE] GET /sessions/:id/status id=${req.params.id}`);
  const s = sessions.get(req.params.id);
  if (!s) {
    console.warn(`[${req.rid}] session_not_found`);
    return res.status(404).json({ error: 'session_not_found' });
  }
  return res.json({
    sessionId: req.params.id,
    status: s.status || 'inactive',
    hasQR: !!s.qr,
    lastUpdate: s.lastUpdate || null,
    startedAt: s.startedAt || null,
  });
});

app.get('/sessions/:id/qr', (req, res) => {
  console.log(`[${req.rid}] [ROUTE] GET /sessions/:id/qr id=${req.params.id}`);
  const s = sessions.get(req.params.id);
  if (!s) {
    console.warn(`[${req.rid}] session_not_found`);
    return res.status(404).json({ error: 'session_not_found' });
  }
  return res.json({ sessionId: req.params.id, qr: s.qr || null, qrAt: s.qrAt || null });
});

app.get('/sessions', (req, res) => {
  console.log(`[${req.rid}] [ROUTE] GET /sessions`);
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
  console.log(`[${req.rid}] [ROUTE] POST /sessions/:id/qr/refresh id=${req.params.id}`);
  const id = req.params.id;
  try {
    const s = await forceQR(id);
    return res.json({
      ok: true,
      sessionId: id,
      status: s?.status || 'connecting',
      qr: s?.qr || null,
      qrAt: s?.qrAt || null,
      lastUpdate: s?.lastUpdate || null,
    });
  } catch (e) {
    console.error(`[${req.rid}] qr_refresh_failed:`, e?.message || e);
    return res.status(500).json({ error: 'qr_refresh_failed' });
  }
});

app.delete('/sessions/:id', async (req, res) => {
  console.log(`[${req.rid}] [ROUTE] DELETE /sessions/:id id=${req.params.id}`);
  const id = req.params.id;
  const s = sessions.get(id);
  try {
    if (s?.sock) {
      console.log(`[${req.rid}] logout/end socket`);
      await s.sock.logout().catch(() => {});
      try { s.sock.end?.(); } catch (_) {}
      try { s.sock.ws?.close?.(); } catch (_) {}
    }
    try { clearInterval(s?.qrTimer); } catch (_) {}
  } finally {
    sessions.delete(id);
    const dir = path.join(AUTH_ROOT, id);
    try {
      if (fs.existsSync(dir)) {
        fs.rmSync(dir, { recursive: true, force: true });
        console.log(`[${req.rid}] auth dir purgado: ${dir}`);
      }
    } catch {}
  }
  res.json({ ok: true, sessionId: id });
});

app.post('/sessions/:id/reset', async (req, res) => {
  console.log(`[${req.rid}] [ROUTE] POST /sessions/:id/reset id=${req.params.id}`);
  const id = req.params.id;
  const dir = path.join(AUTH_ROOT, id);
  try {
    const s = sessions.get(id);
    if (s?.sock) {
      console.log(`[${req.rid}] reset -> logout/end socket`);
      await s.sock.logout().catch(() => {});
      try { s.sock.end?.(); } catch (_) {}
      try { s.sock.ws?.close?.(); } catch (_) {}
    }
    try { clearInterval(s?.qrTimer); } catch (_) {}
    sessions.delete(id);

    if (fs.existsSync(dir)) {
      fs.rmSync(dir, { recursive: true, force: true });
      console.log(`[${req.rid}] reset -> auth dir borrado: ${dir}`);
    }

    const ns = await startSession(id);
    console.log(`[${req.rid}] reset -> nueva sesión status=${ns?.status}`);
    return res.json({ ok: true, sessionId: id, status: ns?.status || 'connecting' });
  } catch (e) {
    console.error(`[${req.rid}] reset_failed:`, e?.message || e);
    return res.status(500).json({ error: 'reset_failed' });
  }
});

app.post('/messages', async (req, res) => {
  console.log(`mensaje recibido`, req.body);
  try {
    const { sessionId, to, text } = req.body;
    console.log(`[${req.rid}] send -> sessionId=${sessionId} to=${to} len=${(text || '').length}`);
    const s = sessions.get(sessionId);
    if (!s || !s.sock) return res.status(404).json({ error: 'session_not_found_or_inactive' });
    if (s.status !== 'active') return res.status(409).json({ error: 'session_not_active' });
    const jid = to.includes('@') ? to : `${to}@s.whatsapp.net`;
    const r = await s.sock.sendMessage(jid, { text });
    console.log(`[${req.rid}] send -> ok id=${r?.key?.id}`);
    res.json({ ok: true, response: r });
  } catch (e) {
    console.error(`[${req.rid}] send_failed:`, e?.message || e);
    res.status(500).json({ error: 'send_failed' });
  }
});

/* ========= Health ========= */
app.get('/health', (req, res) => {
  console.log(`[${req.rid}] [ROUTE] GET /health`);
  res.json({ ok: true });
});

/* ========= Listen ========= */
app.listen(PORT, () => {
  console.log(`🚀 WA Gateway escuchando en http://localhost:${PORT}`);
  if (openapi) console.log(`📑 Docs disponibles en http://localhost:${PORT}/docs`);
});
