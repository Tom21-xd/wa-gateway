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
  fetchLatestBaileysVersion,
  getContentType,
  downloadMediaMessage
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
function jitter(ms, spread = 0.4) {
  const delta = ms * spread;
  return ms + Math.floor((Math.random() * 2 - 1) * delta);
}
async function httpFetch(url, init) {
  const method = (init && init.method) || 'GET';
  console.log(`🌐 httpFetch -> ${method} ${url}`);
  if (typeof globalThis.fetch === 'function') return globalThis.fetch(url, init);
  const { default: nodeFetch } = await import('node-fetch');
  return nodeFetch(url, init);
}
function safeJson(obj, max = 8000) {
  try { return JSON.stringify(obj).slice(0, max); } catch { return '<unserializable>'; }
}
function inferExtFromMime(m) {
  if (!m) return 'bin';
  const map = {
    'image/jpeg': 'jpg',
    'image/jpg': 'jpg',
    'image/png': 'png',
    'image/webp': 'webp',
    'image/gif': 'gif',
    'video/mp4': 'mp4',
    'video/3gpp': '3gp',
    'audio/ogg': 'ogg',
    'audio/opus': 'opus',
    'audio/mpeg': 'mp3',
    'application/pdf': 'pdf',
    'application/zip': 'zip',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx',
    'application/msword': 'doc',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
    'application/vnd.ms-excel': 'xls'
  };
  return map[m] || (m.split('/')[1] || 'bin');
}
function extractTextFromMessage(msg) {
  return (
    msg?.conversation ||
    msg?.extendedTextMessage?.text ||
    msg?.imageMessage?.caption ||
    msg?.videoMessage?.caption ||
    msg?.documentMessage?.caption ||
    msg?.buttonsResponseMessage?.selectedDisplayText ||
    msg?.listResponseMessage?.singleSelectReply?.selectedRowId ||
    msg?.reactionMessage?.text ||
    msg?.ephemeralMessage?.message?.extendedTextMessage?.text ||
    ''
  );
}

/* ========= App setup ========= */
const app = express();
app.use(cors({
  origin: (origin, cb) => cb(null, true), // ajusta a dominios permitidos si expones público
  credentials: false
}));
app.use(body.json({ limit: '2mb' }));

// 🔐 API Key (opcional): exige x-api-key si está definida
const API_KEY = process.env.API_KEY || '';
app.use((req, res, next) => {
  if (!API_KEY) return next();
  const key = req.header('x-api-key');
  if (key !== API_KEY) return res.status(401).json({ error: 'unauthorized' });
  next();
});

// 🔎 Middleware de tracing por request
app.use((req, res, next) => {
  const rid = (Math.random() + 1).toString(36).slice(2, 8);
  req.rid = rid;
  const start = Date.now();
  const shortBody =
    req.method !== 'GET' && req.body
      ? JSON.stringify(req.body).slice(0, 600)
      : '';
  console.log(`[${rid}] ➡️ ${req.method} ${req.originalUrl} body=${shortBody || '<empty>'}`);
  res.on('finish', () => {
    const ms = Date.now() - start;
    console.log(`[${rid}] ⬅️ ${res.statusCode} ${req.method} ${req.originalUrl} (${ms}ms)`);
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

let openapi = null;
try {
  openapi = JSON.parse(fs.readFileSync(path.join(__dirname, 'openapi.json'), 'utf8'));
  app.use('/docs', swaggerUi.serve, swaggerUi.setup(openapi, { explorer: true }));
  console.log('📑 Swagger habilitado en /docs');
} catch (e) {
  console.warn('⚠️ No se encontró openapi.json; se omite /docs');
}

/* ========= In-memory state ========= */
const sessions = new Map();
const sessionLocks = new Map();      // locks de concurrencia start/reset
const reconnectState = new Map();    // backoff por sesión
const lastForce = new Map();         // throttle forceQR
const dailyCounters = new Map();     // cap diario por sesión
const pauseMap = new Map();          // pausa temporal por errores
const optOut = new Set();            // opt-out simple en memoria

function upsertSession(sessionId, patch) {
  const prev = sessions.get(sessionId) || {};
  const next = { ...prev, ...patch, lastUpdate: Date.now() };
  sessions.set(sessionId, next);
  const keysPatched = Object.keys(patch);
  console.log(`[${sessionId}] 🧩 upsertSession -> patched: ${keysPatched.join(', ') || 'none'}`);
  return next;
}

async function withLock(id, fn) {
  const prev = sessionLocks.get(id) || Promise.resolve();
  let release;
  const p = new Promise((res) => (release = res));
  sessionLocks.set(id, prev.then(() => p));
  try { return await fn(); } finally { release(); sessionLocks.delete(id); }
}

function nextDelay(id) {
  const s = reconnectState.get(id) || { attempts: 0, lastTs: 0 };
  s.attempts = Math.min(s.attempts + 1, 8);
  reconnectState.set(id, s);
  const base = Math.min(1000 * Math.pow(2, s.attempts - 1), 60000); // 1s..60s
  return jitter(base, 0.5);
}
function resetBackoff(id) { reconnectState.delete(id); }

function withinBusinessHours(tzOffset = -5) { // Colombia
  const d = new Date();
  const h = d.getUTCHours() + tzOffset;
  const hour = (h + 24) % 24;
  return hour >= 8 && hour <= 21;
}
function canSendToday(sessionId, limit = 400) {
  const key = new Date().toISOString().slice(0,10);
  const cur = dailyCounters.get(sessionId) || { dateKey: key, count: 0 };
  if (cur.dateKey !== key) { cur.dateKey = key; cur.count = 0; }
  const ok = cur.count < limit;
  if (ok) cur.count += 1;
  dailyCounters.set(sessionId, cur);
  return ok;
}
function isPaused(sessionId) {
  const until = pauseMap.get(sessionId) || 0;
  return Date.now() < until;
}
function pause(sessionId, ms = 5*60*1000) {
  pauseMap.set(sessionId, Date.now() + ms);
}

/* ========= Token-bucket Rate Limiter ========= */
const buckets = new Map(); // key -> {tokens, last}
function tokenBucket(key, capacity, refillPerSec) {
  const now = Date.now();
  const b = buckets.get(key) || { tokens: capacity, last: now };
  const elapsed = (now - b.last) / 1000;
  b.tokens = Math.min(capacity, b.tokens + elapsed * refillPerSec);
  b.last = now;
  const ok = b.tokens >= 1;
  if (ok) b.tokens -= 1;
  buckets.set(key, b);
  return ok;
}
function canSendRate(sessionId, jid) {
  const okSession = tokenBucket(`s:${sessionId}`, 10, 2);     // máx 10, repone 2/s
  const okJid     = tokenBucket(`j:${sessionId}:${jid}`, 5, 0.5); // máx 5, repone 0.5/s
  return okSession && okJid;
}

/* ========= Human-like typing ========= */
async function simulateHumanTyping(sock, jid, text) {
  try {
    await sock.presenceSubscribe(jid);
    await sock.sendPresenceUpdate('available', jid);
    await sock.sendPresenceUpdate('composing', jid);
    const base = Math.min(2500, 150 + (text.length * 15));
    await wait(jitter(base));
    await sock.sendPresenceUpdate('paused', jid);
  } catch (_) { /* no bloquear por presence */ }
}

/* ========= Core: start session ========= */
async function startSession(sessionId) {
  return withLock(sessionId, async () => {
    console.log(`[${sessionId}] ▶️ startSession() llamado`);
    const existing = sessions.get(sessionId);
    const isActive = existing?.status === 'active' && !!existing?.sock;
    if (isActive) {
      console.log(`[${sessionId}] ♻️ startSession() reutiliza socket activo`);
      return existing;
    }

    const { state, saveCreds } = await useMultiFileAuthState(path.join(AUTH_ROOT, sessionId));
    console.log(`[${sessionId}] 🔑 Credenciales cargadas (multi-file)`);

    const browserInfo =
      Browsers && typeof Browsers.appropriate === 'function'
        ? Browsers.appropriate('Chrome')
        : ['Chrome', 'Linux', '110.0.0'];

    let version;
    try {
      ({ version } = await fetchLatestBaileysVersion());
    } catch {
      version = [2, 3000, 0]; // fallback razonable
    }
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

    // Watchdog de QR
    const existingTimer = existing?.qrTimer;
    if (existingTimer) { try { clearInterval(existingTimer); } catch (_) {} }
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
          const delay = nextDelay(sessionId);
          console.log(`[${sessionId}] 🔁 Reintento en ${delay}ms`);
          await wait(delay);
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
        resetBackoff(sessionId);
        const s = upsertSession(sessionId, { status: 'active', qr: null, qrAt: null });
        try { clearInterval(s.qrTimer); } catch (_) {}
        upsertSession(sessionId, { qrTimer: null });
      }
    });

    // Inbound handler + opt-out + media download
    sock.ev.on('messages.upsert', async ({ messages }) => {
      const m = messages?.[0];
      if (!m || m.key.fromMe) return;

      const from = m.key.remoteJid || '';
      const msg = m.message || {};
      const type = getContentType(msg) || Object.keys(msg || {}).join(',') || 'unknown';
      const text = extractTextFromMessage(msg) || '';

      console.log(
        `[${sessionId}] ✉️ inbound from=${from} type=${type} keys=${Object.keys(msg || {}).join(',')}`
      );

      // Opt-out básico
      const low = (text || '').trim().toLowerCase();
      if (['stop', 'salir', 'baja', 'no molestar'].includes(low)) {
        optOut.add(from);
        try { await sock.sendMessage(from, { text: "Listo, no volverás a recibir mensajes. ✅" }); } catch {}
        return;
      }
      if (optOut.has(from)) return;

      // Construir payload base
      const basePayload = {
        sessionId,
        from,
        messageId: m.key.id,
        timestamp: (m.messageTimestamp || 0) * 1000,
        type,
        text
      };

      // Si es media, descargar y adjuntar
      let mediaPayload = null;
      try {
        if (['imageMessage','videoMessage','audioMessage','documentMessage','stickerMessage'].includes(type)) {
          const nodeForDownload =
            msg?.ephemeralMessage?.message || msg; // en caso de mensajes efímeros
          const buffer = await downloadMediaMessage(
            { ...m, message: nodeForDownload },
            'buffer',
            {},
            { reuploadRequest: sock.updateMediaMessage }
          );

          // metadatos por tipo
          let mime = 'application/octet-stream';
          let caption = '';
          let suggestedName = `wa_${m.key.id}`;
          if (type === 'imageMessage') {
            mime = nodeForDownload.imageMessage?.mimetype || 'image/jpeg';
            caption = nodeForDownload.imageMessage?.caption || '';
          } else if (type === 'videoMessage') {
            mime = nodeForDownload.videoMessage?.mimetype || 'video/mp4';
            caption = nodeForDownload.videoMessage?.caption || '';
          } else if (type === 'audioMessage') {
            mime = nodeForDownload.audioMessage?.mimetype || 'audio/ogg';
          } else if (type === 'documentMessage') {
            mime = nodeForDownload.documentMessage?.mimetype || 'application/octet-stream';
            suggestedName = nodeForDownload.documentMessage?.fileName || suggestedName;
          } else if (type === 'stickerMessage') {
            mime = 'image/webp';
          }

          const ext = inferExtFromMime(mime);
          const fileName = suggestedName.includes('.') ? suggestedName : `${suggestedName}.${ext}`;
          const base64 = Buffer.from(buffer).toString('base64');

          mediaPayload = {
            kind: type.replace('Message',''), // image, video, audio, document, sticker
            mime,
            bytes: buffer.length,
            base64,
            fileName,
            caption
          };
        }
      } catch (e) {
        console.error(`[${sessionId}] ⚠️ Error descargando media:`, e?.message || e);
      }

      // Reenvío a n8n con reintentos
      if (N8N_INCOMING_WEBHOOK) {
        try {
          const payload = mediaPayload ? { ...basePayload, media: mediaPayload } : basePayload;

          await (async function postToN8N(p) {
            const max = 3;
            for (let i = 1; i <= max; i++) {
              try {
                const r = await httpFetch(N8N_INCOMING_WEBHOOK, {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify(p),
                });
                if (!r.ok) throw new Error(`n8n_status_${r.status}`);
                return true;
              } catch (e) {
                if (i === max) throw e;
                await wait(500 * i);
              }
            }
          })(payload);

          console.log(
            `[${sessionId}] 🔁 reenviado a n8n (type=${type} textLen=${text?.length || 0} media=${!!mediaPayload})`
          );
        } catch (e) {
          console.error(`[${sessionId}] ⚠️ Error enviando a n8n:`, e?.message || e);
        }
      }
    });

    console.log(`[${sessionId}] ◀️ startSession() listo (status=${sessions.get(sessionId)?.status})`);
    return sessions.get(sessionId);
  });
}

/* ========= Helpers ========= */
async function forceQR(sessionId) {
  const now = Date.now();
  const last = lastForce.get(sessionId) || 0;
  if (now - last < 60_000) {
    console.log(`[${sessionId}] ⏳ forceQR throttled`);
    return sessions.get(sessionId) || await startSession(sessionId);
  }
  lastForce.set(sessionId, now);

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
  const msToExpire = s.qrAt ? Math.max(0, QR_TTL_MS - (Date.now() - s.qrAt)) : null;
  return res.json({
    sessionId: req.params.id,
    status: s.status || 'inactive',
    hasQR: !!s.qr,
    qrAt: s.qrAt || null,
    msToExpire,
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
  console.log(`[${req.rid}] [ROUTE] POST /messages`);
  try {
    const { sessionId, to, text } = req.body || {};
    if (!sessionId || !to || typeof text !== 'string' || !text.trim()) {
      return res.status(400).json({ error: 'invalid_payload' });
    }
    if (!withinBusinessHours(-5)) { // 🇨🇴 Ajusta si necesitas otro TZ
      return res.status(423).json({ error: 'outside_business_hours' });
    }
    if (isPaused(sessionId)) {
      return res.status(503).json({ error: 'session_paused' });
    }

    const s = sessions.get(sessionId);
    if (!s || !s.sock) return res.status(404).json({ error: 'session_not_found_or_inactive' });
    if (s.status !== 'active') return res.status(409).json({ error: 'session_not_active' });

    const jid = to.includes('@') ? to : `${to}@s.whatsapp.net`;
    if (optOut.has(jid)) return res.status(403).json({ error: 'recipient_opted_out' });

    if (!canSendRate(sessionId, jid)) {
      return res.status(429).json({ error: 'rate_limited' });
    }
    if (!canSendToday(sessionId, 400)) {
      return res.status(429).json({ error: 'daily_cap_reached' });
    }

    await simulateHumanTyping(s.sock, jid, text);
    const r = await s.sock.sendMessage(jid, { text: text.trim() });
    console.log(`[${req.rid}] send -> ok id=${r?.key?.id}`);
    res.json({ ok: true, response: r });
  } catch (e) {
    console.error(`[${req.rid}] send_failed:`, e?.message || e);
    // si es un error de autorización / patrón raro, pausa la sesión unos minutos
    if (String(e?.message || '').toLowerCase().includes('not-authorized')) {
      pause(req.body?.sessionId);
    }
    res.status(500).json({ error: 'send_failed' });
  }
});

/* ========= Health ========= */
app.get('/health', (req, res) => {
  console.log(`[${req.rid}] [ROUTE] GET /health`);
  res.json({ ok: true });
});

/* ========= Listen + graceful shutdown ========= */
const server = app.listen(PORT, () => {
  console.log(`🚀 WA Gateway escuchando en http://localhost:${PORT}`);
  if (openapi) console.log(`📑 Docs disponibles en http://localhost:${PORT}/docs`);
});

function gracefulExit(signal) {
  console.log(`\n🔚 ${signal} recibido. Cerrando...`);
  for (const [id, s] of sessions.entries()) {
    try { s.sock?.logout?.(); } catch {}
    try { s.sock?.end?.(); } catch {}
    try { s.sock?.ws?.close?.(); } catch {}
    try { clearInterval(s.qrTimer); } catch {}
  }
  server.close(() => process.exit(0));
}
process.on('SIGINT', () => gracefulExit('SIGINT'));
process.on('SIGTERM', () => gracefulExit('SIGTERM'));
