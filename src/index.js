
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

process.on('unhandledRejection', (reason) => {
  console.error('🧨 UnhandledRejection:', reason);
});
process.on('uncaughtException', (err) => {
  console.error('🧨 UncaughtException:', err);
});

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

const app = express();
app.use(cors({
  origin: (origin, cb) => cb(null, true), // AJUSTA dominios si expones público
  credentials: false
}));
app.use(body.json({ limit: '2mb' }));

const API_KEY = process.env.API_KEY || '';
app.use((req, res, next) => {
  if (!API_KEY) return next();
  const key = req.header('x-api-key');
  if (key !== API_KEY) return res.status(401).json({ error: 'unauthorized' });
  next();
});

app.use((req, res, next) => {
  const rid = (Math.random() + 1).toString(36).slice(2, 8);
  req.rid = rid;
  const start = Date.now();
  const shortBody = req.method !== 'GET' && req.body ? JSON.stringify(req.body).slice(0, 600) : '';
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

const sessions = new Map();
const sessionLocks = new Map();
const reconnectState = new Map();
const lastForce = new Map();
const dailyCounters = new Map();
const pauseMap = new Map();
const optOut = new Set();

const lastInbound = new Map();   
const lastSentTo = new Map();   
const recentBroadcasts = [];     

const cooldowns = new Map();
function now() { return Date.now(); }
function msLeft(until) { return Math.max(0, until - now()); }
function fmtMs(ms) {
  const s = Math.ceil(ms/1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s/60), r = s%60;
  if (m < 60) return `${m}m${r?` ${r}s`:''}`;
  const h = Math.floor(m/60), mr = m%60;
  return `${h}h${mr?` ${mr}m`:''}`;
}
function checkCooldown(key) {
  const entry = cooldowns.get(key);
  if (!entry) return { cooling: false, remaining: 0 };
  const remaining = msLeft(entry.until);
  if (remaining <= 0) { cooldowns.delete(key); return { cooling: false, remaining: 0 }; }
  return { cooling: true, remaining, reason: entry.reason, strikes: entry.strikes || 0 };
}
function setCooldown(key, baseMs, reason = 'cooldown') {
  const prev = cooldowns.get(key);
  const strikes = (prev?.strikes || 0) + 1;
  const mult = [1, 2, 3, 4][Math.min(strikes-1, 3)];
  const duration = Math.min(baseMs * mult, 60 * 60 * 1000);
  const until = now() + duration;
  cooldowns.set(key, { until, reason, strikes, lastSet: now() });
  return { strikes, duration };
}
function clearCooldown(key) { cooldowns.delete(key); }
const cdKeys = {
  session: (sessionId) => `cd:session:${sessionId}`,
  contact: (sessionId, jid) => `cd:contact:${sessionId}:${jid}`,
  antiblast: (sessionId) => `cd:antiblast:${sessionId}`,
};

const outbox = new Map(); 
let JOB_SEQ = 0;

const CONTACT_COOLDOWN_BASE_MS = 30_000;
const RAPID_FIRE_WINDOW_MS     = 15_000;
const RAPID_FIRE_COOLDOWN_MS   = 2 * 30_000;
const ANTIBLAST_COOLDOWN_MS    = 10 * 30_000;
const RECENT_INBOUND_MS        = 15_000;
const ACCOUNT_WARMUP_DAYS      = 10;

function outboxKey(sessionId, jid) { return `${sessionId}:${jid}`; }
function enqueueMessage(sessionId, jid, text) {
  const key = outboxKey(sessionId, jid);
  const q = outbox.get(key) || [];
  const id = `job_${++JOB_SEQ}`;
  const job = { id, sessionId, jid, text, attempts: 0, createdAt: Date.now() };
  q.push(job);
  outbox.set(key, q);
  console.log(`[${sessionId}] 📬 Encolado ${id} para ${jid} (len=${q.length})`);
  return job;
}
function dequeueMessage(job) {
  const key = outboxKey(job.sessionId, job.jid);
  const q = outbox.get(key) || [];
  const idx = q.findIndex(j => j.id === job.id);
  if (idx >= 0) q.splice(idx, 1);
  outbox.set(key, q);
}
function peekMessage(sessionId, jid) {
  const q = outbox.get(outboxKey(sessionId, jid)) || [];
  return q[0] || null;
}

function withinBusinessHours(tzOffset = -5) {
  const d = new Date();
  const h = d.getUTCHours() + tzOffset;
  const hour = (h + 24) % 24;
  return hour >= 8 && hour <= 21;
}
function dynamicDailyCap(sessionId, baseCap = 120, maxCap = 600) {
  const s = sessions.get(sessionId);
  const started = s?.startedAt || Date.now();
  const days = Math.max(1, Math.floor((Date.now() - started) / (24*60*60*1000)));
  const factor = Math.min(1, days / ACCOUNT_WARMUP_DAYS);
  return Math.round(baseCap + (maxCap - baseCap) * factor);
}
function canSendToday(sessionId, limit) {
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
const pauseEscalation = new Map();
function escalatePause(sessionId) {
  const cur = (pauseEscalation.get(sessionId) || 0) + 1;
  pauseEscalation.set(sessionId, cur);
  const mins = [1, 5, 15, 60][Math.min(cur-1, 3)];
  const ms = mins * 60_000;
  pause(sessionId, ms);
  console.warn(`[${sessionId}] ⏸️ Pausa escalada por ${mins}min (strikes=${cur})`);
}

const buckets = new Map();
const RL_SESSION_CAPACITY = 6;
const RL_SESSION_REFILL   = 1;
const RL_JID_CAPACITY     = 3;
const RL_JID_REFILL       = 0.25;
function tokenBucket(key, capacity, refillPerSec) {
  const nowTs = Date.now();
  const b = buckets.get(key) || { tokens: capacity, last: nowTs };
  const elapsed = (nowTs - b.last) / 1000;
  b.tokens = Math.min(capacity, b.tokens + elapsed * refillPerSec);
  b.last = nowTs;
  const ok = b.tokens >= 1;
  if (ok) b.tokens -= 1;
  buckets.set(key, b);
  return ok;
}
function canSendRate(sessionId, jid) {
  const okSession = tokenBucket(`s:${sessionId}`, RL_SESSION_CAPACITY, RL_SESSION_REFILL);
  const okJid     = tokenBucket(`j:${sessionId}:${jid}`, RL_JID_CAPACITY, RL_JID_REFILL);
  return okSession && okJid;
}

async function simulateHumanTyping(sock, jid, text) {
  try {
    await sock.presenceSubscribe(jid);
    await sock.sendPresenceUpdate('available', jid);
    await sock.sendPresenceUpdate('composing', jid);
    const base = Math.min(3000, 300 + (text.length * 18));
    await wait(jitter(base));
    await sock.sendPresenceUpdate('paused', jid);
  } catch (_) {}
}

const typingBursts = new Map(); 
async function typingBurst(sessionId, jid, durationMs = 2200) {
  const s = sessions.get(sessionId);
  if (!s?.sock) return;
  try {
    await s.sock.presenceSubscribe(jid);
    await s.sock.sendPresenceUpdate('available', jid);
    await s.sock.sendPresenceUpdate('composing', jid);
    const dur = Math.max(900, Math.min(3500, durationMs));
    await wait(jitter(dur, 0.3));
    await s.sock.sendPresenceUpdate('paused', jid);
  } catch (_) {}
}
function scheduleTypingBurst(sessionId, jid, etaMs) {
  const k = `${sessionId}:${jid}`;
  const prev = typingBursts.get(k) || [];
  prev.forEach((id) => { try { clearTimeout(id); } catch {} });
  const timers = [];
  const t1 = Math.max(etaMs - 7000, 0);
  const t2 = Math.max(etaMs - 2000, 0);
  if (etaMs > 1500 && t1 > 0) timers.push(setTimeout(() => typingBurst(sessionId, jid, 2200), t1));
  if (etaMs > 800 && t2 > 0)  timers.push(setTimeout(() => typingBurst(sessionId, jid, 1600), t2));
  typingBursts.set(k, timers);
}
function clearTypingBursts(sessionId, jid) {
  const k = `${sessionId}:${jid}`;
  const arr = typingBursts.get(k) || [];
  arr.forEach((id) => { try { clearTimeout(id); } catch {} });
  typingBursts.delete(k);
}

function registerBroadcast(sessionId, text, jid) {
  const nowTs = Date.now();
  recentBroadcasts.push({ t: nowTs, sessionId, text: (text||'').trim(), jid });
  while (recentBroadcasts.length && (nowTs - recentBroadcasts[0].t) > (10 * 60_000)) {
    recentBroadcasts.shift();
  }
}
function looksLikeBlast(sessionId, text) {
  const norm = (text||'').trim();
  if (!norm) return false;
  const nowTs = Date.now();
  const win = recentBroadcasts.filter(r =>
    r.sessionId === sessionId &&
    (nowTs - r.t) <= (10 * 60_000) &&
    r.text === norm
  );
  const unique = new Set(win.map(w => w.jid));
  return unique.size >= 8;
}
function hadRecentInbound(sessionId, jid, hours = 48) {
  const ts = lastInbound.get(`${sessionId}:${jid}`) || 0;
  return (Date.now() - ts) <= (hours * 60 * 60 * 1000);
}
function containsLink(s='') { return /\bhttps?:\/\/|\bwww\./i.test(s); }

async function trySendJob(job) {
  job.attempts++;
  const { sessionId, jid, text } = job;

  const sessCD = checkCooldown(cdKeys.session(sessionId));
  if (sessCD.cooling) {
    const delay = sessCD.remaining + jitter(250, 0.4);
    return scheduleJob(job, delay);
  }
  const contactCD = checkCooldown(cdKeys.contact(sessionId, jid));
  if (contactCD.cooling) {
    const delay = contactCD.remaining + jitter(250, 0.4);
    return scheduleJob(job, delay);
  }
  const s = sessions.get(sessionId);
  if (!s || !s.sock || s.status !== 'active') {
    return scheduleJob(job, 5_000);
  }

  try {
    await simulateHumanTyping(s.sock, jid, text);
    await wait(jitter(600, 0.6));
    const r = await s.sock.sendMessage(jid, { text: text.trim() });

    registerBroadcast(sessionId, text, jid);
    lastSentTo.set(`${sessionId}:${jid}`, Date.now());

    dequeueMessage(job);
    clearTypingBursts(sessionId, jid);
    console.log(`[${sessionId}] ✅ ${job.id} enviado -> id=${r?.key?.id}`);
  } catch (e) {
    console.error(`[${sessionId}] ❌ ${job.id} fallo envío:`, e?.message || e);
    setCooldown(cdKeys.contact(sessionId, jid), RAPID_FIRE_COOLDOWN_MS, 'retry_after_error');
    scheduleJob(job, RAPID_FIRE_COOLDOWN_MS + jitter(300, 0.5));
  }
}
function scheduleJob(job, delayMs) {
  scheduleTypingBurst(job.sessionId, job.jid, delayMs);

  setTimeout(() => {
    const head = peekMessage(job.sessionId, job.jid);
    if (!head || head.id !== job.id) return;
    trySendJob(job);
  }, Math.max(0, delayMs));
}

function upsertSession(sessionId, patch) {
  const prev = sessions.get(sessionId) || {};
  const next = { ...prev, ...patch, lastUpdate: Date.now() };
  sessions.set(sessionId, next);
  console.log(`[${sessionId}] 🧩 upsertSession -> patched: ${Object.keys(patch).join(', ') || 'none'}`);
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
  const base = Math.min(1000 * Math.pow(2, s.attempts - 1), 60000);
  return jitter(base, 0.5);
}
function resetBackoff(id) { reconnectState.delete(id); }

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
    try { ({ version } = await fetchLatestBaileysVersion()); }
    catch { version = [2, 3000, 0]; }
    console.log(`[${sessionId}] 📦 Baileys version negociada: ${version?.join?.('.') || version}`);

    const sock = makeWASocket({
      version,
      browser: browserInfo,
      auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys) },
      syncFullHistory: false,
    });

    upsertSession(sessionId, {
      sock, qr: null, qrAt: null,
      status: 'connecting',
      startedAt: Date.now(),
    });

    sock.ev.on('creds.update', (...args) => { console.log(`[${sessionId}] 💾 creds.update`); saveCreds(...args); });

    const existingTimer = existing?.qrTimer;
    if (existingTimer) { try { clearInterval(existingTimer); } catch {} }
    const qrTimer = setInterval(() => {
      const s = sessions.get(sessionId);
      if (!s) { try { clearInterval(qrTimer); } catch {} return; }
      const stillWaiting = s.status !== 'active';
      const nowTs = Date.now();
      const qrExpired =
        (s.qrAt && (nowTs - s.qrAt) > QR_TTL_MS) ||
        (!s.qrAt && s.startedAt && (nowTs - s.startedAt) > QR_TTL_MS);
      if (stillWaiting && qrExpired) {
        console.log(`[${sessionId}] ⏲️ QR expirado; reiniciando socket para forzar nuevo QR`);
        try { s.qr = null; } catch {}
        try { s.qrAt = null; } catch {}
        try { s.sock?.ws?.close?.(); } catch {}
      }
    }, WATCHDOG_INTERVAL_MS);
    upsertSession(sessionId, { qrTimer });

    sock.ev.on('connection.update', async ({ connection, lastDisconnect, qr }) => {
      if (qr) {
        console.log(`[${sessionId}] 🔐 QR recibido a las ${new Date().toISOString()}`);
        upsertSession(sessionId, { qr, qrAt: Date.now() });
        try { qrcode.generate(qr, { small: true }); } catch {}
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
        try { clearInterval(s.qrTimer); } catch {}
        upsertSession(sessionId, { qrTimer: null });
      }
    });

    sock.ev.on('messages.upsert', async ({ messages }) => {
      const m = messages?.[0];
      if (!m || m.key.fromMe) return;

      try { await sock.readMessages([m.key]); } catch {}

      const from = m.key.remoteJid || '';
      const msg = m.message || {};
      const type = getContentType(msg) || Object.keys(msg || {}).join(',') || 'unknown';
      const text = extractTextFromMessage(msg) || '';

      console.log(`[${sessionId}] ✉️ inbound from=${from} type=${type} keys=${Object.keys(msg || {}).join(',')}`);

      lastInbound.set(`${sessionId}:${from}`, Date.now());
      clearCooldown(cdKeys.contact(sessionId, from));

      const low = (text || '').trim().toLowerCase();
      if (['stop', 'salir', 'baja', 'no molestar'].includes(low)) {
        optOut.add(from);
        try { await sock.sendMessage(from, { text: "Listo, no volverás a recibir mensajes. ✅" }); } catch {}
        return;
      }
      if (optOut.has(from)) return;

      const basePayload = {
        sessionId,
        from,
        messageId: m.key.id,
        timestamp: (m.messageTimestamp || 0) * 1000,
        type, text
      };

      let mediaPayload = null;
      try {
        if (['imageMessage','videoMessage','audioMessage','documentMessage','stickerMessage'].includes(type)) {
          const nodeForDownload = msg?.ephemeralMessage?.message || msg;
          const buffer = await downloadMediaMessage(
            { ...m, message: nodeForDownload },
            'buffer',
            {},
            { reuploadRequest: sock.updateMediaMessage }
          );

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
            kind: type.replace('Message',''),
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
          console.log(`[${sessionId}] 🔁 reenviado a n8n (type=${type} textLen=${text?.length || 0} media=${!!mediaPayload})`);
        } catch (e) {
          console.error(`[${sessionId}] ⚠️ Error enviando a n8n:`, e?.message || e);
        }
      }
    });

    console.log(`[${sessionId}] ◀️ startSession() listo (status=${sessions.get(sessionId)?.status})`);
    return sessions.get(sessionId);
  });
}

async function forceQR(sessionId) {
  const nowTs = Date.now();
  const last = lastForce.get(sessionId) || 0;
  if (nowTs - last < 60_000) {
    console.log(`[${sessionId}] ⏳ forceQR throttled`);
    return sessions.get(sessionId) || await startSession(sessionId);
  }
  lastForce.set(sessionId, nowTs);

  console.log(`[${sessionId}] ♻️ forceQR() llamado`);
  let s = sessions.get(sessionId);
  if (!s) {
    console.log(`[${sessionId}] forceQR() -> no había sesión, iniciando`);
    s = await startSession(sessionId);
    return sessions.get(sessionId);
  }
  console.log(`[${sessionId}] forceQR() -> refrescando QR`);
  try { s.qr = null; } catch {}
  try { s.qrAt = null; } catch {}
  try { s.sock?.ws?.close?.(); } catch {}
  upsertSession(sessionId, { sock: null, status: 'connecting' });
  await startSession(sessionId);
  console.log(`[${sessionId}] forceQR() -> completado`);
  return sessions.get(sessionId);
}

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
      try { s.sock.end?.(); } catch {}
      try { s.sock.ws?.close?.(); } catch {}
    }
    try { clearInterval(s?.qrTimer); } catch {}
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
      try { s.sock.end?.(); } catch {}
      try { s.sock.ws?.close?.(); } catch {}
    }
    try { clearInterval(s?.qrTimer); } catch {}
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
    if (!withinBusinessHours(-5)) {
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

    if (looksLikeBlast(sessionId, text)) {
      const { duration, strikes } = setCooldown(cdKeys.session(sessionId), ANTIBLAST_COOLDOWN_MS, 'anti_blast');
      return res.status(429).json({ error: 'anti_blast_triggered', retry_in_ms: duration, strikes });
    }
    const recentConv = hadRecentInbound(sessionId, jid, 48);
    if (!recentConv && containsLink(text)) {
      return res.status(400).json({ error: 'no_links_on_cold_start' });
    }

    const cdContactKey = cdKeys.contact(sessionId, jid);

    const inboundTs = lastInbound.get(`${sessionId}:${jid}`) || 0;
    const sinceInbound = Date.now() - inboundTs;
    if (sinceInbound > 0 && sinceInbound < RECENT_INBOUND_MS) {
      const remaining = Math.max(CONTACT_COOLDOWN_BASE_MS - sinceInbound, 5_000);
      setCooldown(cdContactKey, remaining, 'recent_inbound');
      const job = enqueueMessage(sessionId, jid, text);
      const eta = remaining + jitter(200, 0.5);
      scheduleTypingBurst(sessionId, jid, eta);
      scheduleJob(job, eta);
      return res.status(202).json({ queued: true, jobId: job.id, reason: 'recent_inbound', retry_in_ms: remaining });
    }

    {
      const { cooling, remaining, reason } = checkCooldown(cdContactKey);
      if (cooling) {
        const job = enqueueMessage(sessionId, jid, text);
        const eta = remaining + jitter(200, 0.5);
        scheduleTypingBurst(sessionId, jid, eta);
        scheduleJob(job, eta);
        return res.status(202).json({ queued: true, jobId: job.id, reason: reason || 'contact_cooldown', retry_in_ms: remaining });
      }
    }

    {
      const k = `${sessionId}:${jid}`;
      const last = lastSentTo.get(k) || 0;
      if (Date.now() - last < RAPID_FIRE_WINDOW_MS) {
        const { duration } = setCooldown(cdContactKey, RAPID_FIRE_COOLDOWN_MS, 'rapid_fire_contact');
        const job = enqueueMessage(sessionId, jid, text);
        const eta = duration + jitter(200, 0.5);
        scheduleTypingBurst(sessionId, jid, eta);
        scheduleJob(job, eta);
        return res.status(202).json({ queued: true, jobId: job.id, reason: 'rapid_fire_contact', retry_in_ms: duration });
      }
    }

    if (!canSendRate(sessionId, jid)) {
      return res.status(429).json({ error: 'rate_limited' });
    }

    const todaysCap = dynamicDailyCap(sessionId, 120, 600);
    if (!canSendToday(sessionId, todaysCap)) {
      return res.status(429).json({ error: 'daily_cap_reached' });
    }

    await simulateHumanTyping(s.sock, jid, text);
    await wait(jitter(600, 0.6));
    const r = await s.sock.sendMessage(jid, { text: text.trim() });

    registerBroadcast(sessionId, text, jid);
    lastSentTo.set(`${sessionId}:${jid}`, Date.now());
    clearTypingBursts(sessionId, jid);

    console.log(`[${req.rid}] send -> ok id=${r?.key?.id}`);
    res.json({ ok: true, response: r });
  } catch (e) {
    console.error(`[${req.rid}] send_failed:`, e?.message || e);
    const sessionId = req.body?.sessionId;
    const msg = String(e?.message || '');
    if (msg.toLowerCase().includes('not-authorized') ||
        msg.toLowerCase().includes('blocked') ||
        msg.includes('429')) {
      escalatePause(sessionId);
      setCooldown(cdKeys.session(sessionId), 5 * 60_000, 'provider_signal_risk');
    }
    res.status(500).json({ error: 'send_failed' });
  }
});

app.get('/health', (req, res) => {
  console.log(`[${req.rid}] [ROUTE] GET /health`);
  res.json({ ok: true });
});

app.get('/debug/outbox', (req, res) => {
  const out = [];
  for (const [key, q] of outbox.entries()) {
    out.push({
      key,
      size: q.length,
      jobs: q.map(j => ({ id: j.id, createdAt: new Date(j.createdAt).toISOString(), attempts: j.attempts }))
    });
  }
  res.json(out);
});

app.get('/debug/cooldowns', (req, res) => {
  const out = [];
  for (const [key, v] of cooldowns.entries()) {
    const remaining = msLeft(v.until);
    if (remaining <= 0) continue;
    out.push({
      key,
      reason: v.reason,
      strikes: v.strikes,
      remaining_ms: remaining,
      remaining_pretty: fmtMs(remaining),
      last_set_at: new Date(v.lastSet).toISOString()
    });
  }
  res.json(out.sort((a,b) => b.remaining_ms - a.remaining_ms));
});

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
