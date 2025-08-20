const express = require('express');
const cors = require('cors');
const body = require('body-parser');
const qrcode = require('qrcode-terminal');
const fetch = (...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args));

const makeWASocket = require('@whiskeysockets/baileys').default;
const {
  useMultiFileAuthState,
  makeCacheableSignalKeyStore,
  Browsers,
  DisconnectReason
} = require('@whiskeysockets/baileys');
const { Boom } = require('@hapi/boom');

const app = express();
app.use(cors());
app.use(body.json({ limit: '2mb' }));

const PORT = process.env.PORT || 3000;
const N8N_INCOMING_WEBHOOK = "https://n8n.srv957249.hstgr.cloud/webhook/webhook/wa-in";  //"https://n8n.srv957249.hstgr.cloud/webhook-test/webhook/wa-in" 
const sessions = new Map();

async function startSession(sessionId) {
  const { state, saveCreds } = await useMultiFileAuthState(`./auth/${sessionId}`);

  const sock = makeWASocket({
    browser: Browsers.appropriate('Chrome'),
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys),
    },
    syncFullHistory: false
  });

  let qrStr = null;

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', ({ connection, lastDisconnect, qr }) => {
    if (qr) {
      qrStr = qr;
      console.log('\n📱 Escanea este QR para iniciar sesión:\n');
      qrcode.generate(qr, { small: true });
    }

    if (connection === 'close') {
      const code = lastDisconnect?.error instanceof Boom
        ? lastDisconnect.error.output.statusCode
        : 0;

      const isAuthenticated = !!state?.creds?.signedIdentityKey;

      if (code !== DisconnectReason.loggedOut && isAuthenticated) {
        console.log('🔁 Reconectando...');
        startSession(sessionId);
      } else {
        console.log(`❌ Sesión cerrada: ${sessionId}`);
        sessions.delete(sessionId);
      }
    }

    if (connection === 'open') {
      console.log(`✅ Sesión conectada: ${sessionId}`);
      qrStr = null;
    }
  });

  sock.ev.on('messages.upsert', async ({ messages }) => {
    const m = messages?.[0];
    if (!m || m.key.fromMe || !N8N_INCOMING_WEBHOOK) return;

    const text = m.message?.conversation
      || m.message?.extendedTextMessage?.text
      || m.message?.imageMessage?.caption || '';

    try {
      await fetch(N8N_INCOMING_WEBHOOK, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sessionId, from: m.key.remoteJid, text, raw: m })
      });
      console.log(`📤 Mensaje enviado a n8n: ${text}`);
    } catch (e) {
      console.error('⚠️ Error enviando mensaje a n8n:', e.message);
    }
  });

  sessions.set(sessionId, { sock, qr: qrStr });
  return { sock, getQR: () => qrStr };
}

app.post('/sessions/:id/start', async (req, res) => {
  const id = req.params.id;
  let s = sessions.get(id);
  if (!s) s = await startSession(id);
  return res.json({ sessionId: id, qr: sessions.get(id)?.qr || null });
});

app.get('/sessions/:id/qr', (req, res) => {
  const s = sessions.get(req.params.id);
  if (!s) return res.status(404).json({ error: 'session_not_found' });
  return res.json({ qr: s.qr });
});

app.post('/messages', async (req, res) => {
  console.log('📨 Solicitud para enviar mensaje:', req.body);
  try {
    const { sessionId, to, text } = req.body;
    console.log(`📩 Enviando mensaje a ${to} desde sesión ${sessionId}: ${text}`);
    const s = sessions.get(sessionId);
    if (!s) return res.status(404).json({ error: 'session_not_found' });

    const jid = to.includes('@') ? to : `${to}@s.whatsapp.net`;
    const r = await s.sock.sendMessage(jid, { text });

    res.json({ ok: true, response: r });
  } catch (e) {
    console.error('❌ Error enviando mensaje:', e);
    res.status(500).json({ error: 'send_failed' });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 WA Gateway escuchando en http://localhost:${PORT}`);
});
