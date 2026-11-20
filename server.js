/* Go Tuning Garage — WhatsApp Cloud API webhook + panel backend.
 * Pure Node.js, no dependencies.
 * - GET  /webhook            Meta verification (hub.mode / hub.verify_token / hub.challenge)
 * - POST /webhook            WhatsApp events (messages, statuses)
 * - GET  /api/health
 * - GET  /api/conversations?key=...   list conversations + messages
 * - GET  /api/config?key=...         business info for the panel
 * - POST /api/send   {key, wa_id, text}          send text via Graph API
 * - POST /api/draft  {key, wa_id, message_id, text}  edit a held draft
 * - POST /api/approve {key, wa_id, message_id}   send the stored draft
 * - POST /api/mark-read {key, wa_id}
 * - GET  /media/:messageId?key=...   serve a downloaded client photo (key-gated)
 * Data is persisted to data/conversations.json (atomic writes).
 * The access token is read from ../whatsapp-credentials.json (server-side only).
 * DRAFT-ONLY MODE: nothing is ever sent to a client unless Omar/Gunter
 * explicitly press Enviar / Aprobar in the panel.
 * MEDIA (images only): when an inbound image message arrives, the server
 * records its media id and downloads the image immediately via the Graph API
 * (the temporary download URL expires in minutes, so the download happens
 * at arrival, never lazily). Files are saved under media/ next to this file
 * and served read-only through GET /media/:messageId?key=<panel_key>.
 * A failed download never breaks message storage, drafting, or the
 * webhook 200 response — the message is stored with media_status 'failed'.
 */
'use strict';

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');

const DIR = __dirname;
// Secrets come from ENVIRONMENT VARIABLES first (Render dashboard), with the
// local JSON files as fallback for runs on this VM. Never commit secrets.
// Env vars: WHATSAPP_TOKEN, WEBHOOK_VERIFY_TOKEN, PANEL_KEY,
// WHATSAPP_PHONE_NUMBER_ID, WHATSAPP_WABA_ID.
function loadJsonIfExists(p) {
  try {
    return JSON.parse(fs.readFileSync(p, 'utf8'));
  } catch (e) {
    return {};
  }
}
const CONFIG_FILE = loadJsonIfExists(path.join(DIR, '..', 'webhook-config.json'));
const CREDS_FILE = loadJsonIfExists(path.join(DIR, '..', 'whatsapp-credentials.json'));
const CONFIG = {
  verify_token: process.env.WEBHOOK_VERIFY_TOKEN || CONFIG_FILE.verify_token || '',
  panel_key: process.env.PANEL_KEY || CONFIG_FILE.panel_key || '',
  number: CONFIG_FILE.number || '+52 56 5321 7652',
  phone_number_id: process.env.WHATSAPP_PHONE_NUMBER_ID || CONFIG_FILE.phone_number_id || '',
  waba_id: process.env.WHATSAPP_WABA_ID || CONFIG_FILE.waba_id || '',
  app_name: CONFIG_FILE.app_name || 'Go Tuning Garage',
  webhook_url: process.env.RENDER_EXTERNAL_URL
    ? process.env.RENDER_EXTERNAL_URL + '/webhook'
    : CONFIG_FILE.webhook_url || '',
};
const ACCESS_TOKEN = process.env.WHATSAPP_TOKEN || CREDS_FILE.access_token || '';
const DATA_DIR = path.join(DIR, 'data');
const DATA_FILE = path.join(DATA_DIR, 'conversations.json');
const PORT = parseInt(process.env.PORT || '3100', 10);
const GRAPH_VERSION = 'v21.0';
// Media downloads (images only). The Graph media endpoint is queried with
// v27.0 (the version the webhook subscription uses). The temporary download
// URL Meta returns expires in minutes, so every image is downloaded at
// webhook arrival, never lazily on /media request.
const MEDIA_GRAPH_VERSION = 'v27.0';
const MEDIA_DIR = path.join(DIR, 'media');
const MEDIA_EXT_BY_MIME = {
  'image/jpeg': 'jpg',
  'image/jpg': 'jpg',
  'image/png': 'png',
  'image/webp': 'webp',
  'image/gif': 'gif',
};
const MEDIA_CONTENT_TYPE = {
  jpg: 'image/jpeg',
  jpeg: 'image/jpeg',
  png: 'image/png',
  webp: 'image/webp',
  gif: 'image/gif',
};

const BUSINESS = {
  name: 'GO TUNING GARAGE',
  city: 'Pachuca, Hidalgo',
  hours: 'Lunes a viernes 10:00–18:00 · Sábado 10:00–16:00 · Domingo cerrado',
  valuation: 'Valoración gratuita de 30 minutos en el taller, sin compromiso.',
  maps: 'https://maps.app.goo.gl/qHgUhDxh1cihyQcS9',
  address: 'Prol. Ley Agraria, 43845 Lindavista, Hgo.',
  phone: CONFIG.number,
  services: ['PPF', 'WRAP', 'Polarizado', 'Body kits', 'Radios multimedia', 'Iluminación', 'Equipamiento off-road'],
  greeting:
    'Hola, soy Ronin, asistente de GO TUNING GARAGE en Pachuca. Te ayudamos con personalización automotriz: PPF, WRAP, polarizado, body kits, radios multimedia, iluminación y equipamiento off-road. Cuéntame, ¿qué vehículo tienes y qué te gustaría realizarle?',
  noPriceReply:
    'Para darte una cotización correcta, primero necesitamos conocer tu vehículo y revisar exactamente lo que deseas realizar. En GO TUNING GARAGE hacemos una valoración gratuita y sin compromiso en nuestro taller; después de revisar tu proyecto podremos definir los materiales, el alcance del trabajo y presentarte una cotización adecuada.',
  valuationInvite:
    'Lo ideal es revisar tu vehículo directamente en nuestro taller para orientarte correctamente. La valoración es gratuita y sin compromiso. Puedes compartirnos tu disponibilidad y reviso los horarios para agendarte. Aquí tienes nuestra ubicación: https://maps.app.goo.gl/qHgUhDxh1cihyQcS9',
  holdingReply:
    'Gracias por tu mensaje. Estamos revisando tu caso con el equipo del taller y te confirmamos en breve.',
};

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(MEDIA_DIR)) fs.mkdirSync(MEDIA_DIR, { recursive: true });

function loadStore() {
  try {
    return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
  } catch (e) {
    return { conversations: {} };
  }
}

function saveStore(store) {
  const tmp = DATA_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(store, null, 2));
  fs.renameSync(tmp, DATA_FILE);
}

function norm(s) {
  return (s || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '');
}

const HAS = (text, words) => words.some((w) => norm(text).includes(norm(w)));

/* Classify an inbound client message per the operative document + checklist §3.
 * Returns { category, needs_approval, reason, draft }.
 * Categories: greeting | service | price | scheduling | confirm | routine
 *             | sensitive-sales | sensitive-complaint | sensitive-delay
 *             | sensitive-extra | sensitive-human | sensitive-cancel | out-of-catalog
 */
function classify(text) {
  const t = norm(text || '');
  if (HAS(t, ['precio', 'cuesta', 'cuestan', 'costo', 'cotiz', 'presupuesto', 'descuento', 'rebaja', 'promocion', 'barato', 'oferta', 'cuanto sale', 'cuanto cobra', 'cuanto me sale'])) {
    return {
      category: 'price',
      needs_approval: true,
      reason: 'Pregunta por precio — Ronin NO da precios por chat (documento operativo §4).',
      draft: BUSINESS.noPriceReply,
    };
  }
  if (HAS(t, ['queja', 'reclamo', 'reembolso', 'devolv', 'garantia', 'mal trabajo', 'rayado', 'rayon', 'danado', 'molesto', 'no me gusto', 'quedo mal', 'defecto', 'falla', 'fallo', 'pesimo', 'horrible', 'demanda', 'profeco'])) {
    return {
      category: 'sensitive-complaint',
      needs_approval: true,
      reason: 'Queja o reclamo — requiere aprobación de Omar/Gunter antes de responder.',
      draft: BUSINESS.holdingReply,
    };
  }
  if (HAS(t, ['retraso', 'retrasado', 'demora', 'demorado', 'cuando estara', 'aun no esta', 'todavia no', 'no esta listo', 'tarda mucho', 'se tardo'])) {
    return {
      category: 'sensitive-delay',
      needs_approval: true,
      reason: 'Retraso o demora — requiere aprobación de Omar/Gunter antes de responder.',
      draft: BUSINESS.holdingReply,
    };
  }
  if (HAS(t, ['costo extra', 'extra', 'adicional', 'encontraron', 'hallaron', 'hay que cambiar', 'se necesita cambiar', 'golpe', 'abolladura', 'danos nuevos', 'salio danado'])) {
    return {
      category: 'sensitive-extra',
      needs_approval: true,
      reason: 'Costo extra, hallazgo o daño — requiere aprobación de Omar/Gunter antes de responder.',
      draft: BUSINESS.holdingReply,
    };
  }
  if (HAS(t, ['hablar con una persona', 'hablar con omar', 'hablar con gunter', 'quiero hablar con', 'pasame con', 'un humano', 'una persona', 'el encargado', 'el dueno'])) {
    return {
      category: 'sensitive-human',
      needs_approval: true,
      reason: 'El cliente pide hablar con una persona — requiere aprobación de Omar/Gunter.',
      draft: BUSINESS.holdingReply,
    };
  }
  if (HAS(t, ['cancelar', 'cancela', 'reagendar', 'cambiar la cita', 'mover la cita', 'no puedo ir', 'no voy a poder', 'no podre ir'])) {
    return {
      category: 'sensitive-cancel',
      needs_approval: true,
      reason: 'Cancelación o cambio de cita — requiere aprobación de Omar/Gunter.',
      draft: BUSINESS.holdingReply,
    };
  }
  if (HAS(t, ['hola', 'buenas', 'buenos dias', 'buenas tardes', 'buenas noches', 'que tal'])) {
    return {
      category: 'greeting',
      needs_approval: false,
      reason: 'Saludo — respuesta rutinaria con el saludo oficial.',
      draft: BUSINESS.greeting,
    };
  }
  if (HAS(t, ['agendar', 'cita', 'valoracion', 'horario', 'disponible', 'cuando puedo', 'me pueden recibir', 'apartar'])) {
    return {
      category: 'scheduling',
      needs_approval: false,
      reason: 'Agenda — invitación a valoración gratuita (sin inventar horarios).',
      draft: BUSINESS.valuationInvite,
    };
  }
  if (HAS(t, ['confirmo', 'confirmar', 'si puedo', 'ahi estare', 'nos vemos', 'confirmada'])) {
    return {
      category: 'confirm',
      needs_approval: false,
      reason: 'Confirmación de cita — respuesta rutinaria.',
      draft: 'Listo, tu cita de valoración en GO TUNING GARAGE quedó confirmada. Te esperamos para revisar tu vehículo y conocer mejor tu proyecto.',
    };
  }
  if (HAS(t, ['ppf', 'wrap', 'polarizado', 'body kit', 'radio', 'pantalla', 'iluminacion', 'luces', 'faro', 'off-road', 'off road', 'suspension', 'llantas', 'rines', 'levantamiento'])) {
    return {
      category: 'service',
      needs_approval: false,
      reason: 'Pregunta por un servicio del catálogo — orientación general + pedir datos del vehículo.',
      draft:
        'Claro, con gusto te orientamos. Para ayudarte mejor, cuéntame: ¿qué vehículo tienes (marca, modelo y año) y qué te gustaría realizarle? Lo ideal es revisarlo en el taller con una valoración gratuita y sin compromiso.',
    };
  }
  if (HAS(t, ['gracias', 'perfecto', 'ok', 'vale', 'entendido', 'de acuerdo'])) {
    return {
      category: 'routine',
      needs_approval: false,
      reason: 'Mensaje rutinario de cortesía.',
      draft: 'Con gusto. Quedamos atentos por aquí para lo que necesites.',
    };
  }
  return {
    category: 'out-of-catalog',
    needs_approval: true,
    reason: 'Tema fuera del catálogo o sin clasificar — requiere revisión de Omar/Gunter antes de responder.',
    draft: BUSINESS.holdingReply,
  };
}

function extractText(msg) {
  if (!msg) return '';
  if (msg.type === 'text' && msg.text) return msg.text.body || '';
  if (msg.type === 'image') return '[Imagen recibida]' + (msg.image && msg.image.caption ? ' — ' + msg.image.caption : '');
  if (msg.type === 'video') return '[Video recibido]' + (msg.video && msg.video.caption ? ' — ' + msg.video.caption : '');
  if (msg.type === 'audio') return '[Audio recibido]';
  if (msg.type === 'document') return '[Documento recibido]' + (msg.document && msg.document.caption ? ' — ' + msg.document.caption : '');
  if (msg.type === 'sticker') return '[Sticker recibido]';
  if (msg.type === 'location') return '[Ubicación recibida]';
  if (msg.type === 'reaction') return '[Reacción: ' + (msg.reaction ? msg.reaction.emoji || '' : '') + ']';
  return '[' + (msg.type || 'mensaje') + ' recibido]';
}

function getOrCreateConv(store, waId, name) {
  if (!store.conversations[waId]) {
    store.conversations[waId] = {
      wa_id: waId,
      name: name || waId,
      phone: '+' + waId,
      messages: [],
      unread: 0,
      created_at: new Date().toISOString(),
    };
  } else if (name && store.conversations[waId].name === waId) {
    store.conversations[waId].name = name;
  }
  return store.conversations[waId];
}

/* ---- Media download (images only) ---- */

// File-safe base name for a message id. WhatsApp message ids (wamid.*) are
// base64-ish and may contain '.', '=', '+' or '/'; all of those are mapped
// to '_' so the name can never escape MEDIA_DIR or split a URL path.
// The same function is used when saving and when serving, so the mapping
// is consistent in both directions.
function mediaBaseName(messageId) {
  return String(messageId || '').replace(/[^A-Za-z0-9_-]/g, '_');
}

// GET /{media-id} on the Graph API: returns { url, mime_type, ... } where
// url is a temporary download URL (expires in minutes). Rejects on any
// non-2xx status or a response without a url. Never logs the token or url.
function graphGetMediaJson(mediaId) {
  return new Promise((resolve, reject) => {
    const req = https.request(
      {
        hostname: 'graph.facebook.com',
        path: '/' + MEDIA_GRAPH_VERSION + '/' + encodeURIComponent(String(mediaId)),
        method: 'GET',
        headers: { Authorization: 'Bearer ' + ACCESS_TOKEN },
      },
      (res) => {
        let raw = '';
        res.on('data', (c) => (raw += c));
        res.on('end', () => {
          let body = {};
          try {
            body = JSON.parse(raw);
          } catch (e) {
            body = {};
          }
          if (res.statusCode >= 200 && res.statusCode < 300 && body.url) {
            resolve(body);
          } else {
            const detail = body && body.error && body.error.message ? ': ' + body.error.message : '';
            reject(new Error('Graph media lookup HTTP ' + res.statusCode + detail));
          }
        });
      }
    );
    req.setTimeout(6000, () => req.destroy(new Error('Graph media lookup timed out')));
    req.on('error', reject);
    req.end();
  });
}

// GET a URL as raw bytes, following up to 5 redirects (the temporary media
// URL may redirect to a CDN host). Rejects on non-2xx, timeout or network
// error. Returns { buffer, contentType }.
function httpsGetBuffer(rawUrl, redirectsLeft) {
  return new Promise((resolve, reject) => {
    const u = new URL(rawUrl);
    const req = https.request(
      {
        hostname: u.hostname,
        port: u.port || 443,
        path: u.pathname + u.search,
        method: 'GET',
        headers: { 'User-Agent': 'GTG-media/1.0' },
      },
      (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location && redirectsLeft > 0) {
          res.resume();
          resolve(httpsGetBuffer(new URL(res.headers.location, rawUrl).toString(), redirectsLeft - 1));
          return;
        }
        if (res.statusCode < 200 || res.statusCode >= 300) {
          res.resume();
          reject(new Error('media download HTTP ' + res.statusCode));
          return;
        }
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => resolve({ buffer: Buffer.concat(chunks), contentType: res.headers['content-type'] || '' }));
      }
    );
    req.setTimeout(6000, () => req.destroy(new Error('media download timed out')));
    req.on('error', reject);
    req.end();
  });
}

// Download one inbound image at arrival time and save it under media/ as
// <sanitized message id>.<ext> (extension from the mime type, default jpg).
// At most one retry (two attempts total). Never throws: on failure it logs
// (message id + error only, never the token or the signed URL) and returns
// { ok: false }. Callers set media_status from the result.
async function downloadImageMedia(mediaId, messageId) {
  let lastError = null;
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const meta = await graphGetMediaJson(mediaId);
      const dl = await httpsGetBuffer(meta.url, 5);
      const mime = String(meta.mime_type || dl.contentType || '')
        .toLowerCase()
        .split(';')[0]
        .trim();
      const ext = MEDIA_EXT_BY_MIME[mime] || 'jpg';
      fs.writeFileSync(path.join(MEDIA_DIR, mediaBaseName(messageId) + '.' + ext), dl.buffer);
      return { ok: true, ext: ext };
    } catch (e) {
      lastError = e;
      console.error(
        new Date().toISOString(),
        'Media download failed (attempt ' + attempt + ') for message ' + messageId + ':',
        e && e.message ? e.message : e
      );
      if (attempt < 2) await new Promise((r) => setTimeout(r, 500));
    }
  }
  return { ok: false, error: lastError };
}

async function handleInbound(store, value) {
  const contacts = {};
  (value.contacts || []).forEach((c) => {
    contacts[c.wa_id] = (c.profile && c.profile.name) || c.wa_id;
  });
  for (const m of value.messages || []) {
    const waId = m.from;
    const conv = getOrCreateConv(store, waId, contacts[waId]);
    const text = extractText(m);
    const cls = classify(text);
    const inboundMsg = {
      id: m.id,
      direction: 'in',
      type: m.type || 'text',
      text: text,
      timestamp: new Date(parseInt(m.timestamp, 10) * 1000).toISOString(),
      classification: cls.category,
      needs_approval: cls.needs_approval,
      approval_reason: cls.reason,
      draft_reply: cls.draft,
      draft_status: 'pending', // pending | approved | sent
      status: 'received',
    };
    // Images only: record the media id and the path it will be served from,
    // then download it right now (the temporary URL expires in minutes).
    // A failed download only marks the record 'failed' — it never breaks
    // message storage, drafting, or the webhook 200 response.
    if (m.type === 'image' && m.image && m.image.id) {
      inboundMsg.media_id = String(m.image.id);
      inboundMsg.media_url = '/media/' + encodeURIComponent(m.id);
      inboundMsg.media_status = 'pending';
    }
    conv.messages.push(inboundMsg);
    conv.unread += 1;
    conv.last_message = text;
    conv.last_ts = new Date(parseInt(m.timestamp, 10) * 1000).toISOString();
    if (inboundMsg.media_id) {
      try {
        const dl = await downloadImageMedia(inboundMsg.media_id, m.id);
        inboundMsg.media_status = dl.ok ? 'downloaded' : 'failed';
      } catch (e) {
        console.error(
          new Date().toISOString(),
          'Media download error for message ' + m.id + ':',
          e && e.message ? e.message : e
        );
        inboundMsg.media_status = 'failed';
      }
    }
  }
  (value.statuses || []).forEach((s) => {
    Object.values(store.conversations).forEach((conv) => {
      const msg = conv.messages.find((x) => x.id === s.id);
      if (msg) msg.status = s.status;
    });
  });
}

function graphPost(path, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = https.request(
      {
        hostname: 'graph.facebook.com',
        path: '/' + GRAPH_VERSION + path,
        method: 'POST',
        headers: {
          Authorization: 'Bearer ' + ACCESS_TOKEN,
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(data),
        },
      },
      (res) => {
        let raw = '';
        res.on('data', (c) => (raw += c));
        res.on('end', () => {
          try {
            resolve({ status: res.statusCode, body: JSON.parse(raw) });
          } catch (e) {
            resolve({ status: res.statusCode, body: { raw: raw } });
          }
        });
      }
    );
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

async function sendWhatsApp(waId, text) {
  return graphPost('/' + CONFIG.phone_number_id + '/messages', {
    messaging_product: 'whatsapp',
    recipient_type: 'individual',
    to: waId,
    type: 'text',
    text: { preview_url: false, body: text },
  });
}

function readBody(req) {
  return new Promise((resolve) => {
    let raw = '';
    req.on('data', (c) => (raw += c));
    req.on('end', () => {
      try {
        resolve(raw ? JSON.parse(raw) : {});
      } catch (e) {
        resolve({});
      }
    });
  });
}

function json(res, code, obj) {
  const data = JSON.stringify(obj);
  res.writeHead(code, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'content-type',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  });
  res.end(data);
}

function authorized(req, body) {
  const url = new URL(req.url, 'http://localhost');
  const key = url.searchParams.get('key') || (body && body.key);
  return key === CONFIG.panel_key;
}

function sortedConversations(store) {
  return Object.values(store.conversations)
    .map((c) => ({
      wa_id: c.wa_id,
      name: c.name,
      phone: c.phone,
      unread: c.unread,
      last_message: c.last_message || '',
      last_ts: c.last_ts || c.created_at,
      pending_approval: c.messages.filter((m) => m.direction === 'in' && m.needs_approval && m.draft_status === 'pending').length,
      messages: c.messages,
    }))
    .sort((a, b) => new Date(b.last_ts) - new Date(a.last_ts));
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const pathname = url.pathname;

  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'content-type',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    });
    return res.end();
  }

  // ---- Meta webhook verification ----
  if (pathname === '/webhook' && req.method === 'GET') {
    const mode = url.searchParams.get('hub.mode');
    const token = url.searchParams.get('hub.verify_token');
    const challenge = url.searchParams.get('hub.challenge');
    if (mode === 'subscribe' && token === CONFIG.verify_token) {
      console.log(new Date().toISOString(), 'Webhook verified OK');
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      return res.end(challenge || '');
    }
    console.log(new Date().toISOString(), 'Webhook verification FAILED (wrong token/mode)');
    return json(res, 403, { error: 'verification failed' });
  }

  // ---- Meta webhook events ----
  if (pathname === '/webhook' && req.method === 'POST') {
    const body = await readBody(req);
    try {
      if (body && body.object === 'whatsapp_business_account') {
        const store = loadStore();
        for (const entry of body.entry || []) {
          for (const change of entry.changes || []) {
            if (change.value) await handleInbound(store, change.value);
          }
        }
        saveStore(store);
        console.log(new Date().toISOString(), 'Event stored. Conversations:', Object.keys(store.conversations).length);
      }
    } catch (e) {
      console.error('Error handling webhook event:', e);
    }
    return json(res, 200, { received: true });
  }

  if (pathname === '/api/health' && req.method === 'GET') {
    const store = loadStore();
    return json(res, 200, {
      ok: true,
      app: CONFIG.app_name,
      number: CONFIG.number,
      conversations: Object.keys(store.conversations).length,
      webhook_configured: !!CONFIG.webhook_url,
    });
  }

  // ---- Panel API (key-gated) ----
  if (pathname === '/api/conversations' && req.method === 'GET') {
    if (!authorized(req)) return json(res, 401, { error: 'unauthorized' });
    const store = loadStore();
    return json(res, 200, { conversations: sortedConversations(store) });
  }

  if (pathname === '/api/config' && req.method === 'GET') {
    if (!authorized(req)) return json(res, 401, { error: 'unauthorized' });
    return json(res, 200, { business: BUSINESS, mode: 'DRAFT-ONLY', phone_number_id: CONFIG.phone_number_id, waba_id: CONFIG.waba_id });
  }

  if (pathname === '/api/send' && req.method === 'POST') {
    const body = await readBody(req);
    if (!authorized(req, body)) return json(res, 401, { error: 'unauthorized' });
    const waId = String(body.wa_id || '').replace(/\D/g, '');
    const text = String(body.text || '').trim();
    if (!waId || !text) return json(res, 400, { error: 'wa_id and text are required' });
    try {
      const result = await sendWhatsApp(waId, text);
      if (result.status >= 200 && result.status < 300 && result.body.messages) {
        const store = loadStore();
        const conv = getOrCreateConv(store, waId);
        const mid = result.body.messages[0].id;
        conv.messages.push({
          id: mid,
          direction: 'out',
          type: 'text',
          text: text,
          timestamp: new Date().toISOString(),
          status: 'sent',
        });
        conv.last_message = text;
        conv.last_ts = new Date().toISOString();
        saveStore(store);
        return json(res, 200, { ok: true, message_id: mid });
      }
      console.error('Graph API send error:', result.status, JSON.stringify(result.body));
      return json(res, 502, { error: 'Graph API error', detail: result.body });
    } catch (e) {
      console.error('Send failed:', e);
      return json(res, 500, { error: String(e) });
    }
  }

  if (pathname === '/api/draft' && req.method === 'POST') {
    const body = await readBody(req);
    if (!authorized(req, body)) return json(res, 401, { error: 'unauthorized' });
    const store = loadStore();
    const conv = store.conversations[String(body.wa_id || '')];
    if (!conv) return json(res, 404, { error: 'conversation not found' });
    const msg = conv.messages.find((m) => m.id === body.message_id);
    if (!msg) return json(res, 404, { error: 'message not found' });
    msg.draft_reply = String(body.text || '');
    saveStore(store);
    return json(res, 200, { ok: true });
  }

  if (pathname === '/api/approve' && req.method === 'POST') {
    const body = await readBody(req);
    if (!authorized(req, body)) return json(res, 401, { error: 'unauthorized' });
    const store = loadStore();
    const conv = store.conversations[String(body.wa_id || '')];
    if (!conv) return json(res, 404, { error: 'conversation not found' });
    const msg = conv.messages.find((m) => m.id === body.message_id);
    if (!msg || !msg.draft_reply) return json(res, 404, { error: 'message or draft not found' });
    try {
      const result = await sendWhatsApp(conv.wa_id, msg.draft_reply);
      if (result.status >= 200 && result.status < 300 && result.body.messages) {
        msg.draft_status = 'sent';
        conv.messages.push({
          id: result.body.messages[0].id,
          direction: 'out',
          type: 'text',
          text: msg.draft_reply,
          timestamp: new Date().toISOString(),
          status: 'sent',
          in_reply_to: msg.id,
        });
        conv.last_message = msg.draft_reply;
        conv.last_ts = new Date().toISOString();
        saveStore(store);
        return json(res, 200, { ok: true, message_id: result.body.messages[0].id });
      }
      console.error('Graph API approve-send error:', result.status, JSON.stringify(result.body));
      return json(res, 502, { error: 'Graph API error', detail: result.body });
    } catch (e) {
      console.error('Approve failed:', e);
      return json(res, 500, { error: String(e) });
    }
  }

  if (pathname === '/api/mark-read' && req.method === 'POST') {
    const body = await readBody(req);
    if (!authorized(req, body)) return json(res, 401, { error: 'unauthorized' });
    const store = loadStore();
    const conv = store.conversations[String(body.wa_id || '')];
    if (conv) {
      conv.unread = 0;
      saveStore(store);
    }
    return json(res, 200, { ok: true });
  }

  // ---- Serve a downloaded client photo (key-gated, read-only) ----
  // GET /media/:messageId?key=<panel_key> — the key check is the same as
  // /api/conversations, but the failure code is 403 per the media contract.
  // 404 when no downloaded file exists for that message id.
  if (pathname.startsWith('/media/') && req.method === 'GET') {
    if (!authorized(req)) return json(res, 403, { error: 'forbidden' });
    const rawId = decodeURIComponent(pathname.slice('/media/'.length));
    if (!rawId) return json(res, 404, { error: 'not found' });
    const base = mediaBaseName(rawId);
    let filePath = null;
    for (const ext of ['jpg', 'jpeg', 'png', 'webp', 'gif']) {
      const candidate = path.join(MEDIA_DIR, base + '.' + ext);
      if (fs.existsSync(candidate)) {
        filePath = candidate;
        break;
      }
    }
    if (!filePath) return json(res, 404, { error: 'not found' });
    const ext = path.extname(filePath).slice(1).toLowerCase();
    const data = fs.readFileSync(filePath);
    res.writeHead(200, {
      'Content-Type': MEDIA_CONTENT_TYPE[ext] || 'application/octet-stream',
      'Content-Length': data.length,
      'Cache-Control': 'private, max-age=60',
    });
    return res.end(data);
  }

  return json(res, 404, { error: 'not found' });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('GTG webhook server listening on 0.0.0.0:' + PORT);
  console.log('Verify token set. Webhook path: /webhook');
});
