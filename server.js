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
 * - POST /api/draft-image {key, wa_id, message_id, image_url, caption}  fetch a web image as a held image draft (sends nothing)
 * - POST /api/approve-image {key, wa_id, message_id}  upload + send the stored image draft as a reply
 * - GET  /media/:messageId?key=...   serve a downloaded client photo or voice note (key-gated)
 * Data is persisted to data/conversations.json (atomic writes).
 * The access token is read from ../whatsapp-credentials.json (server-side only).
 * SEND MODE (controlled test, restricted): automatic replies are enabled
 * ONLY for the test numbers in AUTO_SEND_ALLOWLIST (5215660419427 Omar,
 * 5217716013896 Gunter): the official greeting for greetings, and the
 * holding ack ("ya te atiendo") for queries that need Ronin's analysis.
 * Every other number stays DRAFT-ONLY: nothing is sent unless Omar/Gunter
 * explicitly press Enviar / Aprobar in the panel. Sensitive topics (price,
 * complaints, delays, extras, human handoff, cancellations) are never
 * answered automatically — they only get the holding ack and stay pending
 * approval.
 * Safety: all store read-modify-write sequences run through a single
 * in-process queue (withStore), and every automatic send is at-most-once:
 * an attempt marker (auto_reply_attempted) is persisted BEFORE the Graph
 * API call, and a message with that marker never triggers another
 * automatic send, even if the first attempt failed.
 * WORKER CONTRACT (full auto-replies, controlled test only): when the
 * server auto-sends, it records auto_sent_kind ('greeting' | 'holding')
 * and auto_sent_text on the inbound message, visible via
 * GET /api/conversations. The greeting already includes the name
 * question, so for greetings the worker drafts nothing (the first contact
 * is complete); for holding cases the worker's auto-approved draft is
 * the full reply.
 * The worker uses /api/draft + /api/approve on the same message_id —
 * a second approve after a sent draft sends the new text.
 * MEDIA (images + audio): when an inbound image or audio message arrives,
 * the server records its media id and downloads the file immediately via
 * the Graph API (the temporary download URL expires in minutes, so the
 * download happens at arrival, never lazily). Files are saved under media/
 * next to this file and served read-only through
 * GET /media/:messageId?key=<panel_key>. Audio is a voice note (WhatsApp
 * sends audio/ogg, Opus); Ronin transcribes it locally, the server only
 * stores and serves the file.
 * A failed download never breaks message storage, drafting, or the
 * webhook 200 response — the message is stored with media_status 'failed'.
 * OUTGOING IMAGES (reference photos for clients): a web image is fetched
 * into media/outgoing/ (JPEG/PNG/WebP only, max 5 MB) as a held draft and
 * is only sent when Omar/Gunter explicitly approves that image draft.
 * The web fetch NEVER carries the WhatsApp token (unlike the Meta CDN
 * download, which requires it) — see httpsGetBuffer's auth option.
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
// Media downloads (images + audio). The Graph media endpoint is queried with
// v27.0 (the version the webhook subscription uses). The temporary download
// URL Meta returns expires in minutes, so every image and audio file is
// downloaded at webhook arrival, never lazily on /media request.
const MEDIA_GRAPH_VERSION = 'v27.0';
const MEDIA_DIR = path.join(DIR, 'media');
const OUTGOING_DIR = path.join(DIR, 'media', 'outgoing');
const MAX_OUTGOING_IMAGE_BYTES = 5 * 1024 * 1024; // 5 MB
// Outgoing reference photos: JPEG/PNG/WebP only (GIF is not accepted here).
const OUTGOING_IMAGE_MIMES = {
  'image/jpeg': true,
  'image/jpg': true,
  'image/png': true,
  'image/webp': true,
};
const MEDIA_EXT_BY_MIME = {
  'image/jpeg': 'jpg',
  'image/jpg': 'jpg',
  'image/png': 'png',
  'image/webp': 'webp',
  'image/gif': 'gif',
  // Audio: WhatsApp voice notes arrive as audio/ogg (Opus). Other audio
  // types a client might send as a file.
  'audio/ogg': 'ogg',
  'audio/opus': 'opus',
  'audio/mpeg': 'mp3',
  'audio/mp3': 'mp3',
  'audio/mp4': 'm4a',
  'audio/aac': 'aac',
  'audio/amr': 'amr',
  'audio/wav': 'wav',
  'audio/x-wav': 'wav',
  'audio/webm': 'webm',
};
const MEDIA_CONTENT_TYPE = {
  jpg: 'image/jpeg',
  jpeg: 'image/jpeg',
  png: 'image/png',
  webp: 'image/webp',
  gif: 'image/gif',
  ogg: 'audio/ogg',
  opus: 'audio/ogg',
  mp3: 'audio/mpeg',
  m4a: 'audio/mp4',
  aac: 'audio/aac',
  amr: 'audio/amr',
  wav: 'audio/wav',
  webm: 'audio/webm',
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
    '¡Hola! 👋 ¡Bienvenido a *GO TUNING GARAGE | PACHUCA* 🔧\n\nSoy *Ronin*, asistente virtual del taller. Estoy aquí para ayudarte a personalizar, proteger y equipar tu vehículo. 🏎️\n\nTrabajamos con:\n\n• PPF y WRAP\n• Body Kits y modificaciones estéticas\n• Iluminación y ambientación interior\n• Sistemas multimedia con Android Auto y Apple CarPlay\n• Polarizado\n• Equipamiento y accesorios off-road\n\nCuéntame, ¿qué vehículo tienes y qué te gustaría hacerle?\n\nSi tienes alguna fotografía o referencia de lo que buscas, puedes compartirla para orientarte mejor.\n\nAntes de continuar, ¿con quién tengo el gusto? 😎',
  noPriceReply:
    'Para darte una cotización correcta, primero necesitamos conocer tu vehículo y revisar exactamente lo que deseas realizar. En GO TUNING GARAGE hacemos una valoración gratuita y sin compromiso en nuestro taller; después de revisar tu proyecto podremos definir los materiales, el alcance del trabajo y presentarte una cotización adecuada.',
  valuationInvite:
    'Lo ideal es revisar tu vehículo directamente en nuestro taller para orientarte correctamente. La valoración es gratuita y sin compromiso. Puedes compartirnos tu disponibilidad y reviso los horarios para agendarte. Aquí tienes nuestra ubicación: https://maps.app.goo.gl/qHgUhDxh1cihyQcS9',
  holdingReply:
    'Gracias por tu mensaje. Estamos revisando tu caso con el equipo del taller y te confirmamos en breve.',
};

/* Auto-send allowlist (CONTROLLED TEST ONLY). Only these wa_ids may receive
 * automatic replies. The allowlist is checked BEFORE any automatic send;
 * every other number stays draft-only. Expand/remove only with Omar's
 * explicit authorization after the controlled tests pass. */
const AUTO_SEND_ALLOWLIST = ['5215660419427', '5217716013896'];

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(MEDIA_DIR)) fs.mkdirSync(MEDIA_DIR, { recursive: true });
if (!fs.existsSync(OUTGOING_DIR)) fs.mkdirSync(OUTGOING_DIR, { recursive: true });

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

/* Serialize every store read-modify-write sequence through a single
 * in-process promise chain. Node serves requests concurrently, so without
 * this two POST /webhook deliveries (or a webhook racing /api/send) could
 * interleave: both load the same store, both pass the dedup check, and the
 * last save wins — losing a message or sending a duplicate auto-reply.
 * Every write path (POST /webhook, /api/send, /api/draft, /api/approve,
 * /api/mark-read) runs inside withStore, so the dedup check, the
 * auto_reply_attempted/sent marking and the awaiting_ronin update are
 * atomic with respect to other requests. Read-only GETs stay outside the
 * queue (saveStore writes atomically via rename, so a reader sees either
 * the old or the new file, never a torn one). */
let storeQueue = Promise.resolve();
function withStore(fn) {
  const run = storeQueue.then(fn, fn);
  storeQueue = run.catch(() => {});
  return run;
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
// URL may redirect to a CDN host). Meta requires the same Bearer token on
// the media download itself — without it the CDN answers 401. Rejects on
// non-2xx, timeout or network error. Returns { buffer, contentType }.
// opts: { auth: true } sends the WhatsApp token (Meta CDN only, default);
// { auth: false } sends no token — REQUIRED for downloads from the open
// web, where the token must never be disclosed. { maxBytes } aborts the
// download once the body exceeds that size.
function httpsGetBuffer(rawUrl, redirectsLeft, opts) {
  const auth = !opts || opts.auth !== false;
  const maxBytes = (opts && opts.maxBytes) || 0;
  return new Promise((resolve, reject) => {
    const u = new URL(rawUrl);
    // Plain http:// URLs must use the http module; https.request against
    // one fails the TLS handshake ("wrong version number"). Web image
    // hosts may serve either scheme (including across redirects).
    const lib = u.protocol === 'http:' ? http : https;
    const headers = { 'User-Agent': 'GTG-media/1.0' };
    if (auth) headers.Authorization = 'Bearer ' + ACCESS_TOKEN;
    const req = lib.request(
      {
        hostname: u.hostname,
        port: u.port || 443,
        path: u.pathname + u.search,
        method: 'GET',
        headers: headers,
      },
      (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location && redirectsLeft > 0) {
          res.resume();
          resolve(httpsGetBuffer(new URL(res.headers.location, rawUrl).toString(), redirectsLeft - 1, opts));
          return;
        }
        if (res.statusCode < 200 || res.statusCode >= 300) {
          res.resume();
          reject(new Error('media download HTTP ' + res.statusCode));
          return;
        }
        const chunks = [];
        let total = 0;
        let tooLarge = false;
        res.on('data', (c) => {
          total += c.length;
          if (maxBytes > 0 && total > maxBytes) {
            tooLarge = true;
            req.destroy(new Error('media download exceeds ' + maxBytes + ' bytes'));
            return;
          }
          chunks.push(c);
        });
        res.on('end', () => {
          if (tooLarge) return;
          resolve({ buffer: Buffer.concat(chunks), contentType: res.headers['content-type'] || '' });
        });
      }
    );
    req.setTimeout(6000, () => req.destroy(new Error('media download timed out')));
    req.on('error', reject);
    req.end();
  });
}

// Download one inbound image or audio file at arrival time and save it
// under media/ as <sanitized message id>.<ext> (extension from the mime
// type, default jpg). At most one retry (two attempts total). Never throws:
// on failure it logs (message id + error only, never the token or the
// signed URL) and returns { ok: false }. Callers set media_status from
// the result.
async function downloadInboundMedia(mediaId, messageId) {
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

/* Restricted automatic reply (TEST PHASE ONLY).
 * Runs only for inbound messages, only when the conversation's wa_id is in
 * AUTO_SEND_ALLOWLIST (checked first, before any send), and at most once
 * per inbound message. Exactly one automatic reply per inbound message:
 *  - greeting            -> official BUSINESS.greeting, sent immediately.
 *  - needs_approval=true -> BUSINESS.holdingReply, but ONLY if no holding
 *    ack is already outstanding for this conversation (conv.awaiting_ronin).
 *    Sending it sets conv.awaiting_ronin = true until Ronin answers via
 *    /api/send or /api/approve, which clears it for a new cycle.
 *  - any other category  -> no automatic send; the draft stays pending.
 * At-most-once: BEFORE the Graph API call, auto_reply_attempted=true and
 * auto_reply_attempted_at are persisted to the store. A message with that
 * marker never triggers another automatic send — even if the first attempt
 * failed (no automatic retry; the draft stays pending for Omar/Gunter/
 * Ronin). auto_reply_sent / auto_reply_at / auto_reply_message_id are set
 * ONLY when Graph API answers success (2xx with messages[0].id).
 * auto_sent_kind/auto_sent_text record what was sent (for the hook worker).
 * MUST be called from inside a withStore section (it saves the store
 * directly); never call it from outside the queue.
 */
async function tryAutoReply(store, conv, inboundMsg, cls) {
  if (!AUTO_SEND_ALLOWLIST.includes(conv.wa_id)) {
    console.log(new Date().toISOString(), 'AUTO-SEND skipped (not allowlisted):', conv.wa_id, inboundMsg.id);
    return;
  }
  // At-most-once: a message that already had an automatic-send attempt
  // (succeeded or failed) never triggers another one. A failed attempt
  // leaves the draft pending for human approval / Ronin — no auto retry.
  if (inboundMsg.auto_reply_sent || inboundMsg.auto_reply_attempted) {
    console.log(new Date().toISOString(), 'AUTO-SEND skipped (already attempted):', inboundMsg.id);
    return;
  }
  let text = null;
  if (cls.category === 'greeting') {
    text = BUSINESS.greeting;
  } else if (cls.needs_approval) {
    if (conv.awaiting_ronin === true) {
      console.log(new Date().toISOString(), 'AUTO-SEND skipped (ack outstanding):', inboundMsg.id);
      return;
    }
    text = BUSINESS.holdingReply;
  } else {
    return;
  }
  // Crash-safe attempt marker: persist BEFORE the network call, so a crash
  // (or a Meta redelivery) between here and the send can never produce a
  // second automatic send for this message.
  inboundMsg.auto_reply_attempted = true;
  inboundMsg.auto_reply_attempted_at = new Date().toISOString();
  saveStore(store);
  console.log(new Date().toISOString(), 'AUTO-SEND attempt:', conv.wa_id, cls.category, inboundMsg.id);
  try {
    const result = await sendWhatsApp(conv.wa_id, text);
    if (result.status >= 200 && result.status < 300 && result.body.messages) {
      const mid = result.body.messages[0].id;
      inboundMsg.auto_reply_sent = true;
      inboundMsg.auto_reply_at = new Date().toISOString();
      inboundMsg.auto_reply_message_id = mid;
      // Worker contract: record WHAT was auto-sent, so the hook worker can
      // see it via GET /api/conversations. The greeting already includes the
      // name question, so for greetings the worker drafts nothing;
      // holding cases get the full reply from the worker instead.
      inboundMsg.auto_sent_kind = cls.category === 'greeting' ? 'greeting' : 'holding';
      inboundMsg.auto_sent_text = text;
      inboundMsg.draft_status = 'sent';
      if (cls.needs_approval) conv.awaiting_ronin = true;
      conv.messages.push({
        id: mid,
        direction: 'out',
        type: 'text',
        text: text,
        timestamp: new Date().toISOString(),
        status: 'sent',
        in_reply_to: inboundMsg.id,
      });
      conv.last_message = text;
      conv.last_ts = new Date().toISOString();
      saveStore(store);
      console.log(new Date().toISOString(), 'AUTO-SEND sent:', mid);
    } else {
      console.error(new Date().toISOString(), 'AUTO-SEND failed (draft stays pending, no retry):', result.status, JSON.stringify(result.body));
    }
  } catch (e) {
    console.error(new Date().toISOString(), 'AUTO-SEND error (draft stays pending, no retry):', String(e));
  }
}

async function handleInbound(store, value) {
  const contacts = {};
  (value.contacts || []).forEach((c) => {
    contacts[c.wa_id] = (c.profile && c.profile.name) || c.wa_id;
  });
  for (const m of value.messages || []) {
    const waId = m.from;
    const conv = getOrCreateConv(store, waId, contacts[waId]);
    // Dedup: ignore Meta redeliveries of a message we already stored.
    if (conv.messages.some((x) => x.id === m.id)) {
      console.log(new Date().toISOString(), 'Duplicate inbound ignored:', m.id);
      continue;
    }
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
    // Images and audio: record the media id and the path it will be served
    // from, then download it right now (the temporary URL expires in
    // minutes). A failed download only marks the record 'failed' — it never
    // breaks message storage, drafting, or the webhook 200 response.
    if (m.type === 'image' && m.image && m.image.id) {
      inboundMsg.media_id = String(m.image.id);
      inboundMsg.media_url = '/media/' + encodeURIComponent(m.id);
      inboundMsg.media_status = 'pending';
    }
    if (m.type === 'audio' && m.audio && m.audio.id) {
      inboundMsg.media_id = String(m.audio.id);
      inboundMsg.media_url = '/media/' + encodeURIComponent(m.id);
      inboundMsg.media_status = 'pending';
    }
    conv.messages.push(inboundMsg);
    conv.unread += 1;
    conv.last_message = text;
    conv.last_ts = new Date(parseInt(m.timestamp, 10) * 1000).toISOString();
    if (inboundMsg.media_id) {
      try {
        const dl = await downloadInboundMedia(inboundMsg.media_id, m.id);
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
    // Restricted auto-reply (allowlisted test numbers only). Runs after
    // the media download so the file is already saved when the reply
    // goes out. Awaits the Graph API result so the store is saved
    // consistently afterwards.
    await tryAutoReply(store, conv, inboundMsg, cls);
  }
  (value.statuses || []).forEach((s) => {
    Object.values(store.conversations).forEach((conv) => {
      const msg = conv.messages.find((x) => x.id === s.id);
      if (msg) msg.status = s.status;
    });
  });
}

function graphPost(path, body, version) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = https.request(
      {
        hostname: 'graph.facebook.com',
        path: '/' + (version || GRAPH_VERSION) + path,
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

// Upload a local image file to Meta and return { status, body } where
// body.id is the media id on success. multipart/form-data, v27.0.
// The file is read from disk; the token is only ever sent to graph.facebook.com.
function graphUploadMedia(filePath, mimeType) {
  return new Promise((resolve, reject) => {
    let fileBuffer;
    try {
      fileBuffer = fs.readFileSync(filePath);
    } catch (e) {
      reject(e);
      return;
    }
    const boundary = '----GTGMedia' + Date.now().toString(36) + Math.random().toString(36).slice(2);
    const head = Buffer.concat([
      Buffer.from('--' + boundary + '\r\nContent-Disposition: form-data; name="messaging_product"\r\n\r\nwhatsapp\r\n'),
      Buffer.from('--' + boundary + '\r\nContent-Disposition: form-data; name="type"\r\n\r\n' + mimeType + '\r\n'),
      Buffer.from(
        '--' + boundary + '\r\nContent-Disposition: form-data; name="file"; filename="' + path.basename(filePath) + '"\r\nContent-Type: ' + mimeType + '\r\n\r\n'
      ),
    ]);
    const tail = Buffer.from('\r\n--' + boundary + '--\r\n');
    const body = Buffer.concat([head, fileBuffer, tail]);
    const req = https.request(
      {
        hostname: 'graph.facebook.com',
        path: '/' + MEDIA_GRAPH_VERSION + '/' + CONFIG.phone_number_id + '/media',
        method: 'POST',
        headers: {
          Authorization: 'Bearer ' + ACCESS_TOKEN,
          'Content-Type': 'multipart/form-data; boundary=' + boundary,
          'Content-Length': body.length,
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
    req.setTimeout(15000, () => req.destroy(new Error('media upload timed out')));
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

// Send an image message (by media id) as a reply to an inbound message.
// v27.0, like the media upload. Caption is optional (max 1024 chars).
async function sendWhatsAppImage(waId, mediaId, caption, inReplyToId) {
  const image = { id: mediaId };
  const cap = String(caption || '').slice(0, 1024);
  if (cap) image.caption = cap;
  const body = {
    messaging_product: 'whatsapp',
    recipient_type: 'individual',
    to: waId,
    type: 'image',
    image: image,
  };
  if (inReplyToId) body.context = { message_id: inReplyToId };
  return graphPost('/' + CONFIG.phone_number_id + '/messages', body, MEDIA_GRAPH_VERSION);
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
        // Serialized: the whole load -> handleInbound (incl. auto-reply)
        // -> save sequence is atomic with respect to other requests.
        await withStore(async () => {
          const store = loadStore();
          for (const entry of body.entry || []) {
            for (const change of entry.changes || []) {
              if (change.value) await handleInbound(store, change.value);
            }
          }
          saveStore(store);
          console.log(new Date().toISOString(), 'Event stored. Conversations:', Object.keys(store.conversations).length);
        });
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
      // Serialized with the webhook path: the Graph call and the store
      // recording happen atomically with respect to other requests.
      // The callback returns {code, obj}; the response is sent HERE, in the
      // handler, so control never falls through to the final 404.
      const out = await withStore(async () => {
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
          conv.awaiting_ronin = false; // Ronin's reply releases the outstanding holding ack.
          conv.last_message = text;
          conv.last_ts = new Date().toISOString();
          saveStore(store);
          return { code: 200, obj: { ok: true, message_id: mid } };
        }
        console.error('Graph API send error:', result.status, JSON.stringify(result.body));
        return { code: 502, obj: { error: 'Graph API error', detail: result.body } };
      });
      return json(res, out.code, out.obj);
    } catch (e) {
      console.error('Send failed:', e);
      if (!res.headersSent) return json(res, 500, { error: String(e) });
    }
  }

  if (pathname === '/api/draft' && req.method === 'POST') {
    const body = await readBody(req);
    if (!authorized(req, body)) return json(res, 401, { error: 'unauthorized' });
    try {
      // The callback returns {code, obj}; the response is sent HERE, in the
      // handler, so control never falls through to the final 404.
      const out = await withStore(async () => {
        const store = loadStore();
        const conv = store.conversations[String(body.wa_id || '')];
        if (!conv) return { code: 404, obj: { error: 'conversation not found' } };
        const msg = conv.messages.find((m) => m.id === body.message_id);
        if (!msg) return { code: 404, obj: { error: 'message not found' } };
        msg.draft_reply = String(body.text || '');
        saveStore(store);
        return { code: 200, obj: { ok: true } };
      });
      return json(res, out.code, out.obj);
    } catch (e) {
      console.error('Draft failed:', e);
      if (!res.headersSent) return json(res, 500, { error: String(e) });
    }
  }

  if (pathname === '/api/approve' && req.method === 'POST') {
    const body = await readBody(req);
    if (!authorized(req, body)) return json(res, 401, { error: 'unauthorized' });
    try {
      // Serialized with the webhook path: the Graph call and the store
      // recording happen atomically with respect to other requests.
      // The callback returns {code, obj}; the response is sent HERE, in the
      // handler, so control never falls through to the final 404.
      const out = await withStore(async () => {
        const store = loadStore();
        const conv = store.conversations[String(body.wa_id || '')];
        if (!conv) return { code: 404, obj: { error: 'conversation not found' } };
        const msg = conv.messages.find((m) => m.id === body.message_id);
        if (!msg || !msg.draft_reply) return { code: 404, obj: { error: 'message or draft not found' } };
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
          conv.awaiting_ronin = false; // Approved reply releases the outstanding holding ack.
          conv.last_message = msg.draft_reply;
          conv.last_ts = new Date().toISOString();
          saveStore(store);
          return { code: 200, obj: { ok: true, message_id: result.body.messages[0].id } };
        }
        console.error('Graph API approve-send error:', result.status, JSON.stringify(result.body));
        return { code: 502, obj: { error: 'Graph API error', detail: result.body } };
      });
      return json(res, out.code, out.obj);
    } catch (e) {
      console.error('Approve failed:', e);
      if (!res.headersSent) return json(res, 500, { error: String(e) });
    }
  }

  // ---- Outgoing image drafts (reference photos for clients) ----
  // POST /api/draft-image {key, wa_id, message_id, image_url, caption}
  // Fetches a web image into media/outgoing/ and records it as a held
  // image draft on the target inbound message. SENDS NOTHING to WhatsApp.
  // The web fetch never carries the WhatsApp token (auth: false).
  if (pathname === '/api/draft-image' && req.method === 'POST') {
    const body = await readBody(req);
    if (!authorized(req, body)) return json(res, 401, { error: 'unauthorized' });
    const store = loadStore();
    const conv = store.conversations[String(body.wa_id || '')];
    if (!conv) return json(res, 404, { error: 'conversation not found' });
    const msg = conv.messages.find((m) => m.id === body.message_id);
    if (!msg) return json(res, 404, { error: 'message not found' });
    const imageUrl = String(body.image_url || '').trim();
    let u;
    try {
      u = new URL(imageUrl);
    } catch (e) {
      return json(res, 400, { error: 'image_url must be a valid URL' });
    }
    if (u.protocol !== 'http:' && u.protocol !== 'https:') {
      return json(res, 400, { error: 'image_url must be http(s)' });
    }
    const caption = String(body.caption || '').slice(0, 1024);
    let dl;
    try {
      dl = await httpsGetBuffer(imageUrl, 5, { auth: false, maxBytes: MAX_OUTGOING_IMAGE_BYTES });
    } catch (e) {
      return json(res, 400, { error: 'could not download image', detail: e && e.message ? e.message : String(e) });
    }
    const mime = String(dl.contentType || '')
      .toLowerCase()
      .split(';')[0]
      .trim();
    if (!OUTGOING_IMAGE_MIMES[mime]) {
      return json(res, 400, { error: 'not a supported image (JPEG, PNG or WebP required)', content_type: mime || 'unknown' });
    }
    const ext = MEDIA_EXT_BY_MIME[mime] || 'jpg';
    const rel = 'media/outgoing/' + mediaBaseName(msg.id) + '.' + ext;
    // Re-drafting replaces a previous image draft for the same message.
    if (msg.draft_image_file && msg.draft_image_file !== rel) {
      try {
        fs.unlinkSync(path.join(DIR, msg.draft_image_file));
      } catch (e) {
        /* best effort */
      }
    }
    fs.writeFileSync(path.join(DIR, rel), dl.buffer);
    msg.draft_image_url = imageUrl;
    msg.draft_image_file = rel;
    msg.draft_image_mime = mime;
    msg.draft_image_caption = caption;
    msg.draft_image_status = 'pending';
    saveStore(store);
    return json(res, 200, { ok: true, file: rel, bytes: dl.buffer.length, mime: mime });
  }

  // POST /api/approve-image {key, wa_id, message_id}
  // Uploads the stored image draft to Meta and sends it as an image
  // message replying to message_id. Only acts when explicitly called.
  if (pathname === '/api/approve-image' && req.method === 'POST') {
    const body = await readBody(req);
    if (!authorized(req, body)) return json(res, 401, { error: 'unauthorized' });
    const store = loadStore();
    const conv = store.conversations[String(body.wa_id || '')];
    if (!conv) return json(res, 404, { error: 'conversation not found' });
    const msg = conv.messages.find((m) => m.id === body.message_id);
    if (!msg || !msg.draft_image_file) return json(res, 404, { error: 'message or image draft not found' });
    if (msg.draft_image_status === 'sent') return json(res, 409, { error: 'image draft already sent' });
    const filePath = path.join(DIR, msg.draft_image_file);
    if (!filePath.startsWith(OUTGOING_DIR + path.sep) || !fs.existsSync(filePath)) {
      return json(res, 404, { error: 'image file not found' });
    }
    const mime = msg.draft_image_mime || 'image/jpeg';
    try {
      const up = await graphUploadMedia(filePath, mime);
      if (!(up.status >= 200 && up.status < 300 && up.body && up.body.id)) {
        console.error('Graph API media upload error:', up.status, JSON.stringify(up.body));
        return json(res, 502, { error: 'Graph API error', detail: up.body });
      }
      const result = await sendWhatsAppImage(conv.wa_id, up.body.id, msg.draft_image_caption || '', msg.id);
      if (result.status >= 200 && result.status < 300 && result.body.messages) {
        msg.draft_image_status = 'sent';
        conv.awaiting_ronin = false; // Approved reply releases the outstanding holding ack.
        conv.messages.push({
          id: result.body.messages[0].id,
          direction: 'out',
          type: 'image',
          text: msg.draft_image_caption || '',
          timestamp: new Date().toISOString(),
          status: 'sent',
          in_reply_to: msg.id,
          media_id: up.body.id,
        });
        conv.last_message = msg.draft_image_caption || '[Imagen enviada]';
        conv.last_ts = new Date().toISOString();
        saveStore(store);
        return json(res, 200, { ok: true, message_id: result.body.messages[0].id });
      }
      console.error('Graph API image send error:', result.status, JSON.stringify(result.body));
      return json(res, 502, { error: 'Graph API error', detail: result.body });
    } catch (e) {
      console.error('Approve-image failed:', e);
      return json(res, 500, { error: String(e) });
    }
  }

  if (pathname === '/api/mark-read' && req.method === 'POST') {
    const body = await readBody(req);
    if (!authorized(req, body)) return json(res, 401, { error: 'unauthorized' });
    try {
      // The callback returns {code, obj}; the response is sent HERE, in the
      // handler, so control never falls through to the final 404.
      const out = await withStore(async () => {
        const store = loadStore();
        const conv = store.conversations[String(body.wa_id || '')];
        if (conv) {
          conv.unread = 0;
          saveStore(store);
        }
        return { code: 200, obj: { ok: true } };
      });
      return json(res, out.code, out.obj);
    } catch (e) {
      console.error('Mark-read failed:', e);
      if (!res.headersSent) return json(res, 500, { error: String(e) });
    }
  }

  // ---- Serve a downloaded client photo or voice note (key-gated, read-only) ----
  // GET /media/:messageId?key=<panel_key> — the key check is the same as
  // /api/conversations, but the failure code is 403 per the media contract.
  // 404 when no downloaded file exists for that message id.
  if (pathname.startsWith('/media/') && req.method === 'GET') {
    if (!authorized(req)) return json(res, 403, { error: 'forbidden' });
    const rawId = decodeURIComponent(pathname.slice('/media/'.length));
    if (!rawId) return json(res, 404, { error: 'not found' });
    const base = mediaBaseName(rawId);
    let filePath = null;
    for (const ext of ['jpg', 'jpeg', 'png', 'webp', 'gif', 'ogg', 'opus', 'mp3', 'm4a', 'aac', 'amr', 'wav', 'webm']) {
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

  // Safety net: a handler that already answered must never reach this line
  // (a second write would throw ERR_HTTP_HEADERS_SENT and crash the process).
  if (!res.headersSent) return json(res, 404, { error: 'not found' });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('GTG webhook server listening on 0.0.0.0:' + PORT);
  console.log('Verify token set. Webhook path: /webhook');
});
