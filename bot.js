// bot.js
require("dotenv").config();
const path      = require("path");
const crypto    = require("crypto");
const fs        = require("fs");
const http      = require("http");
const fsp       = fs.promises;
const { spawn } = require("child_process");
const Database  = require("better-sqlite3");
const QRCode    = require("qrcode");
const E         = require("./emojis.js");

/* ═══════════════════════════════════════════════════════════════════════════
   CONFIG
   ═══════════════════════════════════════════════════════════════════════════ */
const TOKEN            = process.env.TELEGRAM_BOT_TOKEN    || "";
const API              = (process.env.VPN_API_BASE_URL     || "").replace(/\/+$/, "");
const APP_SECRET       = process.env.APP_SECRET            || "";
const ADMIN_ID         = Number(process.env.ADMIN_TELEGRAM_ID || 0);
const DB_FILE          = process.env.SQLITE_PATH           || path.join(__dirname, "bot.db");
const NEWS_URL         = process.env.BOT_NEWS_URL          || "";
const SUPPORT_URL      = process.env.BOT_SUPPORT_URL       || "https://t.me/dreinnvpnsupportbot";
const FREE_PROXY       = process.env.BOT_FREE_PROXY_URL    || "";
const BOT_USERNAME     = process.env.BOT_USERNAME          || "";
const FK_EMAIL_DOMAIN  = process.env.FK_EMAIL_DOMAIN       || "bot.user";
const CRYPTOBOT_TOKEN  = process.env.CRYPTOBOT_TOKEN       || "";
const CRYPTOBOT_API    = "https://pay.crypt.bot/api";
const USDT_FALLBACK    = Number(process.env.CRYPTOBOT_FALLBACK_RATE || 90);
const CRYPTO_MIN_RUB   = Number(process.env.CRYPTOBOT_MIN_RUB      || 50);
const CRYPTO_INVOICE_TTL = 3600;
const CRYPTOBOT_WEBHOOK_PATH = process.env.CRYPTOBOT_WEBHOOK_PATH || "/cryptobot/webhook";
const FK_API_BASE      = "https://api.fk.life/v1";
const FK_SHOP_ID_ENV   = Number(process.env.FREEKASSA_SHOP_ID || 0);
const FK_API_KEY       = process.env.FREEKASSA_API_KEY || "";
const FK_SECRET2       = process.env.FREEKASSA_SECRET2 || "";
const FK_SERVER_IP_ENV = process.env.FREEKASSA_SERVER_IP || "";
const FK_DOMAIN        = process.env.FREEKASSA_DOMAIN || "dreinn.bothost.tech";
const FK_PORT          = Number(process.env.PORT || process.env.FREEKASSA_PORT || 3000);
const FK_PATH_NOTIFY_ENV = process.env.FREEKASSA_NOTIFY_PATH || "/freekassa/notify";
const FK_MIN_RUB_ENV   = Number(process.env.FREEKASSA_MIN_RUB || 50);
const FK_ENABLE_IP_CHECK = process.env.FREEKASSA_CHECK_IPS === "1";
const FK_ALLOWED_IPS   = new Set([
  "168.119.157.136","168.119.60.227","178.154.197.79","51.250.54.238",
]);

if (!TOKEN || !API || !APP_SECRET || !ADMIN_ID) {
  console.error("Missing env: TELEGRAM_BOT_TOKEN, VPN_API_BASE_URL, APP_SECRET, ADMIN_TELEGRAM_ID");
  process.exit(1);
}

const TG_BASE = `https://api.telegram.org/bot${TOKEN}`;
let   offset  = 0;
const db      = new Database(DB_FILE);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");

/* ═══════════════════════════════════════════════════════════════════════════
   RATE LIMITING
   ═══════════════════════════════════════════════════════════════════════════ */
const _cbCooldown = new Map();
function checkCbRateLimit(uid) {
  const n = Date.now(), last = _cbCooldown.get(uid)||0;
  if (n - last < 400) return false;
  _cbCooldown.set(uid, n);
  if (_cbCooldown.size > 500) {
    const cutoff = n - 60000;
    for (const [k,v] of _cbCooldown) if (v < cutoff) _cbCooldown.delete(k);
  }
  return true;
}

process.on("uncaughtException",  e => console.error("[uncaughtException]",  e));
process.on("unhandledRejection", e => console.error("[unhandledRejection]", e));

/* ═══════════════════════════════════════════════════════════════════════════
   HELPERS
   ═══════════════════════════════════════════════════════════════════════════ */
const now     = () => Date.now();
const esc     = s => String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
const rub     = n => `${Number(n||0).toLocaleString("ru-RU")} ₽`;
const dt      = (ts,lang="ru") => ts ? new Date(ts).toLocaleDateString(lang==="en"?"en-GB":"ru-RU") : "—";
const dts     = ts => ts ? new Date(ts).toLocaleString("ru-RU") : "—";
const isAdmin = id => Number(id) === ADMIN_ID;
const sleep   = ms => new Promise(r => setTimeout(r, ms));
const refLink = code => BOT_USERNAME
  ? `https://t.me/${BOT_USERNAME}?start=partner_${code}`
  : `https://t.me/?start=partner_${code}`;

function parseLinks(text) {
  if (!text) return "";
  return String(text).replace(/\[([^\]|]+)\|([^\]]+)\]/g, (_, label, url) =>
    `<a href="${url.trim()}">${esc(label.trim())}</a>`
  );
}

/* ── Premium Emoji helpers ─────────────────────────────────────────────── */
function em(emoji, emojiKey) {
  if (!emojiKey || !E[emojiKey]) return emoji;
  return `<tg-emoji emoji-id="${E[emojiKey]}">${emoji}</tg-emoji>`;
}

function btn(text, cbOrUrl, emojiKey, isUrl=false) {
  const b = { text };
  if (isUrl) b.url = cbOrUrl; else b.callback_data = cbOrUrl;
  if (emojiKey && E[emojiKey]) b.icon_custom_emoji_id = E[emojiKey];
  return b;
}

/* ═══════════════════════════════════════════════════════════════════════════
   SETTINGS
   ═══════════════════════════════════════════════════════════════════════════ */
function setting(k, f="")  { return db.prepare("SELECT value v FROM settings WHERE key=?").get(k)?.v ?? f; }
function setSetting(k, v)  { db.prepare("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").run(k, String(v??"")); }
function delSetting(k)     { db.prepare("DELETE FROM settings WHERE key=?").run(k); }
function fkShopId()        { return Number(setting("fk_shop_id", String(FK_SHOP_ID_ENV))||0); }
function fkMinRub()        { return Math.max(1, Number(setting("fk_min_rub", String(FK_MIN_RUB_ENV))||FK_MIN_RUB_ENV||50)); }
function fkNotifyPath() {
  let p = String(setting("fk_notify_path", FK_PATH_NOTIFY_ENV)||FK_PATH_NOTIFY_ENV||"/freekassa/notify").trim();
  if (!p.startsWith("/")) p = `/${p}`;
  return p.replace(/\s+/g, "");
}
function isFkEnabled()     { return !!(fkShopId()>0 && FK_API_KEY && FK_SECRET2); }
function isValidPublicIpv4(ip) {
  if (!/^\d{1,3}(\.\d{1,3}){3}$/.test(String(ip||""))) return false;
  const p = String(ip).split(".").map(x=>Number(x));
  if (p.some(n=>!Number.isInteger(n)||n<0||n>255)) return false;
  if (p[0]===10||p[0]===127||p[0]===0) return false;
  if (p[0]===169&&p[1]===254) return false;
  if (p[0]===172&&p[1]>=16&&p[1]<=31) return false;
  if (p[0]===192&&p[1]===168) return false;
  return true;
}
function fkServerIp() {
  const fromDb = String(setting("fk_server_ip","")||"").trim();
  if (isValidPublicIpv4(fromDb)) return fromDb;
  const fromEnv = String(FK_SERVER_IP_ENV||"").trim();
  if (isValidPublicIpv4(fromEnv)) return fromEnv;
  return "";
}

function normalizeUrl(v) {
  if (!v) return "";
  v = String(v).trim();
  if (v.startsWith("@")) return `https://t.me/${v.slice(1)}`;
  if (!v.startsWith("http") && !v.startsWith("tg://")) return `https://t.me/${v}`;
  return v;
}
const lnk = {
  support: () => normalizeUrl(setting("url_support")||SUPPORT_URL),
  privacy: () => normalizeUrl(setting("url_privacy")||""),
  terms:   () => normalizeUrl(setting("url_terms")||""),
  proxy:   () => normalizeUrl(setting("url_proxy")||FREE_PROXY),
  news:    () => normalizeUrl(setting("url_news")||NEWS_URL),
  status:  () => normalizeUrl(setting("url_status")||"https://dreinnvpn.vercel.app"),
};

/* ═══════════════════════════════════════════════════════════════════════════
   LANGUAGE
   ═══════════════════════════════════════════════════════════════════════════ */
function getLang(uid) { return db.prepare("SELECT lang FROM users WHERE tg_id=?").get(Number(uid))?.lang || "ru"; }
function setLang(uid, lg) { db.prepare("UPDATE users SET lang=? WHERE tg_id=?").run(lg, Number(uid)); }

/* ═══════════════════════════════════════════════════════════════════════════
   DB HELPERS
   ═══════════════════════════════════════════════════════════════════════════ */
function user(id)     { return db.prepare("SELECT * FROM users WHERE tg_id=?").get(Number(id)); }
function sub(id)      { return db.prepare("SELECT * FROM subscriptions WHERE tg_id=?").get(Number(id)); }
function activeSub(s) { return !!(s && s.is_active===1 && s.expires_at>now() && s.sub_url); }
function isTrialSub(uid) { const s=sub(uid); return activeSub(s) && s.plan_code==="trial"; }
function tariffs()    { return db.prepare("SELECT * FROM tariffs ORDER BY sort_order").all(); }
function tariff(c) {
  if (c==="trial") {
    const days = trialDays();
    return {code:"trial",title:`Пробный период (${days} дн.)`,duration_days:days,price_rub:0};
  }
  return db.prepare("SELECT * FROM tariffs WHERE code=?").get(c);
}

function getRefBalance(uid) {
  return Number(user(uid)?.ref_balance_rub || 0);
}

function addRefBalance(uid, amount) {
  db.prepare("UPDATE users SET ref_balance_rub=ref_balance_rub+?,updated_at=? WHERE tg_id=?")
    .run(Math.round(amount), now(), Number(uid));
}

function deductRefBalance(uid, amount) {
  const amt = Math.round(amount);
  const res = db.prepare(
    "UPDATE users SET ref_balance_rub=ref_balance_rub-?,updated_at=? WHERE tg_id=? AND ref_balance_rub>=?"
  ).run(amt, now(), Number(uid), amt);
  return res.changes > 0;
}

function addReferralReward(buyerId, purchaseAmount) {
  const b = user(buyerId);
  if (!b || !b.referred_by) return;
  const r = user(b.referred_by);
  if (!r) return;
  if (Number(b.referred_by) === Number(buyerId)) return;
  const pct = Math.max(0, Math.min(100, Number(setting("ref_percent","30"))||30));
  const reward = Math.floor((Number(purchaseAmount)*pct)/100);
  if (reward <= 0) return;
  db.prepare("UPDATE users SET ref_balance_rub=ref_balance_rub+?,ref_earned=ref_earned+?,updated_at=? WHERE tg_id=?")
    .run(reward, reward, now(), Number(r.tg_id));
  db.prepare("INSERT INTO referrals(referrer_tg_id,invited_tg_id,amount_rub,percent,reward_rub,created_at) VALUES(?,?,?,?,?,?)")
    .run(Number(r.tg_id), Number(buyerId), Number(purchaseAmount), pct, reward, now());
  const isRu = getLang(r.tg_id)==="ru";
  tg("sendMessage",{
    chat_id: r.tg_id,
    text: isRu
      ? `${em("💰","money")} <b>Реферальное вознаграждение</b>\n\n<blockquote>+${rub(reward)} (${pct}% от покупки)</blockquote>`
      : `${em("💰","money")} <b>Referral reward</b>\n\n<blockquote>+${rub(reward)} (${pct}% of purchase)</blockquote>`,
    parse_mode:"HTML"
  }).catch(()=>{});
}

function usersPage(page, me, size=8) {
  const p = Math.max(0,Number(page||0)), off = p*size;
  const items = db.prepare("SELECT tg_id,username,first_name FROM users WHERE tg_id!=? ORDER BY updated_at DESC LIMIT ? OFFSET ?").all(Number(me),size,off);
  const total = Number(db.prepare("SELECT COUNT(*) c FROM users WHERE tg_id!=?").get(Number(me)).c||0);
  return {items,total,page:p,size};
}

/* ═══════════════════════════════════════════════════════════════════════════
   STATES
   ═══════════════════════════════════════════════════════════════════════════ */
function setAdminState(id,state,payload="") { db.prepare("INSERT INTO admin_states(admin_tg_id,state,payload,updated_at) VALUES(?,?,?,?) ON CONFLICT(admin_tg_id) DO UPDATE SET state=excluded.state,payload=excluded.payload,updated_at=excluded.updated_at").run(Number(id),state,String(payload),now()); }
function getAdminState(id)                  { return db.prepare("SELECT * FROM admin_states WHERE admin_tg_id=?").get(Number(id)); }
function clearAdminState(id)                { db.prepare("DELETE FROM admin_states WHERE admin_tg_id=?").run(Number(id)); }
function setUserState(id,state,payload="")  { db.prepare("INSERT INTO user_states(tg_id,state,payload,updated_at) VALUES(?,?,?,?) ON CONFLICT(tg_id) DO UPDATE SET state=excluded.state,payload=excluded.payload,updated_at=excluded.updated_at").run(Number(id),state,String(payload),now()); }
function getUserState(id)                   { return db.prepare("SELECT * FROM user_states WHERE tg_id=?").get(Number(id)); }
function clearUserState(id)                 { db.prepare("DELETE FROM user_states WHERE tg_id=?").run(Number(id)); }

async function sendPrompt(chatId, text, cancelCb="cancel:input", extraButtons=[]) {
  const rows = [...extraButtons, [btn("Отмена", cancelCb, "cross")]];
  const m = await tg("sendMessage",{
    chat_id:chatId, text, parse_mode:"HTML",
    reply_markup:{inline_keyboard:rows},
    disable_web_page_preview:true,
  });
  return Number(m?.message_id||0);
}

function delMsg(chatId, msgId) {
  if (!chatId||!msgId) return;
  tg("deleteMessage",{chat_id:chatId,message_id:msgId}).catch(()=>{});
}

/* ═══════════════════════════════════════════════════════════════════════════
   DB INIT + MIGRATIONS
   ═══════════════════════════════════════════════════════════════════════════ */
function init() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS users(
      tg_id INTEGER PRIMARY KEY, username TEXT NOT NULL DEFAULT '', first_name TEXT NOT NULL DEFAULT '',
      ref_balance_rub INTEGER NOT NULL DEFAULT 0,
      referred_by INTEGER, ref_code TEXT, ref_earned INTEGER NOT NULL DEFAULT 0,
      payout_method TEXT NOT NULL DEFAULT '', payout_details TEXT NOT NULL DEFAULT '',
      last_chat_id INTEGER, last_menu_id INTEGER,
      lang TEXT NOT NULL DEFAULT 'ru',
      trial_used INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_users_ref_code ON users(ref_code);
    CREATE TABLE IF NOT EXISTS subscriptions(
      tg_id INTEGER PRIMARY KEY, plan_code TEXT NOT NULL DEFAULT '', plan_title TEXT NOT NULL DEFAULT '',
      sub_url TEXT NOT NULL DEFAULT '', expires_at INTEGER, is_active INTEGER NOT NULL DEFAULT 0,
      devices INTEGER NOT NULL DEFAULT 3,
      created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS tariffs(code TEXT PRIMARY KEY, title TEXT NOT NULL, duration_days INTEGER NOT NULL, price_rub INTEGER NOT NULL, sort_order INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '');
    CREATE TABLE IF NOT EXISTS referrals(id INTEGER PRIMARY KEY AUTOINCREMENT, referrer_tg_id INTEGER NOT NULL, invited_tg_id INTEGER NOT NULL, amount_rub INTEGER NOT NULL, percent INTEGER NOT NULL, reward_rub INTEGER NOT NULL, created_at INTEGER NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_referrals_ref ON referrals(referrer_tg_id);
    CREATE TABLE IF NOT EXISTS gifts(id INTEGER PRIMARY KEY AUTOINCREMENT, from_tg_id INTEGER NOT NULL, to_tg_id INTEGER NOT NULL, tariff_code TEXT NOT NULL, tariff_title TEXT NOT NULL, amount_rub INTEGER NOT NULL, created_at INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS purchases(id INTEGER PRIMARY KEY AUTOINCREMENT, tg_id INTEGER NOT NULL, tariff_code TEXT NOT NULL, tariff_title TEXT NOT NULL, amount_rub INTEGER NOT NULL, kind TEXT NOT NULL, created_at INTEGER NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_purchases_tg_id ON purchases(tg_id);
    CREATE TABLE IF NOT EXISTS admin_states(admin_tg_id INTEGER PRIMARY KEY, state TEXT NOT NULL, payload TEXT NOT NULL DEFAULT '', updated_at INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS user_states(tg_id INTEGER PRIMARY KEY, state TEXT NOT NULL, payload TEXT NOT NULL DEFAULT '', updated_at INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS withdrawal_requests(
      id INTEGER PRIMARY KEY AUTOINCREMENT, tg_id INTEGER NOT NULL,
      amount_rub INTEGER NOT NULL, method TEXT NOT NULL DEFAULT '', details TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'pending', admin_note TEXT NOT NULL DEFAULT '',
      created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_wr_status ON withdrawal_requests(status);
    CREATE TABLE IF NOT EXISTS crypto_payments(
      id INTEGER PRIMARY KEY AUTOINCREMENT, tg_id INTEGER NOT NULL,
      amount_rub INTEGER NOT NULL, amount_usdt REAL NOT NULL, rate_rub REAL NOT NULL,
      invoice_id TEXT NOT NULL, pay_url TEXT NOT NULL DEFAULT '',
      pending_order_id INTEGER,
      status TEXT NOT NULL DEFAULT 'pending',
      created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_cp_tg_id ON crypto_payments(tg_id);
    CREATE INDEX IF NOT EXISTS idx_cp_invoice_id ON crypto_payments(invoice_id);
    CREATE TABLE IF NOT EXISTS freekassa_payments(
      id INTEGER PRIMARY KEY AUTOINCREMENT, tg_id INTEGER NOT NULL,
      amount_rub INTEGER NOT NULL, method_id INTEGER NOT NULL,
      payment_id TEXT NOT NULL UNIQUE, fk_order_id INTEGER,
      pending_order_id INTEGER, location TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'pending', credited_at INTEGER,
      created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_fk_tg_id ON freekassa_payments(tg_id);
    CREATE INDEX IF NOT EXISTS idx_fk_payment_id ON freekassa_payments(payment_id);
    CREATE TABLE IF NOT EXISTS promo_codes(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      code TEXT NOT NULL UNIQUE COLLATE NOCASE,
      discount_pct INTEGER NOT NULL DEFAULT 10,
      uses_max INTEGER NOT NULL DEFAULT 0,
      uses_current INTEGER NOT NULL DEFAULT 0,
      is_active INTEGER NOT NULL DEFAULT 1,
      created_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS promo_uses(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tg_id INTEGER NOT NULL, promo_code TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_promo_uses ON promo_uses(tg_id, promo_code);
    CREATE TABLE IF NOT EXISTS notified_expiry(
      tg_id INTEGER NOT NULL, level TEXT NOT NULL,
      notified_at INTEGER NOT NULL, PRIMARY KEY(tg_id, level)
    );
  `);

  /* ── pending_orders: safe CREATE IF NOT EXISTS + column migrations ─── */
  db.exec(`
    CREATE TABLE IF NOT EXISTS pending_orders(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tg_id INTEGER NOT NULL,
      tariff_code TEXT NOT NULL,
      kind TEXT NOT NULL DEFAULT 'new',
      promo_code TEXT NOT NULL DEFAULT '',
      promo_pct INTEGER NOT NULL DEFAULT 0,
      devices INTEGER NOT NULL DEFAULT 3,
      recipient_tg_id INTEGER NOT NULL DEFAULT 0,
      use_ref_balance INTEGER NOT NULL DEFAULT 0,
      ref_deducted INTEGER NOT NULL DEFAULT 0,
      expires_at INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_po_tg_id ON pending_orders(tg_id);
  `);

  // Migrations — add columns if missing
  const migrations = [
    "ALTER TABLE users ADD COLUMN ref_balance_rub INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE users ADD COLUMN ref_earned INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE users ADD COLUMN lang TEXT NOT NULL DEFAULT 'ru'",
    "ALTER TABLE users ADD COLUMN trial_used INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE subscriptions ADD COLUMN devices INTEGER NOT NULL DEFAULT 3",
    "ALTER TABLE freekassa_payments ADD COLUMN pending_order_id INTEGER",
    "ALTER TABLE crypto_payments ADD COLUMN pending_order_id INTEGER",
    "ALTER TABLE pending_orders ADD COLUMN use_ref_balance INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE pending_orders ADD COLUMN ref_deducted INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE pending_orders ADD COLUMN recipient_tg_id INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE pending_orders ADD COLUMN promo_code TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE pending_orders ADD COLUMN promo_pct INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE pending_orders ADD COLUMN devices INTEGER NOT NULL DEFAULT 3",
  ];
  for (const m of migrations) { try { db.exec(m); } catch {} }

  // Default tariffs
  const st = db.prepare("INSERT INTO tariffs(code,title,duration_days,price_rub,sort_order) VALUES(?,?,?,?,?) ON CONFLICT(code) DO NOTHING");
  [
    ["d1","1 день",1,49,1],
    ["d7","7 дней",7,149,2],
    ["d14","14 дней",14,249,3],
    ["m1","30 дней",30,399,4],
    ["m3","90 дней",90,899,5],
    ["y1","365 дней",365,2499,6],
  ].forEach(r=>st.run(...r));
  db.prepare("DELETE FROM tariffs WHERE code='m6'").run();

  // Default settings
  const ss = db.prepare("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO NOTHING");
  const defaults = [
    ["ref_percent","30"],
    ["ref_min_withdraw","3000"],
    ["guide_text","📋 <b>Инструкция по подключению VPN:</b>\n\n1. Скачайте [Happ по инструкции|https://telegra.ph/Instrukciya-po-skachivaniyu-Happ-03-22].\n2. Скопируйте ваш ключ доступа из раздела «Моя подписка» и вставьте его в клиент.\n3. Всё готово! Теперь вы можете подключиться к защищённому интернету.\n\n💬 Если возникнут вопросы — обращайтесь в [поддержку|https://t.me/dreinnvpnsupportbot]"],
    ["guide_text_en","📋 <b>VPN Connection Guide:</b>\n\n1. Download [Happ|https://www.happ.su/main/ru] or [v2RayTun|https://v2raytun.com/].\n2. Copy your access key from «My Subscription» and paste it into the app.\n3. Done! Your internet now routes through our server.\n\n💬 Questions? Contact [support|https://t.me/dreinnvpnsupportbot]"],
    ["gif_main_menu",""],["gif_purchase_success",""],["gif_gift_success",""],["gif_broadcast",""],
    ["img_home",""],["img_sub",""],["img_buy",""],["img_ref",""],
    ["img_gift",""],["img_guide",""],["img_about",""],
    ["url_support","https://t.me/dreinnvpnsupportbot"],
    ["url_privacy",""],["url_terms",""],["url_proxy",""],["url_news",""],
    ["url_status","https://dreinnvpn.vercel.app"],
    ["channel_id",""],["channel_invite_url",""],
    ["trial_enabled","1"],["trial_days","7"],
    ["subscription_required","1"],
    ["fk_shop_id",String(FK_SHOP_ID_ENV||"")],
    ["fk_min_rub",String(FK_MIN_RUB_ENV||50)],
    ["fk_notify_path",FK_PATH_NOTIFY_ENV||"/freekassa/notify"],
    ["fk_server_ip",String(FK_SERVER_IP_ENV||"")],
    ["devices_extra_price","360"],
    ["direct_payment_enabled","1"],
  ];
  defaults.forEach(([k,v])=>ss.run(k,v));
}

/* ═══════════════════════════════════════════════════════════════════════════
   TELEGRAM API
   ═══════════════════════════════════════════════════════════════════════════ */
async function tg(method, params, _retry=0) {
  const isLP = method === "getUpdates";
  const ctrl = new AbortController();
  const tid = setTimeout(()=>ctrl.abort(), isLP ? 45000 : 30000);
  try {
    const r = await fetch(`${TG_BASE}/${method}`,{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify(params),
      signal:ctrl.signal,
    });
    const j = await r.json().catch(()=>({}));
    if (r.status===429 && _retry<3) {
      const ra = Number(j?.parameters?.retry_after||5);
      await sleep((ra+1)*1000);
      return tg(method,params,_retry+1);
    }
    if (!r.ok || j.ok===false) throw new Error(j.description||`TG HTTP ${r.status}`);
    return j.result;
  } finally { clearTimeout(tid); }
}

async function tgSendFile(method, chatId, fieldName, filePath, extra={}) {
  const buf = await fsp.readFile(filePath);
  const form = new FormData();
  form.append("chat_id", String(chatId));
  form.append(fieldName, new Blob([buf],{type:"application/octet-stream"}), path.basename(filePath));
  for (const [k,v] of Object.entries(extra)) form.append(k,String(v));
  const r = await fetch(`${TG_BASE}/${method}`,{method:"POST",body:form});
  const j = await r.json().catch(()=>({}));
  if (!r.ok||j.ok===false) throw new Error(j.description||`TG HTTP ${r.status}`);
  return j.result;
}

async function sendPhotoBuffer(chatId, buffer, mimeType, caption, replyMarkup) {
  const form = new FormData();
  form.append("chat_id", String(chatId));
  form.append("photo", new Blob([buffer],{type:mimeType||"image/png"}), "photo.png");
  if (caption) { form.append("caption", caption); form.append("parse_mode","HTML"); }
  if (replyMarkup) form.append("reply_markup", JSON.stringify(replyMarkup));
  const r = await fetch(`${TG_BASE}/sendPhoto`,{method:"POST",body:form});
  const j = await r.json().catch(()=>({}));
  if (!r.ok||j.ok===false) throw new Error(j.description||`TG HTTP ${r.status}`);
  return j.result;
}

async function renderMsg(chatId, msgId, text, kb, photo=null) {
  if (photo) {
    if (msgId) {
      try {
        await tg("editMessageMedia",{
          chat_id:chatId, message_id:msgId,
          media:{type:"photo",media:photo,caption:text,parse_mode:"HTML"},
          reply_markup:kb,
        });
        return Number(msgId);
      } catch { await tg("deleteMessage",{chat_id:chatId,message_id:msgId}).catch(()=>{}); }
    }
    const m = await tg("sendPhoto",{chat_id:chatId,photo,caption:text,parse_mode:"HTML",reply_markup:kb});
    return Number(m.message_id);
  } else {
    if (msgId) {
      try {
        await tg("editMessageText",{chat_id:chatId,message_id:msgId,text,parse_mode:"HTML",disable_web_page_preview:true,reply_markup:kb});
        return Number(msgId);
      } catch(e) {
        if (String(e.message).includes("message is not modified")) return Number(msgId);
        await tg("deleteMessage",{chat_id:chatId,message_id:msgId}).catch(()=>{});
      }
    }
    const m = await tg("sendMessage",{chat_id:chatId,text,parse_mode:"HTML",disable_web_page_preview:true,reply_markup:kb});
    return Number(m.message_id);
  }
}

async function gif(chatId, key) {
  const g = setting(key,"");
  if (g) await tg("sendAnimation",{chat_id:chatId,animation:g}).catch(()=>{});
}

/* ═══════════════════════════════════════════════════════════════════════════
   CRYPTOBOT
   ═══════════════════════════════════════════════════════════════════════════ */
let _rateCache = { val: USDT_FALLBACK, ts: 0 };

async function getUsdtRate() {
  if (Date.now() - _rateCache.ts < 5*60*1000) return _rateCache.val;
  if (!CRYPTOBOT_TOKEN) return USDT_FALLBACK;
  try {
    const r = await fetch(CRYPTOBOT_API+"/getExchangeRates",
      {headers:{"Crypto-Pay-API-Token":CRYPTOBOT_TOKEN},signal:AbortSignal.timeout(8000)});
    const d = await r.json().catch(()=>({}));
    for (const it of (d.result||[])) {
      if ((it.source||"").toUpperCase()==="USDT" && (it.target||"").toUpperCase()==="RUB") {
        const rate = parseFloat(it.rate);
        if (rate>1) { _rateCache={val:rate,ts:Date.now()}; return rate; }
      }
    }
  } catch(e) { console.warn("[CryptoBot] getRate:", e.message); }
  return USDT_FALLBACK;
}

async function createCryptoInvoice(amountRub) {
  if (!CRYPTOBOT_TOKEN) return null;
  try {
    const rate = await getUsdtRate();
    const amountUsdt = Math.max(0.01, Math.round(amountRub/rate*100)/100);
    const r = await fetch(CRYPTOBOT_API+"/createInvoice",{
      method:"POST",
      headers:{"Crypto-Pay-API-Token":CRYPTOBOT_TOKEN,"Content-Type":"application/json"},
      body:JSON.stringify({asset:"USDT",amount:String(amountUsdt),description:`Пополнение ${amountRub} ₽`,expires_in:CRYPTO_INVOICE_TTL}),
      signal:AbortSignal.timeout(12000),
    });
    const d = await r.json().catch(()=>({}));
    if (d.ok) { const inv=d.result; return {invoiceId:String(inv.invoice_id),payUrl:inv.pay_url,rate,amountUsdt}; }
  } catch(e) { console.error("[CryptoBot] createInvoice:", e.message); }
  return null;
}

async function checkCryptoInvoice(invoiceId) {
  if (!CRYPTOBOT_TOKEN) return false;
  try {
    const r = await fetch(CRYPTOBOT_API+"/getInvoices?invoice_ids="+invoiceId,
      {headers:{"Crypto-Pay-API-Token":CRYPTOBOT_TOKEN},signal:AbortSignal.timeout(10000)});
    const d = await r.json().catch(()=>({}));
    const items = (d.result||{}).items||[];
    return items.length>0 && items[0].status==="paid";
  } catch(e) { console.error("[CryptoBot] checkInvoice:", e.message); }
  return false;
}

function createCryptoPaymentRow(tgId, amountRub, amountUsdt, rateRub, invoiceId, payUrl, poId=null) {
  return db.prepare(
    "INSERT INTO crypto_payments(tg_id,amount_rub,amount_usdt,rate_rub,invoice_id,pay_url,pending_order_id,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'pending',?,?)"
  ).run(Number(tgId),Math.round(amountRub),amountUsdt,rateRub,invoiceId,payUrl,poId?Number(poId):null,now(),now()).lastInsertRowid;
}
function getCryptoPayment(id) { return db.prepare("SELECT * FROM crypto_payments WHERE id=?").get(Number(id)); }
function markCryptoPaid(id)   { db.prepare("UPDATE crypto_payments SET status='paid',updated_at=? WHERE id=?").run(now(),Number(id)); }
function markCryptoCancelled(id) { db.prepare("UPDATE crypto_payments SET status='cancelled',updated_at=? WHERE id=?").run(now(),Number(id)); }

function expireOldCryptoPayments(tgId) {
  db.prepare("UPDATE crypto_payments SET status='expired',updated_at=? WHERE tg_id=? AND status='pending' AND created_at<?")
    .run(now(),Number(tgId),now()-CRYPTO_INVOICE_TTL*1000);
}
function expireOldFkPayments(tgId) {
  db.prepare("UPDATE freekassa_payments SET status='expired',updated_at=? WHERE tg_id=? AND status='pending' AND created_at<?")
    .run(now(),Number(tgId),now()-3600*1000);
}
function expireOldPendingOrders() {
  db.prepare("UPDATE pending_orders SET status='expired',updated_at=? WHERE status='pending' AND expires_at<?")
    .run(now(),now());
}

/* ── Pending orders ──────────────────────────────────────────────────── */
function createPendingOrder(tgId, tariffCode, kind="new", promoCd="", promoPct=0, devices=3, recipientTgId=0, useRefBal=0) {
  expireOldPendingOrders();
  const expires = now() + 30*60*1000;
  return db.prepare(
    `INSERT INTO pending_orders(tg_id,tariff_code,kind,promo_code,promo_pct,devices,recipient_tg_id,use_ref_balance,ref_deducted,expires_at,status,created_at,updated_at)
     VALUES(?,?,?,?,?,?,?,?,0,?,'pending',?,?)`
  ).run(
    Number(tgId), tariffCode, kind, promoCd, promoPct,
    Number(devices)||3, Number(recipientTgId)||0, useRefBal?1:0,
    expires, now(), now()
  ).lastInsertRowid;
}
function getPendingOrder(id)       { return db.prepare("SELECT * FROM pending_orders WHERE id=?").get(Number(id)); }
function getPendingOrderByUser(tgId) {
  return db.prepare("SELECT * FROM pending_orders WHERE tg_id=? AND status='pending' AND expires_at>? ORDER BY id DESC LIMIT 1").get(Number(tgId),now());
}
function closePendingOrder(id, status="done") {
  db.prepare("UPDATE pending_orders SET status=?,updated_at=? WHERE id=?").run(status,now(),Number(id));
}

/* ── Promo codes ─────────────────────────────────────────────────────── */
function getPromo(code) { return db.prepare("SELECT * FROM promo_codes WHERE code=? COLLATE NOCASE").get(String(code||"").trim()); }
function hasUsedPromo(tgId, code) { return !!db.prepare("SELECT 1 FROM promo_uses WHERE tg_id=? AND promo_code=? COLLATE NOCASE").get(Number(tgId),String(code)); }
function usePromo(tgId, code) {
  db.prepare("INSERT OR IGNORE INTO promo_uses(tg_id,promo_code,created_at) VALUES(?,?,?)").run(Number(tgId),String(code).toUpperCase(),now());
  db.prepare("UPDATE promo_codes SET uses_current=uses_current+1 WHERE code=? COLLATE NOCASE").run(String(code));
}
function validatePromo(tgId, code) {
  if (hasUsedPromo(tgId, code)) return {ok:false, reason:"used"};
  const p = getPromo(code);
  if (!p || !p.is_active) return {ok:false, reason:"invalid"};
  if (p.uses_max>0 && p.uses_current>=p.uses_max) return {ok:false, reason:"invalid"};
  return {ok:true, promo:p};
}

function calcPrice(basePrice, promoPct) {
  if (!promoPct) return Number(basePrice);
  return Math.max(1, Math.round(Number(basePrice)*(100-promoPct)/100));
}
function devicesExtraPrice() { return Math.max(0, Number(setting("devices_extra_price","360"))||0); }
function devicesSurcharge(devices) { return Math.max(0, Number(devices||3)-3) * devicesExtraPrice(); }
function calcFinalPrice(basePrice, promoPct, devices) {
  return calcPrice(Number(basePrice) + devicesSurcharge(devices), promoPct);
}

/* ═══════════════════════════════════════════════════════════════════════════
   FREEKASSA
   ═══════════════════════════════════════════════════════════════════════════ */
let _fkNonce = 0;
function nextFkNonce() { const n=Math.floor(Date.now()/1000); _fkNonce=Math.max(_fkNonce+1,n); return _fkNonce; }
function fkSignPayload(payload) {
  const data={...payload}; delete data.signature;
  const keys=Object.keys(data).sort();
  return crypto.createHmac("sha256",FK_API_KEY).update(keys.map(k=>String(data[k]??"")).join("|")).digest("hex");
}
function methodTitle(i, lang) {
  const ru={44:"СБП (QR)",36:"Банковская карта РФ",43:"SberPay"};
  const en={44:"SBP (QR)",36:"Russian bank card",43:"SberPay"};
  return (lang==="en"?en:ru)[Number(i)]||`i=${i}`;
}

async function detectPublicIpv4() {
  const probes = [
    {url:"https://api.ipify.org?format=json",parse:t=>{try{return JSON.parse(t).ip||"";}catch{return "";}}},
    {url:"https://ifconfig.me/ip",parse:t=>String(t||"").trim()},
  ];
  for (const p of probes) {
    try {
      const r = await fetch(p.url,{signal:AbortSignal.timeout(5000)});
      if (!r.ok) continue;
      const ip = p.parse(await r.text());
      if (isValidPublicIpv4(ip)) return ip;
    } catch {}
  }
  return "";
}
async function ensureFkServerIp() {
  const existing = fkServerIp();
  if (existing) { setSetting("fk_server_ip",existing); return existing; }
  const detected = await detectPublicIpv4();
  if (detected) { setSetting("fk_server_ip",detected); return detected; }
  return "";
}

function createFkPaymentRow(tgId, amountRub, methodId, paymentId, location, fkOrderId=null, poId=null) {
  return db.prepare(
    "INSERT INTO freekassa_payments(tg_id,amount_rub,method_id,payment_id,fk_order_id,pending_order_id,location,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'pending',?,?)"
  ).run(Number(tgId),Math.round(amountRub),Number(methodId),String(paymentId),fkOrderId?Number(fkOrderId):null,poId?Number(poId):null,String(location||""),now(),now()).lastInsertRowid;
}
function getFkPayment(id) { return db.prepare("SELECT * FROM freekassa_payments WHERE id=?").get(Number(id)); }
function getFkPaymentByPaymentId(pid) { return db.prepare("SELECT * FROM freekassa_payments WHERE payment_id=?").get(String(pid)); }
function markFkPaid(id, fkOrderId=null) {
  db.prepare("UPDATE freekassa_payments SET status='paid',fk_order_id=COALESCE(?,fk_order_id),credited_at=?,updated_at=? WHERE id=?")
    .run(fkOrderId?Number(fkOrderId):null,now(),now(),Number(id));
}
function markFkCancelled(id) { db.prepare("UPDATE freekassa_payments SET status='cancelled',updated_at=? WHERE id=?").run(now(),Number(id)); }

async function fkApiPost(pathname, payload) {
  const body = {...payload, signature:fkSignPayload(payload)};
  const r = await fetch(`${FK_API_BASE}${pathname}`,{
    method:"POST",
    headers:{"Content-Type":"application/json","Authorization":`Merchant ${fkShopId()}:${FK_API_KEY}`},
    body:JSON.stringify(body),
    signal:AbortSignal.timeout(15000),
  });
  const txt = await r.text();
  let data = {}; try { data = txt ? JSON.parse(txt) : {}; } catch {}
  if (!r.ok) throw new Error(data?.message||`FreeKassa HTTP ${r.status}`);
  return data;
}

async function createFkOrder({uid, amountRub, methodId, email, ip}) {
  const paymentId = `tg${uid}_${Date.now()}_${Math.floor(Math.random()*10000)}`;
  const data = await fkApiPost("/orders/create",{
    shopId:fkShopId(), nonce:nextFkNonce(), paymentId,
    i:Number(methodId), email, ip,
    amount:Number(amountRub).toFixed(2), currency:"RUB",
  });
  return {paymentId, orderId:data.orderId||null, location:data.location||""};
}

async function checkFkOrderByPaymentId(paymentId) {
  const data = await fkApiPost("/orders",{shopId:fkShopId(),nonce:nextFkNonce(),paymentId:String(paymentId)});
  const list = Array.isArray(data?.orders) ? data.orders : [];
  return list.length ? list[0] : null;
}

function getRequestIp(req) {
  return String(req.headers["x-real-ip"]||"").trim()
    || String(req.headers["x-forwarded-for"]||"").split(",")[0].trim()
    || (req.socket?.remoteAddress||"").replace(/^::ffff:/,"");
}
function parseBodyByContentType(raw, ct) {
  ct = String(ct||"").toLowerCase();
  if (ct.includes("application/json")) { try { return JSON.parse(raw||"{}"); } catch { return {}; } }
  const out = {};
  try { new URLSearchParams(raw||"").forEach((v,k)=>{out[k]=v;}); } catch {}
  return out;
}
function validateFkWebhookSign(p) {
  const sign = String(p.SIGN||p.sign||"").toLowerCase();
  const mid = String(p.MERCHANT_ID||p.merchant_id||"");
  const amt = String(p.AMOUNT||p.amount||"");
  const moid = String(p.MERCHANT_ORDER_ID||p.merchant_order_id||"");
  if (!sign||!mid||!amt||!moid) return false;
  return crypto.createHash("md5").update(`${mid}:${amt}:${FK_SECRET2}:${moid}`).digest("hex").toLowerCase() === sign;
}

async function creditFkPayment(paymentId, fkOrderId=null, paidAmount=null) {
  const fp = getFkPaymentByPaymentId(paymentId);
  if (!fp) return {ok:false,reason:"NOT_FOUND"};
  if (fp.status==="paid") return {ok:true,reason:"ALREADY_PAID",fp};
  if (fp.status!=="pending") return {ok:false,reason:"CLOSED",fp};
  if (paidAmount!=null) {
    const pa = Math.round(Number(paidAmount));
    if (!Number.isFinite(pa)||pa!==Number(fp.amount_rub)) return {ok:false,reason:"WRONG_AMOUNT",fp};
  }
  markFkPaid(fp.id, fkOrderId);
  tg("sendMessage",{chat_id:ADMIN_ID,text:[
    `<b>${em("💳","wallet")} FreeKassa</b>`,
    `(<code>${fp.tg_id}</code>) ${rub(fp.amount_rub)}`,
    `${esc(methodTitle(fp.method_id,"ru"))}`,
  ].join("\n"),parse_mode:"HTML"}).catch(()=>{});
  return {ok:true,reason:"PAID",fp};
}

/* ═══════════════════════════════════════════════════════════════════════════
   CHANNEL GATE + TRIAL
   ═══════════════════════════════════════════════════════════════════════════ */
function getChannelId() { return setting("channel_id","").trim(); }
function isSubscriptionRequired() { return setting("subscription_required","1")==="1"; }

async function checkChannelMembership(userId) {
  const chanId = getChannelId();
  if (!chanId) return true;
  try {
    const m = await tg("getChatMember",{chat_id:chanId,user_id:Number(userId)});
    return ["member","administrator","creator"].includes(m?.status);
  } catch { return false; }
}
function getChannelUrl() {
  const inv = setting("channel_invite_url","").trim();
  if (inv) return inv;
  const id = getChannelId();
  if (id && id.startsWith("@")) return `https://t.me/${id.slice(1)}`;
  return "";
}
function trialEnabled() { return setting("trial_enabled","1")==="1"; }
function trialDays()    { return Math.max(1,Math.min(365,Number(setting("trial_days","7"))||7)); }
function hasUsedTrial(uid) { return !!(db.prepare("SELECT trial_used FROM users WHERE tg_id=?").get(Number(uid))?.trial_used); }
function markTrialUsed(uid) { db.prepare("UPDATE users SET trial_used=1,updated_at=? WHERE tg_id=?").run(now(),Number(uid)); }

async function enforceChannelGate(uid, chatId, lang) {
  if (!isSubscriptionRequired()) return true;
  if (!getChannelId()) return true;
  if (await checkChannelMembership(uid)) return true;
  const chanUrl = getChannelUrl();
  const rows = [];
  if (chanUrl) rows.push([btn("Подписаться",chanUrl,"megaphone",true)]);
  rows.push([btn("Я подписался","gate:check","check")]);
  await tg("sendMessage",{
    chat_id:chatId,
    text:`${em("👋","smile")} Чтобы пользоваться ботом, подпишитесь на наш канал.`,
    parse_mode:"HTML",
    reply_markup:{inline_keyboard:rows},
  });
  return false;
}

/* ═══════════════════════════════════════════════════════════════════════════
   VPN API
   ═══════════════════════════════════════════════════════════════════════════ */
async function createSubViaApi(target, tr, giftMode, devices=3) {
  const ctrl = new AbortController();
  const tid = setTimeout(()=>ctrl.abort(), 20000);
  try {
    const r = await fetch(`${API}/api/bot-subscription`,{
      method:"POST",
      headers:{"Content-Type":"application/json","x-app-secret":APP_SECRET},
      body:JSON.stringify({
        telegramUserId:String(target.tg_id),
        telegramUsername:target.username||"",
        firstName:target.first_name||"",
        durationDays:tr.duration_days,
        name:`VPN ${tr.title}`,
        description:giftMode?`Подарок: ${tr.title}`:`Тариф: ${tr.title}`,
        devices:Number(devices)||3,
      }),
      signal:ctrl.signal,
    });
    const j = await r.json().catch(()=>({}));
    if (!r.ok) throw new Error(j.error||`API HTTP ${r.status}`);
    return j;
  } finally { clearTimeout(tid); }
}

/* ═══════════════════════════════════════════════════════════════════════════
   PURCHASE LOGIC — FIXED: days add up, no early-renew restriction
   ═══════════════════════════════════════════════════════════════════════════ */
async function doPurchase(payerId, receiverId, code, kind, promoCd="", promoPct=0, devices=3, refDeducted=0) {
  const payer = user(payerId), receiver = user(receiverId), tr = tariff(code);
  if (!payer||!receiver||!tr) throw new Error("INVALID");

  if (tr.code === "trial") throw new Error("TRIAL_NOT_PURCHASABLE");

  const s = sub(receiverId), act = activeSub(s);
  const isTrial = act && s.plan_code==="trial";

  // For "new" — block only if active non-trial subscription exists
  if (kind==="new" && act && !isTrial) throw new Error("ACTIVE");
  // For "gift" — block if receiver has active subscription
  if (kind==="gift" && act) throw new Error("ACTIVE");
  // For "renew" — always allowed (no time restriction)

  const devCount = Math.max(1,Math.min(10,Number(devices)||3));
  const finalPrice = calcFinalPrice(tr.price_rub, promoPct, devCount);

  const api = await createSubViaApi(receiver, tr, kind==="gift", devCount);
  const subUrl = api.subscriptionUrl || api.sub_url || "";
  if (!subUrl) throw new Error("API не вернул ссылку подписки");

  // FIXED: For renew — add days to current expiry (if still active), not from now()
  let newExp;
  if (kind==="renew" && !isTrial) {
    const base = (s && s.expires_at > now()) ? s.expires_at : now();
    newExp = base + tr.duration_days * 86400000;
  } else {
    newExp = now() + tr.duration_days * 86400000;
  }

  db.transaction(()=>{
    addReferralReward(payerId, finalPrice);
    if (promoCd) usePromo(payerId, promoCd);
    db.prepare(`INSERT INTO subscriptions(tg_id,plan_code,plan_title,sub_url,expires_at,is_active,devices,created_at,updated_at)
      VALUES(?,?,?,?,?,1,?,?,?) ON CONFLICT(tg_id) DO UPDATE SET
      plan_code=excluded.plan_code,plan_title=excluded.plan_title,sub_url=excluded.sub_url,
      expires_at=excluded.expires_at,is_active=1,devices=excluded.devices,updated_at=excluded.updated_at`)
      .run(Number(receiverId),tr.code,tr.title,subUrl,newExp,devCount,now(),now());
    db.prepare("INSERT INTO purchases(tg_id,tariff_code,tariff_title,amount_rub,kind,created_at) VALUES(?,?,?,?,?,?)")
      .run(Number(payerId),tr.code,tr.title,finalPrice,kind,now());
    if (kind==="gift")
      db.prepare("INSERT INTO gifts(from_tg_id,to_tg_id,tariff_code,tariff_title,amount_rub,created_at) VALUES(?,?,?,?,?,?)")
        .run(Number(payerId),Number(receiverId),tr.code,tr.title,finalPrice,now());
  })();
  db.prepare("DELETE FROM notified_expiry WHERE tg_id=?").run(Number(receiverId));
  return {tr, url:subUrl, exp:newExp, finalPrice, devices:devCount};
}

async function completePurchaseAfterPayment(tgId, po) {
  const u = user(tgId), tr = tariff(po.tariff_code);
  if (!u||!tr) return;
  const chatId = u.last_chat_id;
  if (!chatId) return;

  const isGift = po.kind==="gift" && Number(po.recipient_tg_id||0)>0;
  const receiverId = isGift ? Number(po.recipient_tg_id) : tgId;

  let refDeducted = Number(po.ref_deducted || 0);
  if (po.use_ref_balance && refDeducted === 0) {
    const finalPrice = calcFinalPrice(tr.price_rub, po.promo_pct, Number(po.devices||3));
    const refBal = getRefBalance(tgId);
    const toDeduct = Math.min(refBal, finalPrice);
    if (toDeduct > 0) {
      if (deductRefBalance(tgId, toDeduct)) {
        refDeducted = toDeduct;
        db.prepare("UPDATE pending_orders SET ref_deducted=?,updated_at=? WHERE id=?").run(refDeducted, now(), po.id);
      }
    }
  }

  try {
    const res = await doPurchase(tgId, receiverId, po.tariff_code, isGift?"gift":po.kind, po.promo_code, po.promo_pct, Number(po.devices||3), refDeducted);
    const s = sub(receiverId);
    const lang = getLang(tgId);

    if (isGift) {
      const to = user(receiverId);
      const toName = to?.first_name||(to?.username?`@${to.username}`:String(receiverId));
      await tg("sendMessage",{chat_id:chatId,text:[
        `${em("🎁","gift")} <b>Подарок отправлен!</b>`,
        "",`Получатель: <b>${esc(toName)}</b>`,
        `Тариф: <b>${esc(tariffTitle(tr,lang))}</b>`,
      ].join("\n"),parse_mode:"HTML",reply_markup:{inline_keyboard:[[btn("Главное меню","v:home","home")]]}}).catch(()=>{});
      if (to) {
        tg("sendMessage",{chat_id:receiverId,text:[
          `${em("🎁","gift")} <b>Вам подарили подписку!</b>`,
          "",`Тариф: <b>${esc(tariffTitle(tr,getLang(receiverId)))}</b>`,
          `Истекает: <b>${dt(s?.expires_at)}</b>`,
          "",`<code>${esc(s?.sub_url||"")}</code>`,
        ].join("\n"),parse_mode:"HTML",reply_markup:{inline_keyboard:[[btn("Подключить",s?.sub_url||"","send",true)]]}}).catch(()=>{});
      }
    } else {
      const lines = [
        `${em("✅","check")} <b>Оплата прошла успешно!</b>`,
        "",`Тариф: <b>${esc(tariffTitle(tr,lang))}</b>`,
        `Истекает: <b>${dt(res.exp,lang)}</b>`,
      ];
      if (refDeducted > 0) lines.push(`Списано с реф. баланса: <b>${rub(refDeducted)}</b>`);
      lines.push("",`<code>${esc(res.url)}</code>`);
      await tg("sendMessage",{chat_id:chatId,text:lines.join("\n"),parse_mode:"HTML",reply_markup:{inline_keyboard:[
        [btn("Подключить устройство",res.url,"send",true)],
        [btn("Моя подписка","v:sub","lockOpen"),btn("Главное меню","v:home","home")],
      ]}}).catch(()=>{});
    }
  } catch(e) {
    if (refDeducted > 0) {
      addRefBalance(tgId, refDeducted);
      db.prepare("UPDATE pending_orders SET ref_deducted=0,updated_at=? WHERE id=?").run(now(), po.id);
    }
    tg("sendMessage",{chat_id:chatId,text:`${em("❌","cross")} Ошибка: ${esc(e.message)}`,parse_mode:"HTML"}).catch(()=>{});
  }
}

async function doTrial(uid, chatId, msgId) {
  if (hasUsedTrial(uid)||activeSub(sub(uid))) return;
  const days = trialDays();
  const u = user(uid);
  const fakeTariff = {code:"trial",title:`Пробный период (${days} дн.)`,duration_days:days,price_rub:0};
  const api = await createSubViaApi(u, fakeTariff, false, 1);
  const subUrl = api.subscriptionUrl||api.sub_url||"";
  if (!subUrl) { await tg("sendMessage",{chat_id:chatId,text:`${em("❌","cross")} Ошибка API.`,parse_mode:"HTML"}); return; }
  const exp = now()+days*86400000;
  db.transaction(()=>{
    db.prepare(`INSERT INTO subscriptions(tg_id,plan_code,plan_title,sub_url,expires_at,is_active,devices,created_at,updated_at)
      VALUES(?,?,?,?,?,1,1,?,?) ON CONFLICT(tg_id) DO UPDATE SET
      plan_code=excluded.plan_code,plan_title=excluded.plan_title,sub_url=excluded.sub_url,
      expires_at=excluded.expires_at,is_active=1,devices=1,updated_at=excluded.updated_at`)
      .run(Number(uid),"trial",fakeTariff.title,subUrl,exp,now(),now());
    markTrialUsed(uid);
  })();
  const lines = [
    `${em("✅","check")} <b>Пробный период активирован!</b>`,
    "",`Доступ открыт на <b>${days} дней</b>.`,
    "",`<code>${esc(subUrl)}</code>`,
  ];
  const kb = {inline_keyboard:[
    [btn("Подключить устройство",subUrl,"send",true)],
    [btn("Моя подписка","v:sub","lockOpen"),btn("Главное меню","v:home","home")],
  ]};
  const nm = await renderMsg(chatId,msgId,lines.join("\n"),kb);
  setMenu(uid,chatId,nm);
}

/* ═══════════════════════════════════════════════════════════════════════════
   PAYMENT FLOW
   ═══════════════════════════════════════════════════════════════════════════ */
function tariffTitle(t, lang) {
  if (lang==="ru") return t.title;
  return t.title.replace(/1\s*день/i,"1 day").replace(/(\d+)\s*дн/i,"$1 days")
    .replace(/30\s*дней/i,"30 days").replace(/90\s*дней/i,"90 days").replace(/365\s*дней/i,"365 days");
}

async function showPaymentOptions(uid, chatId, msgId, code, mode, promoCd="", promoPct=0, devices=3, recipientTgId=0) {
  if (code === "trial") {
    await render(uid, chatId, msgId, "buy");
    return;
  }

  const tr = tariff(code);
  if (!tr) return;
  const lang = getLang(uid);
  const devCount = Math.max(1,Math.min(10,Number(devices)||3));
  const finalPrice = calcFinalPrice(tr.price_rub, promoPct, devCount);
  const surcharge = devicesSurcharge(devCount);
  const refBal = getRefBalance(uid);
  const isGift = mode==="gift" && recipientTgId>0;

  const poId = createPendingOrder(uid, code, mode, promoCd, promoPct, devCount, recipientTgId, 0);

  const lines = [];
  if (isGift) {
    const to = user(recipientTgId);
    const toName = to?.first_name||(to?.username?`@${to.username}`:String(recipientTgId));
    lines.push(`${em("🎁","gift")} <b>Подарок</b>`);
    lines.push("",`Получатель: <b>${esc(toName)}</b>`);
  } else {
    lines.push(`${em("💳","wallet")} <b>Оплата подписки</b>`);
  }
  lines.push("",`Тариф: <b>${esc(tariffTitle(tr,lang))}</b>`);
  lines.push(`Сумма: <b>${rub(finalPrice)}</b>`);
  if (surcharge) lines.push(`<i>+${rub(surcharge)} за доп. устройства</i>`);
  if (promoPct) lines.push(`<i>Промокод: скидка ${promoPct}%</i>`);

  // Show info about days being added for renew
  if (mode === "renew") {
    const s = sub(uid);
    if (s && s.expires_at > now()) {
      const currentDaysLeft = Math.ceil((s.expires_at - now()) / 86400000);
      lines.push("",`${em("ℹ️","info")} <i>+${tr.duration_days} дн. к текущим ${currentDaysLeft} дн.</i>`);
    }
  }

  if (refBal > 0) lines.push("",`${em("💰","money")} Реферальный баланс: <b>${rub(refBal)}</b>`);
  lines.push("","Выберите способ оплаты:");

  const hasUsedAnyPromo = !!db.prepare("SELECT 1 FROM promo_uses WHERE tg_id=?").get(Number(uid));
  const rows = [];

  if (!promoCd && !hasUsedAnyPromo && !isGift) {
    rows.push([btn("Ввести промокод",`promo:ask:${code}:${mode}:${devCount}`,"tag")]);
  }

  if (refBal > 0 && finalPrice > 0) {
    if (refBal >= finalPrice) {
      rows.push([btn(`Оплатить с реф. баланса (${rub(finalPrice)})`,`pay:ref:${poId}`,"money")]);
    } else {
      rows.push([btn(`Списать ${rub(refBal)} с реф. баланса`,`pay:ref_partial:${poId}`,"money")]);
    }
  }

  if (CRYPTOBOT_TOKEN) rows.push([btn("Crypto Bot (USDT)",`direct:crypto:${poId}`,"crypto")]);
  if (isFkEnabled()) {
    rows.push([btn("СБП (QR)",`direct:fk:${poId}:44`,"wallet")]);
    rows.push([btn("Банковская карта РФ",`direct:fk:${poId}:36`,"wallet")]);
    rows.push([btn("SberPay",`direct:fk:${poId}:43`,"wallet")]);
  }
  rows.push([btn("Назад","v:buy","back")]);

  const nm = await renderMsg(chatId,msgId,lines.join("\n"),{inline_keyboard:rows});
  setMenu(uid,chatId,nm);
}

/* ═══════════════════════════════════════════════════════════════════════════
   USER HELPERS
   ═══════════════════════════════════════════════════════════════════════════ */
function upsertUser(from, chatId) {
  const cur = user(from.id);
  if (cur) {
    db.prepare("UPDATE users SET username=?,first_name=?,last_chat_id=?,updated_at=? WHERE tg_id=?")
      .run(from.username||"", from.first_name||"", Number(chatId), now(), Number(from.id));
  } else {
    const ref = crypto.randomBytes(5).toString("hex");
    db.prepare(`INSERT INTO users(tg_id,username,first_name,ref_balance_rub,referred_by,ref_code,ref_earned,payout_method,payout_details,last_chat_id,last_menu_id,lang,trial_used,created_at,updated_at)
      VALUES(?,?,?,0,NULL,?,0,'','',?,NULL,'ru',0,?,?)`)
      .run(Number(from.id), from.username||"", from.first_name||"", ref, Number(chatId), now(), now());
  }
}

function setMenu(uid,chatId,mid) { db.prepare("UPDATE users SET last_chat_id=?,last_menu_id=?,updated_at=? WHERE tg_id=?").run(Number(chatId),Number(mid),now(),Number(uid)); }
function findRef(code) { return db.prepare("SELECT * FROM users WHERE ref_code=?").get(String(code||"").trim()); }
function setRef(uid, rid) {
  const u = user(uid);
  if (!u || u.referred_by || Number(uid)===Number(rid)) return;
  const referrer = user(rid);
  if (referrer && Number(referrer.referred_by) === Number(uid)) return;
  db.prepare("UPDATE users SET referred_by=?,updated_at=? WHERE tg_id=?").run(Number(rid),now(),Number(uid));
}

/* ═══════════════════════════════════════════════════════════════════════════
   KEYBOARDS — FIXED: renew shows tariff list
   ═══════════════════════════════════════════════════════════════════════════ */
function homeKb(uid) {
  const s = sub(uid), act = activeSub(s);
  const rows = [];
  rows.push([
    btn(act?"Моя подписка":"Купить VPN", act?"v:sub":"v:buy", act?"lockOpen":"money"),
    btn("Подарить","v:gift","gift"),
  ]);
  rows.push([
    btn("Рефералы","v:ref","users"),
    btn("О нас","v:about","info"),
  ]);
  if (trialEnabled() && !hasUsedTrial(uid) && !act)
    rows.push([btn("Пробный период","trial:start","celebrate")]);
  if (isAdmin(uid)) rows.push([btn("Админ","a:main","settings")]);
  return {inline_keyboard:rows};
}

function buyKb(uid) {
  const lang = getLang(uid);
  const s = sub(uid), act = activeSub(s), trial = isTrialSub(uid);
  const rows = [];
  if (trialEnabled() && !hasUsedTrial(uid) && !act) {
    rows.push([btn(`Пробный период (${trialDays()} дней)`,"trial:start","celebrate")]);
  }
  if (!act || trial) {
    const ts = tariffs();
    for (let i=0; i<ts.length; i+=2) {
      const row = [btn(`${tariffTitle(ts[i],lang)} | ${rub(ts[i].price_rub)}`,`pay:n:${ts[i].code}`)];
      if (ts[i+1]) row.push(btn(`${tariffTitle(ts[i+1],lang)} | ${rub(ts[i+1].price_rub)}`,`pay:n:${ts[i+1].code}`));
      rows.push(row);
    }
  }
  rows.push([btn("Главное меню","v:home","home")]);
  return {inline_keyboard:rows};
}

// NEW: keyboard for renew — shows all tariffs to choose from
function renewKb(uid) {
  const lang = getLang(uid);
  const ts = tariffs();
  const rows = [];
  for (let i=0; i<ts.length; i+=2) {
    const row = [btn(`${tariffTitle(ts[i],lang)} | ${rub(ts[i].price_rub)}`,`pay:rw:${ts[i].code}`)];
    if (ts[i+1]) row.push(btn(`${tariffTitle(ts[i+1],lang)} | ${rub(ts[i+1].price_rub)}`,`pay:rw:${ts[i+1].code}`));
    rows.push(row);
  }
  rows.push([btn("Назад","v:sub","back")]);
  return {inline_keyboard:rows};
}

function refKb(uid) {
  const u = user(uid), isRu = getLang(uid)==="ru";
  const link = refLink(u.ref_code);
  const shareText = isRu?"Привет! Подключись к VPN по моей ссылке:":"Hey! Connect to VPN using my link:";
  const shareUrl = "https://t.me/share/url?url="+encodeURIComponent(link)+"&text="+encodeURIComponent(shareText);
  const refBal = getRefBalance(uid);
  const minW = Number(setting("ref_min_withdraw","3000"))||3000;
  const rows = [];
  rows.push([btn(isRu?"Пригласить друга":"Invite friend",shareUrl,"send",true)]);
  rows.push([
    btn(isRu?"История начислений":"Earnings","ref:hist:0","stats"),
    btn(isRu?"Сменить код":"Reset code","ref:r","loading"),
  ]);
  rows.push([btn(
    refBal>=minW ? (isRu?`Вывести ${rub(refBal)}`:`Withdraw ${rub(refBal)}`) : (isRu?`Вывод (от ${minW}₽)`:`Withdraw (min ${minW}₽)`),
    "ref:withdraw","money"
  )]);
  rows.push([btn(isRu?"Настроить реквизиты":"Set payout details","ref:setpay","settings")]);
  rows.push([btn(isRu?"Главное меню":"Main menu","v:home","home")]);
  return {inline_keyboard:rows};
}

function giftKb(uid) {
  const lang = getLang(uid);
  return {inline_keyboard:[
    ...tariffs().map(t=>[btn(`${tariffTitle(t,lang)} — ${rub(t.price_rub)}`,`g:p:${t.code}`,"gift")]),
    [btn("Главное меню","v:home","home")],
  ]};
}

function pagingKb(prefix, page, total, size, backTarget) {
  const max = Math.max(0,Math.ceil(total/size)-1), nav = [];
  if (page>0) nav.push(btn("←",`${prefix}:${page-1}`,"leftArrow"));
  nav.push({text:`${page+1}/${max+1}`,callback_data:"noop"});
  if (page<max) nav.push(btn("→",`${prefix}:${page+1}`,"rightArrow"));
  return {inline_keyboard:[nav,[btn("Назад",backTarget,"back")]]};
}

/* ═══════════════════════════════════════════════════════════════════════════
   TEXT BUILDERS
   ═══════════════════════════════════════════════════════════════════════════ */
function homeText(u) {
  const s = sub(u.tg_id), hasSub = activeSub(s);
  const lines = [`${em("🐸","frog")} Добро пожаловать, ${esc(u.first_name||String(u.tg_id))}`];
  const refBal = getRefBalance(u.tg_id);
  if (refBal > 0) lines.push(`${em("💰","money")} Реф. баланс: <b>${rub(refBal)}</b>`);
  if (hasSub) {
    const dd = Math.floor(Math.max(0,s.expires_at-now())/86400000);
    lines.push(`${em("✅","check")} Подписка активна — ${dd} дн.`);
  }
  lines.push("",`${em("📣","megaphone")} @DreinnVPN`,`${em("💬","bot")} @DreinnVPNSupportBot`);
  return lines.join("\n");
}

function subText(uid) {
  const s = sub(uid), lang = getLang(uid), isRu = lang==="ru";
  if (!activeSub(s)) return [
    `<b>${isRu?"Моя подписка":"My Subscription"}</b>`,
    "",isRu?"Активная подписка не найдена.":"No active subscription.",
    "",isRu?"<i>Оформите тариф в разделе «Купить VPN».</i>":"<i>Choose a plan in «Buy VPN».</i>",
  ].join("\n");
  const ms = Math.max(0,s.expires_at-now());
  const dd = Math.floor(ms/86400000), hh = Math.floor((ms%86400000)/3600000), mm = Math.floor((ms%3600000)/60000);
  return [
    `${em("⚡️","lightning")} <b>${isRu?"Подключение":"Connection"}</b>`,
    "",
    isRu
      ?`${em("❗","cross")} Для использования VPN установите приложение из раздела «Инструкция».\n\nПосле установки используйте кнопку ниже или отсканируйте QR-код:`
      :`${em("❗","cross")} Install the app from the Guide section.\n\nThen use the button below or scan the QR code:`,
    "",
    `Тариф: <b>${esc(s.plan_title||s.plan_code||"—")}</b>`,
    `Истекает: <b>${dt(s.expires_at,lang)}</b>`,
    `Осталось: <b>${dd} дн. ${hh} ч. ${mm} мин.</b>`,
  ].join("\n");
}

function buyText(uid) {
  const s = sub(uid), act = activeSub(s), trial = isTrialSub(uid), isRu = getLang(uid)==="ru";
  const lines = [isRu?"Выберите тариф и способ оплаты.":"Choose a plan and payment method."];
  if (trial) lines.push("",isRu?`<i>${em("✅","check")} Пробный период активен. Новый тариф заменит его.</i>`:`<i>${em("✅","check")} Trial active. New plan will replace it.</i>`);
  else if (act) lines.push("",isRu?`<i>${em("⚠️","warning")} Подписка активна. Используйте «Продлить» для продления.</i>`:`<i>${em("⚠️","warning")} Subscription active. Use «Renew» to extend.</i>`);
  return lines.join("\n");
}

// NEW: text for renew page
function renewText(uid) {
  const s = sub(uid), isRu = getLang(uid)==="ru";
  const lines = [];
  lines.push(`${em("🔄","loading")} <b>${isRu?"Продление подписки":"Renew subscription"}</b>`);
  lines.push("");
  if (s && s.expires_at > now()) {
    const daysLeft = Math.ceil((s.expires_at - now()) / 86400000);
    lines.push(isRu
      ? `Текущий тариф: <b>${esc(s.plan_title||"—")}</b>`
      : `Current plan: <b>${esc(s.plan_title||"—")}</b>`);
    lines.push(isRu
      ? `Осталось: <b>${daysLeft} дн.</b>`
      : `Days left: <b>${daysLeft} days</b>`);
    lines.push("");
    lines.push(isRu
      ? `${em("ℹ️","info")} Дни выбранного тарифа <b>прибавятся</b> к текущим.`
      : `${em("ℹ️","info")} Days will be <b>added</b> to your current subscription.`);
  } else {
    lines.push(isRu?"Подписка истекла. Выберите новый тариф:":"Subscription expired. Choose a new plan:");
  }
  lines.push("");
  lines.push(isRu?"Выберите тариф:":"Choose a plan:");
  return lines.join("\n");
}

function refText(uid) {
  const u = user(uid), isRu = getLang(uid)==="ru";
  const st = db.prepare("SELECT COUNT(*) c FROM referrals WHERE referrer_tg_id=?").get(Number(uid));
  const pct = Number(setting("ref_percent","30"))||30;
  const minW = Number(setting("ref_min_withdraw","3000"))||3000;
  const refBal = getRefBalance(uid);
  const link = refLink(u.ref_code);
  const payMethod = u.payout_method||"";
  const payDetails = u.payout_details||"";
  return [
    `<b>${em("🤝","users")} ${isRu?"Партнёрская программа":"Partner program"}</b>`,
    "",
    isRu
      ?`Приглашайте друзей и получайте <b>${pct}%</b> с каждой их покупки.`
      :`Invite friends and earn <b>${pct}%</b> of every purchase.`,
    "",
    `<blockquote><code>${link}</code></blockquote>`,
    isRu?"<i>(Нажмите, чтобы скопировать)</i>":"<i>(Tap to copy)</i>",
    "",
    `<b>${em("📊","stats")} ${isRu?"Статистика":"Stats"}:</b>`,
    isRu?`Приглашено: <b>${st.c||0}</b>`:`Invited: <b>${st.c||0}</b>`,
    isRu?`Баланс: <b>${rub(refBal)}</b>`:`Balance: <b>${rub(refBal)}</b>`,
    isRu?`Заработано всего: <b>${rub(u.ref_earned||0)}</b>`:`Total earned: <b>${rub(u.ref_earned||0)}</b>`,
    "",
    isRu?`Способ вывода: <b>${payMethod||"не задан"}</b>`:`Payout: <b>${payMethod||"not set"}</b>`,
    isRu?`Реквизиты: <b>${payDetails||"не указаны"}</b>`:`Details: <b>${payDetails||"not set"}</b>`,
    "",
    isRu?`${em("ℹ️","info")} Вывод от <b>${minW}₽</b>. Баланс можно использовать для покупки тарифов.`
        :`${em("ℹ️","info")} Withdraw from <b>${minW}₽</b>. Balance can be used for purchases.`,
  ].join("\n");
}

function aboutText(uid) {
  const isRu = getLang(uid)==="ru";
  return [
    `<b>${em("🌐","link")} ${isRu?"О сервисе Dreinn VPN":"About Dreinn VPN"}</b>`,
    "",
    `<b>${em("⚡️","lightning")} ${isRu?"Возможности":"Features"}</b>`,
    isRu?"• Выбор стран подключения":"• Choose connection country",
    isRu?"• Быстрое и стабильное соединение":"• Fast and stable connection",
    isRu?"• Работает с любыми приложениями":"• Works with all apps",
    isRu?"• Поддержка всех устройств":"• All devices supported",
    "",
    `<b>${em("🧭","compass")} ${isRu?"Умная маршрутизация":"Smart routing"}</b>`,
    isRu?"Локальные сервисы работают через соответствующий регион. Остальное — через выбранную страну."
        :"Local services route through the appropriate region. Everything else — through your chosen country.",
    "",
    `<b>${em("🛡","lockClosed")} ${isRu?"Конфиденциальность":"Privacy"}</b>`,
    isRu?"Не сохраняем историю действий. Данные о подключениях удаляются автоматически."
        :"No activity logs. Connection data is automatically deleted.",
  ].join("\n");
}

function purchasesText(uid, page=0) {
  const size=5, off=page*size;
  const rows = db.prepare("SELECT * FROM purchases WHERE tg_id=? ORDER BY created_at DESC LIMIT ? OFFSET ?").all(Number(uid),size,off);
  const total = Number(db.prepare("SELECT COUNT(*) c FROM purchases WHERE tg_id=?").get(Number(uid)).c||0);
  if (!rows.length) return {text:`<b>${em("📅","calendar")} История покупок</b>\n\nПокупок пока нет.`,total,page,size};
  const lines = [`<b>${em("📅","calendar")} История покупок</b>`,""]; 
  for (const p of rows) {
    const icon = p.kind==="gift"?em("🎁","gift"):p.kind==="renew"?em("🔄","loading"):em("💳","wallet");
    lines.push(`${icon} <b>${esc(p.tariff_title)}</b> — ${rub(p.amount_rub)}`);
    lines.push(`   <i>${dt(p.created_at)}</i>`);
  }
  lines.push("",`Стр. ${page+1}/${Math.max(1,Math.ceil(total/size))}`);
  return {text:lines.join("\n"),total,page,size};
}

function refHistoryText(uid, page=0) {
  const size=5, off=page*size;
  const rows = db.prepare("SELECT * FROM referrals WHERE referrer_tg_id=? ORDER BY created_at DESC LIMIT ? OFFSET ?").all(Number(uid),size,off);
  const total = Number(db.prepare("SELECT COUNT(*) c FROM referrals WHERE referrer_tg_id=?").get(Number(uid)).c||0);
  if (!rows.length) return {text:`<b>${em("📊","stats")} История начислений</b>\n\nНачислений пока нет.`,total,page,size};
  const lines = [`<b>${em("📊","stats")} История начислений</b>`,""]; 
  for (const r of rows) {
    lines.push(`${em("💰","money")} +<b>${rub(r.reward_rub)}</b> <i>(${r.percent}% от ${rub(r.amount_rub)})</i>`);
    lines.push(`   <i>${dt(r.created_at)}</i>`);
  }
  lines.push("",`Стр. ${page+1}/${Math.max(1,Math.ceil(total/size))}`);
  return {text:lines.join("\n"),total,page,size};
}

/* ═══════════════════════════════════════════════════════════════════════════
   ADMIN TEXT
   ═══════════════════════════════════════════════════════════════════════════ */
function adminStatsText() {
  const uCount = Number(db.prepare("SELECT COUNT(*) c FROM users").get().c);
  const aCount = Number(db.prepare("SELECT COUNT(*) c FROM subscriptions WHERE is_active=1 AND expires_at>?").get(now()).c);
  const revenue = Number(db.prepare("SELECT COALESCE(SUM(amount_rub),0) s FROM purchases").get().s||0);
  const today = new Date(); today.setHours(0,0,0,0); const todayTs = today.getTime();
  const newDay = Number(db.prepare("SELECT COUNT(*) c FROM users WHERE created_at>=?").get(todayTs).c);
  const revDay = Number(db.prepare("SELECT COALESCE(SUM(amount_rub),0) s FROM purchases WHERE created_at>=?").get(todayTs).s||0);
  const refPaid = Number(db.prepare("SELECT COALESCE(SUM(reward_rub),0) s FROM referrals").get().s||0);
  return [
    `<b>${em("📊","stats")} Статистика</b>`,
    "",
    `${em("👥","users")} Пользователей: <b>${uCount}</b> (+${newDay} сегодня)`,
    `${em("🔓","lockOpen")} Активных подписок: <b>${aCount}</b>`,
    `${em("💰","money")} Сегодня: <b>${rub(revDay)}</b>`,
    `${em("📊","stats")} Всего: <b>${rub(revenue)}</b>`,
    `${em("👥","users")} Реф. начислено: <b>${rub(refPaid)}</b>`,
  ].join("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════
   SUB WITH QR — FIXED: renew button goes to tariff selection
   ═══════════════════════════════════════════════════════════════════════════ */
async function renderSubWithQR(uid, chatId, msgId) {
  let s = sub(uid);
  const lang = getLang(uid), isRu = lang==="ru";

  // Sync the authoritative expiry date from the API before displaying.
  // The API's KV store is the source of truth; the local SQLite may lag
  // (e.g. after an admin renewal or extension via the web panel).
  const _token = extractTokenFromSubUrl(s.sub_url);
  if (_token) {
    const info = await fetchSubInfo(_token).catch(() => ({ valid: true, expiresAt: null }));
    if (info.expiresAt && Math.abs(info.expiresAt - s.expires_at) > 60000) {
      db.prepare("UPDATE subscriptions SET expires_at=?, updated_at=? WHERE tg_id=?")
        .run(info.expiresAt, now(), Number(uid));
      s = sub(uid); // re-read with fresh value
    }
  }

  const ms = Math.max(0,s.expires_at-now());
  const dd = Math.floor(ms/86400000), hh = Math.floor((ms%86400000)/3600000), mm = Math.floor((ms%3600000)/60000);
  const caption = [
    `${em("⚡️","lightning")} <b>${isRu?"Подключение":"Connection"}</b>`,
    "",
    isRu?`${em("❗","cross")} Установите приложение из раздела «Инструкция», затем используйте кнопку или QR-код.`
        :`${em("❗","cross")} Install the app from Guide, then use button or QR code.`,
    "",
    `Тариф: <b>${esc(s.plan_title||"—")}</b>`,
    `Истекает: <b>${dt(s.expires_at,lang)}</b>`,
    `Осталось: <b>${dd} дн. ${hh} ч. ${mm} мин.</b>`,
  ].join("\n");

  const isTrial = s.plan_code === "trial";
  // FIXED: renew always goes to tariff selection page
  const renewCb = "v:renew";

  const kb = {inline_keyboard:[
    [btn("Инструкции","v:guide","write"),btn("Подключить",s.sub_url,"send",true)],
    [btn(isTrial ? "Купить подписку" : "Продлить", isTrial ? "v:buy" : renewCb, isTrial ? "money" : "loading")],
    [btn("Главное меню","v:home","home")],
  ]};
  try {
    const buf = await QRCode.toBuffer(s.sub_url,{width:512,margin:2,errorCorrectionLevel:"M"});
    if (msgId) delMsg(chatId,msgId);
    const m = await sendPhotoBuffer(chatId,buf,"image/png",caption,kb);
    setMenu(uid,chatId,m.message_id);
  } catch(e) {
    console.error("[subQR]",e.message);
    const nm = await renderMsg(chatId,msgId,subText(uid),{inline_keyboard:[
      [btn("Инструкции","v:guide","write"),btn("Подключить",s.sub_url,"send",true)],
      [btn(isTrial ? "Купить подписку" : "Продлить", isTrial ? "v:buy" : renewCb, isTrial ? "money" : "loading")],
      [btn("Главное меню","v:home","home")],
    ]});
    setMenu(uid,chatId,nm);
  }
}

/* ═══════════════════════════════════════════════════════════════════════════
   ADMIN GRANT
   ═══════════════════════════════════════════════════════════════════════════ */
async function adminGrantSub(adminId, targetId, tariffCode) {
  const tr = tariff(tariffCode), tu = user(targetId);
  if (!tr||!tu) throw new Error("Not found");
  const api = await createSubViaApi(tu, tr, false);
  const subUrl = api.subscriptionUrl||api.sub_url||"";
  if (!subUrl) throw new Error("API error");
  const exp = now()+tr.duration_days*86400000;
  db.transaction(()=>{
    db.prepare(`INSERT INTO subscriptions(tg_id,plan_code,plan_title,sub_url,expires_at,is_active,devices,created_at,updated_at)
      VALUES(?,?,?,?,?,1,3,?,?) ON CONFLICT(tg_id) DO UPDATE SET
      plan_code=excluded.plan_code,plan_title=excluded.plan_title,sub_url=excluded.sub_url,
      expires_at=excluded.expires_at,is_active=1,devices=excluded.devices,updated_at=excluded.updated_at`)
      .run(Number(targetId),tr.code,tr.title,subUrl,exp,now(),now());
    db.prepare("DELETE FROM notified_expiry WHERE tg_id=?").run(Number(targetId));
  })();
  tg("sendMessage",{chat_id:targetId,text:[
    `${em("🎁","gift")} <b>Вам выдана подписка!</b>`,
    "",`Тариф: <b>${esc(tr.title)}</b>`,`Доступ на <b>${tr.duration_days} дн.</b>`,
    "",`<code>${esc(subUrl)}</code>`,
  ].join("\n"),parse_mode:"HTML",reply_markup:{inline_keyboard:[[btn("Подключить",subUrl,"send",true)]]}}).catch(()=>{});
  return {name:tu.first_name||(tu.username?`@${tu.username}`:String(targetId)), plan:tr.title};
}

/* ═══════════════════════════════════════════════════════════════════════════
   RENDER — FIXED: added "renew" view
   ═══════════════════════════════════════════════════════════════════════════ */
async function render(uid, chatId, msgId, view, data={}) {
  const u = user(uid); if (!u) return;
  let text="", kb={};

  switch(view) {
    case "home":
      text=homeText(u); kb=homeKb(uid); break;
    case "sub": {
      const _s = sub(uid);
      if (activeSub(_s)) {
        // On-demand check: verify the subscription still exists in the API
        // and sync the authoritative expiry date in one request.
        const _token = extractTokenFromSubUrl(_s.sub_url);
        const _info  = _token ? await fetchSubInfo(_token) : { valid: true, expiresAt: null };
        if (!_info.valid) {
          // Deleted from admin panel — deactivate locally without a separate notification
          // since we're already in the user's chat and will show them the "no sub" UI.
          await deactivateLocalSub(uid, false);
          text = [
            `${em("❌","cross")} <b>Подписка удалена</b>`,
            "",
            "Ваша подписка была удалена администратором.",
            "Вы можете оформить новую подписку.",
          ].join("\n");
          kb = { inline_keyboard: [
            [btn("Купить VPN", "v:buy", "money")],
            [btn("Главное меню", "v:home", "home")],
          ]};
        } else {
          // Sync expiry from API if it differs (admin renewal, extension, etc.)
          if (_info.expiresAt && Math.abs(_info.expiresAt - _s.expires_at) > 60000) {
            db.prepare("UPDATE subscriptions SET expires_at=?, updated_at=? WHERE tg_id=?")
              .run(_info.expiresAt, now(), Number(uid));
          }
          await renderSubWithQR(uid, chatId, msgId);
          return;
        }
      } else {
        text = subText(uid);
        kb = { inline_keyboard: [[btn("Купить подписку","v:buy","money")],[btn("Главное меню","v:home","home")]] };
      }
      break;
    }
    case "buy":
      text=buyText(uid); kb=buyKb(uid); break;
    // NEW: renew view — shows tariff list for renewal
    case "renew":
      text=renewText(uid); kb=renewKb(uid); break;
    case "guide": {
      const lang = getLang(uid);
      const rawGuide = lang==="en" ? (setting("guide_text_en","")||setting("guide_text","")) : setting("guide_text","");
      text = rawGuide ? parseLinks(rawGuide) : "<b>Инструкция</b>\n\n<i>Не настроена.</i>";
      const kbRows = [];
      if (lnk.support()) kbRows.push([btn("Поддержка",lnk.support(),"bot",true)]);
      kbRows.push([btn("Главное меню","v:home","home")]);
      kb = {inline_keyboard:kbRows};
      break;
    }
    case "about": {
      text = aboutText(uid);
      const isRu = getLang(uid)==="ru";
      const kbRows = [];
      const privacyUrl = lnk.privacy();
      const termsUrl = lnk.terms();
      if (privacyUrl && termsUrl) {
        kbRows.push([
          btn(isRu?"Политика":"Privacy Policy", privacyUrl, "lockClosed", true),
          btn(isRu?"Соглашение":"Terms of Service", termsUrl, "file", true),
        ]);
      } else {
        if (privacyUrl) kbRows.push([btn(isRu?"Политика конфиденциальности":"Privacy Policy", privacyUrl, "lockClosed", true)]);
        if (termsUrl) kbRows.push([btn(isRu?"Соглашение":"Terms of Service", termsUrl, "file", true)]);
      }
      if (lnk.support()) kbRows.push([btn(isRu?"Поддержка":"Support", lnk.support(), "bot", true)]);
      if (lnk.status()) kbRows.push([btn(isRu?"Статус серверов":"Server Status", lnk.status(), "check", true)]);
      if (lnk.news()) kbRows.push([btn(isRu?"Новости":"News", lnk.news(), "megaphone", true)]);
      kbRows.push([btn(isRu?"Главное меню":"Main menu", "v:home", "home")]);
      kb = {inline_keyboard:kbRows};
      break;
    }
    case "ref":
      text=refText(uid); kb=refKb(uid); break;
    case "gift":
      text=`${em("🎁","gift")} <b>Подарить подписку</b>\n\nВыберите тариф:`;
      kb=giftKb(uid); break;
    case "purchases": {
      const {text:ht,total,size}=purchasesText(uid,Number(data.page||0));
      text=ht; kb=pagingKb("ph",Number(data.page||0),total,size,"v:home"); break;
    }
    case "ref_hist": {
      const {text:ht,total,size}=refHistoryText(uid,Number(data.page||0));
      text=ht; kb=pagingKb("ref:hist",Number(data.page||0),total,size,"v:ref"); break;
    }

    // ── ADMIN ──
    case "a_main":
      text=adminStatsText();
      kb={inline_keyboard:[
        [btn("Тарифы","a:t","money"),btn("GIF","a:g","media")],
        [btn("Рассылка","a:b","megaphone"),btn("Ссылки","a:links","link")],
        [btn("Реф. %","a:r","users"),btn("Инструкция","a:guide_edit","write")],
        [btn("Канал + Пробный","a:channel","notification")],
        [btn("FreeKassa","a:fk","wallet")],
        [btn("Заявки на вывод","a:withdrawals:0","download")],
        [btn("Промокоды","a:promo","tag"),btn("Поиск юзера","a:find","eye")],
        [btn("Пользователи","a:users:0","users"),btn("База данных","a:db","box")],
        [btn("Назад","v:home","back")],
      ]}; break;

    case "a_guide_edit":
      text=[
        `<b>${em("✍","write")} Редактирование инструкции</b>`,
        "",
        `<i>Формат ссылок:</i> <code>[Название|URL]</code>`,
        `<i>Пример:</i> <code>[Скачать Happ|https://happ.su]</code>`,
        "",
        `<b>Текущий текст (RU):</b>`,
        `<blockquote>${esc(setting("guide_text","")).slice(0,300)||"не задан"}</blockquote>`,
        "",
        `<b>Current text (EN):</b>`,
        `<blockquote>${esc(setting("guide_text_en","")).slice(0,300)||"not set"}</blockquote>`,
        "",
        `<b>Предпросмотр (RU):</b>`,
        parseLinks(setting("guide_text","")).slice(0,500)||"<i>пусто</i>",
      ].join("\n");
      kb={inline_keyboard:[
        [btn("Изменить Русский","a:guide_ru","write")],
        [btn("Edit English","a:guide_en","write")],
        [btn("Назад","a:main","back")],
      ]}; break;

    case "a_tariffs":
      text=`<b>${em("💰","money")} Тарифы</b>\n\n${tariffs().map(x=>`${x.title}: <b>${rub(x.price_rub)}</b>`).join("\n")}\n\n<i>Доп. устройства (от 4+): +${rub(devicesExtraPrice())} за каждое</i>`;
      kb={inline_keyboard:[
        ...tariffs().map(x=>[btn(`${x.title} — ${rub(x.price_rub)}`,`a:te:${x.code}`,"pencil")]),
        [btn(`Доп. устройство — ${rub(devicesExtraPrice())}`,"a:dev_price","settings")],
        [btn("Назад","a:main","back")],
      ]}; break;

    case "a_gif":
      text=`<b>${em("🖼","media")} GIF-анимации</b>`;
      kb={inline_keyboard:[
        [btn(`Главная${setting("gif_main_menu")?" ✓":""}`,"a:ge:gif_main_menu","home")],
        [btn(`Покупка${setting("gif_purchase_success")?" ✓":""}`,"a:ge:gif_purchase_success","money")],
        [btn(`Подарок${setting("gif_gift_success")?" ✓":""}`,"a:ge:gif_gift_success","gift")],
        [btn(`Рассылка${setting("gif_broadcast")?" ✓":""}`,"a:ge:gif_broadcast","megaphone")],
        [btn("Назад","a:main","back")],
      ]}; break;

    case "a_links": {
      const linkRows = [
        ["url_support","Поддержка",lnk.support()],
        ["url_privacy","Политика конф.",lnk.privacy()],
        ["url_terms","Соглашение",lnk.terms()],
        ["url_proxy","Прокси",lnk.proxy()],
        ["url_news","Канал",lnk.news()],
        ["url_status","Статус серверов",lnk.status()],
      ];
      const lines = [`<b>${em("🔗","link")} Ссылки</b>`,""]; 
      linkRows.forEach(([,label,val])=>lines.push(`${val?em("✅","check"):em("⬜","hidden")} ${label}: ${val?`<code>${esc(val)}</code>`:"<i>—</i>"}`));
      text=lines.join("\n");
      kb={inline_keyboard:[
        ...linkRows.map(([k,label])=>[btn(label,`a:lnk:${k}`,"pencil")]),
        [btn("Назад","a:main","back")],
      ]}; break;
    }

    case "a_bcast":
      text=[
        `<b>${em("📣","megaphone")} Рассылка</b>`,
        "",
        "Отправьте сообщение: текст, фото, видео, GIF, документ.",
        "<b>Premium эмодзи сохраняются автоматически.</b>",
        "",
        "Вы сможете:",
        "• Выбрать аудиторию (все / с подпиской / без)",
        "• Добавить инлайн-кнопку",
      ].join("\n");
      kb={inline_keyboard:[
        [btn("Создать рассылку","a:bs","pencil")],
        [btn("Назад","a:main","back")],
      ]}; break;

    case "a_ref":
      text=[
        `<b>${em("👥","users")} Реферальная программа</b>`,
        "",`Ставка: <b>${setting("ref_percent","30")}%</b>`,
        `Мин. вывод: <b>${setting("ref_min_withdraw","3000")}₽</b>`,
      ].join("\n");
      kb={inline_keyboard:[
        [btn("Изменить ставку","a:rp","write"),btn("Мин. вывод","a:rmin","write")],
        [btn("Заявки на вывод","a:withdrawals:0","money")],
        [btn("Назад","a:main","back")],
      ]}; break;

    case "a_channel": {
      const chanId = setting("channel_id","")||"не задан";
      const tEnabled = trialEnabled();
      const subReq = isSubscriptionRequired();
      text=[
        `<b>${em("📣","megaphone")} Канал + Пробный период</b>`,
        "",`Канал: <code>${esc(chanId)}</code>`,
        `Ссылка: ${esc(setting("channel_invite_url","")||"не задана")}`,
        `Обязательная подписка: <b>${subReq?"вкл":"выкл"}</b>`,
        "",`Пробный: <b>${tEnabled?"вкл":"выкл"}</b> (${trialDays()} дн.)`,
      ].join("\n");
      kb={inline_keyboard:[
        [btn("Установить канал","a:chan_id","write")],
        [btn("Ссылка-приглашение","a:chan_url","link")],
        [btn(subReq?"Отключить обяз. подписку":"Включить обяз. подписку","a:sub_req_toggle",subReq?"cross":"check")],
        [btn(tEnabled?"Отключить пробный":"Включить пробный","a:trial_toggle",tEnabled?"cross":"check")],
        [btn(`Длительность: ${trialDays()} дн.`,"a:trial_days","clock")],
        [btn("Назад","a:main","back")],
      ]}; break;
    }

    case "a_fk": {
      const sid=fkShopId(), min=fkMinRub(), p_=fkNotifyPath(), ip=fkServerIp();
      text=[
        `<b>${em("💳","wallet")} FreeKassa</b>`,
        "",`Статус: <b>${isFkEnabled()?"вкл":"выкл"}</b>`,
        `shop_id: <code>${sid||"—"}</code>`,
        `min: <code>${min}</code>`,
        `path: <code>${esc(p_)}</code>`,
        `ip: <code>${esc(ip||"—")}</code>`,
      ].join("\n");
      kb={inline_keyboard:[
        [btn("shop_id","a:fk_shop","pencil")],
        [btn("min amount","a:fk_min","pencil")],
        [btn("webhook path","a:fk_path","pencil")],
        [btn("Назад","a:main","back")],
      ]}; break;
    }

    case "a_promo": {
      const promos = db.prepare("SELECT * FROM promo_codes ORDER BY rowid DESC LIMIT 20").all();
      const lines = [`<b>${em("🏷","tag")} Промокоды</b>`,""]; 
      if (!promos.length) lines.push("<i>Нет промокодов.</i>");
      else promos.forEach(p=>{
        const st = p.is_active?em("✅","check"):em("❌","cross");
        const uses = p.uses_max>0?`${p.uses_current}/${p.uses_max}`:`${p.uses_current}/∞`;
        lines.push(`${st} <code>${esc(p.code)}</code> — <b>${p.discount_pct}%</b> (${uses})`);
      });
      text=lines.join("\n");
      kb={inline_keyboard:[
        [btn("Добавить","a:promo_add","check")],
        [btn("Деактивировать","a:promo_del","cross")],
        [btn("Назад","a:main","back")],
      ]}; break;
    }

    case "a_users": {
      const page=Number(data.page||0), size=10, off=page*size;
      const rows=db.prepare("SELECT u.*,(SELECT is_active FROM subscriptions s WHERE s.tg_id=u.tg_id AND s.is_active=1) as sub_active FROM users u ORDER BY u.created_at DESC LIMIT ? OFFSET ?").all(size,off);
      const total=Number(db.prepare("SELECT COUNT(*) c FROM users").get().c||0);
      const lines=[`<b>${em("👥","users")} Пользователи</b>`,""]; 
      rows.forEach(u2=>{
        lines.push(`${u2.sub_active?em("⭐","celebrate"):""} <code>${u2.tg_id}</code> ${esc(u2.first_name||"")}${u2.username?` @${esc(u2.username)}`:""}`);
      });
      text=lines.join("\n");
      const nav=[];
      if(page>0) nav.push(btn("◀",`a:users:${page-1}`,"leftChevron"));
      nav.push({text:`${page+1}/${Math.ceil(total/size)||1}`,callback_data:"noop"});
      if((page+1)*size<total) nav.push(btn("▶",`a:users:${page+1}`,"rightChevron"));
      kb={inline_keyboard:[nav,[btn("Назад","a:main","back")]]}; break;
    }

    case "a_user_info": {
      const tu=user(data.id);
      if(!tu){text="Не найден.";kb={inline_keyboard:[[btn("Назад","a:main","back")]]};break;}
      const ts=sub(tu.tg_id), hasSub=activeSub(ts);
      const pCount=Number(db.prepare("SELECT COUNT(*) c FROM purchases WHERE tg_id=?").get(tu.tg_id).c||0);
      text=[
        `<b>${em("👤","profile")} Пользователь</b>`,
        "",`ID: <code>${tu.tg_id}</code>`,
        `Имя: ${esc(tu.first_name)}`,
        `Username: ${tu.username?`@${esc(tu.username)}`:"—"}`,
        `Реф. баланс: <b>${rub(tu.ref_balance_rub||0)}</b>`,
        `Реф. заработано: <b>${rub(tu.ref_earned||0)}</b>`,
        `Покупок: <b>${pCount}</b>`,
        `Подписка: ${hasSub?`<b>активна</b> до ${dt(ts.expires_at)}`:"нет"}`,
      ].join("\n");
      kb={inline_keyboard:[
        [btn("Выдать подписку",`a:grant:${tu.tg_id}`,"gift")],
        ...(hasSub?[[btn("Отобрать подписку",`a:sub_revoke:${tu.tg_id}`,"cross")]]:[]),
        [btn("Сбросить пробный",`a:trial_reset:${tu.tg_id}`,"loading")],
        [btn("Пополнить реф. баланс",`a:ref_add:${tu.tg_id}`,"money")],
        [btn("Назад","a:main","back")],
      ]}; break;
    }

    case "a_db":
      text=`<b>${em("📦","box")} База данных</b>`;
      kb={inline_keyboard:[
        [btn("Скачать","a:db_export","download")],
        [btn("Импорт","a:db_import_start","download")],
        [btn("Назад","a:main","back")],
      ]}; break;

    case "a_withdrawals": {
      const wPage=Number(data?.page||0), wSize=10, wOff=wPage*wSize;
      const wRows=db.prepare("SELECT w.*,u.first_name,u.username FROM withdrawal_requests w LEFT JOIN users u ON u.tg_id=w.tg_id WHERE w.status='pending' ORDER BY w.created_at DESC LIMIT ? OFFSET ?").all(wSize,wOff);
      const wTotal=Number(db.prepare("SELECT COUNT(*) c FROM withdrawal_requests WHERE status='pending'").get().c||0);
      const wLines=[`<b>${em("💸","sendMoney")} Заявки на вывод</b>`,""]; 
      if(!wRows.length) wLines.push("<i>Нет заявок.</i>");
      else wRows.forEach((w,i)=>{
        wLines.push(`${wOff+i+1}. <b>${esc(w.first_name||String(w.tg_id))}</b> (<code>${w.tg_id}</code>)`);
        wLines.push(`   ${rub(w.amount_rub)} | ${esc(w.method)}: ${esc(w.details)}`);
      });
      text=wLines.join("\n");
      const wKbRows=[];
      wRows.forEach(w=>{
        wKbRows.push([
          btn(`#${w.id} Выплачено`,`a:wd_done:${w.id}:${wPage}`,"check"),
          btn(`#${w.id} Отклонить`,`a:wd_reject:${w.id}:${wPage}`,"cross"),
        ]);
      });
      wKbRows.push([btn("Назад","a:main","back")]);
      kb={inline_keyboard:wKbRows}; break;
    }

    case "a_grant": {
      const targetId=Number(data.id||0);
      text=`<b>${em("🎁","gift")} Выдать подписку</b>\n\nID: <code>${targetId}</code>\n\nВыберите тариф:`;
      kb={inline_keyboard:[
        ...tariffs().map(t=>[btn(`${t.title} (${t.duration_days} дн.)`,`a:grant_ok:${targetId}:${t.code}`,"gift")]),
        [btn("Назад",`a:user_back:${targetId}`,"back")],
      ]}; break;
    }

    default:
      text=homeText(u); kb=homeKb(uid);
  }

  const nm = await renderMsg(chatId,msgId,text,kb);
  setMenu(uid,chatId,nm);
}

/* ═══════════════════════════════════════════════════════════════════════════
   ADMIN STATE HANDLER
   ═══════════════════════════════════════════════════════════════════════════ */
async function handleAdminState(msg) {
  const aid=Number(msg.from?.id||0); if(!isAdmin(aid)) return false;
  const row=getAdminState(aid); if(!row) return false;
  const text=String(msg.text||"").trim(), chatId=Number(msg.chat?.id||0);

  if(row.state!=="broadcast"&&row.state!=="broadcast_preview") delMsg(chatId,Number(msg.message_id||0));
  if(text==="/cancel"){clearAdminState(aid);await render(aid,chatId,user(aid)?.last_menu_id||null,"a_main");return true;}

  switch(row.state) {
    case "tariff_price": {
      const n=Number(text); if(!Number.isFinite(n)||n<=0){await tg("sendMessage",{chat_id:chatId,text:"Введите цену > 0."});return true;}
      db.prepare("UPDATE tariffs SET price_rub=? WHERE code=?").run(Math.round(n),row.payload);
      clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:`✅ Цена: ${rub(Math.round(n))}`,parse_mode:"HTML"});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_tariffs"); return true;
    }
    case "dev_extra_price": {
      const n=Number(text); if(!Number.isFinite(n)||n<0) return true;
      setSetting("devices_extra_price",String(Math.round(n))); clearAdminState(aid);
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_tariffs"); return true;
    }
    case "gif": {
      const v=msg.animation?.file_id||msg.video?.file_id||text; if(!v) return true;
      setSetting(row.payload,v); clearAdminState(aid);
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_gif"); return true;
    }
    case "edit_link": {
      const val=text.trim();
      if(val==="-"||val==="") delSetting(row.payload); else setSetting(row.payload,val);
      clearAdminState(aid);
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_links"); return true;
    }
    case "guide_text": {
      setSetting("guide_text",text); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:`✅ Инструкция (RU) обновлена.\n\n<b>Предпросмотр:</b>\n${parseLinks(text)}`,parse_mode:"HTML",disable_web_page_preview:true});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_guide_edit"); return true;
    }
    case "guide_text_en": {
      setSetting("guide_text_en",text); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:`✅ Guide (EN) updated.\n\n<b>Preview:</b>\n${parseLinks(text)}`,parse_mode:"HTML",disable_web_page_preview:true});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_guide_edit"); return true;
    }
    case "broadcast": {
      const hasContent = !!(msg.text||msg.photo||msg.video||msg.document||msg.animation||msg.voice||msg.audio||msg.sticker||msg.video_note);
      if(!hasContent) return true;
      clearAdminState(aid);
      const msgMeta={chat_id:Number(chatId),message_id:Number(msg.message_id)};
      setAdminState(aid,"broadcast_settings",JSON.stringify({msg_meta:msgMeta,filter:"all",button:null}));
      await tg("sendMessage",{chat_id:chatId,text:`${em("🎯","users")} <b>Выберите аудиторию:</b>`,parse_mode:"HTML",reply_markup:{inline_keyboard:[
        [btn("Все","a:bs_filter_all","users")],
        [btn("С подпиской","a:bs_filter_with_sub","check")],
        [btn("Без подписки","a:bs_filter_no_sub","cross")],
        [btn("Отмена","a:bs_cancel","back")],
      ]}});
      return true;
    }
    case "broadcast_preview": {
      const hasContent = !!(msg.text||msg.photo||msg.video||msg.document||msg.animation||msg.voice||msg.audio||msg.sticker||msg.video_note);
      if(!hasContent) return true;
      let oldData; try{oldData=JSON.parse(row.payload||"{}");}catch{oldData={};}
      clearAdminState(aid);
      const msgMeta={chat_id:Number(chatId),message_id:Number(msg.message_id)};
      setAdminState(aid,"broadcast_settings",JSON.stringify({msg_meta:msgMeta,filter:oldData.filter||"all",button:oldData.button||null}));
      await tg("sendMessage",{chat_id:chatId,text:`${em("🎯","users")} <b>Выберите аудиторию:</b>`,parse_mode:"HTML",reply_markup:{inline_keyboard:[
        [btn("Все","a:bs_filter_all","users")],
        [btn("С подпиской","a:bs_filter_with_sub","check")],
        [btn("Без подписки","a:bs_filter_no_sub","cross")],
        [btn("Отмена","a:bs_cancel","back")],
      ]}});
      return true;
    }
    case "ref_percent": {
      const n=Number(text); if(!Number.isFinite(n)||n<0||n>100) return true;
      setSetting("ref_percent",Math.round(n)); clearAdminState(aid);
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_ref"); return true;
    }
    case "ref_min_withdraw": {
      const n=Number(text); if(!Number.isFinite(n)||n<1) return true;
      setSetting("ref_min_withdraw",Math.round(n)); clearAdminState(aid);
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_ref"); return true;
    }
    case "ref_add": {
      const targetId=Number(row.payload), n=Number(text);
      if(!Number.isFinite(n)||n<=0) return true;
      addRefBalance(targetId, n); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:`✅ +${rub(n)} реф. баланс для ${targetId}`,parse_mode:"HTML"});
      tg("sendMessage",{chat_id:targetId,text:`${em("💰","money")} <b>Реферальный баланс пополнен на ${rub(n)}</b>`,parse_mode:"HTML"}).catch(()=>{});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_user_info",{id:targetId}); return true;
    }
    case "find_user": {
      clearAdminState(aid);
      let found=null;
      if(/^\d+$/.test(text)) found=user(Number(text));
      if(!found) found=db.prepare("SELECT * FROM users WHERE username=?").get(text.replace(/^@/,""));
      if(!found){await tg("sendMessage",{chat_id:chatId,text:"Не найден."});return true;}
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_user_info",{id:found.tg_id}); return true;
    }
    case "chan_id": {
      const val=text.trim();
      if(val==="-"||val==="") delSetting("channel_id"); else setSetting("channel_id",val);
      clearAdminState(aid);
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_channel"); return true;
    }
    case "chan_url": {
      const val=text.trim();
      if(val==="-"||val==="") delSetting("channel_invite_url"); else setSetting("channel_invite_url",val);
      clearAdminState(aid);
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_channel"); return true;
    }
    case "trial_days": {
      const n=parseInt(text,10);
      if(!Number.isFinite(n)||n<1||n>365) return true;
      setSetting("trial_days",String(n)); clearAdminState(aid);
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_channel"); return true;
    }
    case "fk_shop_id": {
      const n=parseInt(text,10); if(!Number.isFinite(n)||n<=0) return true;
      setSetting("fk_shop_id",String(n)); clearAdminState(aid);
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_fk"); return true;
    }
    case "fk_min_rub": {
      const n=parseInt(text,10); if(!Number.isFinite(n)||n<1) return true;
      setSetting("fk_min_rub",String(n)); clearAdminState(aid);
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_fk"); return true;
    }
    case "fk_notify_path": {
      let p=text.trim(); if(!p) return true;
      if(!p.startsWith("/")) p="/"+p;
      setSetting("fk_notify_path",p.replace(/\s+/g,"")); clearAdminState(aid);
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_fk"); return true;
    }
    case "promo_add": {
      const parts=text.trim().split(/\s+/);
      if(parts.length<2) return true;
      const code=parts[0].toUpperCase(), pct=parseInt(parts[1],10), maxUses=parts[2]?parseInt(parts[2],10):0;
      if(!code||isNaN(pct)||pct<1||pct>99) return true;
      try {
        db.prepare("INSERT INTO promo_codes(code,discount_pct,uses_max,uses_current,is_active,created_at) VALUES(?,?,?,0,1,?) ON CONFLICT(code) DO UPDATE SET discount_pct=excluded.discount_pct,uses_max=excluded.uses_max,is_active=1")
          .run(code,pct,maxUses||0,now());
      } catch {}
      clearAdminState(aid);
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_promo"); return true;
    }
    case "promo_deactivate": {
      db.prepare("UPDATE promo_codes SET is_active=0 WHERE code=? COLLATE NOCASE").run(text.trim().toUpperCase());
      clearAdminState(aid);
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_promo"); return true;
    }
    case "db_import_wait": {
      if(!msg.document?.file_id) return true;
      try {
        const f = await tg("getFile",{file_id:msg.document.file_id});
        const resp = await fetch(`https://api.telegram.org/file/bot${TOKEN}/${f.file_path}`);
        const buf = Buffer.from(await resp.arrayBuffer());
        if(buf.length<100||buf.slice(0,16).toString("binary")!=="SQLite format 3\x00") throw new Error("Not SQLite");
        const tmp=`${DB_FILE}.import_${Date.now()}.tmp`;
        await fsp.writeFile(tmp,buf);
        clearAdminState(aid);
        await tg("sendMessage",{chat_id:chatId,text:"✅ Перезапуск..."});
        try{db.pragma("wal_checkpoint(TRUNCATE)");}catch{}
        try{db.close();}catch{}
        fs.copyFileSync(DB_FILE,`${DB_FILE}.backup.${Date.now()}`);
        fs.renameSync(tmp,DB_FILE);
        spawn(process.execPath,[path.join(__dirname,"bot.js")],{cwd:__dirname,detached:true,stdio:"ignore",env:process.env}).unref();
        process.exit(0);
      } catch(e) { await tg("sendMessage",{chat_id:chatId,text:`❌ ${esc(e.message)}`,parse_mode:"HTML"}); }
      return true;
    }
  }
  return false;
}

/* ═══════════════════════════════════════════════════════════════════════════
   USER STATE HANDLER
   ═══════════════════════════════════════════════════════════════════════════ */
async function handleUserState(msg) {
  const uid=Number(msg.from?.id||0), chatId=Number(msg.chat?.id||0);
  const ustate=getUserState(uid);
  if(!ustate) return false;
  const text=String(msg.text||"").trim();
  const userMsgId=Number(msg.message_id||0);
  delMsg(chatId,userMsgId);

  if(text==="/cancel"){
    clearUserState(uid);
    const promptId=Number((ustate.payload||"").split(":").pop())||0;
    delMsg(chatId,promptId);
    await render(uid,chatId,user(uid)?.last_menu_id||null,"home");
    return true;
  }

  switch(ustate.state) {
    case "promo_input": {
      if(!msg.text||msg.text.startsWith("/")) return true;
      const parts=(ustate.payload||"").split(":");
      const code2=parts[0],mode2=parts[1],devices2=Number(parts[2]||3),promptMsgId=Number(parts[3]||0);
      clearUserState(uid);
      delMsg(chatId,promptMsgId);
      const result=validatePromo(uid,text.trim());
      if(!result.ok){
        const errMsg=result.reason==="used"
          ?`${em("❌","cross")} Промокод уже использован.`
          :`${em("❌","cross")} Промокод не найден или истёк.`;
        await tg("sendMessage",{chat_id:chatId,text:errMsg,parse_mode:"HTML"});
        await showPaymentOptions(uid,chatId,user(uid)?.last_menu_id||null,code2,mode2,"",0,devices2);
        return true;
      }
      const pct=result.promo.discount_pct;
      await tg("sendMessage",{chat_id:chatId,text:`${em("✅","check")} Промокод <b>${esc(text.trim().toUpperCase())}</b> — скидка <b>${pct}%</b>`,parse_mode:"HTML"});
      await showPaymentOptions(uid,chatId,user(uid)?.last_menu_id||null,code2,mode2,text.trim().toUpperCase(),pct,devices2);
      return true;
    }
    case "gift_recipient_id": {
      if(!msg.text||msg.text.startsWith("/")) return true;
      const parts=(ustate.payload||"").split(":");
      const code=parts[0],promptMsgId=Number(parts[1]||0);
      clearUserState(uid);
      delMsg(chatId,promptMsgId);
      let found=null;
      if(/^\d+$/.test(text)) found=user(Number(text));
      if(!found&&text.startsWith("@")) found=db.prepare("SELECT * FROM users WHERE username=?").get(text.slice(1));
      if(!found){
        await tg("sendMessage",{chat_id:chatId,text:`${em("❌","cross")} Пользователь не найден.`,parse_mode:"HTML"});
        return true;
      }
      if(Number(found.tg_id)===uid){
        await tg("sendMessage",{chat_id:chatId,text:`${em("❌","cross")} Нельзя подарить самому себе.`,parse_mode:"HTML"});
        return true;
      }
      const toSub=sub(found.tg_id);
      if(activeSub(toSub)){
        await tg("sendMessage",{chat_id:chatId,text:`${em("❌","cross")} У получателя уже есть подписка.`,parse_mode:"HTML"});
        return true;
      }
      await showPaymentOptions(uid,chatId,user(uid)?.last_menu_id||null,code,"gift","",0,3,found.tg_id);
      return true;
    }
    case "ref_setpay": {
      if(!msg.text||msg.text.startsWith("/")) return true;
      const promptMsgId=Number(ustate.payload||0);
      clearUserState(uid);
      delMsg(chatId,promptMsgId);
      const raw=text.trim();
      const sep=raw.includes("|")?"|":"-";
      const parts2=raw.split(sep).map(s=>s.trim());
      if(parts2.length<2||!parts2[0]||!parts2[1]){
        await tg("sendMessage",{chat_id:chatId,text:`${em("❌","cross")} Формат: Способ | Реквизиты`,parse_mode:"HTML"});
        return true;
      }
      const method=parts2[0].slice(0,100), details=parts2.slice(1).join("|").trim().slice(0,200);
      db.prepare("UPDATE users SET payout_method=?,payout_details=?,updated_at=? WHERE tg_id=?").run(method,details,now(),Number(uid));
      await tg("sendMessage",{chat_id:chatId,text:`${em("✅","check")} Сохранено: <b>${esc(method)}</b> — <b>${esc(details)}</b>`,parse_mode:"HTML"});
      await render(uid,chatId,user(uid)?.last_menu_id||null,"ref");
      return true;
    }
  }
  return false;
}

/* ═══════════════════════════════════════════════════════════════════════════
   MESSAGE HANDLER
   ═══════════════════════════════════════════════════════════════════════════ */
async function handleMessage(msg) {
  const from=msg.from||{}, chatId=Number(msg.chat?.id||0);
  if(!chatId||!from.id||msg.chat?.type!=="private") return;
  upsertUser(from,chatId);

  if(isAdmin(from.id) && await handleAdminState(msg)) return;
  if(await handleUserState(msg)) return;

  const text=String(msg.text||"").trim();

  if(text.startsWith("/start")){
    const m=text.match(/^\/start\s+partner_([a-zA-Z0-9]+)$/);
    if(m){const r=findRef(m[1]);if(r)setRef(from.id,r.tg_id);}
    const passed=await enforceChannelGate(from.id,chatId,getLang(from.id));
    if(!passed) return;
    await gif(chatId,"gif_main_menu");
    await render(from.id,chatId,null,"home"); return;
  }
  if(text==="/menu"||text==="/sub"||text==="/referral"){
    const passed=await enforceChannelGate(from.id,chatId,getLang(from.id));
    if(!passed) return;
    const view=text==="/sub"?"sub":text==="/referral"?"ref":"home";
    await render(from.id,chatId,null,view); return;
  }
  if(text==="/admin"&&isAdmin(from.id)){await render(from.id,chatId,user(from.id)?.last_menu_id,"a_main");return;}

  await tg("sendMessage",{chat_id:chatId,text:"Используйте /start"});
}

/* ═══════════════════════════════════════════════════════════════════════════
   CALLBACK HANDLER — FIXED: renew flow
   ═══════════════════════════════════════════════════════════════════════════ */
async function handleCallback(q) {
  const data=q.data||"", uid=Number(q.from?.id||0), chatId=Number(q.message?.chat?.id||0), msgId=Number(q.message?.message_id||0);
  if(!uid||!chatId||!msgId||q.message?.chat?.type!=="private") { await tg("answerCallbackQuery",{callback_query_id:q.id}).catch(()=>{}); return; }
  upsertUser(q.from,chatId);
  const ans = (text="",alert=false) => tg("answerCallbackQuery",{callback_query_id:q.id,...(text?{text,show_alert:alert}:{})}).catch(()=>{});

  if(!data.startsWith("cp:")&&!data.startsWith("fk:")&&!checkCbRateLimit(uid)){await ans();return;}
  if(data==="noop"){await ans();return;}

  // Cancel states
  if(data.startsWith("cancel:")){
    clearUserState(uid);
    delMsg(chatId,msgId);
    await ans();
    await render(uid,chatId,user(uid)?.last_menu_id||null,"home");
    return;
  }

  // Channel gate
  if(data==="gate:check"){
    if(!await checkChannelMembership(uid)){await ans("Вы ещё не подписались.",true);return;}
    delMsg(chatId,msgId);
    await gif(chatId,"gif_main_menu");
    await render(uid,chatId,null,"home");
    await ans(); return;
  }

  if(!isAdmin(uid)){
    const passed=await enforceChannelGate(uid,chatId,getLang(uid));
    if(!passed){await ans();return;}
  }

  // Trial
  if(data==="trial:start"){
    if(!trialEnabled()){await ans("Пробный период недоступен.",true);return;}
    if(hasUsedTrial(uid)){await ans("Пробный период уже использован.",true);return;}
    if(activeSub(sub(uid))){await ans("У вас уже есть подписка.",true);return;}
    const kb={inline_keyboard:[
      [btn("Подтвердить","trial:confirm","check")],
      [btn("Отмена","v:home","cross")],
    ]};
    await renderMsg(chatId,msgId,`<b>Пробный период — ${trialDays()} дней</b>\n\n<i>Бесплатно, один раз.</i>\n\nАктивировать?`,kb);
    await ans(); return;
  }
  if(data==="trial:confirm"){
    if(!trialEnabled()||hasUsedTrial(uid)||activeSub(sub(uid))){await ans("Недоступно.",true);return;}
    await ans();
    try{await doTrial(uid,chatId,msgId);}catch(e){await tg("sendMessage",{chat_id:chatId,text:`❌ ${esc(e.message)}`,parse_mode:"HTML"});}
    return;
  }

  // Navigation — FIXED: added v:renew
  const navMap={"v:home":"home","v:sub":"sub","v:buy":"buy","v:ref":"ref","v:guide":"guide","v:about":"about","v:gift":"gift","v:renew":"renew"};
  if(navMap[data]){await render(uid,chatId,msgId,navMap[data]);await ans();return;}

  // Buy flow — new purchase
  if(data.startsWith("pay:n:")){
    const code=data.split(":")[2];
    if (code === "trial") {
      await ans("Пробный период нельзя купить.", true);
      return;
    }
    await ans();
    await showPaymentOptions(uid,chatId,msgId,code,"new");
    return;
  }

  // FIXED: Renew flow — no time restriction, user picks tariff from list
  if(data.startsWith("pay:rw:")){
    const code=data.split(":")[2];
    if (code === "trial") {
      await ans();
      await render(uid, chatId, msgId, "buy");
      return;
    }
    const s=sub(uid);
    if(!s){await ans("Подписка не найдена.",true);return;}
    // No time restriction — renew allowed anytime
    await ans();
    await showPaymentOptions(uid,chatId,msgId,code,"renew");
    return;
  }

  // Pay with ref balance (full)
  if(data.startsWith("pay:ref:")){
    const poId=Number(data.split(":")[2]);
    const po=getPendingOrder(poId);
    if(!po||po.tg_id!==uid||po.status!=="pending"){await ans("Заказ истёк.",true);return;}
    if (po.tariff_code === "trial") { await ans("Недоступно.", true); return; }
    const tr=tariff(po.tariff_code); if(!tr){await ans("Тариф не найден.",true);return;}
    const finalPrice=calcFinalPrice(tr.price_rub,po.promo_pct,Number(po.devices||3));
    const refBal=getRefBalance(uid);
    if(refBal<finalPrice){await ans(`Недостаточно средств. Нужно ${finalPrice}₽, у вас ${refBal}₽`,true);return;}
    await ans("⏳ Обработка...");
    if(!deductRefBalance(uid, finalPrice)){await tg("sendMessage",{chat_id:chatId,text:"❌ Не удалось списать баланс.",parse_mode:"HTML"});return;}
    closePendingOrder(poId);
    const isGift=po.kind==="gift"&&Number(po.recipient_tg_id||0)>0;
    const receiverId=isGift?Number(po.recipient_tg_id):uid;
    try {
      const res = await doPurchase(uid,receiverId,po.tariff_code,isGift?"gift":po.kind,po.promo_code,po.promo_pct,Number(po.devices||3),finalPrice);
      const s2=sub(receiverId), lang=getLang(uid);
      if(isGift){
        const to=user(receiverId);
        const toName=to?.first_name||(to?.username?`@${to.username}`:String(receiverId));
        await tg("sendMessage",{chat_id:chatId,text:`${em("🎁","gift")} <b>Подарок отправлен!</b>\n\nПолучатель: ${esc(toName)}`,parse_mode:"HTML",reply_markup:{inline_keyboard:[[btn("Главное меню","v:home","home")]]}}).catch(()=>{});
        if(to) tg("sendMessage",{chat_id:receiverId,text:`${em("🎁","gift")} <b>Вам подарили подписку!</b>\n\n<code>${esc(s2?.sub_url||"")}</code>`,parse_mode:"HTML",reply_markup:{inline_keyboard:[[btn("Подключить",s2?.sub_url||"","send",true)]]}}).catch(()=>{});
      } else {
        await tg("sendMessage",{chat_id:chatId,text:[
          `${em("✅","check")} <b>Оплата прошла!</b>`,
          "",`Тариф: <b>${esc(tariffTitle(tr,lang))}</b>`,
          `Списано с реф. баланса: <b>${rub(finalPrice)}</b>`,
          "",`<code>${esc(s2?.sub_url||"")}</code>`,
        ].join("\n"),parse_mode:"HTML",reply_markup:{inline_keyboard:[
          [btn("Подключить",s2?.sub_url||"","send",true)],
          [btn("Моя подписка","v:sub","lockOpen"),btn("Главное меню","v:home","home")],
        ]}}).catch(()=>{});
      }
    } catch(e) {
      addRefBalance(uid, finalPrice);
      await tg("sendMessage",{chat_id:chatId,text:`❌ ${esc(e.message)}`,parse_mode:"HTML"});
    }
    return;
  }

  // Pay with ref balance (partial)
  if(data.startsWith("pay:ref_partial:")){
    const poId=Number(data.split(":")[2]);
    const po=getPendingOrder(poId);
    if(!po||po.tg_id!==uid||po.status!=="pending"){await ans("Заказ истёк.",true);return;}
    if (po.tariff_code === "trial") { await ans("Недоступно.", true); return; }
    db.prepare("UPDATE pending_orders SET use_ref_balance=1,updated_at=? WHERE id=?").run(now(),poId);
    const tr=tariff(po.tariff_code); if(!tr){await ans("Тариф не найден.",true);return;}
    const finalPrice=calcFinalPrice(tr.price_rub,po.promo_pct,Number(po.devices||3));
    const refBal=getRefBalance(uid);
    const remaining=finalPrice-Math.min(refBal,finalPrice);
    await ans();
    const lang=getLang(uid);
    const lines=[
      `${em("💳","wallet")} <b>Доплата</b>`,
      "",`Сумма: <b>${rub(finalPrice)}</b>`,
      `Реф. баланс: <b>-${rub(Math.min(refBal,finalPrice))}</b>`,
      `К оплате: <b>${rub(remaining)}</b>`,
      "","Выберите способ оплаты:",
    ];
    const rows=[];
    if(CRYPTOBOT_TOKEN) rows.push([btn("Crypto Bot (USDT)",`direct:crypto:${poId}`,"crypto")]);
    if(isFkEnabled()){
      rows.push([btn("СБП (QR)",`direct:fk:${poId}:44`,"wallet")]);
      rows.push([btn("Банковская карта РФ",`direct:fk:${poId}:36`,"wallet")]);
      rows.push([btn("SberPay",`direct:fk:${poId}:43`,"wallet")]);
    }
    rows.push([btn("Назад","v:buy","back")]);
    await renderMsg(chatId,msgId,lines.join("\n"),{inline_keyboard:rows});
    return;
  }

  // Promo code
  if(data.startsWith("promo:ask:")){
    const parts=data.split(":"), code=parts[2], mode=parts[3], devices=Number(parts[4]||3);
    await ans();
    const promptId=await sendPrompt(chatId,"Введите промокод:",`cancel:promo:${code}:${mode}:${devices}`);
    setUserState(uid,"promo_input",`${code}:${mode}:${devices}:${promptId}`);
    return;
  }

  // Direct crypto payment
  if(data.startsWith("direct:crypto:")){
    if(!CRYPTOBOT_TOKEN){await ans("CryptoBot не настроен.",true);return;}
    const poId=Number(data.split(":")[2]);
    const po=getPendingOrder(poId);
    if(!po||po.tg_id!==uid||po.status!=="pending"){await ans("Заказ истёк. Начните заново.",true);return;}
    if (po.tariff_code === "trial") { await ans("Недоступно.", true); return; }
    const tr=tariff(po.tariff_code); if(!tr){await ans("Тариф не найден.",true);return;}
    const finalPrice=calcFinalPrice(tr.price_rub,po.promo_pct,Number(po.devices||3));
    const useRef=!!po.use_ref_balance;
    const refBal=useRef?getRefBalance(uid):0;
    const toPay=Math.max(1,finalPrice-Math.min(refBal,finalPrice));
    await ans();
    const inv=await createCryptoInvoice(toPay);
    if(!inv){await tg("sendMessage",{chat_id:chatId,text:"❌ CryptoBot недоступен.",parse_mode:"HTML"});return;}
    const cpId = createCryptoPaymentRow(uid,toPay,inv.amountUsdt,inv.rate,inv.invoiceId,inv.payUrl,poId);
    await tg("sendMessage",{chat_id:chatId,text:[
      `<b>Счёт создан</b>`,
      "",`Сумма: <b>${rub(toPay)}</b> → <b>${inv.amountUsdt} USDT</b>`,
      "",`1. Нажмите «Оплатить»`,`2. Переведите USDT в @CryptoBot`,`3. Проверьте оплату`,
      "",`<i>Счёт действителен 1 час.</i>`,
    ].join("\n"),parse_mode:"HTML",reply_markup:{inline_keyboard:[
      [btn("Оплатить",inv.payUrl,"crypto",true)],
      [btn("Проверить оплату",`cp:check:${cpId}`,"check")],
      [btn("Отмена",`cp:cancel:${cpId}`,"cross")],
    ]}});
    return;
  }

  // Direct FK payment
  if(data.startsWith("direct:fk:")){
    if(!isFkEnabled()){await ans("Способ недоступен.",true);return;}
    const parts=data.split(":"), poId=Number(parts[2]), methodId=Number(parts[3]||44);
    const po=getPendingOrder(poId);
    if(!po||po.tg_id!==uid||po.status!=="pending"){await ans("Заказ истёк.",true);return;}
    if (po.tariff_code === "trial") { await ans("Недоступно.", true); return; }
    const tr=tariff(po.tariff_code); if(!tr){await ans("Тариф не найден.",true);return;}
    const finalPrice=calcFinalPrice(tr.price_rub,po.promo_pct,Number(po.devices||3));
    const useRef=!!po.use_ref_balance;
    const refBal=useRef?getRefBalance(uid):0;
    const toPay=Math.max(1,finalPrice-Math.min(refBal,finalPrice));
    const serverIp=fkServerIp();
    if(!serverIp){await ans("IP не определён.",true);return;}
    await ans();
    const email=`user${uid}@${FK_EMAIL_DOMAIN}`;
    let order;
    try{order=await createFkOrder({uid,amountRub:toPay,methodId,email,ip:serverIp});}
    catch(e){await tg("sendMessage",{chat_id:chatId,text:`❌ Не удалось создать счёт: ${esc(e.message)}`,parse_mode:"HTML"});return;}
    if(!order.location){await tg("sendMessage",{chat_id:chatId,text:"❌ Нет ссылки оплаты.",parse_mode:"HTML"});return;}
    const fkId=createFkPaymentRow(uid,toPay,methodId,order.paymentId,order.location,order.orderId,poId);
    await tg("sendMessage",{chat_id:chatId,text:[
      `<b>Счёт создан</b>`,
      "",`Сумма: <b>${rub(toPay)}</b>`,
      `Метод: <b>${esc(methodTitle(methodId,getLang(uid)))}</b>`,
      "",`1. Нажмите «Оплатить»`,`2. Завершите платёж`,`3. Подписка активируется автоматически`,
    ].join("\n"),parse_mode:"HTML",disable_web_page_preview:true,reply_markup:{inline_keyboard:[
      [btn("Оплатить",order.location,"money",true)],
      [btn("Проверить оплату",`fk:check:${fkId}`,"check")],
      [btn("Отмена",`fk:cancel:${fkId}`,"cross")],
    ]}});
    return;
  }

  // Crypto check/cancel
  if(data.startsWith("cp:check:")){
    const cpId=Number(data.split(":")[2]), cp=getCryptoPayment(cpId);
    if(!cp||cp.tg_id!==uid){await ans("Не найден.",true);return;}
    if(cp.status==="paid"){await ans("Уже зачислено!",true);return;}
    if(cp.status!=="pending"){await ans("Закрыт.",true);return;}
    await ans("⏳ Проверяю...");
    if(await checkCryptoInvoice(cp.invoice_id)){
      markCryptoPaid(cpId);
      const poId=cp.pending_order_id;
      const po=poId?getPendingOrder(poId):getPendingOrderByUser(uid);
      if(po&&po.status==="pending"){
        closePendingOrder(po.id);
        await completePurchaseAfterPayment(uid,po);
      } else {
        await tg("sendMessage",{chat_id:chatId,text:"❌ Заказ не найден. Обратитесь в поддержку.",parse_mode:"HTML",reply_markup:{inline_keyboard:[[btn("Главное меню","v:home","home")]]}});
      }
    } else {
      await tg("sendMessage",{chat_id:chatId,text:"❌ Оплата не найдена. Попробуйте позже.",parse_mode:"HTML",reply_markup:{inline_keyboard:[[btn("Проверить",`cp:check:${cpId}`,"check")],[btn("Главное меню","v:home","home")]]}});
    }
    return;
  }
  if(data.startsWith("cp:cancel:")){
    const cpId=Number(data.split(":")[2]), cp=getCryptoPayment(cpId);
    if(!cp||cp.tg_id!==uid||cp.status!=="pending"){await ans("Закрыт.",true);return;}
    markCryptoCancelled(cpId);
    if(cp.pending_order_id) closePendingOrder(cp.pending_order_id,"cancelled");
    await ans("Отменено.");
    await tg("editMessageReplyMarkup",{chat_id:chatId,message_id:msgId,reply_markup:{inline_keyboard:[[btn("Главное меню","v:home","home")]]}}).catch(()=>{});
    return;
  }

  // FK check/cancel
  if(data.startsWith("fk:check:")){
    const fpId=Number(data.split(":")[2]), fp=getFkPayment(fpId);
    if(!fp||fp.tg_id!==uid){await ans("Не найден.",true);return;}
    if(fp.status==="paid"){await ans("Уже зачислено!",true);return;}
    if(fp.status!=="pending"){await ans("Закрыт.",true);return;}
    await ans("⏳ Проверяю...");
    try {
      const ord=await checkFkOrderByPaymentId(fp.payment_id);
      const isPaid=ord&&(Number(ord.status)===1||String(ord.status||"").toLowerCase()==="paid");
      if(!isPaid){
        await tg("sendMessage",{chat_id:chatId,text:"❌ Оплата не найдена.",parse_mode:"HTML",reply_markup:{inline_keyboard:[[btn("Проверить",`fk:check:${fpId}`,"check")],[btn("Главное меню","v:home","home")]]}});
        return;
      }
      await creditFkPayment(fp.payment_id,ord.id||ord.orderId||null,ord.amount||null);
      const poId=fp.pending_order_id;
      const po=poId?getPendingOrder(poId):getPendingOrderByUser(uid);
      if(po&&po.status==="pending"){
        closePendingOrder(po.id);
        await completePurchaseAfterPayment(uid,po);
      } else {
        await tg("sendMessage",{chat_id:chatId,text:"❌ Заказ не найден. Обратитесь в поддержку.",parse_mode:"HTML",reply_markup:{inline_keyboard:[[btn("Главное меню","v:home","home")]]}});
      }
    } catch(e) { await ans(`❌ ${String(e.message).slice(0,200)}`,true); }
    return;
  }
  if(data.startsWith("fk:cancel:")){
    const fpId=Number(data.split(":")[2]), fp=getFkPayment(fpId);
    if(!fp||fp.tg_id!==uid||fp.status!=="pending"){await ans("Закрыт.",true);return;}
    markFkCancelled(fpId);
    if(fp.pending_order_id) closePendingOrder(fp.pending_order_id,"cancelled");
    await ans("Отменено.");
    await tg("editMessageReplyMarkup",{chat_id:chatId,message_id:msgId,reply_markup:{inline_keyboard:[[btn("Главное меню","v:home","home")]]}}).catch(()=>{});
    return;
  }

  // Paging
  if(data.startsWith("ph:")){await render(uid,chatId,msgId,"purchases",{page:Number(data.split(":")[1]||0)});await ans();return;}
  if(data.startsWith("ref:hist:")){await render(uid,chatId,msgId,"ref_hist",{page:Number(data.split(":")[2]||0)});await ans();return;}

  // Ref actions
  if(data==="ref:r"){
    await renderMsg(chatId,msgId,`<b>Сменить реферальный код?</b>\n\n<i>Старые ссылки перестанут работать.</i>`,{inline_keyboard:[
      [btn("Подтвердить","ref:r:yes","check"),btn("Отмена","v:ref","cross")],
    ]});
    await ans(); return;
  }
  if(data==="ref:r:yes"){
    db.prepare("UPDATE users SET ref_code=?,updated_at=? WHERE tg_id=?").run(crypto.randomBytes(5).toString("hex"),now(),uid);
    await render(uid,chatId,msgId,"ref"); await ans("✅"); return;
  }
  if(data==="ref:setpay"){
    const u2=user(uid);
    await ans();
    const promptId=await sendPrompt(chatId,`${em("⚙️","settings")} <b>Реквизиты для вывода</b>\n\nТекущий: <b>${esc(u2.payout_method||"не задан")}</b>\nРеквизиты: <b>${esc(u2.payout_details||"—")}</b>\n\nФормат: <code>Способ | Реквизиты</code>\nПример: <code>Сбербанк | +79001234567</code>`,"cancel:input");
    setUserState(uid,"ref_setpay",String(promptId));
    return;
  }
  if(data==="ref:withdraw"){
    const u2=user(uid), refBal=getRefBalance(uid);
    const minW=Number(setting("ref_min_withdraw","3000"))||3000;
    if(refBal<minW){await ans(`Минимум ${minW}₽. У вас ${Math.floor(refBal)}₽`,true);return;}
    if(!u2.payout_method||!u2.payout_details){await ans("Сначала настройте реквизиты.",true);return;}
    db.prepare("INSERT INTO withdrawal_requests(tg_id,amount_rub,method,details,status,admin_note,created_at,updated_at) VALUES(?,?,?,?,'pending','',?,?)")
      .run(Number(uid),Math.round(refBal),u2.payout_method,u2.payout_details,now(),now());
    db.prepare("UPDATE users SET ref_balance_rub=0,updated_at=? WHERE tg_id=?").run(now(),Number(uid));
    await ans("✅ Заявка отправлена.",true);
    tg("sendMessage",{chat_id:ADMIN_ID,text:`${em("💸","sendMoney")} <b>Заявка на вывод</b>\n\n${esc(u2.first_name||String(uid))} (<code>${uid}</code>)\n${rub(refBal)} | ${esc(u2.payout_method)}: ${esc(u2.payout_details)}`,parse_mode:"HTML"}).catch(()=>{});
    await render(uid,chatId,msgId,"ref");
    return;
  }

  // Gift
  if(data.startsWith("g:p:")){
    const code=data.split(":")[2];
    await ans();
    const promptId=await sendPrompt(chatId,`${em("🎁","gift")} Введите Telegram ID или @username получателя:`,"cancel:gift");
    setUserState(uid,"gift_recipient_id",`${code}:${promptId}`);
    return;
  }

  // Admin navigation
  const adminNav={"a:main":"a_main","a:t":"a_tariffs","a:g":"a_gif","a:b":"a_bcast","a:r":"a_ref","a:db":"a_db","a:links":"a_links","a:guide_edit":"a_guide_edit","a:channel":"a_channel","a:fk":"a_fk","a:promo":"a_promo"};
  if(adminNav[data]&&isAdmin(uid)){await render(uid,chatId,msgId,adminNav[data]);await ans();return;}

  if(data==="a:cancel_admin"&&isAdmin(uid)){
    clearAdminState(uid);
    delMsg(chatId,msgId);
    await render(uid,chatId,user(uid)?.last_menu_id||null,"a_main");
    await ans(); return;
  }

  // Admin callbacks
  if(data.startsWith("a:")&&isAdmin(uid)){
    // Users
    if(data.startsWith("a:users:")){await render(uid,chatId,msgId,"a_users",{page:Number(data.split(":")[2]||0)});await ans();return;}
    // Withdrawals
    if(data.startsWith("a:withdrawals")){
      const pg=Number(data.split(":")[2]||0);
      await render(uid,chatId,msgId,"a_withdrawals",{page:pg});await ans();return;
    }
    if(data.startsWith("a:wd_done:")){
      const parts=data.split(":"), wid=Number(parts[2]), pg=Number(parts[3]||0);
      const wr=db.prepare("SELECT * FROM withdrawal_requests WHERE id=?").get(wid);
      if(!wr||wr.status!=="pending"){await ans("Обработано.",true);return;}
      db.prepare("UPDATE withdrawal_requests SET status='paid',updated_at=? WHERE id=?").run(now(),wid);
      tg("sendMessage",{chat_id:wr.tg_id,text:`${em("✅","check")} <b>Выплата произведена!</b>\n\n${rub(wr.amount_rub)} | ${esc(wr.method)}`,parse_mode:"HTML"}).catch(()=>{});
      await ans("✅ Выплачено.");
      await render(uid,chatId,msgId,"a_withdrawals",{page:pg}); return;
    }
    if(data.startsWith("a:wd_reject:")){
      const parts=data.split(":"), wid=Number(parts[2]), pg=Number(parts[3]||0);
      const wr=db.prepare("SELECT * FROM withdrawal_requests WHERE id=?").get(wid);
      if(!wr||wr.status!=="pending"){await ans("Обработано.",true);return;}
      db.prepare("UPDATE withdrawal_requests SET status='rejected',updated_at=? WHERE id=?").run(now(),wid);
      addRefBalance(wr.tg_id, wr.amount_rub);
      tg("sendMessage",{chat_id:wr.tg_id,text:`${em("❌","cross")} <b>Заявка отклонена.</b> ${rub(wr.amount_rub)} возвращено на реф. баланс.`,parse_mode:"HTML"}).catch(()=>{});
      await ans("Отклонено.");
      await render(uid,chatId,msgId,"a_withdrawals",{page:pg}); return;
    }
    // Tariff edit
    if(data.startsWith("a:te:")){
      const code=data.split(":")[2], tr=tariff(code);
      setAdminState(uid,"tariff_price",code);
      await sendPrompt(chatId,`«${esc(tr?.title||code)}» — ${rub(tr?.price_rub||0)}\n\nНовая цена (₽):`,"a:cancel_admin");
      await ans(); return;
    }
    if(data==="a:dev_price"){
      setAdminState(uid,"dev_extra_price","");
      await sendPrompt(chatId,`Цена за доп. устройство: <b>${rub(devicesExtraPrice())}</b>\n\nНовая цена:`,"a:cancel_admin");
      await ans(); return;
    }
    // GIF edit
    if(data.startsWith("a:ge:")){
      setAdminState(uid,"gif",data.split(":")[2]);
      await sendPrompt(chatId,"Отправьте GIF или file_id.","a:cancel_admin");
      await ans(); return;
    }
    // Link edit
    if(data.startsWith("a:lnk:")){
      const key=data.split(":").slice(2).join(":");
      setAdminState(uid,"edit_link",key);
      await sendPrompt(chatId,`«${key.replace("url_","")}»: <code>${esc(setting(key))}</code>\n\nНовый URL (или «-»):`,"a:cancel_admin");
      await ans(); return;
    }
    // Guide edit
    if(data==="a:guide_ru"){setAdminState(uid,"guide_text","");await sendPrompt(chatId,"Отправьте текст инструкции (RU).\n\nФормат ссылок: <code>[Название|URL]</code>\nПример: <code>[Скачать Happ|https://happ.su]</code>","a:cancel_admin");await ans();return;}
    if(data==="a:guide_en"){setAdminState(uid,"guide_text_en","");await sendPrompt(chatId,"Send guide text (EN).\n\nLink format: <code>[Label|URL]</code>","a:cancel_admin");await ans();return;}
    // Broadcast
    if(data==="a:bs"){
      await sendPrompt(chatId,`${em("📨","envelope")} Отправьте сообщение для рассылки.\n\n<b>Premium эмодзи сохраняются.</b>`,"a:cancel_admin");
      setAdminState(uid,"broadcast","");
      await ans(); return;
    }
    if(data.startsWith("a:bs_filter_")){
      const row=getAdminState(uid);
      if(!row||row.state!=="broadcast_settings"){await ans("Ошибка.",true);return;}
      let sd; try{sd=JSON.parse(row.payload);}catch{return;}
      if(data.includes("_all")) sd.filter="all";
      else if(data.includes("_with_sub")) sd.filter="with_sub";
      else if(data.includes("_no_sub")) sd.filter="no_sub";
      setAdminState(uid,"broadcast_settings",JSON.stringify(sd));
      const names={all:"всем",with_sub:"с подпиской",no_sub:"без подписки"};
      await tg("sendMessage",{chat_id:chatId,text:`✅ Аудитория: <b>${names[sd.filter]}</b>\n\nВыберите кнопку:`,parse_mode:"HTML",reply_markup:{inline_keyboard:[
        [btn("Купить VPN","a:bs_btn_buy","money")],
        [btn("Подарить","a:bs_btn_gift","gift")],
        [btn("Без кнопки","a:bs_btn_none","cross")],
      ]}});
      await ans(); return;
    }
    if(data.startsWith("a:bs_btn_")){
      const row=getAdminState(uid);
      if(!row||row.state!=="broadcast_settings") return;
      let sd; try{sd=JSON.parse(row.payload);}catch{return;}
      if(data.includes("_buy")) sd.button={text:"Купить VPN",action:"v:buy",emoji:"money"};
      else if(data.includes("_gift")) sd.button={text:"Подарить",action:"v:gift",emoji:"gift"};
      else sd.button=null;
      setAdminState(uid,"broadcast_settings",JSON.stringify(sd));
      await tg("sendMessage",{chat_id:chatId,text:`Готово к рассылке.`,parse_mode:"HTML",reply_markup:{inline_keyboard:[
        [btn("Разослать","a:bs_do","megaphone")],
        [btn("Изменить сообщение","a:bs","pencil")],
        [btn("Отмена","a:bs_cancel","cross")],
      ]}});
      await ans(); return;
    }
    if(data==="a:bs_do"){
      const row=getAdminState(uid);
      if(!row||row.state!=="broadcast_settings"){await ans("Нет данных.",true);return;}
      clearAdminState(uid);
      let sd; try{sd=JSON.parse(row.payload);}catch{return;}
      await ans("⏳ Рассылка...");
      let ids;
      if(sd.filter==="with_sub") ids=db.prepare("SELECT tg_id FROM users WHERE tg_id IN (SELECT tg_id FROM subscriptions WHERE is_active=1 AND expires_at>?)").all(now());
      else if(sd.filter==="no_sub") ids=db.prepare("SELECT tg_id FROM users WHERE tg_id NOT IN (SELECT tg_id FROM subscriptions WHERE is_active=1 AND expires_at>?)").all(now());
      else ids=db.prepare("SELECT tg_id FROM users").all();
      let ok=0,fail=0;
      for(const {tg_id} of ids){
        try{
          await tg("copyMessage",{chat_id:tg_id,from_chat_id:sd.msg_meta.chat_id,message_id:sd.msg_meta.message_id});
          ok++;
          if(sd.button){
            await sleep(50);
            const bObj=btn(sd.button.text,sd.button.action,sd.button.emoji);
            await tg("sendMessage",{chat_id:tg_id,text:"\u200B",reply_markup:{inline_keyboard:[[bObj]]}}).catch(()=>{});
          }
        }catch{fail++;}
        await sleep(35);
      }
      delMsg(chatId,sd.msg_meta.message_id);
      await tg("sendMessage",{chat_id:chatId,text:`📨 Рассылка: ✅ ${ok} ❌ ${fail}`});
      return;
    }
    if(data==="a:bs_cancel"){
      const row=getAdminState(uid);
      if(row) try{const sd=JSON.parse(row.payload||"{}");if(sd.msg_meta?.message_id)delMsg(chatId,sd.msg_meta.message_id);}catch{}
      clearAdminState(uid);
      await ans("Отменено.");
      await render(uid,chatId,msgId,"a_bcast"); return;
    }
    // Ref settings
    if(data==="a:rp"){setAdminState(uid,"ref_percent","");await sendPrompt(chatId,`Ставка: ${setting("ref_percent","30")}%\n\nНовая (0..100):`,"a:cancel_admin");await ans();return;}
    if(data==="a:rmin"){setAdminState(uid,"ref_min_withdraw","");await sendPrompt(chatId,`Мин. вывод: ${setting("ref_min_withdraw","3000")}₽\n\nНовая сумма:`,"a:cancel_admin");await ans();return;}
    // Channel
    if(data==="a:chan_id"){setAdminState(uid,"chan_id","");await sendPrompt(chatId,`Канал: <code>${esc(setting("channel_id","")||"—")}</code>\n\nВведите @username или ID (или «-»):`,"a:cancel_admin");await ans();return;}
    if(data==="a:chan_url"){setAdminState(uid,"chan_url","");await sendPrompt(chatId,"Ссылка-приглашение (или «-»):","a:cancel_admin");await ans();return;}
    if(data==="a:sub_req_toggle"){
      setSetting("subscription_required",isSubscriptionRequired()?"0":"1");
      await ans(isSubscriptionRequired()?"✅ Обязательная подписка включена":"❌ Обязательная подписка отключена");
      await render(uid,chatId,msgId,"a_channel"); return;
    }
    if(data==="a:trial_toggle"){
      setSetting("trial_enabled",trialEnabled()?"0":"1");
      await ans(trialEnabled()?"✅ Включён":"❌ Выключен");
      await render(uid,chatId,msgId,"a_channel"); return;
    }
    if(data==="a:trial_days"){setAdminState(uid,"trial_days","");await sendPrompt(chatId,`Длительность: ${trialDays()} дн.\n\nНовое (1..365):`,"a:cancel_admin");await ans();return;}
    // FK
    if(data==="a:fk_shop"){setAdminState(uid,"fk_shop_id","");await sendPrompt(chatId,`shop_id: ${fkShopId()||"—"}\n\nНовый:`,"a:cancel_admin");await ans();return;}
    if(data==="a:fk_min"){setAdminState(uid,"fk_min_rub","");await sendPrompt(chatId,`min: ${fkMinRub()}\n\nНовый:`,"a:cancel_admin");await ans();return;}
    if(data==="a:fk_path"){setAdminState(uid,"fk_notify_path","");await sendPrompt(chatId,`path: ${esc(fkNotifyPath())}\n\nНовый:`,"a:cancel_admin");await ans();return;}
    // Promo
    if(data==="a:promo_add"){setAdminState(uid,"promo_add","");await sendPrompt(chatId,"Формат: <code>КОД СКИДКА% [МАКС]</code>\nПример: <code>SALE10 10 100</code>","a:cancel_admin");await ans();return;}
    if(data==="a:promo_del"){setAdminState(uid,"promo_deactivate","");await sendPrompt(chatId,"Введите код для деактивации:","a:cancel_admin");await ans();return;}
    // Find user
    if(data==="a:find"){setAdminState(uid,"find_user","");await sendPrompt(chatId,"ID или @username:","a:cancel_admin");await ans();return;}
    // User info
    if(data.startsWith("a:user_back:")){await render(uid,chatId,msgId,"a_user_info",{id:Number(data.split(":")[2])});await ans();return;}
    if(data.startsWith("a:grant:")){await render(uid,chatId,msgId,"a_grant",{id:Number(data.split(":")[2])});await ans();return;}
    if(data.startsWith("a:grant_ok:")){
      const parts=data.split(":"), targetId=Number(parts[2]), tc=parts[3];
      await ans("⏳");
      try{const res=await adminGrantSub(uid,targetId,tc);await tg("sendMessage",{chat_id:chatId,text:`✅ «${esc(res.plan)}» → ${esc(res.name)}`,parse_mode:"HTML"});}
      catch(e){await tg("sendMessage",{chat_id:chatId,text:`❌ ${esc(e.message)}`,parse_mode:"HTML"});}
      await render(uid,chatId,user(uid)?.last_menu_id||null,"a_user_info",{id:Number(data.split(":")[2])}); return;
    }
    if(data.startsWith("a:sub_revoke:")){
      const targetId=Number(data.split(":")[2]);
      db.prepare("UPDATE subscriptions SET is_active=0,expires_at=?,updated_at=? WHERE tg_id=?").run(now()-1,now(),targetId);
      tg("sendMessage",{chat_id:targetId,text:"<b>Ваша подписка деактивирована администратором.</b>",parse_mode:"HTML"}).catch(()=>{});
      await ans("✅ Отозвана.");
      await render(uid,chatId,msgId,"a_user_info",{id:targetId}); return;
    }
    if(data.startsWith("a:trial_reset:")){
      const targetId=Number(data.split(":")[2]);
      db.prepare("UPDATE users SET trial_used=0,updated_at=? WHERE tg_id=?").run(now(),targetId);
      tg("sendMessage",{chat_id:targetId,text:"<b>Пробный период сброшен. Можете активировать снова.</b>",parse_mode:"HTML",reply_markup:{inline_keyboard:[[btn("Активировать","trial:start","celebrate")]]}}).catch(()=>{});
      await ans("✅ Сброшен.");
      await render(uid,chatId,msgId,"a_user_info",{id:targetId}); return;
    }
    if(data.startsWith("a:ref_add:")){
      const targetId=data.split(":")[2];
      setAdminState(uid,"ref_add",targetId);
      await sendPrompt(chatId,`Пополнение реф. баланса для <code>${targetId}</code>\n\nВведите сумму:`,"a:cancel_admin");
      await ans(); return;
    }
    // DB
    if(data==="a:db_export"){
      await ans("Формирую...");
      try{db.pragma("wal_checkpoint(TRUNCATE)");}catch{}
      await tgSendFile("sendDocument",chatId,"document",DB_FILE,{caption:"📦 База данных"});
      return;
    }
    if(data==="a:db_import_start"){
      setAdminState(uid,"db_import_wait","");
      await tg("sendMessage",{chat_id:chatId,text:"Отправьте SQLite файл документом."});
      await ans(); return;
    }
  }

  await ans("Неизвестная команда.");
}

/* ═══════════════════════════════════════════════════════════════════════════
   EXPIRY JOBS
   ═══════════════════════════════════════════════════════════════════════════ */
/* ═══════════════════════════════════════════════════════════════════════════
   SUBSCRIPTION SYNC — detect deletions made via admin panel
   ═══════════════════════════════════════════════════════════════════════════ */
function extractTokenFromSubUrl(subUrl) {
  if (!subUrl) return null;
  try {
    return new URL(subUrl).searchParams.get("token") || null;
  } catch {
    const m = String(subUrl).match(/[?&]token=([^&]+)/);
    return m ? m[1] : null;
  }
}

/**
 * Fetches subscription info from the API in a single request.
 * Returns { valid: bool, expiresAt: number|null }
 *
 * - valid=false only on explicit 404 (subscription deleted); true on network errors (fail open)
 * - expiresAt is parsed from the subscription-userinfo response header (authoritative date from KV)
 *
 * Uses a VPN-client User-Agent so the endpoint returns the payload + headers
 * instead of the HTML page.
 */
async function fetchSubInfo(token) {
  if (!token) return { valid: false, expiresAt: null };
  try {
    const r = await fetch(`${API}/api/sub?token=${encodeURIComponent(token)}`, {
      headers: { "User-Agent": "happ/2.0" },
      signal: AbortSignal.timeout(8000),
    });
    if (r.status === 404) return { valid: false, expiresAt: null };
    if (!r.ok)           return { valid: true,  expiresAt: null }; // fail open

    // Parse expire from: "upload=0; download=0; total=...; expire=UNIX_SEC; device_limit=3"
    const info = r.headers.get("subscription-userinfo") || "";
    const m    = info.match(/expire=(\d+)/);
    const expiresAt = m ? Number(m[1]) * 1000 : null; // convert seconds → ms

    // Discard body to free socket
    r.body?.cancel().catch(() => {});

    return { valid: true, expiresAt };
  } catch {
    return { valid: true, expiresAt: null }; // fail open on network error
  }
}

/**
 * Marks subscription as inactive locally.
 * If notify=true, sends a message to the user.
 */
async function deactivateLocalSub(tgId, notify = true) {
  db.prepare("UPDATE subscriptions SET is_active=0, expires_at=?, updated_at=? WHERE tg_id=?")
    .run(now() - 1, now(), Number(tgId));
  db.prepare("DELETE FROM notified_expiry WHERE tg_id=?").run(Number(tgId));
  if (notify) {
    await tg("sendMessage", {
      chat_id: tgId,
      text: [
        `${em("❌","cross")} <b>Подписка удалена</b>`,
        "",
        "Ваша подписка была удалена администратором.",
        "Для возобновления доступа оформите новую подписку.",
      ].join("\n"),
      parse_mode: "HTML",
      reply_markup: { inline_keyboard: [
        [btn("Купить VPN", "v:buy", "money")],
        [btn("Поддержка", lnk.support(), "bot", true)],
      ]},
    }).catch(() => {});
  }
}

/**
 * Background job: every 30 minutes checks all active subscriptions against the API.
 * If a subscription was deleted via admin panel (token returns 404), deactivates locally
 * and notifies the user.
 */
function startSubSyncJob() {
  const runSync = async () => {
    try {
      const rows = db.prepare(
        "SELECT tg_id, sub_url, expires_at FROM subscriptions WHERE is_active=1 AND expires_at>?"
      ).all(now());
      for (const row of rows) {
        const token = extractTokenFromSubUrl(row.sub_url);
        if (!token) continue;
        const info = await fetchSubInfo(token);
        if (!info.valid) {
          console.log(`[SubSync] token invalid for tg_id=${row.tg_id} — deactivating`);
          await deactivateLocalSub(row.tg_id, true);
        } else if (info.expiresAt && Math.abs(info.expiresAt - row.expires_at) > 60000) {
          // Expiry differs by more than 1 minute — sync from API (e.g. after admin renewal)
          db.prepare("UPDATE subscriptions SET expires_at=?, updated_at=? WHERE tg_id=?")
            .run(info.expiresAt, now(), Number(row.tg_id));
        }
        await sleep(600); // avoid hammering the API
      }
    } catch(e) { console.error("[SubSync]", e.message); }
  };
  // Run once shortly after boot, then every 30 minutes
  setTimeout(runSync, 15 * 1000);
  setInterval(runSync, 30 * 60 * 1000);
}

function startFkExpireJob() {
  setInterval(()=>{
    try {
      const cutoff=now()-3600*1000;
      const stale=db.prepare("SELECT * FROM freekassa_payments WHERE status='pending' AND created_at<?").all(cutoff);
      for(const fp of stale){
        db.prepare("UPDATE freekassa_payments SET status='expired',updated_at=? WHERE id=? AND status='pending'").run(now(),fp.id);
        if(fp.pending_order_id) closePendingOrder(fp.pending_order_id,"expired");
      }
    } catch(e) { console.error("[FkExpire]",e.message); }
  }, 5*60*1000);
}

function startExpiryNotificationJob() {
  const runJob = () => {
    try {
      const subs=db.prepare("SELECT s.tg_id,s.expires_at,s.plan_title,s.plan_code FROM subscriptions s WHERE s.is_active=1 AND s.expires_at>? AND s.plan_code!='trial'").all(now());
      for(const s of subs){
        const daysLeft=Math.floor((s.expires_at-now())/86400000);
        let level=null;
        if(daysLeft<=1) level="1day";
        else if(daysLeft<=3) level="3days";
        if(!level) continue;
        if(db.prepare("SELECT 1 FROM notified_expiry WHERE tg_id=? AND level=?").get(s.tg_id,level)) continue;
        const text=level==="1day"
          ?`${em("🔴","cross")} <b>Подписка истекает завтра!</b>\n\nТариф «${esc(s.plan_title)}» заканчивается.`
          :`${em("⏰","clock")} <b>Напоминание</b>\n\nПодписка «${esc(s.plan_title)}» истекает через <b>${daysLeft} дн.</b>`;
        tg("sendMessage",{chat_id:s.tg_id,text,parse_mode:"HTML",reply_markup:{inline_keyboard:[[btn("Продлить","v:renew","loading")]]}}).catch(()=>{});
        db.prepare("INSERT OR REPLACE INTO notified_expiry(tg_id,level,notified_at) VALUES(?,?,?)").run(s.tg_id,level,now());
      }
      // Expired
      const expired=db.prepare("SELECT s.tg_id,s.plan_title FROM subscriptions s WHERE s.is_active=1 AND s.expires_at<=? AND s.expires_at>=?").all(now(),now()-2*3600*1000);
      for(const s of expired){
        if(db.prepare("SELECT 1 FROM notified_expiry WHERE tg_id=? AND level='expired'").get(s.tg_id)) continue;
        tg("sendMessage",{chat_id:s.tg_id,text:`${em("❌","cross")} <b>Подписка истекла</b>\n\nТариф «${esc(s.plan_title)}» закончился.`,parse_mode:"HTML",reply_markup:{inline_keyboard:[[btn("Купить VPN","v:buy","money")]]}}).catch(()=>{});
        db.prepare("INSERT OR REPLACE INTO notified_expiry(tg_id,level,notified_at) VALUES(?,?,?)").run(s.tg_id,"expired",now());
        db.prepare("UPDATE subscriptions SET is_active=0,updated_at=? WHERE tg_id=?").run(now(),s.tg_id);
      }
    } catch(e) { console.error("[ExpiryJob]",e.message); }
  };
  setTimeout(runJob, 20 * 1000);
  setInterval(runJob, 60*60*1000);
}

/* ═══════════════════════════════════════════════════════════════════════════
   WEBHOOK SERVER
   ═══════════════════════════════════════════════════════════════════════════ */
function startWebhookServer() {
  const server = http.createServer(async(req,res)=>{
    try {
      const url=new URL(req.url||"/",`http://${req.headers.host||"localhost"}`);
      if(req.method==="GET"&&url.pathname==="/healthz"){res.writeHead(200);res.end('{"ok":true}');return;}

      const chunks=[]; let size=0;
      await new Promise(r=>{req.on("data",c=>{size+=c.length;if(size<1048576)chunks.push(c);});req.on("end",r);req.on("error",r);});
      const raw=Buffer.concat(chunks).toString("utf8");

      // CryptoBot webhook
      if(req.method==="POST"&&url.pathname===CRYPTOBOT_WEBHOOK_PATH){
        const hdr=req.headers["crypto-pay-api-token"]||"";
        if(!CRYPTOBOT_TOKEN||String(hdr)!==CRYPTOBOT_TOKEN){res.writeHead(403);res.end("Forbidden");return;}
        let body={}; try{body=JSON.parse(raw);}catch{}
        if(body.update_type==="invoice_paid"){
          const inv=body.payload;
          if(inv?.invoice_id){
            const cp=db.prepare("SELECT * FROM crypto_payments WHERE invoice_id=?").get(String(inv.invoice_id));
            if(cp&&cp.status==="pending"){
              markCryptoPaid(cp.id);
              const poId=cp.pending_order_id;
              const po=poId?getPendingOrder(poId):getPendingOrderByUser(cp.tg_id);
              if(po&&po.status==="pending"){
                closePendingOrder(po.id);
                completePurchaseAfterPayment(cp.tg_id,po).catch(()=>{});
              }
            }
          }
        }
        res.writeHead(200);res.end("OK");return;
      }

      // FreeKassa webhook
      if(req.method!=="POST"||url.pathname!==fkNotifyPath()){res.writeHead(404);res.end("Not found");return;}
      const remoteIp=getRequestIp(req);
      if(FK_ENABLE_IP_CHECK&&!FK_ALLOWED_IPS.has(remoteIp)){res.writeHead(403);res.end("IP denied");return;}
      const payload=parseBodyByContentType(raw,req.headers["content-type"]);
      if(!validateFkWebhookSign(payload)){res.writeHead(400);res.end("Bad sign");return;}
      if(Number(payload.MERCHANT_ID||payload.merchant_id||0)!==fkShopId()){res.writeHead(400);res.end("Wrong shop");return;}
      const paymentId=String(payload.MERCHANT_ORDER_ID||payload.merchant_order_id||"");
      const fkOrderId=payload.intid||payload.INTID||null;
      const paidAmount=payload.AMOUNT||payload.amount||null;
      const credited=await creditFkPayment(paymentId,fkOrderId,paidAmount);
      if(credited.ok&&credited.fp){
        const fp=credited.fp;
        const poId=fp.pending_order_id;
        const po=poId?getPendingOrder(poId):getPendingOrderByUser(fp.tg_id);
        if(po&&po.status==="pending"){
          closePendingOrder(po.id);
          completePurchaseAfterPayment(fp.tg_id,po).catch(()=>{});
        }
      }
      res.writeHead(200);res.end("YES");
    } catch(e) { console.error("[webhook]",e); res.writeHead(500);res.end("Error"); }
  });
  server.listen(FK_PORT,"0.0.0.0",()=>console.log(`[Webhook] :${FK_PORT}`));
}

/* ═══════════════════════════════════════════════════════════════════════════
   POLL + BOOT
   ═══════════════════════════════════════════════════════════════════════════ */
async function poll() {
  console.log("🤖 VPN Bot запущен.");
  while(true){
    try{
      const ups=await tg("getUpdates",{timeout:30,offset,allowed_updates:["message","callback_query"]});
      for(const u of ups){
        offset=u.update_id+1;
        if(u.message) handleMessage(u.message).catch(e=>console.error("[msg]",e.message));
        else if(u.callback_query) handleCallback(u.callback_query).catch(e=>console.error("[cb]",e.message));
      }
    }catch(e){console.error("[poll]",e.message);await sleep(2000);}
  }
}

async function boot() {
  init();
  await tg("setMyCommands",{commands:[
    {command:"start",description:"Перезапустить бота"},
    {command:"sub",description:"Моя подписка"},
  ]}).catch(()=>{});
  await ensureFkServerIp();
  startWebhookServer();
  startFkExpireJob();
  startExpiryNotificationJob();
  startSubSyncJob();
  poll();
}

boot().catch(e=>{console.error("[boot]",e);process.exit(1);});
