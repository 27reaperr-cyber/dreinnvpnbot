require("dotenv").config();
const path      = require("path");
const crypto    = require("crypto");
const fs        = require("fs");
const fsp       = fs.promises;
const { spawn } = require("child_process");
const Database  = require("better-sqlite3");

// ─────────────────────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────────────────────
const TOKEN            = process.env.TELEGRAM_BOT_TOKEN    || "";
const API              = (process.env.VPN_API_BASE_URL     || "").replace(/\/+$/, "");
const APP_SECRET       = process.env.APP_SECRET            || "";
const ADMIN_ID         = Number(process.env.ADMIN_TELEGRAM_ID || 0);
const DB_FILE          = process.env.SQLITE_PATH           || path.join(__dirname, "bot.db");
const NEWS_URL         = process.env.BOT_NEWS_URL          || "";
const SUPPORT_URL      = process.env.BOT_SUPPORT_URL       || "";
const FREE_PROXY       = process.env.BOT_FREE_PROXY_URL    || "";
const BOT_USERNAME     = process.env.BOT_USERNAME          || "";
// CryptoBot
const CRYPTOBOT_TOKEN  = process.env.CRYPTOBOT_TOKEN       || "";
const CRYPTOBOT_API    = "https://pay.crypt.bot/api";
const USDT_FALLBACK    = Number(process.env.CRYPTOBOT_FALLBACK_RATE || 90);
const CRYPTO_MIN_RUB   = Number(process.env.CRYPTOBOT_MIN_RUB      || 50);
const CRYPTO_INVOICE_TTL = 3600;

if (!TOKEN || !API || !APP_SECRET || !ADMIN_ID) {
  console.error("Отсутствуют обязательные env: TELEGRAM_BOT_TOKEN, VPN_API_BASE_URL, APP_SECRET, ADMIN_TELEGRAM_ID");
  process.exit(1);
}

const TG_BASE = `https://api.telegram.org/bot${TOKEN}`;
let   offset  = 0;
const db      = new Database(DB_FILE);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");

// ─────────────────────────────────────────────────────────────────────────────
// i18n — Translations
// ─────────────────────────────────────────────────────────────────────────────
const I18N = {
  ru: {
    // Buttons — navigation
    btn_back:       "« Назад",
    btn_home:       "« Главное меню",
    btn_profile:    "👤 Профиль",
    btn_buy:        "💳 Купить VPN",
    btn_ref:        "🤝 Рефералы",
    btn_about:      "ℹ️ О нас",
    btn_lang:       "🌐 Язык",
    btn_guide:      "📋 Инструкции",
    btn_sub:        "⭐ Моя подписка",
    btn_sub_active: "⭐ Открыть подписку",
    btn_hist:       "🗂 История",
    btn_other:      "⚙️ Остальное",
    btn_other_topup:"💰 Пополнение",
    btn_other_gift: "🎁 Подарить подписку",
    btn_back_profile:"« Назад в профиль",
    btn_topup:      "💰 Способы пополнения",
    btn_buy_sub:    "💳 Купить подписку",
    btn_gift_send:  "🎁 Подарить",
    btn_invite:     "📨 Пригласить друга",

    btn_qr:         "📷 QR-код подписки",
    btn_ref_code:   "🔄 Сменить код реферала",
    ref_code_confirm: "⚠️ <b>Сменить реферальный код?</b>\n\n<i>Все старые ссылки перестанут работать.</i>",
    sub_qr_caption: "📷 <b>QR-код подписки</b>\n\n<i>Отсканируйте камерой или приложением для подключения.</i>",
    btn_ref_hist:   "📋 История начислений",
    btn_support:    "💬 Поддержка",
    btn_privacy:    "🔒 Политика конфиденциальности",
    btn_terms:      "📄 Пользовательское соглашение",
    btn_status:     "📊 Статус серверов",
    btn_proxy:      "🆓 Бесплатные прокси",
    btn_copy_link:  "📋 Скопировать ссылку",
    btn_connect:    "📲 Подключить устройство",
    btn_renew:      "🔄 Продлить",
    btn_gift_tab:   "🎁 Подарить подписку",
    btn_confirm:    "✅ Подтвердить",
    btn_cancel:     "❌ Отмена",
    btn_check:      "✅ Проверить оплату",
    btn_pay_crypto: "💎 Crypto Bot (USDT)",
    btn_pay_other:  "💳 Другие способы оплаты",
    // Language
    lang_title:  "🌐 <b>Выбор языка</b>",
    lang_current:"Текущий язык",
    lang_ru:     "🇷🇺 Русский",
    lang_en:     "🇬🇧 English",
    // Home
    home_title:  (name) => `<b>${esc(name||"")}</b>`,
    home_balance:(bal) => `<blockquote>Баланс: <b>${rub(bal)}</b></blockquote>`,
    home_sub_ok: (days) => `<i>Подписка активна — осталось ${days} дн.</i>`,
    // Profile
    prof_title:  "<b>Профиль</b>",
    prof_bal:    (v) => `Баланс: <b>${rub(v)}</b>`,
    prof_refs:   (v) => `Рефералов: <b>${v}</b>`,
    prof_id:     (v) => `ID: <code>${v}</code>`,
    // Subscription
    sub_title:   "<b>Моя подписка</b>",
    sub_none:    "Активная подписка не найдена.\n\n<i>Оформите тариф в разделе «Купить VPN».</i>",
    sub_plan:    (v) => `Тариф: <b>${esc(v)}</b>`,
    sub_exp:     (v) => `Истекает: <b>${dt(v)}</b>`,
    sub_left:    (d,h,m) => `Осталось: <b>${d} дн. ${h} ч. ${m} мин.</b>`,
    sub_devices: "До 3 устройств",
    sub_link_hdr:"Ссылка подписки:",
    // Buy
    buy_title:   "<b>Тарифы VPN</b>",
    buy_balance: (v) => `<blockquote>Ваш баланс: <b>${rub(v)}</b></blockquote>`,
    buy_active:  "<i>⚠️ Подписка уже активна. Купить новую можно после истечения.</i>",
    buy_new:     "<i>Выберите тариф для оформления.</i>",
    // Topup
    topup_title: "<b>Способы пополнения</b>",
    topup_other: (v) => v || "<i>Другие способы не настроены.</i>",
    // Referrals
    ref_title:   "<b>Реферальная программа</b>",
    ref_desc:    (pct) => `Приглашайте друзей и получайте <b>${pct}%</b> с каждой их покупки.`,
    ref_invited: (v) => `Приглашено: <b>${v}</b>`,
    ref_earned:  (v) => `Заработано всего: <b>${rub(v)}</b>`,

    ref_link_hdr:"Ваша реферальная ссылка:",
    ref_bonus:   (pct, amt) => `<blockquote>+${rub(amt)} — реф. вознаграждение ${pct}%</blockquote>`,
    // Gift
    gift_title:  "<b>Подарить подписку</b>",
    gift_choose: "Выберите тариф:",
    gift_recv:   "<b>Выберите получателя</b>",
    gift_recv_d: "Выберите из списка или введите ID:",
    gift_confirm_title: "<b>Подтверждение подарка</b>",
    gift_to:     (v) => `Получатель: <b>${esc(v)}</b>`,
    gift_plan:   (v) => `Тариф: <b>${esc(v)}</b>`,
    gift_price:  (v) => `Спишется: <b>${rub(v)}</b>`,
    gift_after:  (v) => `Баланс после: <b>${rub(v)}</b>`,
    gift_sent:   "<b>Подарок отправлен!</b>",
    gift_rcvd:   "<b>Вам подарили подписку!</b>",
    gift_no_bal: (need,have) => `Нужно ${rub(need)}, у вас ${rub(have)}`,
    gift_self:   "Нельзя подарить самому себе.",
    gift_enter_id: "Введите Telegram ID или @username получателя:",
    // About
    about_title: "<b>О сервисе</b>",
    about_text:  "<i>Надёжный VPN с быстрой выдачей ключа, продлением через Telegram и реферальной программой.</i>",
    // Guide — stored in settings, parsed at render time
    guide_title: "<b>Инструкция по подключению</b>",
    // Confirm buy
    confirm_title: (mode) => `<b>${mode==="renew"?"Продление подписки":"Покупка подписки"}</b>`,
    confirm_plan:  (v) => `Тариф: <b>${esc(v)}</b>`,
    confirm_price: (v) => `Стоимость: <b>${rub(v)}</b>`,
    confirm_bal:   (v) => `Баланс: <b>${rub(v)}</b>`,
    confirm_after: (v) => `После оплаты: <b>${rub(v)}</b>`,
    confirm_low:   "⚠️ <i>Недостаточно средств. Пополните баланс.</i>",
    confirm_ok:    "Подтвердите оплату ↓",
    // Success buy
    success_title: "<b>Оплата прошла успешно!</b>",
    success_plan:  (v) => `Тариф: <b>${esc(v)}</b>`,
    success_paid:  (v) => `Списано: <b>${rub(v)}</b>`,
    success_bal:   (v) => `Баланс: <b>${rub(v)}</b>`,
    success_exp:   (v) => `Истекает: <b>${dt(v)}</b>`,

    // Crypto
    crypto_title:  "<b>Пополнение через Crypto Bot</b>",
    crypto_desc:   "Оплата в USDT (TRC20). Мгновенное зачисление.",
    crypto_min:    (v) => `Минимум: <b>${rub(v)}</b>`,
    crypto_rate:   (v) => `Курс USDT: <b>${v.toFixed(2)} ₽</b>`,
    crypto_enter:  "Введите сумму в рублях:",
    crypto_inv:    "<b>Счёт создан</b>",
    crypto_sum:    (rub_,usdt) => `Сумма: <b>${rub_}</b> → <b>${usdt} USDT</b>`,
    crypto_steps:  "1 — Нажмите «Оплатить»\n2 — Переведите USDT в @CryptoBot\n3 — Вернитесь и проверьте оплату",
    crypto_ttl:    "<i>Счёт действителен 1 час.</i>",
    crypto_ok:     (v) => `<b>Зачислено ${rub(v)}</b>`,
    // Purchases history
    ph_title:      "<b>История покупок</b>",
    ph_empty:      "Покупок пока нет.",
    ph_page:       (p,t) => `Страница ${p+1} из ${t}`,
    // Ref history
    rh_title:      "<b>История начислений</b>",
    rh_empty:      "Начислений пока нет.",
    // Other
    other_title:   "<b>Настройки</b>",
    other_proxy:   "🆓 Бесплатные прокси для Telegram",
  },
  en: {
    btn_back:       "« Back",
    btn_home:       "« Main menu",
    btn_profile:    "👤 Profile",
    btn_buy:        "💳 Buy VPN",
    btn_ref:        "🤝 Referrals",
    btn_about:      "ℹ️ About",
    btn_lang:       "🌐 Language",
    btn_guide:      "📋 Guide",
    btn_sub:        "⭐ My subscription",
    btn_sub_active: "⭐ Open subscription",
    btn_hist:       "🗂 History",
    btn_other:      "⚙️ Other",
    btn_other_topup:"💰 Top up",
    btn_other_gift: "🎁 Gift subscription",
    btn_back_profile:"« Back to profile",
    btn_topup:      "💰 Top up methods",
    btn_buy_sub:    "💳 Buy subscription",
    btn_gift_send:  "🎁 Gift",
    btn_invite:     "📨 Invite a friend",

    btn_qr:         "📷 Subscription QR Code",
    btn_ref_code:   "🔄 Reset ref code",
    ref_code_confirm: "⚠️ <b>Reset referral code?</b>\n\n<i>All old links will stop working.</i>",
    sub_qr_caption: "📷 <b>Subscription QR Code</b>\n\n<i>Scan with your camera or VPN app to connect.</i>",
    btn_ref_hist:   "📋 Earnings history",
    btn_support:    "💬 Support",
    btn_privacy:    "🔒 Privacy policy",
    btn_terms:      "📄 Terms of service",
    btn_status:     "📊 Server status",
    btn_proxy:      "🆓 Free proxies",
    btn_copy_link:  "📋 Copy link",
    btn_connect:    "📲 Connect device",
    btn_renew:      "🔄 Renew",
    btn_gift_tab:   "🎁 Gift subscription",
    btn_confirm:    "✅ Confirm",
    btn_cancel:     "❌ Cancel",
    btn_check:      "✅ Check payment",
    btn_pay_crypto: "💎 Crypto Bot (USDT)",
    btn_pay_other:  "💳 Other payment methods",
    lang_title:  "🌐 <b>Language</b>",
    lang_current:"Current language",
    lang_ru:     "🇷🇺 Русский",
    lang_en:     "🇬🇧 English",
    home_title:  (name) => `<b>${esc(name||"")}</b>`,
    home_balance:(bal) => `<blockquote>Balance: <b>${rub(bal)}</b></blockquote>`,
    home_sub_ok: (days) => `<i>Subscription active — ${days} days left</i>`,
    prof_title:  "<b>Profile</b>",
    prof_bal:    (v) => `Balance: <b>${rub(v)}</b>`,
    prof_refs:   (v) => `Referrals: <b>${v}</b>`,
    prof_id:     (v) => `ID: <code>${v}</code>`,
    sub_title:   "<b>My subscription</b>",
    sub_none:    "No active subscription found.\n\n<i>Get a plan in «Buy VPN».</i>",
    sub_plan:    (v) => `Plan: <b>${esc(v)}</b>`,
    sub_exp:     (v) => `Expires: <b>${dt(v)}</b>`,
    sub_left:    (d,h,m) => `Remaining: <b>${d}d ${h}h ${m}m</b>`,
    sub_devices: "Up to 3 devices",
    sub_link_hdr:"Subscription link:",
    buy_title:   "<b>VPN Plans</b>",
    buy_balance: (v) => `<blockquote>Your balance: <b>${rub(v)}</b></blockquote>`,
    buy_active:  "<i>⚠️ Subscription is active. You can buy a new one after it expires.</i>",
    buy_new:     "<i>Choose a plan to subscribe.</i>",
    topup_title: "<b>Top up methods</b>",
    topup_other: (v) => v || "<i>Other methods not configured.</i>",
    ref_title:   "<b>Referral program</b>",
    ref_desc:    (pct) => `Invite friends and earn <b>${pct}%</b> of each purchase.`,
    ref_invited: (v) => `Invited: <b>${v}</b>`,
    ref_earned:  (v) => `Total earned: <b>${rub(v)}</b>`,

    ref_link_hdr:"Your referral link:",
    ref_bonus:   (pct, amt) => `<blockquote>+${rub(amt)} referral bonus ${pct}%</blockquote>`,
    gift_title:  "<b>Gift subscription</b>",
    gift_choose: "Choose a plan:",
    gift_recv:   "<b>Choose recipient</b>",
    gift_recv_d: "Select from list or enter ID:",
    gift_confirm_title: "<b>Confirm gift</b>",
    gift_to:     (v) => `To: <b>${esc(v)}</b>`,
    gift_plan:   (v) => `Plan: <b>${esc(v)}</b>`,
    gift_price:  (v) => `Will be charged: <b>${rub(v)}</b>`,
    gift_after:  (v) => `Balance after: <b>${rub(v)}</b>`,
    gift_sent:   "<b>Gift sent!</b>",
    gift_rcvd:   "<b>You received a gift subscription!</b>",
    gift_no_bal: (need,have) => `Need ${rub(need)}, you have ${rub(have)}`,
    gift_self:   "You can't gift to yourself.",
    gift_enter_id: "Enter recipient's Telegram ID or @username:",
    about_title: "<b>About</b>",
    about_text:  "<i>Reliable VPN with instant key generation, Telegram renewal and referral program.</i>",
    guide_title: "<b>Connection guide</b>",
    confirm_title: (mode) => `<b>${mode==="renew"?"Renew subscription":"Buy subscription"}</b>`,
    confirm_plan:  (v) => `Plan: <b>${esc(v)}</b>`,
    confirm_price: (v) => `Price: <b>${rub(v)}</b>`,
    confirm_bal:   (v) => `Balance: <b>${rub(v)}</b>`,
    confirm_after: (v) => `After payment: <b>${rub(v)}</b>`,
    confirm_low:   "⚠️ <i>Insufficient balance. Please top up.</i>",
    confirm_ok:    "Confirm payment ↓",
    success_title: "<b>Payment successful!</b>",
    success_plan:  (v) => `Plan: <b>${esc(v)}</b>`,
    success_paid:  (v) => `Charged: <b>${rub(v)}</b>`,
    success_bal:   (v) => `Balance: <b>${rub(v)}</b>`,
    success_exp:   (v) => `Expires: <b>${dt(v)}</b>`,

    crypto_title:  "<b>Top up via Crypto Bot</b>",
    crypto_desc:   "Pay in USDT (TRC20). Instant credit.",
    crypto_min:    (v) => `Minimum: <b>${rub(v)}</b>`,
    crypto_rate:   (v) => `USDT rate: <b>${v.toFixed(2)} ₽</b>`,
    crypto_enter:  "Enter amount in rubles:",
    crypto_inv:    "<b>Invoice created</b>",
    crypto_sum:    (rub_,usdt) => `Amount: <b>${rub_}</b> → <b>${usdt} USDT</b>`,
    crypto_steps:  "1 — Tap «Pay»\n2 — Send USDT in @CryptoBot\n3 — Come back and check payment",
    crypto_ttl:    "<i>Invoice valid for 1 hour.</i>",
    crypto_ok:     (v) => `<b>Credited ${rub(v)}</b>`,
    ph_title:      "<b>Purchase history</b>",
    ph_empty:      "No purchases yet.",
    ph_page:       (p,t) => `Page ${p+1} of ${t}`,
    rh_title:      "<b>Earnings history</b>",
    rh_empty:      "No earnings yet.",
    other_title:   "<b>Settings</b>",
    other_proxy:   "🆓 Free Telegram proxies",
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────
const now     = () => Date.now();
const esc     = (s) => String(s || "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
const rub     = (n) => `${Number(n||0).toLocaleString("ru-RU")} ₽`;
const dt      = (ts) => ts ? new Date(ts).toLocaleDateString("ru-RU")  : "—";
const dts     = (ts) => ts ? new Date(ts).toLocaleString("ru-RU")      : "—";
const isAdmin = (id) => Number(id) === ADMIN_ID;
const sleep   = (ms) => new Promise(r => setTimeout(r, ms));
const refLink = (code) => BOT_USERNAME
  ? `https://t.me/${BOT_USERNAME}?start=partner_${code}`
  : `https://t.me/?start=partner_${code}`;

// [Label|URL] → <a href="URL">Label</a>
function parseLinks(text) {
  return String(text||"").replace(/\[([^\]|]+)\|([^\]]+)\]/g, (_, label, url) =>
    `<a href="${url.trim()}">${esc(label.trim())}</a>`);
}

// ─────────────────────────────────────────────────────────────────────────────
// Settings & dynamic links
// ─────────────────────────────────────────────────────────────────────────────
function setting(k, f = "")  { return db.prepare("SELECT value v FROM settings WHERE key=?").get(k)?.v ?? f; }
function setSetting(k, v)    { db.prepare("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").run(k, String(v ?? "")); }
function delSetting(k)       { db.prepare("DELETE FROM settings WHERE key=?").run(k); }

// Configurable links (fallback to env / hardcoded defaults)
// Normalize: @username → https://t.me/username, bare username → same
function normalizeUrl(v) {
  if(!v) return "";
  v = String(v).trim();
  if(v.startsWith("@")) return `https://t.me/${v.slice(1)}`;
  if(!v.startsWith("http") && !v.startsWith("tg://")) return `https://t.me/${v}`;
  return v;
}
const lnk = {
  support : () => normalizeUrl(setting("url_support") || SUPPORT_URL),
  privacy : () => normalizeUrl(setting("url_privacy") || ""),
  terms   : () => normalizeUrl(setting("url_terms")   || ""),
  proxy   : () => normalizeUrl(setting("url_proxy")   || FREE_PROXY),
  news    : () => normalizeUrl(setting("url_news")    || NEWS_URL),
  status  : () => normalizeUrl(setting("url_status")  || "https://dreinnvpn.vercel.app"),
};

// Per-section header images
function viewImg(view) { return setting(`img_${view}`) || ""; }

// ─────────────────────────────────────────────────────────────────────────────
// Language helpers
// ─────────────────────────────────────────────────────────────────────────────
function getLang(uid)     { return db.prepare("SELECT lang FROM users WHERE tg_id=?").get(Number(uid))?.lang || "ru"; }
function setLang(uid, lg) { db.prepare("UPDATE users SET lang=? WHERE tg_id=?").run(lg, Number(uid)); }
function T(uid)           { return I18N[getLang(uid)] || I18N.ru; }

// ─────────────────────────────────────────────────────────────────────────────
// DB helpers
// ─────────────────────────────────────────────────────────────────────────────
function user(id)     { return db.prepare("SELECT * FROM users WHERE tg_id=?").get(Number(id)); }
function sub(id)      { return db.prepare("SELECT * FROM subscriptions WHERE tg_id=?").get(Number(id)); }
function activeSub(s) { return !!(s && s.is_active===1 && s.expires_at>now() && s.sub_url); }
function tariffs()    { return db.prepare("SELECT * FROM tariffs ORDER BY sort_order").all(); }
function tariff(c)    { return db.prepare("SELECT * FROM tariffs WHERE code=?").get(c); }

function updateBalance(uid, delta) {
  if (!user(uid)) throw new Error("NO_USER");
  const d = Number(delta);
  if (d < 0) {
    // Atomic deduction: only succeeds if balance stays >= 0
    const res = db.prepare(
      "UPDATE users SET balance_rub=balance_rub+?,updated_at=? WHERE tg_id=? AND balance_rub+?>=0"
    ).run(d, now(), Number(uid), d);
    if (res.changes === 0) throw new Error("NO_MONEY");
  } else {
    db.prepare("UPDATE users SET balance_rub=balance_rub+?,updated_at=? WHERE tg_id=?")
      .run(d, now(), Number(uid));
  }
  return Number(db.prepare("SELECT balance_rub FROM users WHERE tg_id=?").get(Number(uid))?.balance_rub ?? 0);
}

function usersPage(page, me, size=8) {
  const p=Math.max(0,Number(page||0)), off=p*size;
  const items = db.prepare("SELECT tg_id,username,first_name FROM users WHERE tg_id!=? ORDER BY updated_at DESC LIMIT ? OFFSET ?").all(Number(me),size,off);
  const total = Number(db.prepare("SELECT COUNT(*) c FROM users WHERE tg_id!=?").get(Number(me)).c||0);
  return {items,total,page:p,size};
}
function addReferralReward(buyerId, amount) {
  const b=user(buyerId); if(!b||!b.referred_by) return;
  const r=user(b.referred_by); if(!r) return;
  const pct    = Math.max(0,Math.min(100,Number(setting("ref_percent","30"))||30));
  const reward = Math.floor((Number(amount)*pct)/100);
  if(reward<=0) return;
  // Reward goes directly to main balance (not separate ref_balance)
  updateBalance(r.tg_id, reward);
  db.prepare("UPDATE users SET ref_earned=ref_earned+?,updated_at=? WHERE tg_id=?").run(reward,now(),Number(r.tg_id));
  db.prepare("INSERT INTO referrals(referrer_tg_id,invited_tg_id,amount_rub,percent,reward_rub,created_at) VALUES(?,?,?,?,?,?)").run(Number(r.tg_id),Number(buyerId),Number(amount),pct,reward,now());
  const isRu=getLang(r.tg_id)==="ru";
  const msg=isRu
    ? `<blockquote>+${rub(reward)} — реферальное вознаграждение ${pct}%</blockquote>`
    : `<blockquote>+${rub(reward)} — referral bonus ${pct}%</blockquote>`;
  tg("sendMessage",{chat_id:r.tg_id,text:msg,parse_mode:"HTML"}).catch(()=>{});
}

// Withdrawal system removed — ref rewards go directly to main balance

function setAdminState(id,state,payload="") { db.prepare("INSERT INTO admin_states(admin_tg_id,state,payload,updated_at) VALUES(?,?,?,?) ON CONFLICT(admin_tg_id) DO UPDATE SET state=excluded.state,payload=excluded.payload,updated_at=excluded.updated_at").run(Number(id),state,String(payload),now()); }
function getAdminState(id)                  { return db.prepare("SELECT * FROM admin_states WHERE admin_tg_id=?").get(Number(id)); }
function clearAdminState(id)                { db.prepare("DELETE FROM admin_states WHERE admin_tg_id=?").run(Number(id)); }
function setUserState(id,state,payload="")  { db.prepare("INSERT INTO user_states(tg_id,state,payload,updated_at) VALUES(?,?,?,?) ON CONFLICT(tg_id) DO UPDATE SET state=excluded.state,payload=excluded.payload,updated_at=excluded.updated_at").run(Number(id),state,String(payload),now()); }
function getUserState(id)                   { return db.prepare("SELECT * FROM user_states WHERE tg_id=?").get(Number(id)); }
function clearUserState(id)                 { db.prepare("DELETE FROM user_states WHERE tg_id=?").run(Number(id)); }

// ─────────────────────────────────────────────────────────────────────────────
// DB init + migrations
// ─────────────────────────────────────────────────────────────────────────────
function init() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS users(
      tg_id INTEGER PRIMARY KEY, username TEXT NOT NULL DEFAULT '', first_name TEXT NOT NULL DEFAULT '',
      balance_rub INTEGER NOT NULL DEFAULT 0, ref_balance_rub INTEGER NOT NULL DEFAULT 0,
      referred_by INTEGER, ref_code TEXT, ref_earned INTEGER NOT NULL DEFAULT 0,
      payout_method TEXT NOT NULL DEFAULT '', payout_details TEXT NOT NULL DEFAULT '',
      last_chat_id INTEGER, last_menu_id INTEGER,
      lang TEXT NOT NULL DEFAULT 'ru',
      created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_users_ref_code ON users(ref_code);
    CREATE TABLE IF NOT EXISTS subscriptions(
      tg_id INTEGER PRIMARY KEY, plan_code TEXT NOT NULL DEFAULT '', plan_title TEXT NOT NULL DEFAULT '',
      sub_url TEXT NOT NULL DEFAULT '', expires_at INTEGER, is_active INTEGER NOT NULL DEFAULT 0,
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
    CREATE INDEX IF NOT EXISTS idx_wr_tg_id  ON withdrawal_requests(tg_id);
    CREATE TABLE IF NOT EXISTS crypto_payments(
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      tg_id       INTEGER NOT NULL,
      amount_rub  INTEGER NOT NULL,
      amount_usdt REAL    NOT NULL,
      rate_rub    REAL    NOT NULL,
      invoice_id  TEXT    NOT NULL,
      pay_url     TEXT    NOT NULL DEFAULT '',
      status      TEXT    NOT NULL DEFAULT 'pending',
      created_at  INTEGER NOT NULL,
      updated_at  INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_cp_tg_id      ON crypto_payments(tg_id);
    CREATE INDEX IF NOT EXISTS idx_cp_invoice_id ON crypto_payments(invoice_id);
    CREATE INDEX IF NOT EXISTS idx_cp_status     ON crypto_payments(status);
  `);

  // Migrations — idempotent
  for (const m of [
    "ALTER TABLE users ADD COLUMN ref_balance_rub INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE users ADD COLUMN ref_earned INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE users ADD COLUMN lang TEXT NOT NULL DEFAULT 'ru'",
  ]) { try { db.exec(m); } catch {} }

  // Seed tariffs
  const st = db.prepare("INSERT INTO tariffs(code,title,duration_days,price_rub,sort_order) VALUES(?,?,?,?,?) ON CONFLICT(code) DO NOTHING");
  [["m1","1 месяц",30,100,1],["m6","6 месяцев",180,600,2],["y1","1 год",365,900,3]].forEach(r=>st.run(...r));

  // Seed settings
  const ss = db.prepare("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO NOTHING");
  const defaults = [
    ["payment_methods",""],["gif_main_menu",""],["gif_purchase_success",""],["gif_gift_success",""],["gif_broadcast",""],
    ["ref_percent","30"],
    ["guide_text","📋 <b>Инструкция по подключению VPN:</b>\n\n1. Скачайте [Happ|https://www.happ.su/main/ru], [v2RayTun|https://v2raytun.com/] или другие XRay клиенты.\n2. Скопируйте ваш ключ доступа из раздела «Моя подписка» и вставьте его в клиент.\n3. Всё готово! Теперь вы можете подключиться к защищённому интернету.\n\n💬 Если возникнут вопросы — обращайтесь в [поддержку|https://t.me/ke9ab]"],
    // Per-section images (empty = no image)
    ["img_home",""],["img_sub",""],["img_buy",""],["img_bal",""],["img_ref",""],
    ["img_gift",""],["img_guide",""],["img_about",""],["img_topup",""],
    // Configurable links
    ["url_support",""],["url_privacy",""],["url_terms",""],["url_proxy",""],["url_news",""],["url_status","https://dreinnvpn.vercel.app"],
  ];
  defaults.forEach(([k,v])=>ss.run(k,v));
}

// ─────────────────────────────────────────────────────────────────────────────
// Telegram API
// ─────────────────────────────────────────────────────────────────────────────
async function tg(method, params) {
  const ctrl = new AbortController();
  const tid = setTimeout(()=>ctrl.abort(), 30000);
  try {
    const r = await fetch(`${TG_BASE}/${method}`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(params),signal:ctrl.signal});
    const j = await r.json().catch(()=>({}));
    if(!r.ok||j.ok===false) throw new Error(j.description||`TG HTTP ${r.status}`);
    return j.result;
  } finally {
    clearTimeout(tid);
  }
}

async function tgSendFile(method, chatId, fieldName, filePath, extra={}) {
  const buf  = await fsp.readFile(filePath);
  const form = new FormData();
  form.append("chat_id", String(chatId));
  form.append(fieldName, new Blob([buf],{type:"application/octet-stream"}), path.basename(filePath));
  for(const [k,v] of Object.entries(extra)) form.append(k,String(v));
  const r = await fetch(`${TG_BASE}/${method}`,{method:"POST",body:form});
  const j = await r.json().catch(()=>({}));
  if(!r.ok||j.ok===false) throw new Error(j.description||`TG HTTP ${r.status}`);
  return j.result;
}

/**
 * Smart send/edit function supporting both photo and text messages.
 * If photo is provided:  sendPhoto (caption) or editMessageMedia
 * If no photo:           sendMessage / editMessageText
 * Gracefully handles mismatches (photo→text, text→photo) by deleting and resending.
 */
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
      } catch(e) {
        // Was a text message or error — delete it and send fresh photo
        await tg("deleteMessage",{chat_id:chatId,message_id:msgId}).catch(()=>{});
      }
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
        // Was a photo message — delete it
        await tg("deleteMessage",{chat_id:chatId,message_id:msgId}).catch(()=>{});
      }
    }
    const m = await tg("sendMessage",{chat_id:chatId,text,parse_mode:"HTML",disable_web_page_preview:true,reply_markup:kb});
    return Number(m.message_id);
  }
}

async function gif(chatId, key) {
  const g = setting(key,""); if(g) await tg("sendAnimation",{chat_id:chatId,animation:g}).catch(()=>{});
}

// ─────────────────────────────────────────────────────────────────────────────
// CryptoBot API
// ─────────────────────────────────────────────────────────────────────────────
let _rateCache = { val: USDT_FALLBACK, ts: 0 };

async function getUsdtRate() {
  if (Date.now() - _rateCache.ts < 5 * 60 * 1000) return _rateCache.val;
  if (!CRYPTOBOT_TOKEN) return USDT_FALLBACK;
  try {
    const r = await fetch(CRYPTOBOT_API + "/getExchangeRates",
      { headers:{"Crypto-Pay-API-Token":CRYPTOBOT_TOKEN}, signal:AbortSignal.timeout(8000) });
    const d = await r.json().catch(()=>({}));
    const items = d.result || [];
    for (const it of items) {
      if ((it.source||"").toUpperCase()==="USDT" && (it.target||"").toUpperCase()==="RUB") {
        const rate = parseFloat(it.rate);
        if (rate > 1) { _rateCache={val:rate,ts:Date.now()}; return rate; }
      }
    }
    for (const it of items) {
      if ((it.source||"").toUpperCase()==="RUB" && (it.target||"").toUpperCase()==="USDT") {
        const rate = parseFloat(it.rate);
        if (rate > 0) { const inv=Math.round(1/rate*100)/100; _rateCache={val:inv,ts:Date.now()}; return inv; }
      }
    }
  } catch(e) { console.warn("[CryptoBot] getRate:", e.message); }
  return USDT_FALLBACK;
}

async function createCryptoInvoice(amountRub) {
  if (!CRYPTOBOT_TOKEN) return null;
  try {
    const rate       = await getUsdtRate();
    const amountUsdt = Math.max(0.01, Math.round(amountRub / rate * 100) / 100);
    const r = await fetch(CRYPTOBOT_API + "/createInvoice", {
      method:"POST",
      headers:{"Crypto-Pay-API-Token":CRYPTOBOT_TOKEN,"Content-Type":"application/json"},
      body:JSON.stringify({asset:"USDT",amount:String(amountUsdt),description:`Пополнение ${amountRub} ₽`,expires_in:CRYPTO_INVOICE_TTL}),
      signal:AbortSignal.timeout(12000),
    });
    const d = await r.json().catch(()=>({}));
    if (d.ok) { const inv=d.result; return {invoiceId:String(inv.invoice_id),payUrl:inv.pay_url,rate,amountUsdt}; }
    console.error("[CryptoBot] createInvoice:", JSON.stringify(d));
  } catch(e) { console.error("[CryptoBot] createInvoice:", e.message); }
  return null;
}

async function checkCryptoInvoice(invoiceId) {
  if (!CRYPTOBOT_TOKEN) return false;
  try {
    const r = await fetch(CRYPTOBOT_API + "/getInvoices?invoice_ids=" + invoiceId,
      { headers:{"Crypto-Pay-API-Token":CRYPTOBOT_TOKEN}, signal:AbortSignal.timeout(10000) });
    const d = await r.json().catch(()=>({}));
    const items = (d.result||{}).items || [];
    return items.length > 0 && items[0].status === "paid";
  } catch(e) { console.error("[CryptoBot] checkInvoice:", e.message); }
  return false;
}

function createCryptoPaymentRow(tgId, amountRub, amountUsdt, rateRub, invoiceId, payUrl) {
  return db.prepare("INSERT INTO crypto_payments(tg_id,amount_rub,amount_usdt,rate_rub,invoice_id,pay_url,status,created_at,updated_at) VALUES(?,?,?,?,?,?,'pending',?,?)")
    .run(Number(tgId),Math.round(amountRub),amountUsdt,rateRub,invoiceId,payUrl,now(),now()).lastInsertRowid;
}
function getCryptoPayment(id)    { return db.prepare("SELECT * FROM crypto_payments WHERE id=?").get(Number(id)); }
function markCryptoPaid(id)      { db.prepare("UPDATE crypto_payments SET status='paid',updated_at=? WHERE id=?").run(now(),Number(id)); }
function markCryptoCancelled(id) { db.prepare("UPDATE crypto_payments SET status='cancelled',updated_at=? WHERE id=?").run(now(),Number(id)); }
function expireOldCryptoPayments(tgId) {
  db.prepare("UPDATE crypto_payments SET status='expired',updated_at=? WHERE tg_id=? AND status='pending' AND created_at<?")
    .run(now(),Number(tgId),now()-CRYPTO_INVOICE_TTL*1000);
}

// ─────────────────────────────────────────────────────────────────────────────
// DB Import / Export / Restart
// ─────────────────────────────────────────────────────────────────────────────
async function downloadImportFile(fileId) {
  const f = await tg("getFile",{file_id:fileId});
  if(!f?.file_path) throw new Error("file_path not found");
  const resp = await fetch(`https://api.telegram.org/file/bot${TOKEN}/${f.file_path}`);
  if(!resp.ok) throw new Error(`Ошибка скачивания: HTTP ${resp.status}`);
  const buf = Buffer.from(await resp.arrayBuffer());
  if(buf.length<100||buf.slice(0,16).toString("binary")!=="SQLite format 3\x00")
    throw new Error("Файл не является SQLite базой данных");
  const tmp=`${DB_FILE}.import_${Date.now()}.tmp`;
  await fsp.writeFile(tmp,buf);
  return tmp;
}

function restartBotWithFile(tmpPath) {
  try{db.pragma("wal_checkpoint(TRUNCATE)");}catch{}
  try{db.close();}catch{}
  try{fs.copyFileSync(DB_FILE,`${DB_FILE}.backup.${Date.now()}`);}catch{}
  try{fs.renameSync(tmpPath,DB_FILE);}catch(e){console.error("rename failed:",e.message);}
  spawn(process.execPath,[path.join(__dirname,"bot.js")],{cwd:__dirname,detached:true,stdio:"ignore",env:process.env}).unref();
  process.exit(0);
}

function restartBot() {
  try{db.pragma("wal_checkpoint(TRUNCATE)");}catch{}
  try{db.close();}catch{}
  spawn(process.execPath,[path.join(__dirname,"bot.js")],{cwd:__dirname,detached:true,stdio:"ignore",env:process.env}).unref();
  process.exit(0);
}

async function exportDbToAdmin(chatId) {
  try{db.pragma("wal_checkpoint(TRUNCATE)");}catch{}
  await tgSendFile("sendDocument",chatId,"document",DB_FILE,{caption:"📦 База данных"});
}

// ─────────────────────────────────────────────────────────────────────────────
// User helpers
// ─────────────────────────────────────────────────────────────────────────────
function upsertUser(from, chatId) {
  const cur = user(from.id);
  const ref = cur?.ref_code || crypto.randomBytes(5).toString("hex");
  db.prepare(`INSERT INTO users(tg_id,username,first_name,balance_rub,ref_balance_rub,referred_by,ref_code,ref_earned,payout_method,payout_details,last_chat_id,lang,created_at,updated_at)
    VALUES(@id,@u,@f,0,0,NULL,@r,0,'','',@c,'ru',@t,@t)
    ON CONFLICT(tg_id) DO UPDATE SET username=excluded.username,first_name=excluded.first_name,last_chat_id=excluded.last_chat_id,updated_at=excluded.updated_at`)
    .run({id:Number(from.id),u:from.username||"",f:from.first_name||"",r:ref,c:Number(chatId),t:now()});
}
function setMenu(uid,chatId,mid){ db.prepare("UPDATE users SET last_chat_id=?,last_menu_id=?,updated_at=? WHERE tg_id=?").run(Number(chatId),Number(mid),now(),Number(uid)); }
function findRef(code)          { return db.prepare("SELECT * FROM users WHERE ref_code=?").get(String(code||"").trim()); }
function setRef(uid,rid)        { const u=user(uid); if(!u||u.referred_by||Number(uid)===Number(rid)) return; db.prepare("UPDATE users SET referred_by=?,updated_at=? WHERE tg_id=?").run(Number(rid),now(),Number(uid)); }

// ─────────────────────────────────────────────────────────────────────────────
// Purchase logic  (FIXED: renewal adds days, doesn't reset)
// ─────────────────────────────────────────────────────────────────────────────
async function createSubViaApi(target, tr, giftMode) {
  const r = await fetch(`${API}/api/bot-subscription`,{
    method:"POST",
    headers:{"Content-Type":"application/json","x-app-secret":APP_SECRET},
    body:JSON.stringify({telegramUserId:String(target.tg_id),telegramUsername:target.username||"",firstName:target.first_name||"",durationDays:tr.duration_days,name:`VPN ${tr.title}`,description:giftMode?`Подарок: ${tr.title}`:`Тариф: ${tr.title}`}),
  });
  const j = await r.json().catch(()=>({}));
  if(!r.ok) throw new Error(j.error||`API HTTP ${r.status}`);
  return j;
}

async function doPurchase(payerId, receiverId, code, kind) {
  const payer=user(payerId), receiver=user(receiverId), tr=tariff(code);
  if(!payer||!receiver||!tr) throw new Error("INVALID");
  const s=sub(receiverId), act=activeSub(s);
  if(kind==="new"   && act)  throw new Error("ACTIVE");
  if(kind==="renew" && !act) throw new Error("NO_ACTIVE");
  if(Number(payer.balance_rub)<Number(tr.price_rub)) throw new Error("NO_MONEY");

  const api    = await createSubViaApi(receiver,tr,kind==="gift");
  const subUrl = api.subscriptionUrl || api.sub_url || "";
  if(!subUrl) throw new Error("API не вернул ссылку подписки");

  // ── FIX: Renewal adds days to existing expiry ──────────────────────────────
  let newExpiresAt;
  if (kind==="renew") {
    const base = (s && s.expires_at > now()) ? s.expires_at : now();
    newExpiresAt = base + tr.duration_days * 86400000;
  } else if (kind==="gift") {
    const base = (s && s.expires_at > now()) ? s.expires_at : now();
    newExpiresAt = base + tr.duration_days * 86400000;
  } else {
    newExpiresAt = Number(api.subscription?.expiresAt || api.expiresAt || (now() + tr.duration_days*86400000));
  }
  // ──────────────────────────────────────────────────────────────────────────

  db.transaction(()=>{
    updateBalance(payerId,-Number(tr.price_rub));
    addReferralReward(payerId, tr.price_rub); // always reward referrer of payer
    db.prepare("INSERT INTO subscriptions(tg_id,plan_code,plan_title,sub_url,expires_at,is_active,created_at,updated_at) VALUES(?,?,?,?,?,1,?,?) ON CONFLICT(tg_id) DO UPDATE SET plan_code=excluded.plan_code,plan_title=excluded.plan_title,sub_url=excluded.sub_url,expires_at=excluded.expires_at,is_active=1,updated_at=excluded.updated_at")
      .run(Number(receiverId),tr.code,tr.title,subUrl,newExpiresAt,now(),now());
    db.prepare("INSERT INTO purchases(tg_id,tariff_code,tariff_title,amount_rub,kind,created_at) VALUES(?,?,?,?,?,?)").run(Number(payerId),tr.code,tr.title,Number(tr.price_rub),kind,now());
    if(kind==="gift") db.prepare("INSERT INTO gifts(from_tg_id,to_tg_id,tariff_code,tariff_title,amount_rub,created_at) VALUES(?,?,?,?,?,?)").run(Number(payerId),Number(receiverId),tr.code,tr.title,Number(tr.price_rub),now());
  })();
  return {tr, url:subUrl, exp:newExpiresAt};
}

async function buySelf(uid, chatId, msgId, code, mode, cbid) {
  try {
    const res=await doPurchase(uid,uid,code,mode);
    await gif(chatId,"gif_purchase_success");
    const me=user(uid), tx=T(uid);
    const lines=[tx.success_title,"",tx.success_plan(tariffTitle(res.tr,getLang(uid))),tx.success_paid(res.tr.price_rub),tx.success_bal(me.balance_rub),tx.success_exp(res.exp),"",`<code>${esc(res.url)}</code>`];
    const kb={inline_keyboard:[[{text:tx.btn_connect,url:res.url}],[{text:tx.btn_sub,callback_data:"v:sub"},{text:tx.btn_home,callback_data:"v:home"}]]};
    const nm=await renderMsg(chatId,msgId,lines.join("\n"),kb,viewImg("sub"));
    setMenu(uid,chatId,nm);
    await tg("answerCallbackQuery",{callback_query_id:cbid,text:"✅"});
  } catch(e) {
    const tx=T(uid);
    const map={ACTIVE:getLang(uid)==="en"?"Already active. Choose Renew.":"Подписка уже активна.",NO_ACTIVE:getLang(uid)==="en"?"No active sub to renew.":"Нет активной подписки для продления.",NO_MONEY:getLang(uid)==="en"?"Insufficient balance.":"Недостаточно средств."};
    await tg("answerCallbackQuery",{callback_query_id:cbid,text:map[e.message]||e.message,show_alert:true});
    if(e.message==="NO_MONEY") await render(uid,chatId,msgId,"topup");
  }
}

async function askBuyConfirm(uid, chatId, msgId, code, mode, cbid) {
  const tr=tariff(code); if(!tr){await tg("answerCallbackQuery",{callback_query_id:cbid,text:"Тариф не найден",show_alert:true});return;}
  const u=user(uid), tx=T(uid), lang=getLang(uid), diff=Number(u.balance_rub)-Number(tr.price_rub);
  const lines=[
    tx.confirm_title(mode),"",
    tx.confirm_plan(tariffTitle(tr,lang)),
    tx.confirm_price(tr.price_rub),
    tx.confirm_bal(u.balance_rub),
    tx.confirm_after(Math.max(0,diff)),"",
    diff<0?tx.confirm_low:tx.confirm_ok,
  ];
  const kb=diff<0
    ?{inline_keyboard:[[{text:tx.btn_topup,callback_data:"v:topup"}],[{text:tx.btn_back,callback_data:"v:home"}]]}
    :{inline_keyboard:[[{text:tx.btn_confirm,callback_data:`pay:c:${mode}:${code}`}],[{text:tx.btn_cancel,callback_data:"v:home"}]]};
  const nm=await renderMsg(chatId,msgId,lines.join("\n"),kb,viewImg("buy"));
  setMenu(uid,chatId,nm);
  await tg("answerCallbackQuery",{callback_query_id:cbid});
}

// Gift: confirmation step before sending
async function askGiftConfirm(uid, chatId, msgId, code, toId, cbid) {
  if(Number(uid)===Number(toId)){await tg("answerCallbackQuery",{callback_query_id:cbid,text:T(uid).gift_self,show_alert:true});return;}
  const tr=tariff(code), to=user(toId), u=user(uid), tx=T(uid);
  if(!tr){await tg("answerCallbackQuery",{callback_query_id:cbid,text:"Тариф не найден",show_alert:true});return;}
  if(!to){await tg("answerCallbackQuery",{callback_query_id:cbid,text:"Пользователь не найден",show_alert:true});return;}
  if(Number(u.balance_rub)<Number(tr.price_rub)){await tg("answerCallbackQuery",{callback_query_id:cbid,text:tx.gift_no_bal(tr.price_rub,u.balance_rub),show_alert:true});return;}
  const toName=to.first_name||(to.username?`@${to.username}`:`ID ${to.tg_id}`);
  const lang=getLang(uid);
  const lines=[
    tx.gift_confirm_title,"",
    tx.gift_to(toName),
    tx.gift_plan(tariffTitle(tr,lang)),
    tx.gift_price(tr.price_rub),
    tx.gift_after(Number(u.balance_rub)-Number(tr.price_rub)),
  ];
  const kb={inline_keyboard:[
    [{text:tx.btn_confirm,callback_data:`g:cf:${code}:${toId}`}],
    [{text:tx.btn_cancel,callback_data:`g:p:${code}`}],
  ]};
  const nm=await renderMsg(chatId,msgId,lines.join("\n"),kb,viewImg("gift"));
  setMenu(uid,chatId,nm);
  await tg("answerCallbackQuery",{callback_query_id:cbid});
}

async function giftToUser(fromId, toId, code, chatId, msgId, cbid) {
  try {
    if(Number(fromId)===Number(toId)){if(cbid)await tg("answerCallbackQuery",{callback_query_id:cbid,text:T(fromId).gift_self,show_alert:true});return;}
    const res=await doPurchase(fromId,toId,code,"gift");
    await gif(chatId,"gif_gift_success");
    const to=user(toId), me=user(fromId), tx=T(fromId);
    const lines=[tx.gift_sent,"",tx.gift_to(to?.first_name||to?.username||String(toId)),tx.gift_plan(res.tr.title),tx.gift_price(res.tr.price_rub),tx.gift_after(me.balance_rub)];
    const nm=await renderMsg(chatId,msgId,lines.join("\n"),{inline_keyboard:[[{text:tx.btn_gift_send,callback_data:"v:gift"},{text:tx.btn_home,callback_data:"v:home"}]]},viewImg("gift"));
    setMenu(fromId,chatId,nm);
    if(to){
      const rtx=T(to.tg_id);
      tg("sendMessage",{chat_id:to.tg_id,text:[rtx.gift_rcvd,"",rtx.gift_plan(res.tr.title),`${rtx.sub_exp(res.exp)}`,"",`<code>${esc(res.url)}</code>`].join("\n"),parse_mode:"HTML",reply_markup:{inline_keyboard:[[{text:rtx.btn_connect,url:res.url}]]}}).catch(()=>{});
    }
    if(cbid) await tg("answerCallbackQuery",{callback_query_id:cbid,text:"🎁"});
  } catch(e) {
    const tx=T(fromId);
    const map={
      NO_MONEY: getLang(fromId)==="en" ? "Insufficient balance." : "Недостаточно средств.",
      ACTIVE:   getLang(fromId)==="en" ? "Recipient already has an active subscription." : "У получателя уже есть активная подписка.",
    };
    if(cbid) await tg("answerCallbackQuery",{callback_query_id:cbid,text:map[e.message]||e.message,show_alert:true});
    if(msgId) await render(fromId,chatId,msgId,"home");
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Keyboard builders
// ─────────────────────────────────────────────────────────────────────────────
function homeKb(uid) {
  const tx=T(uid);
  const rows=[
    [{text:tx.btn_profile,callback_data:"v:profile"},{text:tx.btn_buy,callback_data:"v:buy"}],
    [{text:tx.btn_ref,callback_data:"v:ref"},{text:tx.btn_about,callback_data:"v:about"}],
    [{text:tx.btn_guide,callback_data:"v:guide"},{text:tx.btn_lang,callback_data:"v:lang"}],
  ];
  if(isAdmin(uid)) rows.push([{text:"🛠 Панель администратора",callback_data:"a:main"}]);
  return{inline_keyboard:rows};
}

function profileKb(uid) {
  const tx=T(uid), s=sub(uid), act=activeSub(s);
  const rows=[];
  // Always callback — shows subscription info inline
  rows.push([{text:act?tx.btn_sub_active:tx.btn_sub, callback_data:"v:sub"}]);
  rows.push([{text:tx.btn_hist,callback_data:"ph:0"},{text:tx.btn_other,callback_data:"v:other"}]);
  rows.push([{text:tx.btn_home,callback_data:"v:home"}]);
  return{inline_keyboard:rows};
}

function subKb(uid) {
  const tx=T(uid), s=sub(uid), rows=[];
  if(activeSub(s)){
    rows.push([{text:tx.btn_connect,url:s.sub_url}]);
    rows.push([{text:tx.btn_qr,callback_data:"sub:qr"}]);
    rows.push([{text:tx.btn_guide,callback_data:"v:guide"}]);
  } else {
    // Expired or no sub — show buy button
    rows.push([{text:tx.btn_buy_sub,callback_data:"v:buy"}]);
  }
  rows.push([{text:tx.btn_back_profile,callback_data:"v:profile"},{text:tx.btn_home,callback_data:"v:home"}]);
  return{inline_keyboard:rows};
}

function buyKb(uid) {
  const tx=T(uid);
  // Always "new" purchase — renewal of active sub is blocked in doPurchase
  const lang=getLang(uid);
  const rows=tariffs().map(t=>[{text:`${tariffTitle(t,lang)} — ${rub(t.price_rub)}`,callback_data:`pay:n:${t.code}`}]);
  rows.push([{text:tx.btn_home,callback_data:"v:home"}]);
  return{inline_keyboard:rows};
}

function topupKb(uid) {
  const tx=T(uid), rows=[];
  if(CRYPTOBOT_TOKEN) rows.push([{text:tx.btn_pay_crypto,callback_data:"topup:crypto"}]);
  rows.push([{text:tx.btn_pay_other,callback_data:"v:pay_other"}]);
  rows.push([{text:tx.btn_back,callback_data:"v:profile"}]);
  return{inline_keyboard:rows};
}

function refKb(uid) {
  const tx=T(uid), u=user(uid);
  const link=refLink(u.ref_code);
  // Build t.me/share/url — opens native Telegram chat picker
  const isRu=getLang(uid)==="ru";
  const shareText=isRu
    ? `Привет. Подключись к VPN по моей ссылке:\n\n${link}\n\nРаботает быстро и стабильно.`
    : `Hey! Connect to VPN using my link:\n\n${link}\n\nFast and reliable.`;
  // Pass only &text= so the link appears inside the message body, not prepended by Telegram
  const shareUrl="https://t.me/share/url?text="+encodeURIComponent(shareText);
  const rows=[];
  // URL button → opens Telegram's native share picker
  rows.push([{text:tx.btn_invite, url:shareUrl}]);
  rows.push([
    {text:tx.btn_ref_hist,callback_data:"ref:hist:0"},
    {text:tx.btn_ref_code,callback_data:"ref:r"},
  ]);
  rows.push([{text:tx.btn_home,callback_data:"v:home"}]);
  return{inline_keyboard:rows};
}



function giftKb(uid) {
  const tx=T(uid);
  const lang=getLang(uid);
  return{inline_keyboard:[
    ...tariffs().map(t=>[{text:`🎁 ${tariffTitle(t,lang)} — ${rub(t.price_rub)}`,callback_data:`g:p:${t.code}`}]),
    [{text:tx.btn_home,callback_data:"v:home"}],
  ]};
}

function giftUsersKb(uid, code, page) {
  const tx=T(uid), {items,total,page:p,size}=usersPage(page,uid,8);
  const max=Math.max(0,Math.ceil(total/size)-1);
  const rows=items.map(u=>[{text:`${u.first_name||u.username||u.tg_id} (${u.username?`@${u.username}`:`id:${u.tg_id}`})`,callback_data:`g:u:${code}:${u.tg_id}`}]);
  const nav=[];
  if(p>0)   nav.push({text:"◀",callback_data:`g:l:${code}:${p-1}`});
  nav.push({text:`${p+1}/${max+1}`,callback_data:"noop"});
  if(p<max) nav.push({text:"▶",callback_data:`g:l:${code}:${p+1}`});
  rows.push(nav);
  // Enter by ID
  rows.push([{text:"✏️ Ввести ID / @username",callback_data:`g:id:${code}`}]);
  rows.push([{text:tx.btn_back,callback_data:"v:gift"}]);
  return{inline_keyboard:rows};
}

function langKb(uid) {
  const lang=getLang(uid);
  return{inline_keyboard:[
    [{text:(lang==="ru"?"✓ ":"")+"🇷🇺 Русский",callback_data:"lang:ru"}],
    [{text:(lang==="en"?"✓ ":"")+"🇬🇧 English",callback_data:"lang:en"}],
    [{text:T(uid).btn_home,callback_data:"v:home"}],
  ]};
}

function pagingKb(uid, prefix, page, total, size, backTarget) {
  const tx=T(uid), max=Math.max(0,Math.ceil(total/size)-1), nav=[];
  if(page>0)   nav.push({text:"◀",callback_data:`${prefix}:${page-1}`});
  nav.push({text:`${page+1}/${max+1}`,callback_data:"noop"});
  if(page<max) nav.push({text:"▶",callback_data:`${prefix}:${page+1}`});
  return{inline_keyboard:[nav,[{text:tx.btn_back,callback_data:backTarget}]]};
}

function back(uid,t="v:home"){ return{inline_keyboard:[[{text:T(uid).btn_back,callback_data:t}]]}; }

// ─────────────────────────────────────────────────────────────────────────────
// Text builders
// ─────────────────────────────────────────────────────────────────────────────
function homeText(u) {
  const tx=T(u.tg_id), s=sub(u.tg_id), hasSub=activeSub(s);
  const lines=[tx.home_title(u.first_name||""),"",tx.home_balance(u.balance_rub)];
  if(hasSub){const dd=Math.floor(Math.max(0,s.expires_at-now())/86400000);lines.push(tx.home_sub_ok(dd));}
  return lines.join("\n");
}

function profileText(uid) {
  const tx=T(uid), u=user(uid);
  const refCount=Number(db.prepare("SELECT COUNT(*) c FROM referrals WHERE referrer_tg_id=?").get(uid).c||0);
  return [tx.prof_title,"",tx.prof_bal(u.balance_rub),tx.prof_refs(refCount),tx.prof_id(uid)].join("\n");
}

function subText(uid) {
  const tx=T(uid), s=sub(uid);
  if(!activeSub(s)) return [tx.sub_title,"",tx.sub_none].join("\n");
  const ms=Math.max(0,s.expires_at-now()), dd=Math.floor(ms/86400000), hh=Math.floor((ms%86400000)/3600000), mm=Math.floor((ms%3600000)/60000);
  return [tx.sub_title,"",tx.sub_plan(s.plan_title||s.plan_code||"—"),tx.sub_exp(s.expires_at),tx.sub_left(dd,hh,mm),`<i>${tx.sub_devices}</i>`,"",`${tx.sub_link_hdr}\n<code>${esc(s.sub_url)}</code>`].join("\n");
}

// Translate tariff title to English if needed
function tariffTitle(t, lang) {
  if(lang==="ru") return t.title;
  // Auto-translate common RU patterns
  return t.title
    .replace(/1\s*месяц/i,  "1 month")
    .replace(/3\s*месяц[а-я]*/i, "3 months")
    .replace(/6\s*месяц[а-я]*/i, "6 months")
    .replace(/1\s*год/i,    "1 year")
    .replace(/2\s*год[а-я]*/i, "2 years")
    .replace(/месяц[а-я]*/i, "month(s)")
    .replace(/год[а-я]*/i,   "year");
}

function buyText(uid) {
  const tx=T(uid), u=user(uid), act=activeSub(sub(uid)), lang=getLang(uid);
  const lines=[tx.buy_title,""];
  tariffs().forEach(t=>lines.push(`${tariffTitle(t,lang)} — <b>${rub(t.price_rub)}</b>`));
  lines.push("",tx.buy_balance(u.balance_rub),"",act?tx.buy_active:tx.buy_new);
  return lines.join("\n");
}

function topupText(uid) {
  const tx=T(uid), other=setting("payment_methods","");
  const rate=_rateCache.val;
  const lines=[tx.topup_title,""];
  if(CRYPTOBOT_TOKEN) lines.push(`<blockquote>USDT • ${tx.crypto_rate(rate)}</blockquote>`,"");
  lines.push(tx.topup_other(other));
  return lines.join("\n");
}

function refText(uid) {
  const tx=T(uid), u=user(uid), isRu=getLang(uid)==="ru";
  const st=db.prepare("SELECT COUNT(*) c, COALESCE(SUM(reward_rub),0) s FROM referrals WHERE referrer_tg_id=?").get(Number(uid));
  const pct=Number(setting("ref_percent","30"))||30;
  const totalEarned=Number(u.ref_earned||0); // lifetime earnings credited to main balance
  const link=refLink(u.ref_code);
  const lines=[
    tx.ref_title,"",
    // ref link in blockquote (monospace = tap to copy on mobile)
    `<blockquote><code>${link}</code></blockquote>`,
    isRu?"<i>(Нажмите, чтобы скопировать)</i>":"<i>(Tap to copy)</i>","",
    // stats
    isRu?`Приглашено: <b>${st.c||0}</b>`:`Invited: <b>${st.c||0}</b>`,
    isRu?`Заработано: <b>${totalEarned.toFixed(2)}₽</b>`:`Earned: <b>${totalEarned.toFixed(2)}₽</b>`,"",
    // promo block — reward credited to main balance
    isRu?[
      "⭐ <b>Это выгодно!</b>",
      `<i>${pct}% от каждой покупки подписки реферала</i>`,
      `<i>Начисляется на основной баланс</i>`,
    ].join("\n"):[
      "⭐ <b>Great deal!</b>",
      `<i>${pct}% from every referral's subscription purchase</i>`,
      `<i>Credited to your main balance</i>`,
    ].join("\n"),
  ];
  return lines.join("\n");
}

function aboutText(uid) {
  const tx=T(uid);
  return [tx.about_title,"",tx.about_text].join("\n");
}

function otherText(uid) {
  const isRu=getLang(uid)==="ru", proxy=lnk.proxy();
  const title=isRu?"<b>Остальное</b>":"<b>Other</b>";
  const lines=[title,""];
  if(proxy) lines.push(isRu?
    `<a href="${proxy}">🆓 Бесплатные прокси для Telegram</a>`:
    `<a href="${proxy}">🆓 Free Telegram proxies</a>`
  );
  return lines.join("\n");
}

function purchasesText(uid, page=0) {
  const tx=T(uid), size=5, off=page*size;
  const rows=db.prepare("SELECT * FROM purchases WHERE tg_id=? ORDER BY created_at DESC LIMIT ? OFFSET ?").all(Number(uid),size,off);
  const total=Number(db.prepare("SELECT COUNT(*) c FROM purchases WHERE tg_id=?").get(Number(uid)).c||0);
  if(!rows.length) return{text:[tx.ph_title,"",tx.ph_empty].join("\n"),total,page,size};
  const lines=[tx.ph_title,""];
  for(const p of rows){
    const icon=p.kind==="gift"?"🎁":p.kind==="renew"?"🔄":"💳";
    lines.push(`${icon} <b>${esc(p.tariff_title)}</b> — ${rub(p.amount_rub)}`);
    lines.push(`   <i>${dt(p.created_at)}</i>`);
  }
  lines.push("",tx.ph_page(page,Math.max(1,Math.ceil(total/size))));
  return{text:lines.join("\n"),total,page,size};
}

function refHistoryText(uid, page=0) {
  const tx=T(uid), size=5, off=page*size;
  const rows=db.prepare("SELECT * FROM referrals WHERE referrer_tg_id=? ORDER BY created_at DESC LIMIT ? OFFSET ?").all(Number(uid),size,off);
  const total=Number(db.prepare("SELECT COUNT(*) c FROM referrals WHERE referrer_tg_id=?").get(Number(uid)).c||0);
  if(!rows.length) return{text:[tx.rh_title,"",tx.rh_empty].join("\n"),total,page,size};
  const lines=[tx.rh_title,""];
  for(const r of rows){
    lines.push(`+<b>${rub(r.reward_rub)}</b>  <i>(${r.percent}% от ${rub(r.amount_rub)})</i>`);
    lines.push(`   <i>${dt(r.created_at)}</i>`);
  }
  lines.push("",`Стр. ${page+1}/${Math.max(1,Math.ceil(total/size))}`);
  return{text:lines.join("\n"),total,page,size};
}

// ─────────────────────────────────────────────────────────────────────────────
// Admin panels
// ─────────────────────────────────────────────────────────────────────────────
function adminStatsText() {
  const uCount  = Number(db.prepare("SELECT COUNT(*) c FROM users").get().c);
  const aCount  = Number(db.prepare("SELECT COUNT(*) c FROM subscriptions WHERE is_active=1 AND expires_at>?").get(now()).c);
  const revenue = Number(db.prepare("SELECT COALESCE(SUM(amount_rub),0) s FROM purchases").get().s||0);
  const today   = new Date(); today.setHours(0,0,0,0);
  const todayTs = today.getTime();
  const newDay  = Number(db.prepare("SELECT COUNT(*) c FROM users WHERE created_at>=?").get(todayTs).c);
  const revDay  = Number(db.prepare("SELECT COALESCE(SUM(amount_rub),0) s FROM purchases WHERE created_at>=?").get(todayTs).s||0);
  const refPaid = Number(db.prepare("SELECT COALESCE(SUM(reward_rub),0) s FROM referrals").get().s||0);
  const cryptoTotal=Number(db.prepare("SELECT COALESCE(SUM(amount_rub),0) s FROM crypto_payments WHERE status='paid'").get().s||0);
  const cryptoCount=Number(db.prepare("SELECT COUNT(*) c FROM crypto_payments WHERE status='paid'").get().c||0);
  return [
    "<b>Статистика</b>","",
    `Пользователей: <b>${uCount}</b>  (+${newDay} сегодня)`,
    `Активных подписок: <b>${aCount}</b>`,
    `Выручка за сегодня: <b>${rub(revDay)}</b>`,
    `Общая выручка: <b>${rub(revenue)}</b>`,
    `Начислено рефералам: <b>${rub(refPaid)}</b>`,
    `Crypto платежей: <b>${cryptoCount}</b> (${rub(cryptoTotal)})`,
  ].join("\n");
}

function adminImgsText() {
  const views=[["home","Главная"],["profile","Профиль"],["sub","Подписка"],["buy","Тарифы"],["topup","Пополнение"],["ref","Рефералы"],["gift","Подарок"],["guide","Инструкция"],["about","О нас"]];
  const lines=["<b>Изображения разделов</b>",""];
  views.forEach(([v,label])=>{const has=!!viewImg(v);lines.push(`${has?"✅":"⬜"} ${label}`);});
  return lines.join("\n");
}

function adminLinksText() {
  const rows=[
    ["url_support","Поддержка",lnk.support()],
    ["url_privacy","Политика конф.",lnk.privacy()],
    ["url_terms","Соглашение",lnk.terms()],
    ["url_proxy","Прокси",lnk.proxy()],
    ["url_news","Канал",lnk.news()],
    ["url_status","Статус серверов",lnk.status()],
  ];
  const lines=["<b>Настройка ссылок</b>",""];
  rows.forEach(([,label,val])=>lines.push(`${val?"✅":"⬜"} ${label}: ${val?`<code>${esc(val)}</code>`:"<i>не задано</i>"}`));
  return lines.join("\n");
}



function adminUserInfoText(tu) {
  const ts=sub(tu.tg_id), hasSub=activeSub(ts);
  const pCount=Number(db.prepare("SELECT COUNT(*) c FROM purchases WHERE tg_id=?").get(tu.tg_id).c||0);
  return [
    "<b>Пользователь</b>","",
    `ID: <code>${tu.tg_id}</code>`,
    `Имя: ${esc(tu.first_name)}`,
    `Username: ${tu.username?`@${esc(tu.username)}`:"—"}`,
    `<blockquote>Баланс: <b>${rub(tu.balance_rub)}</b>\nРеф. начислено: <b>${rub(tu.ref_earned||0)}</b></blockquote>`,
    `Покупок: <b>${pCount}</b>`,
    `Подписка: ${hasSub?`<b>активна</b> до ${dt(ts.expires_at)}`:"нет"}`,
    `<i>Зарегистрирован: ${dt(tu.created_at)}</i>`,
  ].join("\n");
}

// ─────────────────────────────────────────────────────────────────────────────
// Render  — the main view dispatcher
// ─────────────────────────────────────────────────────────────────────────────
async function render(uid, chatId, msgId, view, data={}) {
  const u=user(uid); if(!u) return;
  const tx=T(uid);
  let text="", kb={}, photo=viewImg(view)||"";

  switch(view){
    case "home":
      text=homeText(u); kb=homeKb(uid); photo=viewImg("home");
      break;
    case "profile":
      text=profileText(uid); kb=profileKb(uid); photo=viewImg("sub");
      break;
    case "sub":
      text=subText(uid); kb=subKb(uid); photo=viewImg("sub");
      break;
    case "buy":
      text=buyText(uid); kb=buyKb(uid); photo=viewImg("buy");
      break;
    case "topup":
    case "v:topup": {
      const rate=CRYPTOBOT_TOKEN?await getUsdtRate():null;
      if(rate) _rateCache={val:rate,ts:Date.now()};
      text=topupText(uid); kb=topupKb(uid); photo=viewImg("topup");
      break;
    }
    case "pay_other":
      text=`${tx.topup_title}\n\n${parseLinks(setting("payment_methods","<i>Не настроено.</i>"))}`;
      kb={inline_keyboard:[[{text:tx.btn_back,callback_data:"v:topup"}]]}; photo="";
      break;
    case "guide": {
      const rawGuide=setting("guide_text","");
      text=rawGuide?parseLinks(rawGuide):[tx.guide_title,"","<i>Инструкция не настроена.</i>"].join("\n");
      const kbRows=[];
      if(lnk.support()) kbRows.push([{text:tx.btn_support,url:lnk.support()}]);
      kbRows.push([{text:tx.btn_home,callback_data:"v:home"}]);
      kb={inline_keyboard:kbRows}; photo=viewImg("guide");
      break;
    }
    case "about": {
      text=aboutText(uid);
      const kbRows=[];
      if(lnk.support()) kbRows.push([{text:tx.btn_support,url:lnk.support()}]);
      if(lnk.status())  kbRows.push([{text:tx.btn_status,url:lnk.status()}]);
      if(lnk.privacy()) kbRows.push([{text:tx.btn_privacy,url:lnk.privacy()}]);
      if(lnk.terms())   kbRows.push([{text:tx.btn_terms,url:lnk.terms()}]);
      kbRows.push([{text:tx.btn_home,callback_data:"v:home"}]);
      kb={inline_keyboard:kbRows}; photo=viewImg("about");
      break;
    }
    case "other":
      text=otherText(uid);
      kb={inline_keyboard:[
        [{text:tx.btn_other_topup,callback_data:"v:topup"}],
        [{text:tx.btn_other_gift,callback_data:"v:gift"}],
        [{text:tx.btn_back_profile,callback_data:"v:profile"}],
      ]};
      photo="";
      break;
    case "lang":
      text=[tx.lang_title,"",`${tx.lang_current}: <b>${getLang(uid)==="ru"?tx.lang_ru:tx.lang_en}</b>`].join("\n");
      kb=langKb(uid); photo="";
      break;
    case "ref":
      text=refText(uid); kb=refKb(uid); photo=viewImg("ref");
      break;

    case "gift":
      text=[tx.gift_title,"",tx.gift_choose].join("\n"); kb=giftKb(uid); photo=viewImg("gift");
      break;
    case "gift_users": {
      const tr=tariff(data.code);
      text=tr?[tx.gift_recv,"",tx.gift_recv_d].join("\n"):"Тариф не найден.";
      kb=tr?giftUsersKb(uid,tr.code,data.page||0):back(uid,"v:gift"); photo=viewImg("gift");
      break;
    }
    case "purchases": {
      const {text:ht,total,size}=purchasesText(uid,Number(data.page||0));
      text=ht; kb=pagingKb(uid,"ph",Number(data.page||0),total,size,"v:profile"); photo="";
      break;
    }
    case "ref_hist": {
      const {text:ht,total,size}=refHistoryText(uid,Number(data.page||0));
      text=ht; kb=pagingKb(uid,"ref:hist",Number(data.page||0),total,size,"v:ref"); photo="";
      break;
    }

    // ── Admin ──────────────────────────────────────────────────────────────
    case "a_main": {
      text=adminStatsText();
      kb={inline_keyboard:[
        [{text:"💸 Тарифы",callback_data:"a:t"},{text:"🎞 GIF-анимации",callback_data:"a:g"}],
        [{text:"📨 Рассылка",callback_data:"a:b"},{text:"🔗 Ссылки",callback_data:"a:links"}],
        [{text:"🖼 Изображения",callback_data:"a:imgs"},{text:"💰 Текст пополнения",callback_data:"a:p"}],
        [{text:"🤝 Реф. процент",callback_data:"a:r"},{text:"📋 Инструкция",callback_data:"a:guide_edit"}],
        [{text:"🔍 Поиск юзера",callback_data:"a:find"}],
        [{text:"🗄 База данных",callback_data:"a:db"}],
        [{text:"« Назад",callback_data:"v:home"}],
      ]};
      break;
    }

    case "a_tariffs":
      text=`<b>Цены тарифов</b>\n\n${tariffs().map(x=>`${x.title}: <b>${rub(x.price_rub)}</b>`).join("\n")}`;
      kb={inline_keyboard:[...tariffs().map(x=>[{text:`✏️ ${x.title} — ${rub(x.price_rub)}`,callback_data:`a:te:${x.code}`}]),[{text:"« Назад",callback_data:"a:main"}]]};
      break;
    case "a_gif":
      text="<b>GIF-анимации</b>\n\nНастройте анимации для событий:";
      kb={inline_keyboard:[
        [{text:`Главная${setting("gif_main_menu")?" ✅":""}`,callback_data:"a:ge:gif_main_menu"}],
        [{text:`Покупка${setting("gif_purchase_success")?" ✅":""}`,callback_data:"a:ge:gif_purchase_success"}],
        [{text:`Подарок${setting("gif_gift_success")?" ✅":""}`,callback_data:"a:ge:gif_gift_success"}],
        [{text:`Рассылка${setting("gif_broadcast")?" ✅":""}`,callback_data:"a:ge:gif_broadcast"}],
        [{text:"« Назад",callback_data:"a:main"}],
      ]};
      break;
    case "a_imgs": {
      text=adminImgsText();
      const views=[["home","Главная"],["sub","Подписка / Профиль"],["buy","Тарифы"],["topup","Пополнение"],["ref","Рефералы"],["gift","Подарок"],["guide","Инструкция"],["about","О нас"]];
      kb={inline_keyboard:[...views.map(([v,label])=>[{text:`🖼 ${label}${viewImg(v)?" ✅":""}`,callback_data:`a:img:${v}`}]),[{text:"« Назад",callback_data:"a:main"}]]};
      break;
    }
    case "a_links": {
      text=adminLinksText();
      const linkKeys=[["url_support","Поддержка"],["url_privacy","Политика конф."],["url_terms","Соглашение"],["url_proxy","Прокси"],["url_news","Канал"],["url_status","Статус серверов"]];
      kb={inline_keyboard:[...linkKeys.map(([k,label])=>[{text:`✏️ ${label}`,callback_data:`a:lnk:${k}`}]),[{text:"« Назад",callback_data:"a:main"}]]};
      break;
    }
    case "a_bcast":
      text="<b>Рассылка</b>\n\nОтправьте текст. Поддерживается HTML-форматирование.\n<i>Задержка 35 мс/сообщение для соблюдения лимитов Telegram.</i>";
      kb={inline_keyboard:[[{text:"✏️ Написать сообщение",callback_data:"a:bs"}],[{text:"« Назад",callback_data:"a:main"}]]};
      break;
    case "a_pay":
      text=`<b>Текст «Других способов пополнения»</b>\n\n<blockquote>${esc(setting("payment_methods","Пока пусто."))}</blockquote>`;
      kb={inline_keyboard:[[{text:"✏️ Изменить",callback_data:"a:pe"}],[{text:"« Назад",callback_data:"a:main"}]]};
      break;
    case "a_ref":
      text=["<b>Реф. процент</b>","",`Ставка: <b>${setting("ref_percent","30")}%</b>`,`<i>Начисляется на основной баланс пользователя при покупке реферала.</i>`].join("\n");
      kb={inline_keyboard:[[{text:"✏️ Изменить ставку",callback_data:"a:rp"}],[{text:"« Назад",callback_data:"a:main"}]]};
      break;
    case "a_db":
      text="<b>База данных</b>\n\nСкачайте или импортируйте БД.\n⚠️ После импорта бот перезапустится.";
      kb={inline_keyboard:[[{text:"⬇️ Скачать",callback_data:"a:db_export"}],[{text:"⬆️ Импорт",callback_data:"a:db_import_start"}],[{text:"« Назад",callback_data:"a:main"}]]};
      break;
    case "a_user_info": {
      const tu=user(data.id);
      if(!tu){text="Пользователь не найден.";kb={inline_keyboard:[[{text:"« Назад",callback_data:"a:main"}]]};break;}
      text=adminUserInfoText(tu);
      kb={inline_keyboard:[[{text:"➕ Пополнить баланс",callback_data:`a:bal_add:${tu.tg_id}`}],[{text:"✏️ Ред. подписку",callback_data:`a:sub_edit:${tu.tg_id}`}],[{text:"« Назад",callback_data:"a:main"}]]};
      break;
    }
    case "a_guide_edit":
      text=`<b>Инструкция по подключению</b>\n\n<i>Используйте формат [Название|URL] для ссылок.</i>\n\nТекущий текст:\n<blockquote>${esc(setting("guide_text","")).slice(0,300)}</blockquote>`;
      kb={inline_keyboard:[[{text:"✏️ Изменить",callback_data:"a:guide"}],[{text:"« Назад",callback_data:"a:main"}]]};
      break;
    default:
      text=homeText(u); kb=homeKb(uid); photo=viewImg("home");
  }

  const nm=await renderMsg(chatId,msgId,text,kb,photo||null);
  setMenu(uid,chatId,nm);
}

// ─────────────────────────────────────────────────────────────────────────────
// Crypto topup flows
// ─────────────────────────────────────────────────────────────────────────────
async function startCryptoTopup(uid, chatId) {
  expireOldCryptoPayments(uid);
  const rate=await getUsdtRate(), tx=T(uid);
  const text=[tx.crypto_title,"",tx.crypto_desc,tx.crypto_min(CRYPTO_MIN_RUB),tx.crypto_rate(rate),"",tx.crypto_enter].join("\n");
  await tg("sendMessage",{chat_id:chatId,text,parse_mode:"HTML",reply_markup:{keyboard:[[{text:"Отмена"}]],resize_keyboard:true,one_time_keyboard:true}});
  setUserState(uid,"topup_crypto_amount","");
}

async function handleCryptoAmount(uid, chatId, text) {
  const amount=Math.round(parseFloat(text.replace(/[^\d.]/g,""))||0);
  const tx=T(uid);
  if(!amount||amount<CRYPTO_MIN_RUB){
    await tg("sendMessage",{chat_id:chatId,text:`❌ ${tx.crypto_min(CRYPTO_MIN_RUB)}`,parse_mode:"HTML"});
    return;
  }
  clearUserState(uid);
  await tg("sendMessage",{chat_id:chatId,text:"⏳",reply_markup:{remove_keyboard:true}});
  const inv=await createCryptoInvoice(amount);
  if(!inv){
    await tg("sendMessage",{chat_id:chatId,text:"❌ CryptoBot недоступен. Попробуйте позже."});
    return;
  }
  const cpId=createCryptoPaymentRow(uid,amount,inv.amountUsdt,inv.rate,inv.invoiceId,inv.payUrl);
  const msgText=[tx.crypto_inv,"",tx.crypto_sum(rub(amount),inv.amountUsdt),tx.crypto_rate(inv.rate),"",tx.crypto_steps,"",tx.crypto_ttl].join("\n");
  await tg("sendMessage",{chat_id:chatId,text:msgText,parse_mode:"HTML",reply_markup:{inline_keyboard:[
    [{text:tx.btn_pay_crypto,url:inv.payUrl}],
    [{text:tx.btn_check,callback_data:`cp:check:${cpId}`}],
    [{text:tx.btn_cancel,callback_data:`cp:cancel:${cpId}`}],
  ]}});
}

// ─────────────────────────────────────────────────────────────────────────────
// Admin state handler
// ─────────────────────────────────────────────────────────────────────────────
async function handleAdminState(msg) {
  const aid=Number(msg.from?.id||0); if(!isAdmin(aid)) return false;
  const row=getAdminState(aid); if(!row) return false;
  const text=String(msg.text||"").trim(), chatId=Number(msg.chat?.id||0);

  if(text==="/cancel"){clearAdminState(aid);await render(aid,chatId,user(aid)?.last_menu_id||null,"a_main");return true;}

  switch(row.state){
    case "db_import_wait":
      if(!msg.document?.file_id){await tg("sendMessage",{chat_id:chatId,text:"Жду файл SQLite документом."});return true;}
      try{
        await tg("sendMessage",{chat_id:chatId,text:"⏳ Проверяю файл..."});
        const tmp=await downloadImportFile(msg.document.file_id);
        clearAdminState(aid);
        await tg("sendMessage",{chat_id:chatId,text:"✅ Перезапускаю бота с новой базой..."});
        setTimeout(()=>restartBotWithFile(tmp),500);
      }catch(e){await tg("sendMessage",{chat_id:chatId,text:`❌ Ошибка: ${e.message}`});}
      return true;

    case "tariff_price": {
      const n=Number(text); if(!Number.isFinite(n)||n<=0){await tg("sendMessage",{chat_id:chatId,text:"Введите цену > 0."});return true;}
      db.prepare("UPDATE tariffs SET price_rub=? WHERE code=?").run(Math.round(n),row.payload);
      clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:`✅ Цена: ${rub(Math.round(n))}`});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_tariffs"); return true;
    }
    case "gif": {
      const v=msg.animation?.file_id||msg.video?.file_id||text; if(!v){await tg("sendMessage",{chat_id:chatId,text:"Отправьте GIF."});return true;}
      setSetting(row.payload,v); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:"✅ GIF сохранён."});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_gif"); return true;
    }
    case "section_img": {
      const v=msg.photo?msg.photo[msg.photo.length-1].file_id:(msg.document?.file_id||text);
      if(!v){await tg("sendMessage",{chat_id:chatId,text:"Отправьте фото или file_id."});return true;}
      setSetting(`img_${row.payload}`,v); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:`✅ Изображение для «${row.payload}» сохранено.`});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_imgs"); return true;
    }
    case "section_img_clear": {
      delSetting(`img_${row.payload}`); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:"✅ Изображение удалено."});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_imgs"); return true;
    }
    case "edit_link": {
      const urlVal=text.trim();
      if(urlVal&&!urlVal.startsWith("http")){await tg("sendMessage",{chat_id:chatId,text:"Введите корректный URL (https://...) или «-» для очистки."});return true;}
      if(urlVal==="-"||urlVal==="") delSetting(row.payload);
      else setSetting(row.payload,urlVal);
      clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:"✅ Ссылка обновлена."});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_links"); return true;
    }
    case "pay_methods":
      setSetting("payment_methods",text); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:"✅ Текст пополнения обновлён."});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_pay"); return true;

    case "guide_text":
      setSetting("guide_text",text); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:"✅ Инструкция обновлена."});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_main"); return true;

    case "broadcast": {
      clearAdminState(aid);
      const ids=db.prepare("SELECT tg_id FROM users").all();
      let ok=0,fail=0;
      const gifKey=setting("gif_broadcast","");
      const progMsg=await tg("sendMessage",{chat_id:chatId,text:`📨 0/${ids.length}`}).catch(()=>null);
      for(let i=0;i<ids.length;i++){
        const uid=ids[i].tg_id;
        try{
          if(gifKey) await tg("sendAnimation",{chat_id:uid,animation:gifKey,caption:text,parse_mode:"HTML"});
          else       await tg("sendMessage",{chat_id:uid,text,parse_mode:"HTML"});
          ok++;
        }catch{fail++;}
        await sleep(35);
        if(progMsg&&(i+1)%20===0) tg("editMessageText",{chat_id:chatId,message_id:progMsg.message_id,text:`📨 ${i+1}/${ids.length}`}).catch(()=>{});
      }
      await tg("sendMessage",{chat_id:chatId,text:`📨 Рассылка завершена\n✅ ${ok}  ❌ ${fail}`,parse_mode:"HTML"});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_bcast"); return true;
    }
    case "ref_percent": {
      const n=Number(text); if(!Number.isFinite(n)||n<0||n>100){await tg("sendMessage",{chat_id:chatId,text:"Введите 0..100."});return true;}
      setSetting("ref_percent",Math.round(n)); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:`✅ Ставка: ${Math.round(n)}%`});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_ref"); return true;
    }

    case "bal_add": {
      const targetId=Number(row.payload), n=Number(text);
      if(!Number.isFinite(n)){await tg("sendMessage",{chat_id:chatId,text:"Введите число."});return true;}
      const nb=updateBalance(targetId,n); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:`✅ Баланс <code>${targetId}</code>: ${rub(nb)}`,parse_mode:"HTML"});
      if(n>0) tg("sendMessage",{chat_id:targetId,text:`<b>Баланс пополнен на ${rub(n)}</b>\n\n<blockquote>Текущий баланс: ${rub(nb)}</blockquote>`,parse_mode:"HTML"}).catch(()=>{});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_user_info",{id:targetId}); return true;
    }
    case "find_user": {
      clearAdminState(aid);
      let found=null;
      if(/^\d+$/.test(text)) found=user(Number(text));
      if(!found) found=db.prepare("SELECT * FROM users WHERE username=?").get(text.replace(/^@/,""));
      if(!found){await tg("sendMessage",{chat_id:chatId,text:"❌ Пользователь не найден."});return true;}
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_user_info",{id:found.tg_id}); return true;
    }
    case "gift_recipient_id": {
      // Admin/user entered a gift recipient ID or username
      clearUserState(aid);
      const code=row.payload;
      let found=null;
      if(/^\d+$/.test(text)) found=user(Number(text));
      if(!found&&text.startsWith("@")) found=db.prepare("SELECT * FROM users WHERE username=?").get(text.slice(1));
      if(!found){
        await tg("sendMessage",{chat_id:chatId,text:"❌ Пользователь не найден в боте."});
        return true;
      }
      const tr=tariff(code), u=user(aid);
      if(!tr||!u){return true;}
      // Show gift confirm
      const toId=found.tg_id;
      if(Number(aid)===Number(toId)){await tg("sendMessage",{chat_id:chatId,text:T(aid).gift_self});return true;}
      if(Number(u.balance_rub)<Number(tr.price_rub)){await tg("sendMessage",{chat_id:chatId,text:T(aid).gift_no_bal(tr.price_rub,u.balance_rub),parse_mode:"HTML"});return true;}
      const toName=found.first_name||(found.username?`@${found.username}`:`ID ${found.tg_id}`);
      const tx=T(aid);
      const lines=[tx.gift_confirm_title,"",tx.gift_to(toName),tx.gift_plan(tr.title),tx.gift_price(tr.price_rub),tx.gift_after(Number(u.balance_rub)-Number(tr.price_rub))];
      await tg("sendMessage",{chat_id:chatId,text:lines.join("\n"),parse_mode:"HTML",reply_markup:{inline_keyboard:[
        [{text:tx.btn_confirm,callback_data:`g:cf:${code}:${toId}`}],
        [{text:tx.btn_cancel,callback_data:"v:gift"}],
      ]}});
      return true;
    }
  }
  return false;
}

// ─────────────────────────────────────────────────────────────────────────────
// Message handler
// ─────────────────────────────────────────────────────────────────────────────
async function handleMessage(msg) {
  const from=msg.from||{}, chatId=Number(msg.chat?.id||0);
  if(!chatId||!from.id) return;
  upsertUser(from,chatId);
  const ustate=getUserState(from.id);
  const text=String(msg.text||"").trim();

  // Universal cancel
  if((text==="Отмена"||text==="Cancel")&&ustate){
    clearUserState(from.id);
    await tg("sendMessage",{chat_id:chatId,text:"✕",reply_markup:{remove_keyboard:true}});
    const view=ustate.state==="topup_crypto_amount"?"topup":"home";
    await render(from.id,chatId,user(from.id)?.last_menu_id||null,view);
    return;
  }

  // Crypto topup amount
  if(ustate?.state==="topup_crypto_amount"){
    if(!msg.text||msg.text.startsWith("/")) return;
    await handleCryptoAmount(from.id,chatId,text); return;
  }



  // ref_withdraw_amount state removed — referral rewards go to main balance directly

  // Gift recipient ID entry
  if(ustate?.state==="gift_recipient_id"){
    if(!msg.text||msg.text.startsWith("/")) return;
    const code=ustate.payload||"";
    clearUserState(from.id);
    let found=null;
    if(/^\d+$/.test(text)) found=user(Number(text));
    if(!found&&text.startsWith("@")) found=db.prepare("SELECT * FROM users WHERE username=?").get(text.slice(1));
    if(!found){await tg("sendMessage",{chat_id:chatId,text:"❌ Пользователь не найден. Попросите его нажать /start.",reply_markup:{remove_keyboard:true}});return;}
    const tr=tariff(code), u=user(from.id), tx=T(from.id);
    if(!tr||!u) return;
    if(Number(from.id)===Number(found.tg_id)){await tg("sendMessage",{chat_id:chatId,text:tx.gift_self,reply_markup:{remove_keyboard:true}});return;}
    if(Number(u.balance_rub)<Number(tr.price_rub)){await tg("sendMessage",{chat_id:chatId,text:tx.gift_no_bal(tr.price_rub,u.balance_rub),parse_mode:"HTML",reply_markup:{remove_keyboard:true}});return;}
    const toName=found.first_name||(found.username?`@${found.username}`:`ID ${found.tg_id}`);
    const lines=[tx.gift_confirm_title,"",tx.gift_to(toName),tx.gift_plan(tr.title),tx.gift_price(tr.price_rub),tx.gift_after(Number(u.balance_rub)-Number(tr.price_rub))];
    await tg("sendMessage",{chat_id:chatId,text:lines.join("\n"),parse_mode:"HTML",reply_markup:{remove_keyboard:true}});
    await tg("sendMessage",{chat_id:chatId,text:"\u200b",reply_markup:{inline_keyboard:[
      [{text:tx.btn_confirm,callback_data:`g:cf:${code}:${found.tg_id}`}],
      [{text:tx.btn_cancel,callback_data:"v:gift"}],
    ]}});
    return;
  }

  // Gift: system picker result
  if(msg.user_shared&&ustate?.state==="gift_pick"){
    const recipientId=Number(msg.user_shared.user_id||0), code=ustate.payload||"";
    clearUserState(from.id);
    await tg("sendMessage",{chat_id:chatId,text:"",reply_markup:{remove_keyboard:true}});
    if(!user(recipientId)){await tg("sendMessage",{chat_id:chatId,text:"❌ Пользователь не зарегистрирован. Попросите нажать /start."});return;}
    // Show confirm
    const tr=tariff(code), u=user(from.id), tx=T(from.id);
    if(!tr||!u) return;
    if(Number(from.id)===Number(recipientId)){await tg("sendMessage",{chat_id:chatId,text:tx.gift_self});return;}
    if(Number(u.balance_rub)<Number(tr.price_rub)){await tg("sendMessage",{chat_id:chatId,text:tx.gift_no_bal(tr.price_rub,u.balance_rub),parse_mode:"HTML"});return;}
    const to=user(recipientId), toName=to?.first_name||(to?.username?`@${to.username}`:`ID ${recipientId}`);
    const lines=[tx.gift_confirm_title,"",tx.gift_to(toName),tx.gift_plan(tr.title),tx.gift_price(tr.price_rub),tx.gift_after(Number(u.balance_rub)-Number(tr.price_rub))];
    await tg("sendMessage",{chat_id:chatId,text:lines.join("\n"),parse_mode:"HTML",reply_markup:{inline_keyboard:[
      [{text:tx.btn_confirm,callback_data:`g:cf:${code}:${recipientId}`}],
      [{text:tx.btn_cancel,callback_data:"v:gift"}],
    ]}});
    return;
  }

  // Admin state
  if(await handleAdminState(msg)) return;

  // Admin commands
  if(isAdmin(from.id)){
    if(text.startsWith("/add_balance")){
      const p=text.split(/\s+/);
      if(p.length!==3){await tg("sendMessage",{chat_id:chatId,text:"Формат: /add_balance &lt;id&gt; &lt;amount&gt;",parse_mode:"HTML"});return;}
      const tid=Number(p[1]),amt=Number(p[2]);
      if(!user(tid)||!Number.isFinite(amt)){await tg("sendMessage",{chat_id:chatId,text:"Неверные параметры."});return;}
      const nb=updateBalance(tid,amt);
      await tg("sendMessage",{chat_id:chatId,text:`✅ Баланс <code>${p[1]}</code>: <b>${rub(nb)}</b>`,parse_mode:"HTML"}); return;
    }

  }

  // Standard commands
  if(text.startsWith("/start")){
    const m=text.match(/^\/start\s+partner_([a-zA-Z0-9]+)$/);
    if(m){const r=findRef(m[1]);if(r)setRef(from.id,r.tg_id);}
    await gif(chatId,"gif_main_menu");
    await render(from.id,chatId,null,"home"); return;
  }
  if(text==="/menu")            {await render(from.id,chatId,null,"home");return;}
  if(text==="/sub")             {await render(from.id,chatId,null,"sub");return;}
  if(text==="/balance")         {await render(from.id,chatId,null,"topup");return;}
  if(text==="/referral")        {await render(from.id,chatId,null,"ref");return;}
  if(text==="/admin"&&isAdmin(from.id)){await render(from.id,chatId,user(from.id)?.last_menu_id,"a_main");return;}

  await tg("sendMessage",{chat_id:chatId,text:"Используйте /start"});
}

// ─────────────────────────────────────────────────────────────────────────────
// Callback handler
// ─────────────────────────────────────────────────────────────────────────────
async function handleCallback(q) {
  const data=q.data||"", uid=Number(q.from?.id||0), chatId=Number(q.message?.chat?.id||0), msgId=Number(q.message?.message_id||0);
  if(!uid||!chatId||!msgId) return;
  upsertUser(q.from,chatId);
  const ans=(text="",alert=false)=>tg("answerCallbackQuery",{callback_query_id:q.id,...(text?{text,show_alert:alert}:{})}).catch(()=>{});

  if(data==="noop"){await ans();return;}
  if(data.startsWith("a:")&&!isAdmin(uid)){await ans("Нет доступа.",true);return;}

  // ── Language ──────────────────────────────────────────────────────────────
  if(data.startsWith("lang:")){
    const lg=data.split(":")[1];
    if(lg==="ru"||lg==="en"){setLang(uid,lg);}
    await render(uid,chatId,msgId,"lang"); await ans(); return;
  }

  // ── Navigation ────────────────────────────────────────────────────────────
  const navMap={
    "v:home":"home","v:profile":"profile","v:sub":"sub","v:buy":"buy",
    "v:topup":"topup","v:pay_other":"pay_other","v:ref":"ref","v:guide":"guide",
    "v:about":"about","v:gift":"gift","v:lang":"lang","v:other":"other",
  };
  if(navMap[data]){await render(uid,chatId,msgId,navMap[data]);await ans();return;}

  // ── Purchase ──────────────────────────────────────────────────────────────
  if(data.startsWith("pay:n:")){await askBuyConfirm(uid,chatId,msgId,data.split(":")[2],"new",q.id);return;}
  // pay:r: (renew while active) removed from UI
  if(data.startsWith("pay:c:")){const[,,mode,code]=data.split(":");await buySelf(uid,chatId,msgId,code,mode,q.id);return;}

  // ── Purchase history ──────────────────────────────────────────────────────
  if(data.startsWith("ph:")){await render(uid,chatId,msgId,"purchases",{page:Number(data.split(":")[1]||0)});await ans();return;}

  // ── Subscription ──────────────────────────────────────────────────────────
  // sub:copy removed (guide button replaces it)
  if(data==="sub:qr"){
    const s=sub(uid);
    if(!activeSub(s)){await ans(getLang(uid)==="en"?"No active subscription.":"Нет активной подписки.",true);return;}
    await ans();
    const tx=T(uid);
    const qrUrl=`https://api.qrserver.com/v1/create-qr-code/?size=512x512&margin=16&data=${encodeURIComponent(s.sub_url)}`;
    await tg("sendPhoto",{chat_id:chatId,photo:qrUrl,caption:tx.sub_qr_caption,parse_mode:"HTML"})
      .catch(async()=>{
        await tg("sendMessage",{chat_id:chatId,text:getLang(uid)==="en"?"❌ QR generation failed. Use the link above.":"❌ Не удалось сгенерировать QR-код. Используйте ссылку выше."}).catch(()=>{});
      });
    return;
  }

  // ── Referral ──────────────────────────────────────────────────────────────
  // ref:i removed — link shown inline in refText
  // ref:share removed — invite uses t.me/share/url URL button
  if(data==="ref:r"){
    // Show confirmation before changing code
    const isRu=getLang(uid)==="ru";
    const confirmTxt=T(uid).ref_code_confirm;
    const kb={inline_keyboard:[
      [{text:T(uid).btn_confirm,callback_data:"ref:r:yes"},{text:T(uid).btn_cancel,callback_data:"v:ref"}],
    ]};
    const nm=await renderMsg(chatId,msgId,confirmTxt,kb,null);
    setMenu(uid,chatId,nm);
    await ans(); return;
  }
  if(data==="ref:r:yes"){
    db.prepare("UPDATE users SET ref_code=?,updated_at=? WHERE tg_id=?").run(crypto.randomBytes(5).toString("hex"),now(),uid);
    await render(uid,chatId,msgId,"ref"); await ans("✅"); return;
  }
  // ref:p removed — no payout settings needed
  // ref:pm: removed — no payout method needed
  if(data.startsWith("ref:hist:")){await render(uid,chatId,msgId,"ref_hist",{page:Number(data.split(":")[2]||0)});await ans();return;}
  // ref:w removed — referral rewards go to main balance directly

  // ── Gifts ─────────────────────────────────────────────────────────────────
  if(data.startsWith("g:p:")){
    const code=data.split(":")[2],tr=tariff(code),u=user(uid),tx=T(uid);
    if(!tr){await ans("Тариф не найден.",true);return;}
    if(Number(u.balance_rub)<Number(tr.price_rub)){await ans(tx.gift_no_bal(tr.price_rub,u.balance_rub),true);return;}
    await render(uid,chatId,msgId,"gift_users",{code,page:0}); await ans(); return;
  }
  if(data.startsWith("g:l:")){const[,,code,page]=data.split(":");await render(uid,chatId,msgId,"gift_users",{code,page:Number(page||0)});await ans();return;}
  if(data.startsWith("g:u:")){
    const[,,code,rid]=data.split(":");
    await askGiftConfirm(uid,chatId,msgId,code,Number(rid),q.id); return;
  }
  if(data.startsWith("g:cf:")){
    const[,,code,rid]=data.split(":");
    await giftToUser(uid,Number(rid),code,chatId,msgId,q.id); return;
  }
  if(data.startsWith("g:id:")){
    // User wants to enter recipient ID manually
    const code=data.split(":")[2], tx=T(uid);
    setUserState(uid,"gift_recipient_id",code); await ans();
    await tg("sendMessage",{chat_id:chatId,text:tx.gift_enter_id,parse_mode:"HTML",reply_markup:{keyboard:[[{text:"Отмена"}]],resize_keyboard:true,one_time_keyboard:true}});
    return;
  }

  // ── Crypto topup ──────────────────────────────────────────────────────────
  if(data==="topup:crypto"){
    if(!CRYPTOBOT_TOKEN){await ans("CryptoBot не настроен.",true);return;}
    await ans(); await startCryptoTopup(uid,chatId); return;
  }
  if(data.startsWith("cp:check:")){
    const cpId=Number(data.split(":")[2]), cp=getCryptoPayment(cpId);
    if(!cp||cp.tg_id!==uid){await ans("Счёт не найден.",true);return;}
    if(cp.status==="paid"){await ans("✅ Уже зачислено!",true);return;}
    if(cp.status!=="pending"){await ans("Счёт закрыт. Создайте новый.",true);return;}
    await ans("⏳ Проверяю...");
    const paid=await checkCryptoInvoice(cp.invoice_id);
    if(paid){
      markCryptoPaid(cpId);
      updateBalance(uid,cp.amount_rub);
      const me=user(uid), tx=T(uid);
      await tg("editMessageText",{chat_id:chatId,message_id:msgId,text:[tx.crypto_ok(cp.amount_rub),"",tx.success_bal(me.balance_rub)].join("\n"),parse_mode:"HTML",reply_markup:{inline_keyboard:[[{text:tx.btn_buy_sub,callback_data:"v:buy"},{text:tx.btn_home,callback_data:"v:home"}]]}}).catch(()=>{});
      tg("sendMessage",{chat_id:ADMIN_ID,text:[`<b>Crypto пополнение</b>`,"",`${esc(me.first_name||String(uid))} (<code>${uid}</code>)`,`Сумма: <b>${rub(cp.amount_rub)}</b>  (${cp.amount_usdt} USDT @ ${Number(cp.rate_rub).toFixed(2)} ₽)`].join("\n"),parse_mode:"HTML"}).catch(()=>{});
    }else{
      await tg("answerCallbackQuery",{callback_query_id:q.id,text:"❌ Оплата не найдена. Попробуйте через минуту.",show_alert:true}).catch(()=>{});
    }
    return;
  }
  if(data.startsWith("cp:cancel:")){
    const cpId=Number(data.split(":")[2]), cp=getCryptoPayment(cpId);
    if(!cp||cp.tg_id!==uid){await ans("Счёт не найден.",true);return;}
    if(cp.status!=="pending"){await ans("Счёт уже закрыт.",true);return;}
    markCryptoCancelled(cpId); await ans("Отменено.");
    await tg("editMessageReplyMarkup",{chat_id:chatId,message_id:msgId,reply_markup:{inline_keyboard:[[{text:T(uid).btn_topup,callback_data:"v:topup"}]]}}).catch(()=>{});
    return;
  }

  // ── Admin nav ─────────────────────────────────────────────────────────────
  const adminNav={"a:main":"a_main","a:t":"a_tariffs","a:g":"a_gif","a:b":"a_bcast","a:p":"a_pay","a:r":"a_ref","a:db":"a_db","a:imgs":"a_imgs","a:links":"a_links","a:guide_edit":"a_guide_edit"};
  if(adminNav[data]){await render(uid,chatId,msgId,adminNav[data]);await ans();return;}

  // ── Admin edit triggers ───────────────────────────────────────────────────
  if(data.startsWith("a:te:")){
    const code=data.split(":")[2],tr=tariff(code);
    setAdminState(uid,"tariff_price",code);
    await tg("sendMessage",{chat_id:chatId,text:`«${esc(tr?.title||code)}» — ${rub(tr?.price_rub||0)}\n\nВведите новую цену (₽):\n/cancel — отмена.`});
    await ans(); return;
  }
  if(data.startsWith("a:ge:")){
    setAdminState(uid,"gif",data.split(":")[2]);
    await tg("sendMessage",{chat_id:chatId,text:"Отправьте GIF или file_id.\n/cancel — отмена."});
    await ans(); return;
  }
  // Section image set
  if(data.startsWith("a:img:")){
    const viewKey=data.split(":")[2];
    const hasImg=!!viewImg(viewKey);
    setAdminState(uid,"section_img",viewKey);
    const kb={inline_keyboard:[]};
    if(hasImg) kb.inline_keyboard.push([{text:"🗑 Удалить изображение",callback_data:`a:img_del:${viewKey}`}]);
    kb.inline_keyboard.push([{text:"« Назад",callback_data:"a:imgs"}]);
    await tg("sendMessage",{chat_id:chatId,text:`Изображение для раздела «<b>${viewKey}</b>».\n\nОтправьте фото или file_id.\n/cancel — отмена.`,parse_mode:"HTML",reply_markup:kb});
    await ans(); return;
  }
  if(data.startsWith("a:img_del:")){
    const viewKey=data.split(":")[2];
    delSetting(`img_${viewKey}`);
    await ans("✅ Удалено.");
    await render(uid,chatId,msgId,"a_imgs"); return;
  }
  // Link edit
  if(data.startsWith("a:lnk:")){
    const key=data.split(":").slice(2).join(":");
    setAdminState(uid,"edit_link",key);
    await tg("sendMessage",{chat_id:chatId,text:`Ссылка «<b>${key.replace("url_","")}</b>»:\n<code>${esc(setting(key))}</code>\n\nВведите новый URL (или «-» для очистки):\n/cancel — отмена.`,parse_mode:"HTML"});
    await ans(); return;
  }
  if(data==="a:bs"){setAdminState(uid,"broadcast","");await tg("sendMessage",{chat_id:chatId,text:"Отправьте текст рассылки (HTML).\n/cancel — отмена."});await ans();return;}
  if(data==="a:pe"){setAdminState(uid,"pay_methods","");await tg("sendMessage",{chat_id:chatId,text:"Отправьте текст способов пополнения.\n/cancel — отмена."});await ans();return;}
  if(data==="a:guide"){setAdminState(uid,"guide_text","");await tg("sendMessage",{chat_id:chatId,text:"Отправьте текст инструкции.\nФормат ссылок: [Название|URL]\n/cancel — отмена."});await ans();return;}
  if(data==="a:rp"){setAdminState(uid,"ref_percent","");await tg("sendMessage",{chat_id:chatId,text:`Ставка: ${setting("ref_percent","30")}%\n\nВведите новую (0..100):\n/cancel — отмена.`});await ans();return;}

  if(data==="a:find"){setAdminState(uid,"find_user","");await tg("sendMessage",{chat_id:chatId,text:"Введите Telegram ID или @username:\n/cancel — отмена."});await ans();return;}
  // DB
  if(data==="a:db_export"){await ans("Формирую файл...");await exportDbToAdmin(chatId);return;}
  if(data==="a:db_import_start"){setAdminState(uid,"db_import_wait","");await ans("Жду файл .db/.sqlite");await tg("sendMessage",{chat_id:chatId,text:"📤 Отправьте SQLite файл документом.\n⚠️ Бот перезапустится после импорта."});return;}
  // Withdrawal callbacks removed
  // Balance add
  if(data.startsWith("a:bal_add:")){
    const targetId=data.split(":")[2], tu=user(Number(targetId));
    setAdminState(uid,"bal_add",targetId); await ans();
    await tg("sendMessage",{chat_id:chatId,text:`Пополнение для ${esc(tu?.first_name||targetId)}\nБаланс: ${rub(tu?.balance_rub)}\n\nВведите сумму (отрицательная = списание):\n/cancel — отмена.`}); return;
  }
  // Sub edit for user (admin can manually update expiry)
  if(data.startsWith("a:sub_edit:")){
    const targetId=data.split(":")[2], ts=sub(Number(targetId));
    await ans(ts?`Подписка до: ${dt(ts.expires_at)}`:"Нет подписки",true); return;
  }

  await ans("Неизвестная команда");
}

// ─────────────────────────────────────────────────────────────────────────────
// Long-poll
// ─────────────────────────────────────────────────────────────────────────────
async function poll() {
  console.log("🤖 VPN Bot запущен.");
  while(true){
    try{
      const ups=await tg("getUpdates",{timeout:30,offset,allowed_updates:["message","callback_query"]});
      for(const u of ups){
        offset=u.update_id+1;
        if(u.message)             handleMessage(u.message).catch(e=>console.error("[msg]",e.message));
        else if(u.callback_query) handleCallback(u.callback_query).catch(e=>console.error("[cb]",e.message));
      }
    }catch(e){ console.error("[poll]",e.message); await sleep(2000); }
  }
}

init();
poll();
