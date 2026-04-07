require("dotenv").config();
const path      = require("path");
const crypto    = require("crypto");
const fs        = require("fs");
const http      = require("http");
const fsp       = fs.promises;
const { spawn } = require("child_process");
const Database  = require("better-sqlite3");
const QRCode    = require("qrcode");

// ─────────────────────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────────────────────
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
// CryptoBot
const CRYPTOBOT_TOKEN  = process.env.CRYPTOBOT_TOKEN       || "";
const CRYPTOBOT_API    = "https://pay.crypt.bot/api";
const USDT_FALLBACK    = Number(process.env.CRYPTOBOT_FALLBACK_RATE || 90);
const CRYPTO_MIN_RUB   = Number(process.env.CRYPTOBOT_MIN_RUB      || 50);
const CRYPTO_INVOICE_TTL = 3600;
// CryptoBot webhook
const CRYPTOBOT_WEBHOOK_PATH = process.env.CRYPTOBOT_WEBHOOK_PATH || "/cryptobot/webhook";
// FreeKassa API
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
  "168.119.157.136",
  "168.119.60.227",
  "178.154.197.79",
  "51.250.54.238",
]);

if (!TOKEN || !API || !APP_SECRET || !ADMIN_ID) {
  console.error("Отсутствуют обязательные env: TELEGRAM_BOT_TOKEN, VPN_API_BASE_URL, APP_SECRET, ADMIN_TELEGRAM_ID");
  process.exit(1);
}
if (!FK_API_KEY || !FK_SECRET2) console.warn("[FreeKassa] API key/secret2 missing. FreeKassa is disabled.");
if ((!FK_SERVER_IP_ENV || FK_SERVER_IP_ENV === "127.0.0.1")) {
  console.warn("[FreeKassa] FREEKASSA_SERVER_IP is empty/localhost. createOrder can fail due to IP validation.");
}

const TG_BASE = `https://api.telegram.org/bot${TOKEN}`;
let   offset  = 0;
const db      = new Database(DB_FILE);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");

// ── Rate limiting (prevent spam clicks) ──────────────────────────────────────
const _cbCooldown = new Map(); // uid → last callback ts
function checkCbRateLimit(uid) {
  const now_ = Date.now(), last = _cbCooldown.get(uid)||0;
  if (now_ - last < 400) return false; // 400ms between callbacks per user
  _cbCooldown.set(uid, now_);
  // GC: purge stale entries every 500 callbacks
  if (_cbCooldown.size > 500) {
    const cutoff = Date.now() - 60000;
    for (const [k,v] of _cbCooldown) if (v < cutoff) _cbCooldown.delete(k);
  }
  return true;
}

// ── Process crash protection ──────────────────────────────────────────────────
process.on("uncaughtException",  e => console.error("[uncaughtException]",  e));
process.on("unhandledRejection", e => console.error("[unhandledRejection]", e));

// Suppress the harmless "buffer.File is experimental" warning that Node 18
// emits when native FormData.append() is called with a Blob + filename.
process.on("warning", (w) => {
  if (w.name === "ExperimentalWarning" && w.message.includes("buffer.File")) return;
  console.warn("[warning]", w.name, w.message);
});

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
    btn_pay_qr:     "📷 СБП (QR)",
    btn_pay_card:   "💳 Банковская карта РФ",
    btn_pay_sber:   "🟢 SberPay",
    // Channel gate
    btn_check_sub:  "✅ Я подписался",
    btn_open_channel:"📢 Открыть канал",
    gate_text:      (url) => `<b>Для использования бота необходимо подписаться на наш канал.</b>\n\n<a href="${url}">👉 Перейти в канал</a>`,
    gate_not_subscribed: "❌ Вы ещё не подписались на канал. Подпишитесь и нажмите «Я подписался».",
    // Trial
    btn_trial:      "🎁 Пробный период (7 дней)",
    trial_confirm:  (days) => `<b>Пробный период — ${days} дней</b>\n\n<i>Бесплатно, один раз. После истечения можно купить любой тариф.</i>\n\nАктивировать?`,
    trial_activated:(days) => `<b>✅ Пробный период активирован!</b>\n\n<i>Доступ открыт на ${days} дней.</i>`,
    trial_used_msg: "Пробный период уже использован.",
    trial_has_sub:  "У вас уже есть активная подписка.",
    channel_gate:   "👋 Чтобы пользоваться ботом, подпишитесь на наш канал.",
    // Language
    lang_title:  "🌐 <b>Выбор языка</b>",
    lang_current:"Текущий язык",
    lang_ru:     "🇷🇺 Русский",
    lang_en:     "🇬🇧 English",
    // Home
    home_title:  (name) => `🐸 Привет, ${esc(name||"")}`,
    home_info:   (id, bal) => `<blockquote>— Ваш ID: <code>${id}</code>\n— Ваш баланс: <b>${rub(bal)}</b></blockquote>`,
    home_footer: `Канал — @DreinnVPN\nПоддержка — @DreinnVPNSupportBot`,
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
    sub_exp:     (v) => `Истекает: <b>${v}</b>`,
    sub_left:    (d,h,m) => `Осталось: <b>${d} дн. ${h} ч. ${m} мин.</b>`,
    sub_devices: "До 3 устройств",
    sub_link_hdr:"Ссылка подписки:",
    // Buy
    buy_title:   "<b>Тарифы VPN</b>",
    buy_balance: (v) => `<blockquote>Ваш баланс: <b>${rub(v)}</b></blockquote>`,
    buy_active:  "<i>⚠️ Подписка уже активна. Купить новую можно после истечения.</i>",
    buy_trial_active: "<i>✅ Пробный период активен. Купить тариф можно прямо сейчас — пробный будет заменён.</i>",
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
    about_title: "<b>🌐 О сервисе Dreinn VPN</b>",
    about_text:  [
      "<b>⚡️ Возможности</b>",
      "• Выбор стран подключения",
      "• Быстрое и стабильное соединение",
      "• Работает корректно с любыми приложениями — можно оставлять включённым постоянно",
      "• Поддержка всех популярных устройств",
      "",
      "<b>🧭 Умная маршрутизация</b>",
      "Сервисы, требующие локального доступа, автоматически подключаются через соответствующий регион. Остальные ресурсы открываются через выбранную вами страну. Всё работает плавно, без необходимости ручного переключения.",
      "",
      "<b>🛡 Конфиденциальность</b>",
      "Мы заботимся о вашей приватности: не сохраняем историю действий и не передаём данные сторонним сервисам. Все сведения о подключениях удаляются автоматически.",
    ].join("\n"),
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
    success_exp:   (v) => `Истекает: <b>${v}</b>`,

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
    // FreeKassa
    fk_title:      (method) => `<b>Пополнение через ${esc(method || "выбранный способ")}</b>`,
    fk_enter:      "Введите сумму в рублях:",
    fk_min:        (v) => `Минимум: <b>${rub(v)}</b>`,
    fk_created:    "<b>Счёт создан</b>",
    fk_steps:      "1 — Нажмите «Оплатить»\n2 — Завершите платёж на сайте\n3 — Баланс зачислится автоматически",
    fk_wait:       "<i>Если оплата уже выполнена, нажмите «Проверить оплату».</i>",
    fk_ok:         (v) => `<b>Зачислено ${rub(v)}</b>`,
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
    // Promo codes
    btn_promo:      "🎟 Ввести промокод",
    promo_enter:    "Введите промокод:",
    promo_ok:       (pct, code) => `✅ Промокод <b>${esc(code)}</b> применён — скидка <b>${pct}%</b>`,
    promo_invalid:  "❌ Промокод не найден или истёк.",
    promo_used:     "❌ Промокод уже был использован. Каждый пользователь может применить только один промокод.",
    promo_applied:  (pct) => `<i>Промокод: скидка ${pct}%</i>`,
    // Direct payment (when buying tariff without balance)
    btn_pay_balance:"💳 Оплатить с баланса",
    direct_title:   (plan, price) => `<b>Оплата: ${esc(plan)}</b>\n\nСумма: <b>${price}</b>\n\nВыберите способ оплаты:`,
    direct_no_bal:  (need, have) => `⚠️ На балансе <b>${have}</b>, нужно <b>${need}</b>. Пополните или оплатите напрямую:`,
    // Expiry notifications
    notify_3days:   (plan, days) => `⏰ <b>Напоминание</b>\n\nВаша подписка «${esc(plan)}» истекает через <b>${days} дн.</b>\n\nПродлите заранее, чтобы не терять доступ.`,
    notify_1day:    (plan) => `🔴 <b>Подписка истекает завтра!</b>\n\nТариф «${esc(plan)}» заканчивается. Продлите сейчас.`,
    notify_expired: (plan) => `❌ <b>Подписка истекла</b>\n\nТариф «${esc(plan)}» закончился. Оформите новый, чтобы продолжить пользоваться VPN.`,
    btn_renew_now:  "🔄 Продлить сейчас",
    // Admin grant sub
    admin_grant_pick: "Выберите тариф для выдачи:",
    admin_grant_ok: (name, plan) => `✅ Подписка «${esc(plan)}» выдана пользователю ${esc(name)}.`,
    admin_grant_rcvd: (plan, days) => `🎁 <b>Вам выдана подписка!</b>\n\nТариф: <b>${esc(plan)}</b>\nДоступ открыт на <b>${days} дн.</b>`,
    btn_grant_sub:  "🎁 Выдать подписку",
    // Admin promo
    admin_promo_list: "<b>Промокоды</b>",
    admin_promo_empty:"Промокодов нет.",
    admin_promo_add: "Введите промокод в формате:\n<code>КОД СКИДКА_% [MAX_ИСПОЛЬЗОВАНИЙ]</code>\nПример: <code>SALE10 10 100</code>\n",
    // Ref history page (was hardcoded RU)
    rh_page:        (p, t) => `Стр. ${p+1}/${t}`,
    // Device selection
    dev_title:      "🚀 <b>Настройка тарифа</b>",
    dev_base:       (n) => `Базово: <b>${n} устр.</b>`,
    dev_now:        (n) => `Сейчас: <b>${n} устр.</b>`,
    dev_price:      (v) => `💰 К оплате: <b>${rub(v)}</b>`,
    dev_hint:       "Выберите количество устройств:",
    dev_pay:        (v) => `Оплатить ${rub(v)}`,
    dev_surcharge:  (v) => `<i>+${rub(v)} за доп. устройства</i>`,
    btn_devices_extra:"⚙️ Цена за доп. устройство",
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
    sub_qr_caption: "📷 <b>Subscription QR Code</b>\n\n<i>Scan with your camera or Dreinn VPN to connect.</i>",
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
    btn_pay_qr:     "📷 SBP (QR)",
    btn_pay_card:   "💳 Russian bank card",
    btn_pay_sber:   "🟢 SberPay",
    // Channel gate
    btn_check_sub:  "✅ I've subscribed",
    btn_open_channel:"📢 Open channel",
    gate_text:      (url) => `<b>To use the bot you need to subscribe to our channel.</b>\n\n<a href="${url}">👉 Go to channel</a>`,
    gate_not_subscribed: "❌ You haven't subscribed to the channel yet. Subscribe and tap «I've subscribed».",
    // Trial
    btn_trial:      "🎁 Free trial (7 days)",
    trial_confirm:  (days) => `<b>Free trial — ${days} days</b>\n\n<i>Free, one time only. After expiry you can buy any plan.</i>\n\nActivate?`,
    trial_activated:(days) => `<b>✅ Free trial activated!</b>\n\n<i>Access granted for ${days} days.</i>`,
    trial_used_msg: "Free trial already used.",
    trial_has_sub:  "You already have an active subscription.",
    channel_gate:   "👋 To use the bot, please subscribe to our channel.",
    btn_check_sub:  "✅ I've subscribed",
    lang_title:  "🌐 <b>Language</b>",
    lang_current:"Current language",
    lang_ru:     "🇷🇺 Русский",
    lang_en:     "🇬🇧 English",
    home_title:  (name) => `🐸 Hello, ${esc(name||"")}`,
    home_info:   (id, bal) => `<blockquote>— Your ID: <code>${id}</code>\n— Your balance: <b>${rub(bal)}</b></blockquote>`,
    home_footer: `Channel — @DreinnVPN\nSupport — @DreinnVPNSupportBot`,
    home_balance:(bal) => `<blockquote>Balance: <b>${rub(bal)}</b></blockquote>`,
    home_sub_ok: (days) => `<i>Subscription active — ${days} days left</i>`,
    prof_title:  "<b>Profile</b>",
    prof_bal:    (v) => `Balance: <b>${rub(v)}</b>`,
    prof_refs:   (v) => `Referrals: <b>${v}</b>`,
    prof_id:     (v) => `ID: <code>${v}</code>`,
    sub_title:   "<b>My subscription</b>",
    sub_none:    "No active subscription found.\n\n<i>Get a plan in «Buy VPN».</i>",
    sub_plan:    (v) => `Plan: <b>${esc(v)}</b>`,
    sub_exp:     (v) => `Expires: <b>${v}</b>`,
    sub_left:    (d,h,m) => `Remaining: <b>${d}d ${h}h ${m}m</b>`,
    sub_devices: "Up to 3 devices",
    sub_link_hdr:"Subscription link:",
    buy_title:   "<b>VPN Plans</b>",
    buy_balance: (v) => `<blockquote>Your balance: <b>${rub(v)}</b></blockquote>`,
    buy_active:  "<i>⚠️ Subscription is active. You can buy a new one after it expires.</i>",
    buy_trial_active: "<i>✅ Free trial is active. You can buy a plan now — the trial will be replaced.</i>",
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
    about_title: "<b>🌐 About Dreinn VPN</b>",
    about_text:  [
      "<b>⚡️ Features</b>",
      "• Choice of connection countries",
      "• Fast and stable connection",
      "• Works seamlessly with any app — safe to leave on all the time",
      "• Supports all popular devices",
      "",
      "<b>🧭 Smart routing</b>",
      "Services that require local access automatically connect through the appropriate region. Everything else opens through your chosen country. It all works smoothly, with no manual switching needed.",
      "",
      "<b>🛡 Privacy</b>",
      "We care about your privacy: we do not store activity logs or share data with third parties. All connection records are deleted automatically.",
    ].join("\n"),
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
    success_exp:   (v) => `Expires: <b>${v}</b>`,

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
    // FreeKassa
    fk_title:      (method) => `<b>Top up via ${esc(method || "selected method")}</b>`,
    fk_enter:      "Enter amount in rubles:",
    fk_min:        (v) => `Minimum: <b>${rub(v)}</b>`,
    fk_created:    "<b>Invoice created</b>",
    fk_steps:      "1 — Tap «Pay»\n2 — Complete payment on the website\n3 — Balance will be credited automatically",
    fk_wait:       "<i>If you already paid, tap «Check payment».</i>",
    fk_ok:         (v) => `<b>Credited ${rub(v)}</b>`,
    ph_title:      "<b>Purchase history</b>",
    ph_empty:      "No purchases yet.",
    ph_page:       (p,t) => `Page ${p+1} of ${t}`,
    rh_title:      "<b>Earnings history</b>",
    rh_empty:      "No earnings yet.",
    other_title:   "<b>Settings</b>",
    other_proxy:   "🆓 Free Telegram proxies",
    btn_promo:      "🎟 Enter promo code",
    promo_enter:    "Enter promo code:",
    promo_ok:       (pct, code) => `✅ Promo code <b>${esc(code)}</b> applied — <b>${pct}%</b> discount`,
    promo_invalid:  "❌ Promo code not found or expired.",
    promo_used:     "❌ Promo code already used. Each user can apply only one promo code.",
    promo_applied:  (pct) => `<i>Promo code: ${pct}% off</i>`,
    btn_pay_balance:"💳 Pay from balance",
    direct_title:   (plan, price) => `<b>Payment: ${esc(plan)}</b>\n\nAmount: <b>${price}</b>\n\nChoose payment method:`,
    direct_no_bal:  (need, have) => `⚠️ Balance <b>${have}</b>, need <b>${need}</b>. Top up or pay directly:`,
    notify_3days:   (plan, days) => `⏰ <b>Reminder</b>\n\nYour subscription «${esc(plan)}» expires in <b>${days} days</b>.\n\nRenew in advance to keep access.`,
    notify_1day:    (plan) => `🔴 <b>Subscription expires tomorrow!</b>\n\nPlan «${esc(plan)}» ends soon. Renew now.`,
    notify_expired: (plan) => `❌ <b>Subscription expired</b>\n\nPlan «${esc(plan)}» has ended. Get a new one to continue using VPN.`,
    btn_renew_now:  "🔄 Renew now",
    admin_grant_pick: "Choose a plan to grant:",
    admin_grant_ok: (name, plan) => `✅ Subscription «${esc(plan)}» granted to ${esc(name)}.`,
    admin_grant_rcvd: (plan, days) => `🎁 <b>You received a subscription!</b>\n\nPlan: <b>${esc(plan)}</b>\nAccess granted for <b>${days} days</b>.`,
    btn_grant_sub:  "🎁 Grant subscription",
    admin_promo_list: "<b>Promo codes</b>",
    admin_promo_empty:"No promo codes.",
    admin_promo_add: "Enter promo code as:\n<code>CODE DISCOUNT_% [MAX_USES]</code>\nExample: <code>SALE10 10 100</code>\n",
    rh_page:        (p, t) => `Page ${p+1}/${t}`,
    // Device selection
    dev_title:      "🚀 <b>Plan Configuration</b>",
    dev_base:       (n) => `Base: <b>${n} dev.</b>`,
    dev_now:        (n) => `Now: <b>${n} dev.</b>`,
    dev_price:      (v) => `💰 Total: <b>${rub(v)}</b>`,
    dev_hint:       "Choose number of devices:",
    dev_pay:        (v) => `Pay ${rub(v)}`,
    dev_surcharge:  (v) => `<i>+${rub(v)} for extra devices</i>`,
    btn_devices_extra:"⚙️ Extra device price",
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────
const now     = () => Date.now();
const esc     = (s) => String(s || "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
const rub     = (n) => `${Number(n||0).toLocaleString("ru-RU")} ₽`;
const dt      = (ts, lang="ru") => ts ? new Date(ts).toLocaleDateString(lang==="en"?"en-GB":"ru-RU") : "—";
const dts     = (ts) => ts ? new Date(ts).toLocaleString("ru-RU") : "—";
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
function fkShopId()          { return Number(setting("fk_shop_id", String(FK_SHOP_ID_ENV)) || 0); }
function fkMinRub()          { return Math.max(1, Number(setting("fk_min_rub", String(FK_MIN_RUB_ENV)) || FK_MIN_RUB_ENV || 50)); }
function fkNotifyPath() {
  let p = String(setting("fk_notify_path", FK_PATH_NOTIFY_ENV) || FK_PATH_NOTIFY_ENV || "/freekassa/notify").trim();
  if (!p.startsWith("/")) p = `/${p}`;
  return p.replace(/\s+/g, "");
}
function isFkEnabled()       { return !!(fkShopId() > 0 && FK_API_KEY && FK_SECRET2); }
function isValidPublicIpv4(ip) {
  if (!/^\d{1,3}(\.\d{1,3}){3}$/.test(String(ip || ""))) return false;
  const p = String(ip).split(".").map((x) => Number(x));
  if (p.some((n) => !Number.isInteger(n) || n < 0 || n > 255)) return false;
  if (p[0] === 10 || p[0] === 127 || p[0] === 0) return false;
  if (p[0] === 169 && p[1] === 254) return false;
  if (p[0] === 172 && p[1] >= 16 && p[1] <= 31) return false;
  if (p[0] === 192 && p[1] === 168) return false;
  return true;
}
function fkServerIp() {
  const fromDb = String(setting("fk_server_ip", "") || "").trim();
  if (isValidPublicIpv4(fromDb)) return fromDb;
  const fromEnv = String(FK_SERVER_IP_ENV || "").trim();
  if (isValidPublicIpv4(fromEnv)) return fromEnv;
  return "";
}

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
function isTrialSub(uid) { const s=sub(uid); return activeSub(s) && s.plan_code==="trial"; }
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
  // Reward goes to referral balance (ref_balance_rub) — can only be withdrawn, not spent
  db.prepare("UPDATE users SET ref_balance_rub=ref_balance_rub+?,ref_earned=ref_earned+?,updated_at=? WHERE tg_id=?").run(reward,reward,now(),Number(r.tg_id));
  db.prepare("INSERT INTO referrals(referrer_tg_id,invited_tg_id,amount_rub,percent,reward_rub,created_at) VALUES(?,?,?,?,?,?)").run(Number(r.tg_id),Number(buyerId),Number(amount),pct,reward,now());
  const isRu=getLang(r.tg_id)==="ru";
  const msg=isRu
    ? `<blockquote>+${rub(reward)} — реферальное вознаграждение ${pct}%</blockquote>`
    : `<blockquote>+${rub(reward)} — referral bonus ${pct}%</blockquote>`;
  tg("sendMessage",{chat_id:r.tg_id,text:msg,parse_mode:"HTML"}).catch(()=>{});
}

// Withdrawal system removed — ref rewards go directly to main balance

// ── FK auto-expire job (1 h) ──────────────────────────────────────────────────
// Runs every 5 minutes and expires any pending FK payment that is over 1 hour
// old. The linked pending purchase order (if any) is also closed so the user
// can start a fresh payment attempt.
function startFkExpireJob() {
  const CHECK_INTERVAL = 5 * 60 * 1000; // every 5 min

  async function checkExpireFk() {
    try {
      const cutoff = now() - 3600 * 1000; // 1 hour
      const stale = db.prepare(
        "SELECT * FROM freekassa_payments WHERE status='pending' AND created_at<?"
      ).all(cutoff);

      for (const fp of stale) {
        // Atomically mark as expired (guard against concurrent webhook credit)
        const res = db.prepare(
          "UPDATE freekassa_payments SET status='expired',updated_at=? WHERE id=? AND status='pending'"
        ).run(now(), fp.id);
        if (res.changes === 0) continue; // already processed by webhook

        // Cancel the linked pending purchase order
        if (fp.pending_order_id) {
          closePendingOrder(fp.pending_order_id, "expired");
        } else {
          // Fallback: expire any active pending order for this user
          const po = getPendingOrderByUser(fp.tg_id);
          if (po) closePendingOrder(po.id, "expired");
        }

        // Notify the user
        const isRu = getLang(fp.tg_id) === "ru";
        const tx = T(fp.tg_id);
        tg("sendMessage", {
          chat_id: fp.tg_id,
          text: isRu
            ? `⏰ <b>Счёт истёк</b>\n\nЗаявка на пополнение <b>${rub(fp.amount_rub)}</b>была автоматически отменена — оплата не поступила в течение 1 часа.\n\nЕсли вы оплатили — обратитесь в поддержку.`
            : `⏰ <b>Invoice expired</b>\n\ninvoice for <b>${rub(fp.amount_rub)}</b> was automatically cancelled — no payment received within 1 hour.\n\nIf you already paid, please contact support.`,
          parse_mode: "HTML",
          reply_markup: { inline_keyboard: [[{ text: tx.btn_buy, callback_data: "v:buy" }]] },
        }).catch(() => {});
      }
    } catch (e) { console.error("[FkExpireJob]", e.message); }
  }

  checkExpireFk();
  setInterval(checkExpireFk, CHECK_INTERVAL);
}

// ── Expiry notification job ───────────────────────────────────────────────────
function startExpiryNotificationJob() {
  const CHECK_INTERVAL = 60 * 60 * 1000; // every hour
  async function checkExpiry() {
    try {
      const now_ = now();
      const subs = db.prepare(`
        SELECT s.tg_id, s.expires_at, s.plan_title, s.plan_code, u.lang
        FROM subscriptions s JOIN users u ON u.tg_id=s.tg_id
        WHERE s.is_active=1 AND s.expires_at>? AND s.plan_code!='trial'
      `).all(now_);
      for (const s of subs) {
        const msLeft = s.expires_at - now_;
        const daysLeft = Math.floor(msLeft / 86400000);
        const lang = s.lang || "ru";
        const tx = I18N[lang] || I18N.ru;
        const planName = s.plan_title || s.plan_code;
        let level = null;
        if (daysLeft <= 1)      level = "1day";
        else if (daysLeft <= 3) level = "3days";
        if (!level) continue;
        const already = db.prepare("SELECT 1 FROM notified_expiry WHERE tg_id=? AND level=?").get(s.tg_id, level);
        if (already) continue;
        const text = level === "1day" ? tx.notify_1day(planName) : tx.notify_3days(planName, daysLeft);
        const kb = { inline_keyboard: [[{text:tx.btn_renew_now, callback_data:"v:buy"}]] };
        tg("sendMessage",{chat_id:s.tg_id,text,parse_mode:"HTML",reply_markup:kb}).catch(()=>{});
        db.prepare("INSERT OR REPLACE INTO notified_expiry(tg_id,level,notified_at) VALUES(?,?,?)").run(s.tg_id, level, now_);
      }
      // Notify just-expired (within last 2h)
      const expired = db.prepare(`
        SELECT s.tg_id, s.plan_title, s.plan_code, u.lang
        FROM subscriptions s JOIN users u ON u.tg_id=s.tg_id
        WHERE s.is_active=1 AND s.expires_at<=? AND s.expires_at>=?
      `).all(now_, now_ - 2 * 3600 * 1000);
      for (const s of expired) {
        const lang = s.lang || "ru", tx = I18N[lang] || I18N.ru;
        const already = db.prepare("SELECT 1 FROM notified_expiry WHERE tg_id=? AND level='expired'").get(s.tg_id);
        if (already) continue;
        const kb = { inline_keyboard: [[{text:tx.btn_buy,callback_data:"v:buy"}]] };
        tg("sendMessage",{chat_id:s.tg_id,text:tx.notify_expired(s.plan_title||s.plan_code),parse_mode:"HTML",reply_markup:kb}).catch(()=>{});
        db.prepare("INSERT OR REPLACE INTO notified_expiry(tg_id,level,notified_at) VALUES(?,?,?)").run(s.tg_id,"expired",now_);
        // Mark subscription inactive
        db.prepare("UPDATE subscriptions SET is_active=0,updated_at=? WHERE tg_id=?").run(now_,s.tg_id);
      }
    } catch(e) { console.error("[ExpiryJob]", e.message); }
  }
  checkExpiry();
  setInterval(checkExpiry, CHECK_INTERVAL);
}

// ── Admin: grant subscription directly ───────────────────────────────────────
async function adminGrantSub(adminId, targetId, tariffCode, chatId, msgId) {
  const tr = tariff(tariffCode);
  const tu = user(targetId);
  if (!tr || !tu) return;
  const api = await createSubViaApi(tu, tr, false);
  const subUrl = api.subscriptionUrl || api.sub_url || "";
  if (!subUrl) throw new Error("API не вернул ссылку");
  const exp = now() + tr.duration_days * 86400000;
  db.transaction(()=>{
    db.prepare("INSERT INTO subscriptions(tg_id,plan_code,plan_title,sub_url,expires_at,is_active,devices,created_at,updated_at) VALUES(?,?,?,?,?,1,3,?,?) ON CONFLICT(tg_id) DO UPDATE SET plan_code=excluded.plan_code,plan_title=excluded.plan_title,sub_url=excluded.sub_url,expires_at=excluded.expires_at,is_active=1,devices=excluded.devices,updated_at=excluded.updated_at")
      .run(Number(targetId),tr.code,tr.title,subUrl,exp,now(),now());
    db.prepare("DELETE FROM notified_expiry WHERE tg_id=?").run(Number(targetId));
  })();
  // Notify target user
  const lang = getLang(targetId), rtx = I18N[lang]||I18N.ru;
  tg("sendMessage",{
    chat_id:targetId,
    text:[rtx.admin_grant_rcvd(tr.title,tr.duration_days),"",`<code>${esc(subUrl)}</code>`].join("\n"),
    parse_mode:"HTML",
    reply_markup:{inline_keyboard:[[{text:rtx.btn_connect,web_app:{url:subUrl}}]]},
  }).catch(()=>{});
  const name = tu.first_name||(tu.username?`@${tu.username}`:`ID ${targetId}`);
  return { name, plan: tr.title };
}

function setAdminState(id,state,payload="") { db.prepare("INSERT INTO admin_states(admin_tg_id,state,payload,updated_at) VALUES(?,?,?,?) ON CONFLICT(admin_tg_id) DO UPDATE SET state=excluded.state,payload=excluded.payload,updated_at=excluded.updated_at").run(Number(id),state,String(payload),now()); }
function getAdminState(id)                  { return db.prepare("SELECT * FROM admin_states WHERE admin_tg_id=?").get(Number(id)); }
function clearAdminState(id)                { db.prepare("DELETE FROM admin_states WHERE admin_tg_id=?").run(Number(id)); }
function setUserState(id,state,payload="")  { db.prepare("INSERT INTO user_states(tg_id,state,payload,updated_at) VALUES(?,?,?,?) ON CONFLICT(tg_id) DO UPDATE SET state=excluded.state,payload=excluded.payload,updated_at=excluded.updated_at").run(Number(id),state,String(payload),now()); }
function getUserState(id)                   { return db.prepare("SELECT * FROM user_states WHERE tg_id=?").get(Number(id)); }
function clearUserState(id)                 { db.prepare("DELETE FROM user_states WHERE tg_id=?").run(Number(id)); }

// ── Prompt helpers (no reply keyboard — fully inline) ─────────────────────────
// Sends a prompt message with an inline ❌ Отмена button.
// Returns the sent message id so callers can delete/edit it later.
async function sendPrompt(chatId, text, cancelCb="cancel:input", extraButtons=[]) {
  const rows = [...extraButtons, [{text:"❌ Отмена", callback_data:cancelCb}]];
  const m = await tg("sendMessage",{
    chat_id: chatId, text, parse_mode:"HTML",
    reply_markup:{inline_keyboard: rows},
  });
  return Number(m?.message_id||0);
}

// Delete a message silently (ignore errors — message may already be gone)
function delMsg(chatId, msgId) {
  if(!chatId||!msgId) return;
  tg("deleteMessage",{chat_id:chatId,message_id:msgId}).catch(()=>{});
}

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
      trial_used INTEGER NOT NULL DEFAULT 0,
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
    CREATE TABLE IF NOT EXISTS freekassa_payments(
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      tg_id          INTEGER NOT NULL,
      amount_rub     INTEGER NOT NULL,
      method_id      INTEGER NOT NULL,
      payment_id     TEXT    NOT NULL UNIQUE,
      fk_order_id    INTEGER,
      pending_order_id INTEGER,
      location       TEXT    NOT NULL DEFAULT '',
      status         TEXT    NOT NULL DEFAULT 'pending',
      credited_at    INTEGER,
      created_at     INTEGER NOT NULL,
      updated_at     INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_fk_tg_id      ON freekassa_payments(tg_id);
    CREATE INDEX IF NOT EXISTS idx_fk_payment_id ON freekassa_payments(payment_id);
    CREATE INDEX IF NOT EXISTS idx_fk_status     ON freekassa_payments(status);
    CREATE TABLE IF NOT EXISTS pending_orders(
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      tg_id        INTEGER NOT NULL,
      tariff_code  TEXT    NOT NULL,
      kind         TEXT    NOT NULL DEFAULT 'new',
      promo_code   TEXT    NOT NULL DEFAULT '',
      promo_pct    INTEGER NOT NULL DEFAULT 0,
      expires_at   INTEGER NOT NULL,
      status       TEXT    NOT NULL DEFAULT 'pending',
      created_at   INTEGER NOT NULL,
      updated_at   INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_po_tg_id  ON pending_orders(tg_id);
    CREATE INDEX IF NOT EXISTS idx_po_status ON pending_orders(status);
    CREATE TABLE IF NOT EXISTS promo_codes(
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      code        TEXT    NOT NULL UNIQUE COLLATE NOCASE,
      discount_pct INTEGER NOT NULL DEFAULT 10,
      uses_max    INTEGER NOT NULL DEFAULT 0,
      uses_current INTEGER NOT NULL DEFAULT 0,
      is_active   INTEGER NOT NULL DEFAULT 1,
      created_at  INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS promo_uses(
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      tg_id       INTEGER NOT NULL,
      promo_code  TEXT    NOT NULL,
      created_at  INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_promo_uses ON promo_uses(tg_id, promo_code);
    CREATE TABLE IF NOT EXISTS notified_expiry(
      tg_id       INTEGER NOT NULL,
      level       TEXT    NOT NULL,
      notified_at INTEGER NOT NULL,
      PRIMARY KEY(tg_id, level)
    );
  `);

  // Migrations — idempotent
  for (const m of [
    "ALTER TABLE users ADD COLUMN ref_balance_rub INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE users ADD COLUMN ref_earned INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE users ADD COLUMN lang TEXT NOT NULL DEFAULT 'ru'",
    "ALTER TABLE users ADD COLUMN trial_used INTEGER NOT NULL DEFAULT 0",
  ]) { try { db.exec(m); } catch {} }

  // New-tables migration — create them separately so existing DBs get them
  // even if the main db.exec block ran without these tables before.
  for (const sql of [
    `CREATE TABLE IF NOT EXISTS pending_orders(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tg_id INTEGER NOT NULL, tariff_code TEXT NOT NULL,
      kind TEXT NOT NULL DEFAULT 'new', promo_code TEXT NOT NULL DEFAULT '',
      promo_pct INTEGER NOT NULL DEFAULT 0, expires_at INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending', devices INTEGER NOT NULL DEFAULT 3,
      created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
    `CREATE INDEX IF NOT EXISTS idx_po_tg_id ON pending_orders(tg_id)`,
    `CREATE INDEX IF NOT EXISTS idx_po_status ON pending_orders(status)`,
    `CREATE TABLE IF NOT EXISTS promo_codes(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      code TEXT NOT NULL UNIQUE COLLATE NOCASE,
      discount_pct INTEGER NOT NULL DEFAULT 10,
      uses_max INTEGER NOT NULL DEFAULT 0,
      uses_current INTEGER NOT NULL DEFAULT 0,
      is_active INTEGER NOT NULL DEFAULT 1,
      created_at INTEGER NOT NULL)`,
    `CREATE TABLE IF NOT EXISTS promo_uses(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tg_id INTEGER NOT NULL, promo_code TEXT NOT NULL,
      created_at INTEGER NOT NULL)`,
    `CREATE UNIQUE INDEX IF NOT EXISTS idx_promo_uses ON promo_uses(tg_id, promo_code)`,
    `CREATE TABLE IF NOT EXISTS notified_expiry(
      tg_id INTEGER NOT NULL, level TEXT NOT NULL,
      notified_at INTEGER NOT NULL, PRIMARY KEY(tg_id, level))`,
  ]) { try { db.exec(sql); } catch {} }

  // Verify promo_codes has id column; if not (corrupt schema), drop and recreate
  try {
    db.prepare("SELECT id FROM promo_codes LIMIT 1").get();
  } catch {
    try {
      db.exec("DROP TABLE IF EXISTS promo_codes");
      db.exec(`CREATE TABLE promo_codes(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT NOT NULL UNIQUE COLLATE NOCASE,
        discount_pct INTEGER NOT NULL DEFAULT 10,
        uses_max INTEGER NOT NULL DEFAULT 0,
        uses_current INTEGER NOT NULL DEFAULT 0,
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at INTEGER NOT NULL)`);
    } catch(e2) { console.error("[init] promo_codes recreate failed:", e2.message); }
  }

  // Seed new settings into existing DBs (ON CONFLICT DO NOTHING keeps existing values)
  const ssNew = db.prepare("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO NOTHING");
  ssNew.run("guide_text_en", "📋 <b>Connection guide:</b>\n\n1. Download [Happ|https://www.happ.su/main/ru] or [v2RayTun|https://v2raytun.com/].\n2. Copy your access key from «My Subscription» and paste it into the app.\n3. All done — your internet now routes through our server.\n\n💬 Questions? Contact [support|https://t.me/dreinnvpnsupportbot]");
  ssNew.run("channel_id", "");
  ssNew.run("channel_invite_url", "");
  ssNew.run("trial_enabled", "1");
  ssNew.run("trial_days", "7");
  ssNew.run("fk_shop_id", String(FK_SHOP_ID_ENV || ""));
  ssNew.run("fk_min_rub", String(FK_MIN_RUB_ENV || 50));
  ssNew.run("fk_notify_path", FK_PATH_NOTIFY_ENV || "/freekassa/notify");
  ssNew.run("fk_server_ip", String(FK_SERVER_IP_ENV || ""));
  ssNew.run("devices_extra_price", "360");

  // Seed tariffs
  const st = db.prepare("INSERT INTO tariffs(code,title,duration_days,price_rub,sort_order) VALUES(?,?,?,?,?) ON CONFLICT(code) DO NOTHING");
  [
    ["d1","1 день",1,49,1],
    ["d7","7 дней",7,149,2],
    ["d14","14 дней",14,249,3],
    ["m1","30 дней",30,399,4],
    ["m3","90 дней",90,899,5],
    ["y1","365 дней",365,2499,6],
  ].forEach(r=>st.run(...r));
  // Update titles for old tariff codes to match new naming
  db.prepare("UPDATE tariffs SET title='30 дней',sort_order=4 WHERE code='m1' AND title='1 месяц'").run();
  db.prepare("UPDATE tariffs SET title='365 дней',sort_order=6 WHERE code='y1' AND title='1 год'").run();
  db.prepare("DELETE FROM tariffs WHERE code='m6'").run();

  // Seed settings
  const ss = db.prepare("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO NOTHING");
  const defaults = [
    ["payment_methods",""],["gif_main_menu",""],["gif_purchase_success",""],["gif_gift_success",""],["gif_broadcast",""],
    ["ref_percent","30"],
    ["ref_min_withdraw","3000"],
    ["guide_text","📋 <b>Инструкция по подключению:</b>\n\n1. Скачайте [Happ|https://www.happ.su/main/ru] или [v2RayTun|https://v2raytun.com/].\n2. Скопируйте ваш ключ доступа из раздела «Моя подписка» и вставьте его в приложение.\n3. Всё готово — интернет работает через наш сервер.\n\n💬 Если возникнут вопросы — обращайтесь в [поддержку|https://t.me/dreinnvpnsupportbot]"],
    ["guide_text_en","📋 <b>Connection guide:</b>\n\n1. Download [Happ|https://www.happ.su/main/ru] or [v2RayTun|https://v2raytun.com/].\n2. Copy your access key from «My Subscription» and paste it into the app.\n3. All done — your internet now routes through our server.\n\n💬 Questions? Contact [support|https://t.me/dreinnvpnsupportbot]"],
    // Per-section images (empty = no image)
    ["img_home",""],["img_sub",""],["img_buy",""],["img_bal",""],["img_ref",""],
    ["img_gift",""],["img_guide",""],["img_about",""],["img_topup",""],
    // Channel gate & trial
    ["channel_id",""],             // e.g. -1001234567890 or @mychannel
    ["channel_invite_url",""],     // optional direct invite link shown to user
    ["trial_enabled","1"],         // "1" = on, "0" = off
    ["trial_days","7"],            // duration of trial in days
    // FreeKassa runtime settings
    ["fk_shop_id", String(FK_SHOP_ID_ENV || "")],
    ["fk_min_rub", String(FK_MIN_RUB_ENV || 50)],
    ["fk_notify_path", FK_PATH_NOTIFY_ENV || "/freekassa/notify"],
    ["fk_server_ip", String(FK_SERVER_IP_ENV || "")],
    // Configurable links
    ["url_support","https://t.me/dreinnvpnsupportbot"],["url_privacy",""],["url_terms",""],["url_proxy",""],["url_news",""],["url_status","https://dreinnvpn.vercel.app"],
    // Direct payment: auto-complete pending order after topup
    ["direct_payment_enabled","1"],
    // Device count extra price (per device above 3)
    ["devices_extra_price","360"],
  ];
  defaults.forEach(([k,v])=>ss.run(k,v));

  // Migration: add devices column to subscriptions
  try { db.exec("ALTER TABLE subscriptions ADD COLUMN devices INTEGER NOT NULL DEFAULT 3"); } catch {}
  // Migration: add pending_order_id column to freekassa_payments
  try { db.exec("ALTER TABLE freekassa_payments ADD COLUMN pending_order_id INTEGER"); } catch {}
}

// ─────────────────────────────────────────────────────────────────────────────
// Telegram API
// ─────────────────────────────────────────────────────────────────────────────
async function tg(method, params, _retry=0) {
  const isLongPoll = method === "getUpdates";
  const timeoutMs  = isLongPoll ? 45000 : 30000; // long-poll needs > 30s
  const ctrl = new AbortController();
  const tid = setTimeout(()=>ctrl.abort(), timeoutMs);
  try {
    const r = await fetch(`${TG_BASE}/${method}`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(params),signal:ctrl.signal});
    const j = await r.json().catch(()=>({}));
    if(r.status===429&&_retry<3){
      const ra=Number(j?.parameters?.retry_after||j?.retry_after||5);
      await sleep((ra+1)*1000);
      return tg(method,params,_retry+1);
    }
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
 * Send a photo from an in-memory Buffer (multipart upload — avoids Telegram
 * trying to fetch remote URLs which can fail for long QR URLs).
 */
async function sendPhotoBuffer(chatId, buffer, mimeType, caption, replyMarkup) {
  const form = new FormData();
  form.append("chat_id", String(chatId));
  form.append("photo", new Blob([buffer], { type: mimeType || "image/png" }), "photo.png");
  if (caption)      { form.append("caption", caption); form.append("parse_mode", "HTML"); }
  if (replyMarkup)  form.append("reply_markup", JSON.stringify(replyMarkup));
  const r = await fetch(`${TG_BASE}/sendPhoto`, { method: "POST", body: form });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || j.ok === false) throw new Error(j.description || `TG HTTP ${r.status}`);
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

function verifyCryptoBotWebhook(rawBody, headerToken) {
  // CryptoBot sends "Crypto-Pay-API-Token" header with the bot token
  return String(headerToken || "") === CRYPTOBOT_TOKEN;
}

async function handleCryptoBotWebhookPayload(body) {
  try {
    if (body.update_type !== "invoice_paid") return;
    const inv = body.payload;
    if (!inv || !inv.invoice_id) return;
    const invoiceId = String(inv.invoice_id);
    const cp = db.prepare("SELECT * FROM crypto_payments WHERE invoice_id=?").get(invoiceId);
    if (!cp || cp.status !== "pending") return;
    markCryptoPaid(cp.id);
    updateBalance(cp.tg_id, cp.amount_rub);
    const me = user(cp.tg_id), tx = T(cp.tg_id);
    // Check if this payment was for a pending order
    const po = getPendingOrderByUser(cp.tg_id);
    if (po) {
      closePendingOrder(po.id);
      await completePurchaseAfterTopup(cp.tg_id, po).catch(()=>{});
    } else {
      tg("sendMessage",{
        chat_id: cp.tg_id,
        text: [tx.crypto_ok(cp.amount_rub)].join("\n"),
        parse_mode: "HTML",
        reply_markup:{inline_keyboard:[[{text:tx.btn_buy_sub,callback_data:"v:buy"},{text:tx.btn_home,callback_data:"v:home"}]]},
      }).catch(()=>{});
    }
    tg("sendMessage",{chat_id:ADMIN_ID,text:[`<b>Crypto пополнение (webhook)</b>`,"",`${esc(me?.first_name||String(cp.tg_id))} (<code>${cp.tg_id}</code>)`,`Сумма: <b>${rub(cp.amount_rub)}</b>  (${cp.amount_usdt} USDT @ ${Number(cp.rate_rub).toFixed(2)} ₽)`].join("\n"),parse_mode:"HTML"}).catch(()=>{});
  } catch(e) { console.error("[CryptoBot webhook]", e.message); }
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

function expireOldFkPayments(tgId) {
  const cutoff = now() - 3600 * 1000; // FK orders expire after 1h
  db.prepare("UPDATE freekassa_payments SET status='expired',updated_at=? WHERE tg_id=? AND status='pending' AND created_at<?")
    .run(now(), Number(tgId), cutoff);
}

function expireOldPendingOrders() {
  db.prepare("UPDATE pending_orders SET status='expired',updated_at=? WHERE status='pending' AND expires_at<?")
    .run(now(), now());
}

// ── Pending orders (direct payment flow) ─────────────────────────────────────
function createPendingOrder(tgId, tariffCode, kind="new", promoCd="", promoPct=0, devices=3) {
  expireOldPendingOrders();
  const expires = now() + 30 * 60 * 1000; // 30 min TTL
  // Migration: add devices column if missing
  try { db.exec("ALTER TABLE pending_orders ADD COLUMN devices INTEGER NOT NULL DEFAULT 3"); } catch {}
  let id;
  try {
    id = db.prepare("INSERT INTO pending_orders(tg_id,tariff_code,kind,promo_code,promo_pct,expires_at,devices,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'pending',?,?)")
      .run(Number(tgId),tariffCode,kind,promoCd,promoPct,expires,Number(devices)||3,now(),now()).lastInsertRowid;
  } catch {
    id = db.prepare("INSERT INTO pending_orders(tg_id,tariff_code,kind,promo_code,promo_pct,expires_at,status,created_at,updated_at) VALUES(?,?,?,?,?,?,'pending',?,?)")
      .run(Number(tgId),tariffCode,kind,promoCd,promoPct,expires,now(),now()).lastInsertRowid;
  }
  return id;
}
function getPendingOrder(id)      { return db.prepare("SELECT * FROM pending_orders WHERE id=?").get(Number(id)); }
function getPendingOrderByUser(tgId) {
  return db.prepare("SELECT * FROM pending_orders WHERE tg_id=? AND status='pending' AND expires_at>? ORDER BY id DESC LIMIT 1").get(Number(tgId),now());
}
function closePendingOrder(id, status="done") {
  db.prepare("UPDATE pending_orders SET status=?,updated_at=? WHERE id=?").run(status,now(),Number(id));
}

// ── Promo codes ───────────────────────────────────────────────────────────────
function getPromo(code)    { return db.prepare("SELECT * FROM promo_codes WHERE code=? COLLATE NOCASE").get(String(code||"").trim()); }
function hasUsedPromo(tgId, code) { return !!db.prepare("SELECT 1 FROM promo_uses WHERE tg_id=? AND promo_code=? COLLATE NOCASE").get(Number(tgId),String(code)); }
function usePromo(tgId, code) {
  db.prepare("INSERT OR IGNORE INTO promo_uses(tg_id,promo_code,created_at) VALUES(?,?,?)").run(Number(tgId),String(code).toUpperCase(),now());
  db.prepare("UPDATE promo_codes SET uses_current=uses_current+1 WHERE code=? COLLATE NOCASE").run(String(code));
}
function validatePromo(tgId, code) {
  // Each user may only use ONE promo code ever
  const alreadyUsedAny = db.prepare("SELECT 1 FROM promo_uses WHERE tg_id=?").get(Number(tgId));
  if (alreadyUsedAny) return { ok: false, reason: "used" };
  const p = getPromo(code);
  if (!p || !p.is_active) return { ok: false, reason: "invalid" };
  if (p.uses_max > 0 && p.uses_current >= p.uses_max) return { ok: false, reason: "invalid" };
  if (hasUsedPromo(tgId, code)) return { ok: false, reason: "used" };
  return { ok: true, promo: p };
}

function calcPrice(basePrice, promoPct) {
  if (!promoPct) return Number(basePrice);
  return Math.max(1, Math.round(Number(basePrice) * (100 - promoPct) / 100));
}

// Device count helpers
function devicesExtraPrice() { return Math.max(0, Number(setting("devices_extra_price","360"))||0); }
function devicesSurcharge(devices) {
  const extra = Math.max(0, Number(devices||3) - 3);
  return extra * devicesExtraPrice();
}
function calcPriceWithDevices(basePrice, promoPct, devices) {
  return calcPrice(Number(basePrice) + devicesSurcharge(devices), promoPct);
}

// FreeKassa API
let _fkNonce = 0;
function nextFkNonce() {
  const n = Math.floor(Date.now() / 1000);
  _fkNonce = Math.max(_fkNonce + 1, n);
  return _fkNonce;
}
function fkSignPayload(payload) {
  const data = { ...payload };
  delete data.signature;
  const keys = Object.keys(data).sort();
  const joined = keys.map((k) => String(data[k] ?? "")).join("|");
  return crypto.createHmac("sha256", FK_API_KEY).update(joined).digest("hex");
}
function methodTitle(i, lang) {
  const ru = { 44: "СБП (QR)", 36: "Банковская карта РФ", 43: "SberPay" };
  const en = { 44: "SBP (QR)", 36: "Russian bank card", 43: "SberPay" };
  return (lang === "en" ? en : ru)[Number(i)] || `i=${i}`;
}
async function detectPublicIpv4() {
  const probes = [
    { url: "https://api.ipify.org?format=json", parse: (t) => { try { return JSON.parse(t).ip || ""; } catch { return ""; } } },
    { url: "https://ifconfig.me/ip", parse: (t) => String(t || "").trim() },
    { url: "https://ipv4.icanhazip.com", parse: (t) => String(t || "").trim() },
  ];
  for (const p of probes) {
    try {
      const r = await fetch(p.url, { signal: AbortSignal.timeout(5000) });
      if (!r.ok) continue;
      const txt = await r.text();
      const ip = p.parse(txt);
      if (isValidPublicIpv4(ip)) return ip;
    } catch {}
  }
  return "";
}
async function ensureFkServerIp() {
  const existing = fkServerIp();
  if (existing) {
    setSetting("fk_server_ip", existing);
    return existing;
  }
  const detected = await detectPublicIpv4();
  if (detected) {
    setSetting("fk_server_ip", detected);
    console.log(`[FreeKassa] Auto-detected external IP: ${detected}`);
    return detected;
  }
  console.warn("[FreeKassa] Failed to auto-detect external IP.");
  return "";
}
function createFkPaymentRow(tgId, amountRub, methodId, paymentId, location, fkOrderId = null, pendingOrderId = null) {
  return db
    .prepare("INSERT INTO freekassa_payments(tg_id,amount_rub,method_id,payment_id,fk_order_id,pending_order_id,location,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'pending',?,?)")
    .run(Number(tgId), Math.round(amountRub), Number(methodId), String(paymentId), fkOrderId ? Number(fkOrderId) : null, pendingOrderId ? Number(pendingOrderId) : null, String(location || ""), now(), now()).lastInsertRowid;
}
function getFkPayment(id) {
  return db.prepare("SELECT * FROM freekassa_payments WHERE id=?").get(Number(id));
}
function getFkPaymentByPaymentId(paymentId) {
  return db.prepare("SELECT * FROM freekassa_payments WHERE payment_id=?").get(String(paymentId));
}
function markFkCancelled(id) {
  db.prepare("UPDATE freekassa_payments SET status='cancelled',updated_at=? WHERE id=?").run(now(), Number(id));
}
function markFkPaid(id, fkOrderId = null) {
  db.prepare("UPDATE freekassa_payments SET status='paid',fk_order_id=COALESCE(?, fk_order_id),credited_at=?,updated_at=? WHERE id=?")
    .run(fkOrderId ? Number(fkOrderId) : null, now(), now(), Number(id));
}
async function fkApiPost(pathname, payload) {
  const body = { ...payload, signature: fkSignPayload(payload) };
  const r = await fetch(`${FK_API_BASE}${pathname}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Merchant ${fkShopId()}:${FK_API_KEY}`,
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(15000),
  });
  const txt = await r.text();
  let data = {};
  try { data = txt ? JSON.parse(txt) : {}; } catch {}
  if (!r.ok) throw new Error(data?.message || data?.error || `FreeKassa HTTP ${r.status}`);
  if (data?.type && data.type !== "success") throw new Error(data?.message || data?.error || "FreeKassa error");
  return data;
}
async function createFkOrder({ uid, amountRub, methodId, email, ip }) {
  const paymentId = `tg${uid}_${Date.now()}_${Math.floor(Math.random() * 10000)}`;
  const payload = {
    shopId: fkShopId(),
    nonce: nextFkNonce(),
    paymentId,
    i: Number(methodId),
    email,
    ip,
    amount: Number(amountRub).toFixed(2),
    currency: "RUB",
  };
  const data = await fkApiPost("/orders/create", payload);
  return { paymentId, orderId: data.orderId || null, location: data.location || "", raw: data };
}
async function checkFkOrderByPaymentId(paymentId) {
  const payload = {
    shopId: fkShopId(),
    nonce: nextFkNonce(),
    paymentId: String(paymentId),
  };
  const data = await fkApiPost("/orders", payload);
  const list = Array.isArray(data?.orders) ? data.orders : [];
  if (!list.length) return null;
  return list[0];
}
function getRequestIp(req) {
  const xr = String(req.headers["x-real-ip"] || "").trim();
  if (xr) return xr;
  const xff = String(req.headers["x-forwarded-for"] || "").split(",")[0].trim();
  if (xff) return xff;
  return (req.socket?.remoteAddress || "").replace(/^::ffff:/, "");
}
function parseBodyByContentType(raw, contentType) {
  const ct = String(contentType || "").toLowerCase();
  if (ct.includes("application/json")) {
    try { return JSON.parse(raw || "{}"); } catch { return {}; }
  }
  if (ct.includes("application/x-www-form-urlencoded")) {
    const out = {};
    try { new URLSearchParams(raw || "").forEach((v,k)=>{ out[k]=v; }); } catch {}
    return out;
  }
  if (ct.includes("multipart/form-data")) {
    const out = {};
    const boundaryMatch = ct.match(/boundary=([^;]+)/i);
    const boundary = boundaryMatch ? boundaryMatch[1].trim() : "";
    if (!boundary) return out;
    const parts = String(raw || "").split(`--${boundary}`);
    for (const part of parts) {
      if (!part || part === "--\r\n" || part === "--") continue;
      const nameMatch = part.match(/name="([^"]+)"/i);
      if (!nameMatch) continue;
      const sep = part.indexOf("\r\n\r\n");
      if (sep === -1) continue;
      const val = part.slice(sep + 4).replace(/\r\n--$/, "").replace(/\r\n$/, "");
      out[nameMatch[1]] = val;
    }
    return out;
  }
  const out = {};
  try { new URLSearchParams(raw || "").forEach((v,k)=>{ out[k]=v; }); } catch {}
  return out;
}
function validateFkWebhookSign(p) {
  const sign = String(p.SIGN || p.sign || "").toLowerCase();
  const merchantId = String(p.MERCHANT_ID || p.merchant_id || "");
  const amount = String(p.AMOUNT || p.amount || "");
  const merchantOrderId = String(p.MERCHANT_ORDER_ID || p.merchant_order_id || "");
  if (!sign || !merchantId || !amount || !merchantOrderId) return false;
  const local = crypto.createHash("md5").update(`${merchantId}:${amount}:${FK_SECRET2}:${merchantOrderId}`).digest("hex").toLowerCase();
  return local === sign;
}
async function creditFkPaymentByPaymentId(paymentId, fkOrderId = null, paidAmount = null) {
  const fp = getFkPaymentByPaymentId(paymentId);
  if (!fp) return { ok: false, reason: "NOT_FOUND" };
  if (fp.status === "paid") return { ok: true, reason: "ALREADY_PAID", fp };
  if (fp.status !== "pending") return { ok: false, reason: "CLOSED", fp };
  if (paidAmount != null) {
    const pa = Math.round(Number(paidAmount));
    if (!Number.isFinite(pa) || pa !== Number(fp.amount_rub)) return { ok: false, reason: "WRONG_AMOUNT", fp };
  }
  db.transaction(() => {
    markFkPaid(fp.id, fkOrderId);
    updateBalance(fp.tg_id, fp.amount_rub);
  })();
  const me = user(fp.tg_id);
  const tx = T(fp.tg_id);
  tg("sendMessage", {
    chat_id: fp.tg_id,
    text: [tx.fk_ok(fp.amount_rub)].join("\n"),
    parse_mode: "HTML",
    reply_markup: { inline_keyboard: [[{ text: tx.btn_buy_sub, callback_data: "v:buy" }, { text: tx.btn_home, callback_data: "v:home" }]] },
  }).catch(() => {});
  tg("sendMessage", {
    chat_id: ADMIN_ID,
    text: [
      "<b>FreeKassa пополнение</b>",
      "",
      `${esc(me?.first_name || String(fp.tg_id))} (<code>${fp.tg_id}</code>)`,
      `Сумма: <b>${rub(fp.amount_rub)}</b>`,
      `Метод: <b>${esc(methodTitle(fp.method_id, "ru"))}</b>`,
      `paymentId: <code>${esc(fp.payment_id)}</code>`,
      ...(fkOrderId ? [`fk_order_id: <code>${fkOrderId}</code>`] : []),
    ].join("\n"),
    parse_mode: "HTML",
  }).catch(() => {});
  return { ok: true, reason: "PAID", fp };
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

// ── Channel gate helpers ──────────────────────────────────────────────────────
function getChannelId() { return setting("channel_id","").trim(); }

async function checkChannelMembership(userId) {
  const chanId = getChannelId();
  if (!chanId) return true; // no channel configured → always pass
  try {
    const m = await tg("getChatMember", { chat_id: chanId, user_id: Number(userId) });
    return ["member","administrator","creator"].includes(m?.status);
  } catch { return false; }
}

function getChannelUrl() {
  const inv = setting("channel_invite_url","").trim();
  if (inv) return inv;
  const id = getChannelId();
  if (!id) return "";
  if (id.startsWith("@")) return `https://t.me/${id.slice(1)}`;
  return ""; // numeric id without invite link — admin must set channel_invite_url
}

// ── Trial helpers ─────────────────────────────────────────────────────────────
function trialEnabled() { return setting("trial_enabled","1")==="1"; }
function trialDays()    { return Math.max(1,Math.min(365,Number(setting("trial_days","7"))||7)); }
function hasUsedTrial(uid) { return !!(db.prepare("SELECT trial_used FROM users WHERE tg_id=?").get(Number(uid))?.trial_used); }
function markTrialUsed(uid) { db.prepare("UPDATE users SET trial_used=1,updated_at=? WHERE tg_id=?").run(now(),Number(uid)); }

// ── Channel gate: sends subscription prompt and returns false if blocked ───────
async function enforceChannelGate(uid, chatId, lang) {
  if (!getChannelId()) return true;           // no channel configured
  const member = await checkChannelMembership(uid);
  if (member) return true;                    // already subscribed
  const tx = I18N[lang] || I18N.ru;
  const chanUrl = getChannelUrl();
  const rows = [];
  if (chanUrl) rows.push([{text:"📢 Подписаться / Subscribe",url:chanUrl}]);
  rows.push([{text:tx.btn_check_sub,callback_data:"gate:check"}]);
  await tg("sendMessage",{
    chat_id:chatId,
    text:tx.channel_gate,
    parse_mode:"HTML",
    reply_markup:{inline_keyboard:rows},
  });
  return false;
}

// ── Trial purchase execution ───────────────────────────────────────────────────
async function doTrial(uid, chatId, msgId) {
  const tx = T(uid);
  if (hasUsedTrial(uid)) { await tg("answerCallbackQuery",{callback_query_id:"",text:tx.trial_used_msg,show_alert:true}).catch(()=>{}); return; }
  if (activeSub(sub(uid))) { await tg("answerCallbackQuery",{callback_query_id:"",text:tx.trial_has_sub,show_alert:true}).catch(()=>{}); return; }
  const days = trialDays();
  // Create subscription via API (zero cost, duration = trial days)
  const u = user(uid);
  const fakeTariff = {code:"trial",title:getLang(uid)==="en"?`Free Trial (${days}d)`:`Пробный период (${days} дн.)`,duration_days:days,price_rub:0};
  const api = await createSubViaApi(u, fakeTariff, false);
  const subUrl = api.subscriptionUrl || api.sub_url || "";
  if (!subUrl) { await tg("sendMessage",{chat_id:chatId,text:"❌ Ошибка API. Попробуйте позже."}); return; }
  const exp = now() + days * 86400000;
  db.transaction(()=>{
    db.prepare("INSERT INTO subscriptions(tg_id,plan_code,plan_title,sub_url,expires_at,is_active,devices,created_at,updated_at) VALUES(?,?,?,?,?,1,1,?,?) ON CONFLICT(tg_id) DO UPDATE SET plan_code=excluded.plan_code,plan_title=excluded.plan_title,sub_url=excluded.sub_url,expires_at=excluded.expires_at,is_active=1,devices=1,updated_at=excluded.updated_at")
      .run(Number(uid),"trial",fakeTariff.title,subUrl,exp,now(),now());
    markTrialUsed(uid);
  })();
  const lines=[tx.trial_activated(days),"",`<code>${esc(subUrl)}</code>`];
  const kb={inline_keyboard:[[{text:tx.btn_connect,url:subUrl}],[{text:tx.btn_sub,callback_data:"v:sub"},{text:tx.btn_home,callback_data:"v:home"}]]};
  const nm=await renderMsg(chatId,msgId,lines.join("\n"),kb,null);
  setMenu(uid,chatId,nm);
}
// ─────────────────────────────────────────────────────────────────────────────
async function createSubViaApi(target, tr, giftMode, devices=3) {
  const ctrl = new AbortController();
  const tid  = setTimeout(()=>ctrl.abort(), 20000);
  try {
    const r = await fetch(`${API}/api/bot-subscription`,{
      method:"POST",
      headers:{"Content-Type":"application/json","x-app-secret":APP_SECRET},
      body:JSON.stringify({telegramUserId:String(target.tg_id),telegramUsername:target.username||"",firstName:target.first_name||"",durationDays:tr.duration_days,name:`VPN ${tr.title}`,description:giftMode?`Подарок: ${tr.title}`:`Тариф: ${tr.title}`,devices:Number(devices)||3}),
      signal:ctrl.signal,
    });
    const j = await r.json().catch(()=>({}));
    if(!r.ok) throw new Error(j.error||`API HTTP ${r.status}`);
    return j;
  } catch(e) {
    if(e.name==="AbortError") throw new Error("API timeout — сервер не ответил");
    throw e;
  } finally {
    clearTimeout(tid);
  }
}

// Called after balance credited — auto-complete pending order if user has enough balance
async function completePurchaseAfterTopup(tgId, po) {
  const u = user(tgId);
  const tr = tariff(po.tariff_code);
  if (!u || !tr) return;
  const devCount = Number(po.devices||3)||3;
  const finalPrice = calcPriceWithDevices(tr.price_rub, po.promo_pct, devCount);
  if (Number(u.balance_rub) < finalPrice) return; // still not enough
  const chatId = u.last_chat_id;
  const msgId  = u.last_menu_id;
  if (!chatId) return;
  try {
    await doPurchaseWithPromo(tgId, tgId, po.tariff_code, po.kind, po.promo_code, po.promo_pct, devCount);
    const me = user(tgId), tx = T(tgId);
    const s = sub(tgId);
    const lang = getLang(tgId);
    const lines = [tx.success_title,"",tx.success_plan(tariffTitle(tr,lang)),tx.success_paid(finalPrice),tx.success_exp(dt(s?.expires_at,lang)),"",`<code>${esc(s?.sub_url||"")}</code>`];
    await tg("sendMessage",{chat_id:chatId,text:lines.join("\n"),parse_mode:"HTML",reply_markup:{inline_keyboard:[[{text:tx.btn_connect,url:s?.sub_url||""}],[{text:tx.btn_sub,callback_data:"v:sub"},{text:tx.btn_home,callback_data:"v:home"}]]}}).catch(()=>{});
  } catch(e) {
    const tx = T(tgId);
    tg("sendMessage",{chat_id:chatId,text:`❌ Не удалось оформить подписку: ${e.message}`}).catch(()=>{});
  }
}

async function doPurchase(payerId, receiverId, code, kind) {
  return doPurchaseWithPromo(payerId, receiverId, code, kind, "", 0, 3);
}

async function doPurchaseWithPromo(payerId, receiverId, code, kind, promoCd, promoPct, devices=3) {
  const payer=user(payerId), receiver=user(receiverId), tr=tariff(code);
  if(!payer||!receiver||!tr) throw new Error("INVALID");
  const s=sub(receiverId), act=activeSub(s);
  const receiverHasTrial = act && s.plan_code==="trial";
  const renewAllowed = !act || (s && (s.expires_at - now()) <= 86400000); // expired or ≤1 day left
  if(kind==="new"   && act && !receiverHasTrial) throw new Error("ACTIVE");
  if(kind==="renew" && !renewAllowed) throw new Error("RENEW_TOO_EARLY");
  if(kind==="gift"  && act)  throw new Error("ACTIVE");
  const devCount = Math.max(1, Math.min(10, Number(devices)||3));
  const finalPrice = calcPriceWithDevices(tr.price_rub, promoPct || 0, devCount);
  if(Number(payer.balance_rub)<finalPrice) throw new Error("NO_MONEY");

  const api    = await createSubViaApi(receiver,tr,kind==="gift",devCount);
  const subUrl = api.subscriptionUrl || api.sub_url || "";
  if(!subUrl) throw new Error("API не вернул ссылку подписки");

  let newExpiresAt;
  if (kind==="renew") {
    const base = (s && s.expires_at > now()) ? s.expires_at : now();
    newExpiresAt = base + tr.duration_days * 86400000;
  } else if (kind==="gift") {
    const base = (s && s.expires_at > now()) ? s.expires_at : now();
    newExpiresAt = base + tr.duration_days * 86400000;
  } else if (kind==="new" && receiverHasTrial) {
    newExpiresAt = now() + tr.duration_days * 86400000;
  } else {
    newExpiresAt = Number(api.subscription?.expiresAt || api.expiresAt || (now() + tr.duration_days*86400000));
  }

  db.transaction(()=>{
    updateBalance(payerId,-finalPrice);
    addReferralReward(payerId, finalPrice);
    if(promoCd) usePromo(payerId, promoCd);
    db.prepare("INSERT INTO subscriptions(tg_id,plan_code,plan_title,sub_url,expires_at,is_active,devices,created_at,updated_at) VALUES(?,?,?,?,?,1,?,?,?) ON CONFLICT(tg_id) DO UPDATE SET plan_code=excluded.plan_code,plan_title=excluded.plan_title,sub_url=excluded.sub_url,expires_at=excluded.expires_at,is_active=1,devices=excluded.devices,updated_at=excluded.updated_at")
      .run(Number(receiverId),tr.code,tr.title,subUrl,newExpiresAt,devCount,now(),now());
    db.prepare("INSERT INTO purchases(tg_id,tariff_code,tariff_title,amount_rub,kind,created_at) VALUES(?,?,?,?,?,?)").run(Number(payerId),tr.code,tr.title,finalPrice,kind,now());
    if(kind==="gift") db.prepare("INSERT INTO gifts(from_tg_id,to_tg_id,tariff_code,tariff_title,amount_rub,created_at) VALUES(?,?,?,?,?,?)").run(Number(payerId),Number(receiverId),tr.code,tr.title,finalPrice,now());
  })();
  // Reset expiry notification so user gets reminded again when new sub nears expiry
  db.prepare("DELETE FROM notified_expiry WHERE tg_id=?").run(Number(receiverId));
  return {tr, url:subUrl, exp:newExpiresAt, finalPrice, devices:devCount};
}

async function buySelf(uid, chatId, msgId, code, mode, cbid, promoCd="", promoPct=0, devices=3) {
  try {
    const res=await doPurchaseWithPromo(uid,uid,code,mode,promoCd,promoPct,devices);
    const me=user(uid), tx=T(uid), lang=getLang(uid);
    const lines=[tx.success_title,"",tx.success_plan(tariffTitle(res.tr,lang)),tx.success_paid(res.finalPrice),tx.success_exp(dt(res.exp,lang)),"",`<code>${esc(res.url)}</code>`];
    const kb={inline_keyboard:[[{text:tx.btn_connect,url:res.url}],[{text:tx.btn_sub,callback_data:"v:sub"},{text:tx.btn_home,callback_data:"v:home"}]]};
    const nm=await renderMsg(chatId,msgId,lines.join("\n"),kb,null);
    setMenu(uid,chatId,nm);
    await tg("answerCallbackQuery",{callback_query_id:cbid,text:"✅"});
  } catch(e) {
    const map={ACTIVE:getLang(uid)==="en"?"Already active. Choose Renew.":"Подписка уже активна.",NO_ACTIVE:getLang(uid)==="en"?"No active sub to renew.":"Нет активной подписки для продления.",NO_MONEY:getLang(uid)==="en"?"Insufficient balance.":"Недостаточно средств.",RENEW_TOO_EARLY:getLang(uid)==="en"?"Renewal available only when subscription expires or 1 day before.":"Продление доступно только после истечения или за 1 день до конца подписки."};
    await tg("answerCallbackQuery",{callback_query_id:cbid,text:map[e.message]||e.message,show_alert:true});
    if(e.message==="NO_MONEY") {
      // Show direct payment options instead of just redirecting to topup
      await showDirectPayment(uid, chatId, msgId, code, mode, promoCd, promoPct, devices);
    }
  }
}

// Show direct payment screen when balance is insufficient
async function showDirectPayment(uid, chatId, msgId, code, mode, promoCd="", promoPct=0, devices=3) {
  const tr=tariff(code); if(!tr) return;
  const u=user(uid), tx=T(uid), lang=getLang(uid);
  const devCount=Math.max(1,Math.min(10,Number(devices)||3));
  const finalPrice = calcPriceWithDevices(tr.price_rub, promoPct, devCount);
  const rows=[];
  const poId = createPendingOrder(uid, code, mode, promoCd, promoPct, devCount);
  if(CRYPTOBOT_TOKEN){
    rows.push([{text:tx.btn_pay_crypto,callback_data:`direct:crypto:${poId}`}]);
  }
  if(isFkEnabled()){
    rows.push([{text:tx.btn_pay_qr,   callback_data:`direct:fk:${poId}:44`}]);
    rows.push([{text:tx.btn_pay_card, callback_data:`direct:fk:${poId}:36`}]);
    rows.push([{text:tx.btn_pay_sber, callback_data:`direct:fk:${poId}:43`}]);
  }
  rows.push([{text:tx.btn_back,callback_data:"v:buy"}]);
  const planName=tariffTitle(tr,lang);
  const surcharge=devicesSurcharge(devCount);
  const lines=[
    isRu?"Выберите способ оплаты:":"Choose payment method:",
    "",
    `<b>${planName}</b> — <b>${rub(finalPrice)}</b>`,
    ...(surcharge?[tx.dev_surcharge(surcharge)]:[]),
    ...(promoPct?[tx.promo_applied(promoPct)]:[]),
  ];
  const nm=await renderMsg(chatId,msgId,lines.join("\n"),{inline_keyboard:rows},null);
  setMenu(uid,chatId,nm);
}

// ── Device selector ───────────────────────────────────────────────────────────
async function showDeviceSelector(uid, chatId, msgId, code, mode, selectedDevices=3) {
  const tr=tariff(code); if(!tr) return;
  const tx=T(uid), lang=getLang(uid);
  const devCount=Math.max(1,Math.min(10,Number(selectedDevices)||3));
  const basePrice=tr.price_rub;
  const surcharge=devicesSurcharge(devCount);
  const totalPrice=basePrice+surcharge;
  const extraPrice=devicesExtraPrice();
  // Show 10 device buttons in a 2-column grid
  const devRows=[];
  for(let i=1;i<=10;i+=2){
    const row=[];
    for(const d of [i,i+1]){
      if(d>10) break;
      const isSelected=d===devCount;
      row.push({text:`${d} устр.${isSelected?" ✅":""}`,callback_data:`dev:${code}:${mode}:${d}`});
    }
    devRows.push(row);
  }
  const lines=[
    tx.dev_title,"",
    tx.dev_base(3),
    tx.dev_now(devCount),
    ...(surcharge?[tx.dev_surcharge(surcharge)]:[]),
    tx.dev_price(totalPrice),"",
    tx.dev_hint,
  ];
  const kb={inline_keyboard:[
    ...devRows,
    [{text:tx.dev_pay(totalPrice),callback_data:`dev:pay:${code}:${mode}:${devCount}`}],
    [{text:tx.btn_back,callback_data:"v:buy"}],
  ]};
  const nm=await renderMsg(chatId,msgId,lines.join("\n"),kb,null);
  setMenu(uid,chatId,nm);
}

async function askBuyConfirm(uid, chatId, msgId, code, mode, cbid, promoCd="", promoPct=0, devices=3) {
  const tr=tariff(code); if(!tr){if(cbid)await tg("answerCallbackQuery",{callback_query_id:cbid,text:"Тариф не найден",show_alert:true});return;}
  const u=user(uid), tx=T(uid), lang=getLang(uid), isRu=lang==="ru";
  const devCount=Math.max(1,Math.min(10,Number(devices)||3));
  const finalPrice = calcPriceWithDevices(tr.price_rub, promoPct, devCount);
  const surcharge=devicesSurcharge(devCount);
  const trialActive=isTrialSub(uid);
  const extendNote = trialActive
    ? (lang==="en"
        ? `<i>⚠️ Your free trial will be replaced. New expiry: ${dt(now() + tr.duration_days*86400000,lang)}</i>`
        : `<i>⚠️ Пробный период будет заменён. Новый срок: ${dt(now() + tr.duration_days*86400000,lang)}</i>`)
    : null;
  const lines=[
    tx.confirm_title(mode),"",
    tx.confirm_plan(tariffTitle(tr,lang)),
    tx.confirm_price(finalPrice),
    ...(surcharge?[tx.dev_surcharge(surcharge)]:[]),
    ...(promoPct?[tx.promo_applied(promoPct)]:[]),
    ...(extendNote ? ["",extendNote] : []),
    "",
    isRu?"Выберите способ оплаты:":"Choose payment method:",
  ];
  const hasUsedAnyPromo = !!db.prepare("SELECT 1 FROM promo_uses WHERE tg_id=?").get(Number(uid));
  const promoBtn = promoCd
    ? [{text:`🎟 ${promoCd} (${promoPct}%)`, callback_data:"noop"}]
    : hasUsedAnyPromo ? []
    : [{text:tx.btn_promo, callback_data:`promo:ask:${code}:${mode}:${devCount}`}];
  const poId = createPendingOrder(uid, code, mode, promoCd, promoPct, devCount);
  const payRows=[];
  if(promoBtn.length) payRows.push(promoBtn);
  if(CRYPTOBOT_TOKEN) payRows.push([{text:tx.btn_pay_crypto,callback_data:`direct:crypto:${poId}`}]);
  if(isFkEnabled()){
    payRows.push([{text:tx.btn_pay_qr,   callback_data:`direct:fk:${poId}:44`}]);
    payRows.push([{text:tx.btn_pay_card, callback_data:`direct:fk:${poId}:36`}]);
    payRows.push([{text:tx.btn_pay_sber, callback_data:`direct:fk:${poId}:43`}]);
  }
  payRows.push([{text:tx.btn_back,callback_data:"v:buy"}]);
  const kb={inline_keyboard:payRows};
  const nm=await renderMsg(chatId,msgId,lines.join("\n"),kb,null);
  setMenu(uid,chatId,nm);
  if(cbid) await tg("answerCallbackQuery",{callback_query_id:cbid});
}

// Gift: confirmation step before sending
async function askGiftConfirm(uid, chatId, msgId, code, toId, cbid) {
  const cbAns = (text, alert=true) => cbid
    ? tg("answerCallbackQuery",{callback_query_id:cbid,text,show_alert:alert}).catch(()=>{})
    : tg("sendMessage",{chat_id:chatId,text,parse_mode:"HTML"}).catch(()=>{});
  if(Number(uid)===Number(toId)){await cbAns(T(uid).gift_self);return;}
  const tr=tariff(code), to=user(toId), u=user(uid), tx=T(uid);
  if(!tr){await cbAns("Тариф не найден");return;}
  if(!to){await cbAns("Пользователь не найден");return;}
  // Block gift if recipient already has an active subscription
  const toSub=sub(toId);
  if(activeSub(toSub)){
    const msg=getLang(uid)==="en"
      ? "❌ Recipient already has an active subscription."
      : "❌ У получателя уже есть активная подписка.";
    await cbAns(msg); return;
  }
  if(Number(u.balance_rub)<Number(tr.price_rub)){await cbAns(tx.gift_no_bal(tr.price_rub,u.balance_rub));return;}
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
  const nm=await renderMsg(chatId,msgId,lines.join("\n"),kb,null);
  setMenu(uid,chatId,nm);
  if(cbid) await tg("answerCallbackQuery",{callback_query_id:cbid}).catch(()=>{});
}

async function giftToUser(fromId, toId, code, chatId, msgId, cbid) {
  try {
    if(Number(fromId)===Number(toId)){if(cbid)await tg("answerCallbackQuery",{callback_query_id:cbid,text:T(fromId).gift_self,show_alert:true});return;}
    const res=await doPurchase(fromId,toId,code,"gift");
    const to=user(toId), me=user(fromId), tx=T(fromId);
    const lines=[tx.gift_sent,"",tx.gift_to(to?.first_name||to?.username||String(toId)),tx.gift_plan(res.tr.title),tx.gift_price(res.tr.price_rub),tx.gift_after(me.balance_rub)];
    const nm=await renderMsg(chatId,msgId,lines.join("\n"),{inline_keyboard:[[{text:tx.btn_gift_send,callback_data:"v:gift"},{text:tx.btn_home,callback_data:"v:home"}]]},null);
    setMenu(fromId,chatId,nm);
    if(to){
      const rtx=T(to.tg_id);
      tg("sendMessage",{chat_id:to.tg_id,text:[rtx.gift_rcvd,"",rtx.gift_plan(res.tr.title),`${rtx.sub_exp(dt(res.exp,getLang(to.tg_id)))}`,`\n<code>${esc(res.url)}</code>`].join("\n"),parse_mode:"HTML",reply_markup:{inline_keyboard:[[{text:rtx.btn_connect,web_app:{url:res.url}}]]}}).catch(()=>{});
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
  const tx=T(uid), s=sub(uid), act=activeSub(s);
  const rows=[];
  rows.push([
    {text:act?"⭐ Моя подписка":tx.btn_buy, callback_data:act?"v:sub":"v:buy"},
    {text:tx.btn_gift_tab, callback_data:"v:gift"},
  ]);
  rows.push([{text:"🤝 Партнёрская программа", callback_data:"v:ref"}]);
  rows.push([{text:tx.btn_about, callback_data:"v:about"}]);
  if(lnk.proxy()) rows.push([{text:"🔥 Бесплатные прокси для TG",url:lnk.proxy()}]);
  if(isAdmin(uid)) rows.push([{text:"🛠 Панель администратора",callback_data:"a:main"}]);
  return{inline_keyboard:rows};
}

function subKb(uid) {
  const tx=T(uid), s=sub(uid), rows=[];
  if(activeSub(s)){
    const daysLeft=Math.floor(Math.max(0,s.expires_at-now())/86400000);
    rows.push([{text:tx.btn_guide,callback_data:"v:guide"},{text:tx.btn_connect,url:s.sub_url}]);
    if(daysLeft<=1) rows.push([{text:tx.btn_renew,callback_data:`pay:rw:${s.plan_code}`},{text:tx.btn_home,callback_data:"v:home"}]);
    else rows.push([{text:tx.btn_home,callback_data:"v:home"}]);
  } else {
    rows.push([{text:tx.btn_buy_sub,callback_data:"v:buy"}]);
    rows.push([{text:tx.btn_home,callback_data:"v:home"}]);
  }
  return{inline_keyboard:rows};
}

function buyKb(uid) {
  const tx=T(uid), lang=getLang(uid);
  const s=sub(uid), act=activeSub(s), trial=isTrialSub(uid);
  const rows=[];
  // Trial button at top if available
  if(trialEnabled()&&!hasUsedTrial(uid)&&!act){
    const days=trialDays();
    const label=lang==="en"?`🎁 Free trial (${days} days)`:`🎁 Пробный период (${days} дней)`;
    rows.push([{text:label,callback_data:"trial:start"}]);
  }
  // Tariff buttons in 2-column grid
  if(!act||trial){
    const ts=tariffs();
    for(let i=0;i<ts.length;i+=2){
      const row=[];
      row.push({text:`${tariffTitle(ts[i],lang)} | ${rub(ts[i].price_rub)}`,callback_data:`pay:n:${ts[i].code}`});
      if(ts[i+1]) row.push({text:`${tariffTitle(ts[i+1],lang)} | ${rub(ts[i+1].price_rub)}`,callback_data:`pay:n:${ts[i+1].code}`});
      rows.push(row);
    }
  }
  rows.push([{text:tx.btn_home,callback_data:"v:home"}]);
  return{inline_keyboard:rows};
}

function topupKb(uid) {
  const tx=T(uid), rows=[];
  if(CRYPTOBOT_TOKEN) rows.push([{text:tx.btn_pay_crypto,callback_data:"topup:crypto"}]);
  if(isFkEnabled()){
    rows.push([{text:tx.btn_pay_qr,callback_data:"fk:start:44"}]);
    rows.push([{text:tx.btn_pay_card,callback_data:"fk:start:36"}]);
    rows.push([{text:tx.btn_pay_sber,callback_data:"fk:start:43"}]);
  }
  rows.push([{text:tx.btn_back,callback_data:"v:profile"}]);
  return{inline_keyboard:rows};
}

function refKb(uid) {
  const u=user(uid), isRu=getLang(uid)==="ru";
  const link=refLink(u.ref_code);
  const shareText=isRu
    ? "Привет. Подключись к VPN по моей ссылке:\n\nРаботает быстро и стабильно."
    : "Hey! Connect to VPN using my link:\n\nFast and reliable.";
  const shareUrl="https://t.me/share/url?url="+encodeURIComponent(link)+"&text="+encodeURIComponent(shareText);
  const refBal=Number(u.ref_balance_rub||0);
  const minW=Number(setting("ref_min_withdraw","3000"))||3000;
  const canWithdraw=refBal>=minW;
  const rows=[];
  rows.push([{text:isRu?"📨 Пригласить друга":"📨 Invite friend", url:shareUrl}]);
  rows.push([
    {text:isRu?"📋 История начислений":"📋 Earnings",callback_data:"ref:hist:0"},
    {text:isRu?"🔄 Сменить код":"🔄 Reset code",callback_data:"ref:r"},
  ]);
  rows.push([{text:isRu?canWithdraw?`💸 Вывести ${refBal.toFixed(0)} ₽`:`💸 Вывод (от ${minW}₽)`:canWithdraw?`💸 Withdraw ${refBal.toFixed(0)} ₽`:`💸 Withdraw (min ${minW}₽)`, callback_data:"ref:withdraw"}]);
  rows.push([{text:isRu?"⚙️ Настроить реквизиты":"⚙️ Set payout details",callback_data:"ref:setpay"}]);
  rows.push([{text:isRu?"« Главное меню":"« Main menu",callback_data:"v:home"}]);
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
// Extract @username from a t.me URL or plain @handle
function toHandle(url) {
  if(!url) return "";
  const m=url.match(/t\.me\/([A-Za-z0-9_]+)/);
  if(m) return "@"+m[1];
  if(url.startsWith("@")) return url;
  return url;
}

function homeText(u) {
  const tx=T(u.tg_id), s=sub(u.tg_id), hasSub=activeSub(s);
  const isRu=getLang(u.tg_id)==="ru";
  const lines=[
    isRu?`🐸 Привет, ${esc(u.first_name||String(u.tg_id))}`:`🐸 Hello, ${esc(u.first_name||String(u.tg_id))}`,
    `${isRu?"Ваш ID":"Your ID"}: <code>${u.tg_id}</code>`,
  ];
  if(hasSub){
    const dd=Math.floor(Math.max(0,s.expires_at-now())/86400000);
    lines.push(isRu?`Подписка активна — осталось ${dd} дн.`:`Subscription active — ${dd} days left`);
  }
  const newsHandle=toHandle(lnk.news()), supHandle=toHandle(lnk.support());
  if(newsHandle||supHandle){
    lines.push("");
    if(newsHandle) lines.push(isRu?`📢 Канал — ${newsHandle}`:`📢 News — ${newsHandle}`);
    if(supHandle)  lines.push(isRu?`💬 Поддержка — ${supHandle}`:`💬 Support — ${supHandle}`);
  }
  return lines.join("\n");
}

function subText(uid) {
  const tx=T(uid), s=sub(uid), lang=getLang(uid), isRu=lang==="ru";
  if(!activeSub(s)) return [tx.sub_title,"",tx.sub_none].join("\n");
  const ms=Math.max(0,s.expires_at-now()), dd=Math.floor(ms/86400000), hh=Math.floor((ms%86400000)/3600000), mm=Math.floor((ms%3600000)/60000);
  const devCount=Number(s.devices||3)||3;
  const instruction=isRu
    ?"❗ Для использования VPN вам необходимо установить приложение из вкладки Инструкция.\n\nПосле установки приложения, используйте кнопку ниже или отсканируйте QR-код для подключения:"
    :"❗ To use VPN, install the app from the Instruction tab.\n\nAfter installation, use the button below or scan the QR code to connect:";
  return [
    `⚡ <b>${isRu?"Подключение":"Connection"}</b>`,
    "",
    instruction,
    "",
    tx.sub_plan(s.plan_title||s.plan_code||"—"),
    tx.sub_exp(dt(s.expires_at,lang)),
    tx.sub_left(dd,hh,mm),
  ].join("\n");
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
  const tx=T(uid), s=sub(uid), act=activeSub(s), trial=isTrialSub(uid), isRu=getLang(uid)==="ru";
  const lines=[isRu?"Выберите тариф и способ оплаты.":"Choose a plan and payment method."];
  if(trial)     lines.push("",tx.buy_trial_active);
  else if(act)  lines.push("",tx.buy_active);
  return lines.join("\n");
}

function topupText(uid) {
  const tx=T(uid), isRu=getLang(uid)==="ru";
  const rate=_rateCache.val;
  const lines=[tx.topup_title,""];
  if(CRYPTOBOT_TOKEN) lines.push(tx.crypto_rate(rate),"");
  lines.push(isRu?"Выберите способ пополнения:":"Choose top-up method:");
  return lines.join("\n");
}

function refText(uid) {
  const u=user(uid), isRu=getLang(uid)==="ru";
  const st=db.prepare("SELECT COUNT(*) c FROM referrals WHERE referrer_tg_id=?").get(Number(uid));
  const pct=Number(setting("ref_percent","30"))||30;
  const minW=Number(setting("ref_min_withdraw","3000"))||3000;
  const refBal=Number(u.ref_balance_rub||0);
  const link=refLink(u.ref_code);
  const payMethod=u.payout_method||"";
  const payDetails=u.payout_details||"";
  const shareText=isRu
    ? "Привет. Подключись к VPN по моей ссылке:\n\nРаботает быстро и стабильно."
    : "Hey! Connect to VPN using my link:\n\nFast and reliable.";
  const shareUrl="https://t.me/share/url?url="+encodeURIComponent(link)+"&text="+encodeURIComponent(shareText);
  const examplePayment=540, exampleBonus=Math.floor(examplePayment*pct/100);
  const lines=[
    isRu?"<b>🤝 Партнёрская программа</b>":"<b>🤝 Partner program</b>","",
    isRu
      ?`1. Приглашай друзей по своей уникальной ссылке и получай <b>${pct}%</b> с каждого пополнения.\n2. Выводи заработанные средства на удобный способ.`
      :`1. Invite friends with your unique link and earn <b>${pct}%</b> of every payment.\n2. Withdraw earnings to your preferred method.`,
    "",
    `<blockquote><code>${link}</code></blockquote>`,
    isRu?"<i>(Нажмите, чтобы скопировать)</i>":"<i>(Tap to copy)</i>","",
    "<b>Статистика:</b>",
    isRu?`Приглашено: <b>${st.c||0}</b>`:`Invited: <b>${st.c||0}</b>`,
    isRu?`Баланс: <b>${refBal.toFixed(2)} ₽</b>`:`Balance: <b>${refBal.toFixed(2)} ₽</b>`,
    isRu?`Способ вывода: <b>${payMethod||"не задан"}</b>`:`Payout method: <b>${payMethod||"not set"}</b>`,
    isRu?`Реквизиты: <b>${payDetails||"не указаны"}</b>`:`Details: <b>${payDetails||"not specified"}</b>`,"",
    isRu?`ℹ️ Вывод доступен от <b>${minW}₽</b>.`:`ℹ️ Minimum withdrawal: <b>${minW}₽</b>.`,
    isRu?`ℹ️ Текущая ставка: <b>${pct}%</b>`:`ℹ️ Current rate: <b>${pct}%</b>`,
    isRu?`Пример: платёж <b>${examplePayment}₽</b> → бонус <b>${exampleBonus.toFixed(1)}₽</b>`:`Example: payment <b>${examplePayment}₽</b> → bonus <b>${exampleBonus.toFixed(1)}₽</b>`,
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
  lines.push("",tx.rh_page(page,Math.max(1,Math.ceil(total/size))));
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
  const fkTotal=Number(db.prepare("SELECT COALESCE(SUM(amount_rub),0) s FROM freekassa_payments WHERE status='paid'").get().s||0);
  const fkCount=Number(db.prepare("SELECT COUNT(*) c FROM freekassa_payments WHERE status='paid'").get().c||0);
  return [
    "<b>Статистика</b>","",
    `Пользователей: <b>${uCount}</b>  (+${newDay} сегодня)`,
    `Активных подписок: <b>${aCount}</b>`,
    `Выручка за сегодня: <b>${rub(revDay)}</b>`,
    `Общая выручка: <b>${rub(revenue)}</b>`,
    `Начислено рефералам: <b>${rub(refPaid)}</b>`,
    `Crypto платежей: <b>${cryptoCount}</b> (${rub(cryptoTotal)})`,
    `FreeKassa платежей: <b>${fkCount}</b> (${rub(fkTotal)})`,
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

function adminFkText() {
  const sid = fkShopId();
  const min = fkMinRub();
  const path = fkNotifyPath();
  const ip = fkServerIp();
  const enabled = isFkEnabled();
  const notifyUrl = `https://${FK_DOMAIN}:${FK_PORT}${path}`;
  return [
    "<b>FreeKassa настройки</b>",
    "",
    `Статус: <b>${enabled ? "включено ✅" : "выключено ❌"}</b>`,
    `shop_id: <code>${sid || "не задан"}</code>`,
    `min amount: <code>${min}</code>`,
    `webhook path: <code>${esc(path)}</code>`,
    `server ip: <code>${esc(ip || "не определен")}</code>`,
    "",
    `<i>Notification URL:</i>`,
    `<code>${esc(notifyUrl)}</code>`,
  ].join("\n");
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
// Subscription screen with inline QR (Aurora-style)
// ─────────────────────────────────────────────────────────────────────────────
async function renderSubWithQR(uid, chatId, msgId) {
  const tx=T(uid), s=sub(uid), lang=getLang(uid), isRu=lang==="ru";
  const ms=Math.max(0,s.expires_at-now());
  const dd=Math.floor(ms/86400000), hh=Math.floor((ms%86400000)/3600000), mm=Math.floor((ms%3600000)/60000);

  const caption=[
    `⚡ <b>${isRu?"Подключение":"Connection"}</b>`,
    "",
    isRu
      ?"❗ Для использования VPN установите приложение из раздела Инструкция.\n\nПосле установки используйте кнопку ниже или отсканируйте QR-код для подключения:"
      :"❗ To use VPN, install the app from the Guide section.\n\nAfter installation, use the button below or scan the QR code to connect:",
    "",
    tx.sub_plan(s.plan_title||s.plan_code||"—"),
    tx.sub_exp(dt(s.expires_at,lang)),
    tx.sub_left(dd,hh,mm),
  ].join("\n");

  const kb={inline_keyboard:[
    [{text:tx.btn_guide,callback_data:"v:guide"},{text:tx.btn_connect,url:s.sub_url}],
    ...(dd<=1?[[{text:tx.btn_renew,callback_data:`pay:rw:${s.plan_code}`},{text:tx.btn_home,callback_data:"v:home"}]]:[[{text:tx.btn_home,callback_data:"v:home"}]]),
  ]};

  try {
    const buf=await QRCode.toBuffer(s.sub_url,{width:512,margin:2,errorCorrectionLevel:"M"});
    // Delete the old inline menu message and send a fresh photo message
    if(msgId) await tg("deleteMessage",{chat_id:chatId,message_id:msgId}).catch(()=>{});
    const m=await sendPhotoBuffer(chatId,buf,"image/png",caption,kb);
    setMenu(uid,chatId,m.message_id);
  } catch(e) {
    console.error("[subQR]",e.message);
    // Fallback: text-only subscription screen
    const nm=await renderMsg(chatId,msgId,subText(uid),subKb(uid),null);
    setMenu(uid,chatId,nm);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Render  — the main view dispatcher
// ─────────────────────────────────────────────────────────────────────────────
async function render(uid, chatId, msgId, view, data={}) {
  const u=user(uid); if(!u) return;
  const tx=T(uid);
  let text="", kb={}, photo="";

  switch(view){
    case "home":
      text=homeText(u); kb=homeKb(uid);
      break;
    case "sub":
      if(activeSub(sub(uid))){
        // Active subscription → show QR code inline (Aurora-style)
        await renderSubWithQR(uid,chatId,msgId);
        return; // renderSubWithQR handles setMenu — skip the bottom renderMsg/setMenu
      }
      text=subText(uid); kb=subKb(uid);
      break;
    case "buy":
      text=buyText(uid); kb=buyKb(uid);
      break;
    case "guide": {
      const lang=getLang(uid);
      const rawGuide = lang==="en"
        ? (setting("guide_text_en","")||setting("guide_text",""))
        : setting("guide_text","");
      text=rawGuide?parseLinks(rawGuide):[tx.guide_title,"","<i>Инструкция не настроена.</i>"].join("\n");
      const kbRows=[];
      if(lnk.support()) kbRows.push([{text:tx.btn_support,url:lnk.support()}]);
      kbRows.push([{text:tx.btn_home,callback_data:"v:home"}]);
      kb={inline_keyboard:kbRows};
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
      kb={inline_keyboard:kbRows};
      break;
    }
    case "lang":
      text=[tx.lang_title,"",`${tx.lang_current}: <b>${getLang(uid)==="ru"?tx.lang_ru:tx.lang_en}</b>`].join("\n");
      kb=langKb(uid); photo="";
      break;
    case "ref":
      text=refText(uid); kb=refKb(uid);
      break;

    case "gift":
      text=[tx.gift_title,"",tx.gift_choose].join("\n"); kb=giftKb(uid);
      break;
    case "purchases": {
      const {text:ht,total,size}=purchasesText(uid,Number(data.page||0));
      text=ht; kb=pagingKb(uid,"ph",Number(data.page||0),total,size,"v:home"); photo="";
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
        [{text:"🤝 Реф. процент",callback_data:"a:r"},{text:"📋 Инструкция",callback_data:"a:guide_edit"}],
        [{text:"📢 Канал + Пробный период",callback_data:"a:channel"}],
        [{text:"💳 FreeKassa",callback_data:"a:fk"}],
        [{text:"💸 Заявки на вывод",callback_data:"a:withdrawals:0"}],
        [{text:"🎟 Промокоды",callback_data:"a:promo"},{text:"🔍 Поиск юзера",callback_data:"a:find"}],
        [{text:"👥 Пользователи",callback_data:"a:users:0"},{text:"🗄 База данных",callback_data:"a:db"}],
        [{text:"« Назад",callback_data:"v:home"}],
      ]};
      break;
    }

    case "a_withdrawals": {
      const wPage=Number(data?.page||0), wSize=10, wOff=wPage*wSize;
      const wRows=db.prepare("SELECT w.*,u.first_name,u.username FROM withdrawal_requests w LEFT JOIN users u ON u.tg_id=w.tg_id WHERE w.status='pending' ORDER BY w.created_at DESC LIMIT ? OFFSET ?").all(wSize,wOff);
      const wTotal=Number(db.prepare("SELECT COUNT(*) c FROM withdrawal_requests WHERE status='pending'").get().c||0);
      const wLines=["<b>💸 Заявки на вывод</b>",""];
      if(!wRows.length) wLines.push("<i>Нет заявок.</i>");
      else wRows.forEach((w,i)=>{
        const name=esc(w.first_name||w.username||String(w.tg_id));
        wLines.push(`${wOff+i+1}. <b>${name}</b> (<code>${w.tg_id}</code>)`);
        wLines.push(`   Сумма: <b>${rub(w.amount_rub)}</b>  |  ${esc(w.method)}: <b>${esc(w.details)}</b>`);
        wLines.push(`   <i>${dt(w.created_at)}</i>`);
      });
      text=wLines.join("\n");
      const wKbRows=[];
      wRows.forEach(w=>{
        wKbRows.push([
          {text:`✅ #${w.id} Выплачено`,callback_data:`a:wd_done:${w.id}:${wPage}`},
          {text:`❌ #${w.id} Отклонить`,callback_data:`a:wd_reject:${w.id}:${wPage}`},
        ]);
      });
      // pagination
      const wMax=Math.max(0,Math.ceil(wTotal/wSize)-1);
      const wNav=[];
      if(wPage>0) wNav.push({text:"◀",callback_data:`a:withdrawals_p:${wPage-1}`});
      wNav.push({text:`${wPage+1}/${wMax+1}`,callback_data:"noop"});
      if(wPage<wMax) wNav.push({text:"▶",callback_data:`a:withdrawals_p:${wPage+1}`});
      if(wNav.length>1) wKbRows.push(wNav);
      wKbRows.push([{text:"« Назад",callback_data:"a:main"}]);
      kb={inline_keyboard:wKbRows};
      break;
    }

    case "a_promo": {
      const promos = db.prepare("SELECT * FROM promo_codes ORDER BY rowid DESC LIMIT 20").all();
      const lines = ["<b>Промокоды</b>",""];
      if (!promos.length) lines.push("<i>Промокодов нет.</i>");
      else promos.forEach(p=>{
        const status = p.is_active ? "✅" : "❌";
        const uses = p.uses_max > 0 ? `${p.uses_current}/${p.uses_max}` : `${p.uses_current}/∞`;
        lines.push(`${status} <code>${esc(p.code)}</code> — <b>${p.discount_pct}%</b>  (${uses})`);
      });
      text=lines.join("\n");
      kb={inline_keyboard:[
        [{text:"➕ Добавить промокод",callback_data:"a:promo_add"}],
        [{text:"🗑 Деактивировать",callback_data:"a:promo_del"}],
        [{text:"« Назад",callback_data:"a:main"}],
      ]};
      break;
    }

    case "a_users": {
      const page=Number(data.page||0), size=10, off=page*size;
      const rows=db.prepare("SELECT u.*,(SELECT is_active FROM subscriptions s WHERE s.tg_id=u.tg_id AND s.is_active=1) as sub_active FROM users u ORDER BY u.created_at DESC LIMIT ? OFFSET ?").all(size,off);
      const total=Number(db.prepare("SELECT COUNT(*) c FROM users").get().c||0);
      const lines=["<b>Пользователи</b>",""];
      rows.forEach(u=>{
        const subMark=u.sub_active?"⭐":"";
        lines.push(`${subMark} <code>${u.tg_id}</code> ${esc(u.first_name||"")}${u.username?` @${esc(u.username)}`:""}`);
      });
      text=lines.join("\n");
      const nav=[];
      if(page>0)nav.push({text:"◀",callback_data:`a:users:${page-1}`});
      nav.push({text:`${page+1}/${Math.ceil(total/size)||1}`,callback_data:"noop"});
      if((page+1)*size<total)nav.push({text:"▶",callback_data:`a:users:${page+1}`});
      kb={inline_keyboard:[nav,[{text:"« Назад",callback_data:"a:main"}]]};
      break;
    }

    case "a_grant": {
      const targetId=Number(data.id||0);
      if(!targetId){text="Ошибка: ID не передан.";kb={inline_keyboard:[[{text:"« Назад",callback_data:"a:main"}]]};break;}
      const gtu=user(targetId);
      text=`<b>Выдать подписку</b>\n\nПользователь: ${gtu?esc(gtu.first_name||String(targetId)):String(targetId)} (<code>${targetId}</code>)\n\nВыберите тариф:`;
      kb={inline_keyboard:[
        ...tariffs().map(t=>[{text:`${t.title} (${t.duration_days} дн.)`,callback_data:`a:grant_ok:${targetId}:${t.code}`}]),
        [{text:"« Назад",callback_data:`a:user_back:${targetId}`}],
      ]};
      break;
    }

    case "a_tariffs":
      text=`<b>Цены тарифов</b>\n\n${tariffs().map(x=>`${x.title}: <b>${rub(x.price_rub)}</b>`).join("\n")}\n\n<i>Доп. устройства (от 4+): +${rub(devicesExtraPrice())} за каждое</i>`;
      kb={inline_keyboard:[...tariffs().map(x=>[{text:`✏️ ${x.title} — ${rub(x.price_rub)}`,callback_data:`a:te:${x.code}`}]),[{text:`⚙️ Цена за доп. устройство — ${rub(devicesExtraPrice())}`,callback_data:"a:dev_price"}],[{text:"« Назад",callback_data:"a:main"}]]};
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
    case "a_links": {
      text=adminLinksText();
      const linkKeys=[["url_support","Поддержка"],["url_privacy","Политика конф."],["url_terms","Соглашение"],["url_proxy","Прокси"],["url_news","Канал"],["url_status","Статус серверов"]];
      kb={inline_keyboard:[...linkKeys.map(([k,label])=>[{text:`✏️ ${label}`,callback_data:`a:lnk:${k}`}]),[{text:"« Назад",callback_data:"a:main"}]]};
      break;
    }
    case "a_bcast":
      text="<b>Рассылка</b>\n\nОтправьте любое сообщение: текст (с форматированием), фото, видео, GIF, документ или голосовое. Форматирование и медиа сохраняются автоматически.\n\n<i>Задержка 35 мс/сообщение для соблюдения лимитов Telegram.</i>";
      kb={inline_keyboard:[[{text:"✏️ Создать рассылку",callback_data:"a:bs"}],[{text:"« Назад",callback_data:"a:main"}]]};
      break;
    case "a_ref":
      text=["<b>Реферальная программа</b>","",
        `Ставка: <b>${setting("ref_percent","30")}%</b>`,
        `Мин. вывод: <b>${setting("ref_min_withdraw","3000")}₽</b>`,
        "<i>Награда начисляется на реферальный баланс; выводится вручную через заявки.</i>",
      ].join("\n");
      kb={inline_keyboard:[
        [{text:"✏️ Изменить ставку",callback_data:"a:rp"},{text:"✏️ Мин. вывод",callback_data:"a:rmin"}],
        [{text:"💸 Заявки на вывод",callback_data:"a:withdrawals:0"}],
        [{text:"« Назад",callback_data:"a:main"}],
      ]};
      break;
    case "a_db":
      text="<b>База данных</b>\n\nСкачайте или импортируйте БД.\n⚠️ После импорта бот перезапустится.";
      kb={inline_keyboard:[[{text:"⬇️ Скачать",callback_data:"a:db_export"}],[{text:"⬆️ Импорт",callback_data:"a:db_import_start"}],[{text:"« Назад",callback_data:"a:main"}]]};
      break;
    case "a_user_info": {
      const tu=user(data.id);
      if(!tu){text="Пользователь не найден.";kb={inline_keyboard:[[{text:"« Назад",callback_data:"a:main"}]]};break;}
      text=adminUserInfoText(tu);
      const ts=sub(tu.tg_id), hasSub=ts&&activeSub(ts);
      kb={inline_keyboard:[
        [{text:"➕ Пополнить баланс",callback_data:`a:bal_add:${tu.tg_id}`}],
        [{text:"🎁 Выдать подписку",callback_data:`a:grant:${tu.tg_id}`}],
        ...(hasSub?[[{text:"🚫 Отобрать подписку",callback_data:`a:sub_revoke:${tu.tg_id}`}]]:[]),
        [{text:"« Назад",callback_data:"a:main"}],
      ]};
      break;
    }
    case "a_channel": {
      const chanId   = setting("channel_id","") || "<i>не задан</i>";
      const chanUrl  = setting("channel_invite_url","") || "<i>не задана</i>";
      const tEnabled = trialEnabled();
      const tDays    = trialDays();
      text=[
        "<b>📢 Канал + Пробный период</b>",
        "",
        `Канал (ID/@username): <code>${esc(chanId)}</code>`,
        `Ссылка-приглашение: ${esc(chanUrl)}`,
        "",
        `Пробный период: <b>${tEnabled?"включён ✅":"выключен ❌"}</b>`,
        `Длительность: <b>${tDays} дн.</b>`,
        "",
        "<i>Бот должен быть администратором канала для проверки подписки.</i>",
      ].join("\n");
      kb={inline_keyboard:[
        [{text:"✏️ Установить канал",callback_data:"a:chan_id"}],
        [{text:"🔗 Ссылка-приглашение",callback_data:"a:chan_url"}],
        [{text:tEnabled?"🔴 Отключить пробный":"🟢 Включить пробный",callback_data:"a:trial_toggle"}],
        [{text:`⏱ Длительность: ${tDays} дн.`,callback_data:"a:trial_days"}],
        [{text:"« Назад",callback_data:"a:main"}],
      ]};
      break;
    }
    case "a_fk": {
      text = adminFkText();
      kb = { inline_keyboard: [
        [{text:"✏️ shop_id",callback_data:"a:fk_shop"}],
        [{text:"✏️ min amount",callback_data:"a:fk_min"}],
        [{text:"✏️ webhook path",callback_data:"a:fk_path"}],
        [{text:"« Назад",callback_data:"a:main"}],
      ]};
      break;
    }
    case "a_guide_edit":
      text=[
        "<b>Инструкция по подключению</b>",
        "",
        "<i>Формат ссылок: [Название|URL]</i>",
        "",
        `🇷🇺 <b>RU</b>: <blockquote>${esc(setting("guide_text","")).slice(0,200)||"<i>не задана</i>"}</blockquote>`,
        "",
        `🇬🇧 <b>EN</b>: <blockquote>${esc(setting("guide_text_en","")).slice(0,200)||"<i>not set</i>"}</blockquote>`,
      ].join("\n");
      kb={inline_keyboard:[
        [{text:"✏️ Изменить 🇷🇺 Русский",callback_data:"a:guide_ru"}],
        [{text:"✏️ Edit 🇬🇧 English",callback_data:"a:guide_en"}],
        [{text:"« Назад",callback_data:"a:main"}],
      ]};
      break;
    default:
      text=homeText(u); kb=homeKb(uid);
  }

  const nm=await renderMsg(chatId,msgId,text,kb,null);
  setMenu(uid,chatId,nm);
}

// ─────────────────────────────────────────────────────────────────────────────
// Crypto topup flows
// ─────────────────────────────────────────────────────────────────────────────
async function startCryptoTopup(uid, chatId) {
  expireOldCryptoPayments(uid);
  const rate=await getUsdtRate(), tx=T(uid);
  const text=[tx.crypto_title,"",tx.crypto_desc,tx.crypto_min(CRYPTO_MIN_RUB),tx.crypto_rate(rate),"",tx.crypto_enter].join("\n");
  const promptId = await sendPrompt(chatId, text, "cancel:topup_crypto");
  // Store prompt msg id so we can delete it when user replies
  setUserState(uid,"topup_crypto_amount",String(promptId));
}

async function handleCryptoAmount(uid, chatId, text, promptMsgId, userMsgId) {
  const amount=Math.round(parseFloat(text.replace(/[^\d.]/g,""))||0);
  const tx=T(uid);
  // Always delete user's typed message
  delMsg(chatId, userMsgId);
  if(!amount||amount<CRYPTO_MIN_RUB){
    // Edit the prompt to show the error, keep cancel button
    if(promptMsgId) {
      await tg("editMessageText",{
        chat_id:chatId, message_id:promptMsgId,
        text:`❌ ${tx.crypto_min(CRYPTO_MIN_RUB)}\n\n${tx.crypto_enter}`,
        parse_mode:"HTML",
        reply_markup:{inline_keyboard:[[{text:"❌ Отмена",callback_data:"cancel:topup_crypto"}]]},
      }).catch(()=>{});
    }
    return;
  }
  clearUserState(uid);
  // Delete the prompt message
  delMsg(chatId, promptMsgId);
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

async function startFkTopup(uid, chatId, methodId) {
  expireOldFkPayments(uid);
  const tx = T(uid);
  const method = Number(methodId);
  const minRub = fkMinRub();
  const methodName = methodTitle(method, getLang(uid));
  const text = [tx.fk_title(methodName), tx.fk_min(minRub), "", tx.fk_enter].join("\n");
  const promptId = await sendPrompt(chatId, text, `cancel:topup_fk:${method}`);
  setUserState(uid, "topup_fk_amount", `${method}:${promptId}`);
}

async function handleFkAmount(uid, chatId, text, methodId, promptMsgId, userMsgId) {
  const tx = T(uid);
  const minRub = fkMinRub();
  const serverIp = fkServerIp();
  // Always delete user's typed message
  delMsg(chatId, userMsgId);
  if (!isFkEnabled()) {
    delMsg(chatId, promptMsgId);
    await tg("sendMessage", { chat_id: chatId, text: "❌ Способ пополнения временно недоступен." });
    return;
  }
  const amount = Math.round(parseFloat(String(text).replace(/[^\d.]/g, "")) || 0);
  if (!amount || amount < minRub) {
    if(promptMsgId) {
      await tg("editMessageText",{
        chat_id:chatId, message_id:promptMsgId,
        text:`❌ ${tx.fk_min(minRub)}\n\n${tx.fk_enter}`,
        parse_mode:"HTML",
        reply_markup:{inline_keyboard:[[{text:"❌ Отмена",callback_data:`cancel:topup_fk:${methodId}`}]]},
      }).catch(()=>{});
    }
    return;
  }
  if (!serverIp) {
    delMsg(chatId, promptMsgId);
    await tg("sendMessage", { chat_id: chatId, text: "❌ Внешний IP не определен. Перезапустите бота." });
    return;
  }
  clearUserState(uid);
  delMsg(chatId, promptMsgId);
  const email = `user${uid}@${FK_EMAIL_DOMAIN}`;
  let order;
  try {
    order = await createFkOrder({ uid, amountRub: amount, methodId, email, ip: serverIp });
  } catch (e) {
    await tg("sendMessage", { chat_id: chatId, text: "❌ Не удалось создать счёт. Попробуйте позже." });
    return;
  }
  if (!order.location) {
    await tg("sendMessage", { chat_id: chatId, text: "❌ Не удалось получить ссылку оплаты. Попробуйте позже." });
    return;
  }
  const fkId = createFkPaymentRow(uid, amount, Number(methodId), order.paymentId, order.location, order.orderId);
  const msgText = [
    tx.fk_created,
    "",
    `Сумма: <b>${rub(amount)}</b>`,
    `Метод: <b>${esc(methodTitle(methodId, getLang(uid)))}</b>`,
    "",
    tx.fk_steps,
    tx.fk_wait,
  ].join("\n");
  await tg("sendMessage", {
    chat_id: chatId,
    text: msgText,
    parse_mode: "HTML",
    disable_web_page_preview: true,
    reply_markup: {
      inline_keyboard: [
        [{ text: "💳 Оплатить", url: order.location }],
        [{ text: tx.btn_check, callback_data: `fk:check:${fkId}` }],
        [{ text: tx.btn_cancel, callback_data: `fk:cancel:${fkId}` }],
      ],
    },
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Admin state handler
// ─────────────────────────────────────────────────────────────────────────────
async function handleAdminState(msg) {
  const aid=Number(msg.from?.id||0); if(!isAdmin(aid)) return false;
  const row=getAdminState(aid); if(!row) return false;
  const text=String(msg.text||"").trim(), chatId=Number(msg.chat?.id||0);

  // Delete admin's typed message for clean flow
  delMsg(chatId, Number(msg.message_id||0));

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
    case "dev_extra_price": {
      const n=Number(text);
      if(!Number.isFinite(n)||n<0){await tg("sendMessage",{chat_id:chatId,text:"Введите число >= 0."});return true;}
      setSetting("devices_extra_price",String(Math.round(n))); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:`✅ Цена за доп. устройство: ${rub(Math.round(n))}`});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_tariffs"); return true;
    }
    case "gif": {
      const v=msg.animation?.file_id||msg.video?.file_id||text; if(!v){await tg("sendMessage",{chat_id:chatId,text:"Отправьте GIF."});return true;}
      setSetting(row.payload,v); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:"✅ GIF сохранён."});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_gif"); return true;
    }
    case "edit_link": {
      const urlVal=text.trim();
      const validUrl=!urlVal||urlVal==="-"||urlVal.startsWith("http")||urlVal.startsWith("@")||urlVal.startsWith("t.me/");
      if(!validUrl){await tg("sendMessage",{chat_id:chatId,text:"Введите URL (https://...), @username, t.me/... или «-» для очистки."});return true;}
      if(urlVal==="-"||urlVal==="") delSetting(row.payload);
      else setSetting(row.payload,urlVal);
      clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:"✅ Ссылка обновлена."});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_links"); return true;
    }
    case "guide_text":
      setSetting("guide_text",text); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:"✅ Инструкция (RU) обновлена."});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_guide_edit"); return true;

    case "guide_text_en":
      setSetting("guide_text_en",text); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:"✅ Guide (EN) updated."});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_guide_edit"); return true;

    case "broadcast": {
      const promptId = Number(row.payload||0);
      clearAdminState(aid);
      // Clean up the "send your broadcast" prompt message
      if(promptId) delMsg(chatId, promptId);
      // Store chat_id + message_id to use copyMessage later
      const meta=JSON.stringify({chat_id:Number(chatId),message_id:Number(msg.message_id)});
      setAdminState(aid,"broadcast_preview",meta);
      const total=db.prepare("SELECT COUNT(*) c FROM users").get()?.c||0;
      // Show preview with confirm/cancel
      await tg("sendMessage",{chat_id:chatId,text:`👆 <b>Предпросмотр выше.</b>\n\nРазослать <b>${total}</b> пользователям?`,parse_mode:"HTML",reply_markup:{inline_keyboard:[
        [{text:`📨 Разослать ${total} пользователям`,callback_data:"a:bs_confirm"}],
        [{text:"✏️ Изменить",callback_data:"a:bs"}],
        [{text:"❌ Отмена",callback_data:"a:bs_cancel"}],
      ]}});
      return true;
    }
    case "broadcast_preview": {
      // Admin sent another message while preview is pending — treat it as new broadcast content.
      // The previous preview confirmation message stays (user can still cancel it).
      clearAdminState(aid);
      const meta=JSON.stringify({chat_id:Number(chatId),message_id:Number(msg.message_id)});
      setAdminState(aid,"broadcast_preview",meta);
      const total=db.prepare("SELECT COUNT(*) c FROM users").get()?.c||0;
      await tg("sendMessage",{chat_id:chatId,text:`👆 <b>Обновлён предпросмотр.</b>\n\nРазослать <b>${total}</b> пользователям?`,parse_mode:"HTML",reply_markup:{inline_keyboard:[
        [{text:`📨 Разослать ${total} пользователям`,callback_data:"a:bs_confirm"}],
        [{text:"✏️ Изменить",callback_data:"a:bs"}],
        [{text:"❌ Отмена",callback_data:"a:bs_cancel"}],
      ]}});
      return true;
    }
    case "ref_percent": {
      const n=Number(text); if(!Number.isFinite(n)||n<0||n>100){await tg("sendMessage",{chat_id:chatId,text:"Введите 0..100."});return true;}
      setSetting("ref_percent",Math.round(n)); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:`✅ Ставка: ${Math.round(n)}%`});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_ref"); return true;
    }
    case "ref_min_withdraw": {
      const n=Number(text); if(!Number.isFinite(n)||n<1){await tg("sendMessage",{chat_id:chatId,text:"Введите сумму >= 1."});return true;}
      setSetting("ref_min_withdraw",Math.round(n)); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:`✅ Мин. вывод: ${Math.round(n)}₽`});
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
      // User entered a gift recipient ID or username
      clearUserState(aid);
      const code=row.payload;
      let found=null;
      if(/^\d+$/.test(text)) found=user(Number(text));
      if(!found&&text.startsWith("@")) found=db.prepare("SELECT * FROM users WHERE username=?").get(text.slice(1));
      if(!found){
        await tg("sendMessage",{chat_id:chatId,text:"❌ Пользователь не найден в боте."});
        return true;
      }
      // Route through the standard confirm screen
      await askGiftConfirm(aid, chatId, user(aid)?.last_menu_id||null, code, found.tg_id, null);
      return true;
    }
    case "chan_id": {
      clearAdminState(aid);
      const val = text.trim();
      if(val==="-"||val==="") {
        delSetting("channel_id");
        await tg("sendMessage",{chat_id:chatId,text:"✅ Канал отключён."});
      } else {
        setSetting("channel_id", val);
        await tg("sendMessage",{chat_id:chatId,text:`✅ Канал установлен: <code>${esc(val)}</code>`,parse_mode:"HTML"});
      }
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_channel"); return true;
    }
    case "chan_url": {
      clearAdminState(aid);
      const val = text.trim();
      if(val==="-"||val==="") {
        delSetting("channel_invite_url");
        await tg("sendMessage",{chat_id:chatId,text:"✅ Ссылка очищена."});
      } else {
        setSetting("channel_invite_url", val);
        await tg("sendMessage",{chat_id:chatId,text:`✅ Ссылка сохранена.`,parse_mode:"HTML"});
      }
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_channel"); return true;
    }
    case "trial_days": {
      const n = parseInt(text,10);
      if(!Number.isFinite(n)||n<1||n>365){await tg("sendMessage",{chat_id:chatId,text:"Введите число от 1 до 365."});return true;}
      setSetting("trial_days",String(n)); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:`✅ Длительность: ${n} дн.`});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_channel"); return true;
    }
    case "fk_shop_id": {
      const n = parseInt(text, 10);
      if(!Number.isFinite(n) || n <= 0){await tg("sendMessage",{chat_id:chatId,text:"Введите корректный shop_id (> 0)."});return true;}
      setSetting("fk_shop_id", String(n)); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:`✅ shop_id: <code>${n}</code>`,parse_mode:"HTML"});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_fk"); return true;
    }
    case "fk_min_rub": {
      const n = parseInt(text, 10);
      if(!Number.isFinite(n) || n < 1){await tg("sendMessage",{chat_id:chatId,text:"Введите минимальную сумму (целое число >= 1)."});return true;}
      setSetting("fk_min_rub", String(n)); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:`✅ min amount: <code>${n}</code>`,parse_mode:"HTML"});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_fk"); return true;
    }
    case "fk_notify_path": {
      let p = text.trim();
      if(!p){await tg("sendMessage",{chat_id:chatId,text:"Введите путь webhook, например /freekassa/notify"});return true;}
      if(p==="-"||p==="default") p = "/freekassa/notify";
      if(!p.startsWith("/")) p = `/${p}`;
      p = p.replace(/\s+/g, "");
      setSetting("fk_notify_path", p); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:`✅ webhook path: <code>${esc(p)}</code>`,parse_mode:"HTML"});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_fk"); return true;
    }

    case "promo_add": {
      const parts = text.trim().split(/\s+/);
      if (parts.length < 2) { await tg("sendMessage",{chat_id:chatId,text:"Неверный формат. Пример: SALE10 10 100"}); return true; }
      const promoCode = parts[0].toUpperCase();
      const pct = parseInt(parts[1], 10);
      const maxUses = parts[2] ? parseInt(parts[2], 10) : 0;
      if (!promoCode || isNaN(pct) || pct < 1 || pct > 99) { await tg("sendMessage",{chat_id:chatId,text:"Скидка должна быть от 1 до 99%"}); return true; }
      try {
        db.prepare("INSERT INTO promo_codes(code,discount_pct,uses_max,uses_current,is_active,created_at) VALUES(?,?,?,0,1,?) ON CONFLICT(code) DO UPDATE SET discount_pct=excluded.discount_pct,uses_max=excluded.uses_max,is_active=1")
          .run(promoCode, pct, maxUses||0, now());
        clearAdminState(aid);
        await tg("sendMessage",{chat_id:chatId,text:`✅ Промокод <code>${esc(promoCode)}</code> — <b>${pct}%</b> (макс. ${maxUses||"∞"})`,parse_mode:"HTML"});
        await render(aid,chatId,user(aid)?.last_menu_id||null,"a_promo");
      } catch(e) { await tg("sendMessage",{chat_id:chatId,text:`❌ Ошибка: ${e.message}`}); }
      return true;
    }

    case "promo_deactivate": {
      const code = text.trim().toUpperCase();
      const res = db.prepare("UPDATE promo_codes SET is_active=0 WHERE code=? COLLATE NOCASE").run(code);
      clearAdminState(aid);
      if (res.changes) await tg("sendMessage",{chat_id:chatId,text:`✅ Промокод <code>${esc(code)}</code> деактивирован.`,parse_mode:"HTML"});
      else await tg("sendMessage",{chat_id:chatId,text:"❌ Промокод не найден."});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_promo"); return true;
    }
  } // end switch
  return false;
} // end handleAdminState

// ─────────────────────────────────────────────────────────────────────────────
// Message handler
// ─────────────────────────────────────────────────────────────────────────────
async function handleMessage(msg) {
  const from=msg.from||{}, chatId=Number(msg.chat?.id||0);
  if(!chatId||!from.id) return;
  if(msg.chat?.type!=="private") return;
  upsertUser(from,chatId);
  const ustate=getUserState(from.id);
  const text=String(msg.text||"").trim();
  const userMsgId=Number(msg.message_id||0);

  // ── Admin state takes priority — MUST be before any user-state deletion ─────
  // If admin is in an admin-state flow (broadcast, tariff edit, etc.) we must
  // handle the message immediately without touching it.  Placing this before the
  // user-state delete block prevents broadcast content from being wiped when the
  // admin also happens to have a stale user_state.
  if(isAdmin(from.id) && await handleAdminState(msg)) return;

  // ── Delete the user's raw message only when they are in a user input state ──
  if(ustate) delMsg(chatId, userMsgId);

  // Universal /cancel command
  if(text==="/cancel"&&ustate){
    clearUserState(from.id);
    // parse promptId from payload (always last segment after last colon-group)
    const promptId=Number((ustate.payload||"").split(":").pop())||0;
    delMsg(chatId, promptId);
    await render(from.id,chatId,user(from.id)?.last_menu_id||null,"home");
    return;
  }

  // Promo code entry
  if(ustate?.state==="promo_input"){
    if(!msg.text||msg.text.startsWith("/")) return;
    const parts = (ustate.payload||"").split(":");
    const code2=parts[0], mode2=parts[1], devices2=Number(parts[2]||3), promptMsgId=Number(parts[3]||0);
    clearUserState(from.id);
    delMsg(chatId, promptMsgId);
    const result = validatePromo(from.id, text.trim());
    const tx2 = T(from.id);
    if(!result.ok){
      const errMsg = result.reason==="used" ? tx2.promo_used : tx2.promo_invalid;
      await tg("sendMessage",{chat_id:chatId,text:errMsg,parse_mode:"HTML"});
      await askBuyConfirm(from.id,chatId,user(from.id)?.last_menu_id||null,code2,mode2,"","",0,devices2);
      return;
    }
    const pct = result.promo.discount_pct;
    await tg("sendMessage",{chat_id:chatId,text:tx2.promo_ok(pct,text.trim().toUpperCase()),parse_mode:"HTML"});
    await askBuyConfirm(from.id,chatId,user(from.id)?.last_menu_id||null,code2,mode2,null,text.trim().toUpperCase(),pct,devices2);
    return;
  }

  // Gift recipient ID entry
  if(ustate?.state==="gift_recipient_id"){
    if(!msg.text||msg.text.startsWith("/")) return;
    const parts=(ustate.payload||"").split(":");
    const code=parts[0], promptMsgId=Number(parts[1]||0);
    clearUserState(from.id);
    delMsg(chatId, promptMsgId);
    let found=null;
    if(/^\d+$/.test(text)) found=user(Number(text));
    if(!found&&text.startsWith("@")) found=db.prepare("SELECT * FROM users WHERE username=?").get(text.slice(1));
    if(!found){
      await tg("sendMessage",{chat_id:chatId,text:"❌ Пользователь не найден. Попросите его нажать /start."});
      return;
    }
    await askGiftConfirm(from.id, chatId, user(from.id)?.last_menu_id||null, code, found.tg_id, null);
    return;
  }

  // Referral payout details entry
  if(ustate?.state==="ref_setpay"){
    if(!msg.text||msg.text.startsWith("/")) return;
    const promptMsgId=Number(ustate.payload||0);
    clearUserState(from.id);
    delMsg(chatId, promptMsgId);
    const isRu2=getLang(from.id)==="ru";
    const raw=text.trim();
    const sep=raw.includes("|")?"|":"-";
    const parts2=raw.split(sep).map(s=>s.trim());
    if(parts2.length<2||!parts2[0]||!parts2[1]){
      await tg("sendMessage",{chat_id:chatId,text:isRu2?"❌ Неверный формат. Пример: Сбербанк | +79001234567":"❌ Wrong format. Example: Bank | card@email.com"});
      return;
    }
    const method=parts2[0].slice(0,100), details=parts2.slice(1).join("|").trim().slice(0,200);
    db.prepare("UPDATE users SET payout_method=?,payout_details=?,updated_at=? WHERE tg_id=?").run(method,details,now(),Number(from.id));
    await tg("sendMessage",{chat_id:chatId,text:isRu2?`✅ Реквизиты сохранены.\nМетод: <b>${esc(method)}</b>\nРеквизиты: <b>${esc(details)}</b>`:`✅ Payout details saved.\nMethod: <b>${esc(method)}</b>\nDetails: <b>${esc(details)}</b>`,parse_mode:"HTML"});
    await render(from.id,chatId,user(from.id)?.last_menu_id||null,"ref");
    return;
  }

  // Gift: system picker result
  if(msg.user_shared&&ustate?.state==="gift_pick"){
    const parts=(ustate.payload||"").split(":");
    const code=parts[0], promptMsgId=Number(parts[1]||0);
    const recipientId=Number(msg.user_shared.user_id||0);
    clearUserState(from.id);
    delMsg(chatId, promptMsgId);
    if(!user(recipientId)){await tg("sendMessage",{chat_id:chatId,text:"❌ Пользователь не зарегистрирован. Попросите нажать /start."});return;}
    await askGiftConfirm(from.id, chatId, user(from.id)?.last_menu_id||null, code, recipientId, null);
    return;
  }

  // ── Admin commands ────────────────────────────────────────────────────────
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

  // ── Non-command plain text — just reply, do NOT silently delete ─────────────
  // (Admins sending free-form text in an unexpected state should see feedback,
  //  not have their message vanish without explanation.)
  if(!text.startsWith("/")){
    await tg("sendMessage",{chat_id:chatId,text:"Используйте /start"});
    return;
  }

  // Standard /commands — delete the command message for a clean look
  delMsg(chatId, userMsgId);
  if(text.startsWith("/start")){
    const m=text.match(/^\/start\s+partner_([a-zA-Z0-9]+)$/);
    if(m){const r=findRef(m[1]);if(r)setRef(from.id,r.tg_id);}
    const lang=getLang(from.id);
    const passed=await enforceChannelGate(from.id,chatId,lang);
    if(!passed) return;
    await gif(chatId,"gif_main_menu");
    await render(from.id,chatId,null,"home"); return;
  }
  if(text==="/menu"){
    const passed=await enforceChannelGate(from.id,chatId,getLang(from.id));
    if(!passed) return;
    await render(from.id,chatId,null,"home");return;
  }
  if(text==="/sub"){
    const passed=await enforceChannelGate(from.id,chatId,getLang(from.id));
    if(!passed) return;
    await render(from.id,chatId,null,"sub");return;
  }
  if(text==="/referral"){
    const passed=await enforceChannelGate(from.id,chatId,getLang(from.id));
    if(!passed) return;
    await render(from.id,chatId,null,"ref");return;
  }
  if(text==="/admin"&&isAdmin(from.id)){await render(from.id,chatId,user(from.id)?.last_menu_id,"a_main");return;}

  // Unknown /command
  await tg("sendMessage",{chat_id:chatId,text:"Неизвестная команда. Используйте /start",parse_mode:"HTML"});
}

// ─────────────────────────────────────────────────────────────────────────────
// Callback handler
// ─────────────────────────────────────────────────────────────────────────────
async function handleCallback(q) {
  const data=q.data||"", uid=Number(q.from?.id||0), chatId=Number(q.message?.chat?.id||0), msgId=Number(q.message?.message_id||0);
  if(!uid||!chatId||!msgId) return;
  // Ignore callbacks from group/supergroup/channel chats
  if(q.message?.chat?.type!=="private") { await tg("answerCallbackQuery",{callback_query_id:q.id}).catch(()=>{}); return; }
  upsertUser(q.from,chatId);
  const ans=(text="",alert=false)=>tg("answerCallbackQuery",{callback_query_id:q.id,...(text?{text,show_alert:alert}:{})}).catch(()=>{});

  // Rate limit: ignore rapid repeated taps (except noop/check which user intentionally retries)
  if(!data.startsWith("cp:check")&&!data.startsWith("cp:cancel")&&!data.startsWith("fk:check")&&!data.startsWith("fk:cancel")&&!checkCbRateLimit(uid)){await ans();return;}

  if(data==="noop"){await ans();return;}

  // ── Input cancel buttons ──────────────────────────────────────────────────
  if(data.startsWith("cancel:")){
    const ustate=getUserState(uid);
    clearUserState(uid);
    // Delete the prompt message itself
    tg("deleteMessage",{chat_id:chatId,message_id:msgId}).catch(()=>{});
    await ans();
    // Determine where to go back
    const sub2=data.split(":")[1];
    if(sub2==="promo"){
      // Return to buy confirm — extract code/mode/devices from cancel data
      // format: cancel:promo:CODE:MODE:DEVICES
      const parts=data.split(":");
      const code=parts[2],mode=parts[3],devices=Number(parts[4]||3);
      await askBuyConfirm(uid,chatId,user(uid)?.last_menu_id||null,code,mode,null,"",0,devices);
    } else if(sub2==="gift"){
      await render(uid,chatId,user(uid)?.last_menu_id||null,"gift");
    } else {
      await render(uid,chatId,user(uid)?.last_menu_id||null,"home");
    }
    return;
  }

  // ── Channel gate check ────────────────────────────────────────────────────
  if(data==="gate:check"){
    const member=await checkChannelMembership(uid);
    if(!member){
      await ans(getLang(uid)==="en"?"❗ You haven't subscribed yet.":"❗ Вы ещё не подписались.",true);
      return;
    }
    // Passed — delete gate message and show main menu
    await tg("deleteMessage",{chat_id:chatId,message_id:msgId}).catch(()=>{});
    await gif(chatId,"gif_main_menu");
    await render(uid,chatId,null,"home");
    await ans(); return;
  }

  // ── Channel gate: enforce for all remaining callbacks ────────────────────
  if(!isAdmin(uid)){
    const _gPassed=await enforceChannelGate(uid,chatId,getLang(uid));
    if(!_gPassed){ await ans(); return; }
  }

  // ── Trial period ──────────────────────────────────────────────────────────
  if(data==="trial:start"){
    const tx=T(uid);
    if(!trialEnabled()){await ans(getLang(uid)==="en"?"Trial not available.":"Пробный период недоступен.",true);return;}
    if(hasUsedTrial(uid)){await ans(tx.trial_used_msg,true);return;}
    if(activeSub(sub(uid))){await ans(tx.trial_has_sub,true);return;}
    const days=trialDays();
    const lines=[tx.trial_confirm(days)];
    const kb={inline_keyboard:[
      [{text:tx.btn_confirm,callback_data:"trial:confirm"}],
      [{text:tx.btn_cancel,callback_data:"v:home"}],
    ]};
    const nm=await renderMsg(chatId,msgId,lines.join("\n"),kb,viewImg("buy"));
    setMenu(uid,chatId,nm);
    await ans(); return;
  }
  if(data==="trial:confirm"){
    const tx=T(uid);
    if(!trialEnabled()){await ans(getLang(uid)==="en"?"Trial not available.":"Пробный период недоступен.",true);return;}
    if(hasUsedTrial(uid)){await ans(tx.trial_used_msg,true);return;}
    if(activeSub(sub(uid))){await ans(tx.trial_has_sub,true);return;}
    await ans();
    try{
      await doTrial(uid,chatId,msgId);
    }catch(e){
      await tg("sendMessage",{chat_id:chatId,text:`❌ ${e.message}`}).catch(()=>{});
    }
    return;
  }
  if(data.startsWith("a:")&&!isAdmin(uid)){await ans("Нет доступа.",true);return;}

  // ── Language ──────────────────────────────────────────────────────────────
  if(data.startsWith("lang:")){
    const lg=data.split(":")[1];
    if(lg==="ru"||lg==="en"){setLang(uid,lg);}
    await render(uid,chatId,msgId,"lang"); await ans(); return;
  }

  // ── Navigation ────────────────────────────────────────────────────────────
  const navMap={
    "v:home":"home","v:sub":"sub","v:buy":"buy",
    "v:ref":"ref","v:guide":"guide",
    "v:about":"about","v:gift":"gift","v:lang":"lang",
  };
  if(navMap[data]){await render(uid,chatId,msgId,navMap[data]);await ans();return;}

  // ── Purchase ──────────────────────────────────────────────────────────────
  // pay:n: → show device selector for NEW subscription
  if(data.startsWith("pay:n:")){
    const code=data.split(":")[2];
    await ans();
    await showDeviceSelector(uid,chatId,msgId,code,"new",3);
    return;
  }
  // pay:rw: → show device selector for RENEW (active subscription required)
  if(data.startsWith("pay:rw:")){
    const code=data.split(":")[2];
    const s=sub(uid);
    if(!s){await ans(getLang(uid)==="en"?"No subscription found.":"Подписка не найдена.",true);return;}
    const daysLeft=Math.floor(Math.max(0,s.expires_at-now())/86400000);
    const renewOk=!activeSub(s)||(daysLeft<=1);
    if(!renewOk){await ans(getLang(uid)==="en"?"Renewal available only when subscription expires or 1 day before.":"Продление доступно только после истечения или за 1 день до конца подписки.",true);return;}
    await ans();
    await showDeviceSelector(uid,chatId,msgId,code,"renew",Number(s.devices||3));
    return;
  }
  // pay:r: (renew while active) removed from UI
  if(data.startsWith("pay:c:")){
    const parts=data.split(":");
    // format: pay:c:MODE:CODE:PROMO_CD:PROMO_PCT:DEVICES
    const mode=parts[2], code=parts[3], promoCd=parts[4]||"", promoPct=Number(parts[5]||0), devices=Number(parts[6]||3);
    await buySelf(uid,chatId,msgId,code,mode,q.id,promoCd,promoPct,devices);
    return;
  }

  // ── Device selector callbacks ─────────────────────────────────────────────
  if(data.startsWith("dev:")){
    const parts=data.split(":");
    // dev:pay:CODE:MODE:DEVICES → go to confirm
    if(parts[1]==="pay"){
      const code=parts[2], mode=parts[3], devices=Number(parts[4]||3);
      await ans();
      await askBuyConfirm(uid,chatId,msgId,code,mode,null,"",0,devices);
      return;
    }
    // dev:CODE:MODE:DEVICES → update selector (change device count)
    const code=parts[1], mode=parts[2], devices=Number(parts[3]||3);
    await ans();
    await showDeviceSelector(uid,chatId,msgId,code,mode,devices);
    return;
  }

  // ── Promo code ────────────────────────────────────────────────────────────
  if(data.startsWith("promo:ask:")){
    const parts=data.split(":"), code=parts[2], mode=parts[3], devices=Number(parts[4]||3);
    await ans();
    const promptId = await sendPrompt(chatId, T(uid).promo_enter, `cancel:promo:${code}:${mode}:${devices}`);
    setUserState(uid,"promo_input",`${code}:${mode}:${devices}:${promptId}`);
    return;
  }

  // ── Direct payment (pending order) ────────────────────────────────────────
  if(data.startsWith("direct:crypto:")){
    if(!CRYPTOBOT_TOKEN){await ans("CryptoBot не настроен.",true);return;}
    const poId=Number(data.split(":")[2]), po=getPendingOrder(poId);
    if(!po||po.tg_id!==uid||po.status!=="pending"){await ans(getLang(uid)==="en"?"Order expired. Please start again.":"Заказ истёк. Начните заново.",true);return;}
    const tr=tariff(po.tariff_code); if(!tr){await ans("Тариф не найден.",true);return;}
    const devCount=Number(po.devices||3)||3;
    const finalPrice=calcPriceWithDevices(tr.price_rub,po.promo_pct,devCount);
    await ans();
    expireOldCryptoPayments(uid);
    const inv=await createCryptoInvoice(finalPrice);
    if(!inv){await tg("sendMessage",{chat_id:chatId,text:"❌ CryptoBot недоступен. Попробуйте позже."});return;}
    const cpId=createCryptoPaymentRow(uid,finalPrice,inv.amountUsdt,inv.rate,inv.invoiceId,inv.payUrl);
    const tx=T(uid);
    const msgText=[tx.crypto_inv,"",tx.crypto_sum(rub(finalPrice),inv.amountUsdt),tx.crypto_rate(inv.rate),"",tx.crypto_steps,"",tx.crypto_ttl].join("\n");
    await tg("sendMessage",{chat_id:chatId,text:msgText,parse_mode:"HTML",reply_markup:{inline_keyboard:[
      [{text:tx.btn_pay_crypto,url:inv.payUrl}],
      [{text:tx.btn_check,callback_data:`cp:check:${cpId}`}],
      [{text:tx.btn_cancel,callback_data:`cp:cancel:${cpId}`}],
    ]}});
    return;
  }
  if(data.startsWith("direct:fk:")){
    if(!isFkEnabled()){await ans("Способ пополнения временно недоступен.",true);return;}
    const parts=data.split(":"), poId=Number(parts[2]), methodId=Number(parts[3]||44);
    const po=getPendingOrder(poId);
    if(!po||po.tg_id!==uid||po.status!=="pending"){await ans(getLang(uid)==="en"?"Order expired. Please start again.":"Заказ истёк. Начните заново.",true);return;}
    const tr=tariff(po.tariff_code); if(!tr){await ans("Тариф не найден.",true);return;}
    const devCount=Number(po.devices||3)||3;
    const finalPrice=calcPriceWithDevices(tr.price_rub,po.promo_pct,devCount);
    const serverIp=fkServerIp();
    if(!serverIp){await ans("Внешний IP не определён.",true);return;}
    await ans();
    const email=`user${uid}@${FK_EMAIL_DOMAIN}`;
    let order;
    try { order=await createFkOrder({uid,amountRub:finalPrice,methodId,email,ip:serverIp}); }
    catch(e){await tg("sendMessage",{chat_id:chatId,text:"❌ Не удалось создать счёт. Попробуйте позже."});return;}
    if(!order.location){await tg("sendMessage",{chat_id:chatId,text:"❌ Не удалось получить ссылку оплаты."});return;}
    const fkId=createFkPaymentRow(uid,finalPrice,methodId,order.paymentId,order.location,order.orderId,poId);
    // Link this FK payment to the pending order via payment_id comment stored in po
    const tx=T(uid);
    const msgText=[tx.fk_created,"",`Сумма: <b>${rub(finalPrice)}</b>`,`Метод: <b>${esc(methodTitle(methodId,getLang(uid)))}</b>`,"",tx.fk_steps,tx.fk_wait].join("\n");
    await tg("sendMessage",{chat_id:chatId,text:msgText,parse_mode:"HTML",disable_web_page_preview:true,reply_markup:{inline_keyboard:[
      [{text:"💳 Оплатить",url:order.location}],
      [{text:tx.btn_check,callback_data:`fk:check:${fkId}`}],
      [{text:tx.btn_cancel,callback_data:`fk:cancel:${fkId}`}],
    ]}});
    return;
  }

  // ── Purchase history ──────────────────────────────────────────────────────
  if(data.startsWith("ph:")){await render(uid,chatId,msgId,"purchases",{page:Number(data.split(":")[1]||0)});await ans();return;}

  // ── Subscription ──────────────────────────────────────────────────────────
  // sub:qr — re-render the subscription screen with inline QR
  if(data==="sub:qr"){
    const s=sub(uid);
    if(!activeSub(s)){await ans(getLang(uid)==="en"?"No active subscription.":"Нет активной подписки.",true);return;}
    await ans();
    await renderSubWithQR(uid,chatId,msgId);
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
  if(data.startsWith("ref:hist:")){await render(uid,chatId,msgId,"ref_hist",{page:Number(data.split(":")[2]||0)});await ans();return;}

  // ── Referral: set payout details ─────────────────────────────────────────
  if(data==="ref:setpay"){
    const isRu=getLang(uid)==="ru";
    const u2=user(uid);
    const curMethod=u2.payout_method||"";
    const curDetails=u2.payout_details||"";
    const promptText=isRu
      ?`⚙️ <b>Реквизиты для вывода</b>\n\nТекущий способ: <b>${curMethod||"не задан"}</b>\nТекущие реквизиты: <b>${curDetails||"не указаны"}</b>\n\nОтправьте строку в формате:\n<code>Способ | Реквизиты</code>\nПример: <code>Сбербанк | +79001234567</code>`
      :`⚙️ <b>Payout details</b>\n\nCurrent method: <b>${curMethod||"not set"}</b>\nCurrent details: <b>${curDetails||"not set"}</b>\n\nSend in format:\n<code>Method | Details</code>\nExample: <code>Bank | card@email.com</code>`;
    await ans();
    const promptId=await sendPrompt(chatId,promptText,"cancel:input");
    setUserState(uid,"ref_setpay",String(promptId));
    return;
  }

  // ── Referral: withdraw ────────────────────────────────────────────────────
  if(data==="ref:withdraw"){
    const isRu=getLang(uid)==="ru";
    const u2=user(uid);
    const refBal=Number(u2.ref_balance_rub||0);
    const minW=Number(setting("ref_min_withdraw","3000"))||3000;
    if(refBal<minW){
      await ans(isRu?`Минимальная сумма вывода ${minW}₽. У вас ${refBal.toFixed(2)}₽`:`Minimum withdrawal ${minW}₽. You have ${refBal.toFixed(2)}₽`,true);
      return;
    }
    if(!u2.payout_method||!u2.payout_details){
      await ans(isRu?"Сначала настройте реквизиты для вывода.":"Please set your payout details first.",true);
      return;
    }
    // Create withdrawal request
    db.prepare("INSERT INTO withdrawal_requests(tg_id,amount_rub,method,details,status,admin_note,created_at,updated_at) VALUES(?,?,?,?,'pending','',?,?)")
      .run(Number(uid),Math.round(refBal),u2.payout_method,u2.payout_details,now(),now());
    // Deduct from ref_balance
    db.prepare("UPDATE users SET ref_balance_rub=0,updated_at=? WHERE tg_id=?").run(now(),Number(uid));
    await ans(isRu?"✅ Заявка на вывод отправлена.":"✅ Withdrawal request submitted.",true);
    // Notify admin silently (no push sound) — just update their withdrawal panel
    tg("sendMessage",{chat_id:ADMIN_ID,text:isRu?`💸 <b>Новая заявка на вывод</b>\n\n${esc(u2.first_name||String(uid))} (<code>${uid}</code>)\nСумма: <b>${rub(refBal)}</b>\nМетод: <b>${esc(u2.payout_method)}</b>\nРеквизиты: <b>${esc(u2.payout_details)}</b>`:`💸 <b>New withdrawal request</b>\n\n${esc(u2.first_name||String(uid))} (<code>${uid}</code>)\nAmount: <b>${rub(refBal)}</b>\nMethod: <b>${esc(u2.payout_method)}</b>\nDetails: <b>${esc(u2.payout_details)}</b>`,parse_mode:"HTML",disable_notification:true,reply_markup:{inline_keyboard:[[{text:"📋 Заявки на вывод",callback_data:"a:withdrawals:0"}]]}}).catch(()=>{});
    await render(uid,chatId,msgId,"ref");
    return;
  }

  // ── Gifts ─────────────────────────────────────────────────────────────────
  if(data.startsWith("g:p:")){
    const code=data.split(":")[2],tr=tariff(code),u=user(uid),tx=T(uid);
    if(!tr){await ans("Тариф не найден.",true);return;}
    if(Number(u.balance_rub)<Number(tr.price_rub)){await ans(tx.gift_no_bal(tr.price_rub,u.balance_rub),true);return;}
    await ans();
    const lang=getLang(uid);
    const promptText=`🎁 <b>${esc(tariffTitle(tr,lang))}</b>\n\n${tx.gift_enter_id}`;
    const promptId = await sendPrompt(chatId, promptText, `cancel:gift:${code}`);
    setUserState(uid,"gift_recipient_id",`${code}:${promptId}`);
    return;
  }
  // g:l: (user list pagination) removed
  // g:u: (select user from list) removed
  if(data.startsWith("g:cf:")){
    const[,,code,rid]=data.split(":");
    await giftToUser(uid,Number(rid),code,chatId,msgId,q.id); return;
  }
  // g:id: handler merged into g:p: flow above

  // ── Purchase history ──────────────────────────────────────────────────────
  if(data.startsWith("ph:")){await render(uid,chatId,msgId,"purchases",{page:Number(data.split(":")[1]||0)});await ans();return;}

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
      tg("sendMessage",{chat_id:ADMIN_ID,text:[`<b>Crypto пополнение</b>`,"",`${esc(me.first_name||String(uid))} (<code>${uid}</code>)`,`Сумма: <b>${rub(cp.amount_rub)}</b>  (${cp.amount_usdt} USDT @ ${Number(cp.rate_rub).toFixed(2)} ₽)`].join("\n"),parse_mode:"HTML"}).catch(()=>{});
      // Auto-complete pending order if exists
      const po=getPendingOrderByUser(uid);
      if(po){
        closePendingOrder(po.id);
        await completePurchaseAfterTopup(uid,po);
      } else {
        await tg("editMessageText",{chat_id:chatId,message_id:msgId,text:[tx.crypto_ok(cp.amount_rub)].join("\n"),parse_mode:"HTML",reply_markup:{inline_keyboard:[[{text:tx.btn_buy_sub,callback_data:"v:buy"},{text:tx.btn_home,callback_data:"v:home"}]]}}).catch(()=>{});
      }
    }else{
      await tg("sendMessage",{
        chat_id:chatId,
        text:"❌ Оплата пока не найдена. Попробуйте проверить снова через минуту.",
        reply_markup:{inline_keyboard:[[{text:T(uid).btn_check,callback_data:`cp:check:${cpId}`}],[{text:T(uid).btn_home,callback_data:"v:home"}]]}
      }).catch(()=>{});
    }
    return;
  }
  if(data.startsWith("cp:cancel:")){
    const cpId=Number(data.split(":")[2]), cp=getCryptoPayment(cpId);
    if(!cp||cp.tg_id!==uid){await ans("Счёт не найден.",true);return;}
    if(cp.status!=="pending"){await ans("Счёт уже закрыт.",true);return;}
    markCryptoCancelled(cpId);
    // Also cancel any pending purchase order for this user
    const po=getPendingOrderByUser(uid);
    if(po) closePendingOrder(po.id,"cancelled");
    await ans("Отменено.");
    await tg("editMessageReplyMarkup",{chat_id:chatId,message_id:msgId,reply_markup:{inline_keyboard:[[{text:T(uid).btn_home,callback_data:"v:home"}]]}}).catch(()=>{});
    return;
  }
  if(data.startsWith("fk:check:")){
    const fpId=Number(data.split(":")[2]), fp=getFkPayment(fpId);
    if(!fp||fp.tg_id!==uid){await ans("Счёт не найден.",true);return;}
    if(fp.status==="paid"){await ans("✅ Уже зачислено!",true);return;}
    if(fp.status!=="pending"){await ans("Счёт закрыт. Создайте новый.",true);return;}
    await ans("⏳ Проверяю...");
    try{
      const ord=await checkFkOrderByPaymentId(fp.payment_id);
      const paidStatus=ord && (Number(ord.status)===1 || String(ord.status||"").toLowerCase()==="paid");
      if(!paidStatus){
        await tg("sendMessage",{
          chat_id:chatId,
          text:"❌ Оплата пока не найдена. Попробуйте проверить снова через минуту.",
          reply_markup:{inline_keyboard:[[{text:T(uid).btn_check,callback_data:`fk:check:${fpId}`}],[{text:T(uid).btn_home,callback_data:"v:home"}]]}
        }).catch(()=>{});
        return;
      }
      const credit=await creditFkPaymentByPaymentId(fp.payment_id, ord.id || ord.orderId || null, ord.amount || null);
      if(!credit.ok&&credit.reason==="WRONG_AMOUNT"){
        await tg("answerCallbackQuery",{callback_query_id:q.id,text:"❌ Сумма платежа не совпадает.",show_alert:true}).catch(()=>{});
        return;
      }
      // Auto-complete pending order if exists
      const po=getPendingOrderByUser(uid);
      if(po){
        closePendingOrder(po.id);
        await completePurchaseAfterTopup(uid,po);
      } else {
        const me=user(uid), tx=T(uid);
        await tg("editMessageText",{chat_id:chatId,message_id:msgId,text:[tx.fk_ok(fp.amount_rub)].join("\n"),parse_mode:"HTML",reply_markup:{inline_keyboard:[[{text:tx.btn_buy_sub,callback_data:"v:buy"},{text:tx.btn_home,callback_data:"v:home"}]]}}).catch(()=>{});
      }
    }catch(e){
      await tg("answerCallbackQuery",{callback_query_id:q.id,text:`❌ ${String(e.message||"Ошибка проверки")}`.slice(0,200),show_alert:true}).catch(()=>{});
    }
    return;
  }
  if(data.startsWith("fk:cancel:")){
    const fpId=Number(data.split(":")[2]), fp=getFkPayment(fpId);
    if(!fp||fp.tg_id!==uid){await ans("Счёт не найден.",true);return;}
    if(fp.status!=="pending"){await ans("Счёт уже закрыт.",true);return;}
    markFkCancelled(fpId);
    // Cancel the specific pending order this FK payment was created for,
    // or fall back to the user's current pending order.
    if(fp.pending_order_id){
      closePendingOrder(fp.pending_order_id,"cancelled");
    } else {
      const po=getPendingOrderByUser(uid);
      if(po) closePendingOrder(po.id,"cancelled");
    }
    await ans("Отменено.");
    await tg("editMessageReplyMarkup",{chat_id:chatId,message_id:msgId,reply_markup:{inline_keyboard:[[{text:T(uid).btn_home,callback_data:"v:home"}]]}}).catch(()=>{});
    return;
  }

  // ── Admin nav ─────────────────────────────────────────────────────────────
  const adminNav={"a:main":"a_main","a:t":"a_tariffs","a:g":"a_gif","a:b":"a_bcast","a:p":"a_main","a:r":"a_ref","a:db":"a_db","a:links":"a_links","a:guide_edit":"a_guide_edit","a:channel":"a_channel","a:fk":"a_fk","a:promo":"a_promo"};
  if(adminNav[data]){await render(uid,chatId,msgId,adminNav[data]);await ans();return;}

  // Cancel admin input — delete the prompt message and return to admin main
  if(data==="a:cancel_admin"){
    clearAdminState(uid);
    await tg("deleteMessage",{chat_id:chatId,message_id:msgId}).catch(()=>{});
    await render(uid,chatId,user(uid)?.last_menu_id||null,"a_main");
    await ans(); return;
  }
  // Dynamic admin nav with data params
  if(data.startsWith("a:users:")){await render(uid,chatId,msgId,"a_users",{page:Number(data.split(":")[2]||0)});await ans();return;}

  // ── Channel / trial admin actions ─────────────────────────────────────────
  if(data==="a:chan_id"){
    setAdminState(uid,"chan_id","");
    await sendPrompt(chatId,`Текущий канал: <code>${esc(setting("channel_id","") || "не задан")}</code>\n\nВведите @username или числовой ID канала\n(например <code>@dreinnvpn</code> или <code>-1001234567890</code>).\nДля отключения введите «-».`,"a:cancel_admin");
    await ans(); return;
  }
  if(data==="a:chan_url"){
    setAdminState(uid,"chan_url","");
    await sendPrompt(chatId,`Текущая ссылка: <code>${esc(setting("channel_invite_url","") || "не задана")}</code>\n\nВведите ссылку-приглашение (https://t.me/+...) или «-» для очистки.`,"a:cancel_admin");
    await ans(); return;
  }
  if(data==="a:trial_toggle"){
    const next=trialEnabled()?"0":"1";
    setSetting("trial_enabled",next);
    await ans(next==="1"?"✅ Пробный период включён":"❌ Пробный период отключён");
    await render(uid,chatId,msgId,"a_channel"); return;
  }
  if(data==="a:trial_days"){
    setAdminState(uid,"trial_days","");
    await sendPrompt(chatId,`Текущая длительность: <b>${trialDays()} дн.</b>\n\nВведите новое значение (1..365):`,"a:cancel_admin");
    await ans(); return;
  }
  if(data==="a:fk_shop"){
    setAdminState(uid,"fk_shop_id","");
    await sendPrompt(chatId,`Текущий shop_id: <code>${fkShopId() || "не задан"}</code>\n\nВведите новый shop_id (число > 0):`,"a:cancel_admin");
    await ans(); return;
  }
  if(data==="a:fk_min"){
    setAdminState(uid,"fk_min_rub","");
    await sendPrompt(chatId,`Текущий min amount: <code>${fkMinRub()}</code>\n\nВведите новую минимальную сумму (в рублях):`,"a:cancel_admin");
    await ans(); return;
  }
  if(data==="a:fk_path"){
    setAdminState(uid,"fk_notify_path","");
    await sendPrompt(chatId,`Текущий webhook path: <code>${esc(fkNotifyPath())}</code>\n\nВведите новый путь (например /freekassa/notify):`,"a:cancel_admin");
    await ans(); return;
  }

  // ── Admin edit triggers ───────────────────────────────────────────────────
  if(data.startsWith("a:te:")){
    const code=data.split(":")[2],tr=tariff(code);
    setAdminState(uid,"tariff_price",code);
    await sendPrompt(chatId,`«${esc(tr?.title||code)}» — ${rub(tr?.price_rub||0)}\n\nВведите новую цену (₽):`,"a:cancel_admin");
    await ans(); return;
  }
  if(data==="a:dev_price"){
    setAdminState(uid,"dev_extra_price","");
    await sendPrompt(chatId,`Текущая цена за доп. устройство (от 4+): <b>${rub(devicesExtraPrice())}</b>\n\nВведите новую цену в рублях (0 = бесплатно):`,"a:cancel_admin");
    await ans(); return;
  }
  if(data.startsWith("a:ge:")){
    setAdminState(uid,"gif",data.split(":")[2]);
    await sendPrompt(chatId,"Отправьте GIF или file_id."         ,"a:cancel_admin");
    await ans(); return;
  }
  // Link edit
  if(data.startsWith("a:lnk:")){
    const key=data.split(":").slice(2).join(":");
    setAdminState(uid,"edit_link",key);
    await sendPrompt(chatId,`Ссылка «<b>${key.replace("url_","")}</b>»:\n<code>${esc(setting(key))}</code>\n\nВведите новый URL (или «-» для очистки):`,"a:cancel_admin");
    await ans(); return;
  }
  if(data==="a:bs"){
    const promptId = await sendPrompt(chatId,"📨 Отправьте сообщение для рассылки.\n\nПоддерживается: текст (с форматированием Telegram), фото, видео, GIF, документ, голосовое.","a:cancel_admin");
    setAdminState(uid,"broadcast",String(promptId));
    await ans(); return;
  }
  if(data==="a:bs_confirm"){
    const row=getAdminState(uid);
    if(!row||row.state!=="broadcast_preview"){await ans("Нет данных для рассылки.",true);return;}
    clearAdminState(uid);
    let msgMeta;
    try{ msgMeta=JSON.parse(row.payload); }catch{ await ans("Ошибка данных.",true); return; }
    await ans("⏳ Запускаю рассылку...");
    const ids=db.prepare("SELECT tg_id FROM users").all();
    let ok=0,fail=0;
    const progMsg=await tg("sendMessage",{chat_id:chatId,text:`📨 0/${ids.length}`}).catch(()=>null);
    for(let i=0;i<ids.length;i++){
      const toId=ids[i].tg_id;
      try{
        await tg("copyMessage",{chat_id:toId,from_chat_id:msgMeta.chat_id,message_id:msgMeta.message_id});
        ok++;
      }catch{fail++;}
      await sleep(35);
      if(progMsg&&(i+1)%20===0) tg("editMessageText",{chat_id:chatId,message_id:progMsg.message_id,text:`📨 ${i+1}/${ids.length}`}).catch(()=>{});
    }
    await tg("sendMessage",{chat_id:chatId,text:`📨 Рассылка завершена\n✅ ${ok}  ❌ ${fail}`,parse_mode:"HTML"});
    await render(uid,chatId,user(uid)?.last_menu_id||null,"a_bcast"); return;
  }
  if(data==="a:bs_cancel"){
    clearAdminState(uid);
    await ans("Отменено.");
    await render(uid,chatId,msgId,"a_bcast"); return;
  }
  if(data==="a:guide_ru"){setAdminState(uid,"guide_text","");await sendPrompt(chatId,"🇷🇺 Отправьте текст инструкции на русском.\nФормат ссылок: [Название|URL]","a:cancel_admin");await ans();return;}
  if(data==="a:guide_en"){setAdminState(uid,"guide_text_en","");await sendPrompt(chatId,"🇬🇧 Send the guide text in English.\nLink format: [Label|URL]","a:cancel_admin");await ans();return;}
  if(data==="a:pe"){await ans("Раздел удален.",true);await render(uid,chatId,msgId,"a_main");return;}
  if(data==="a:rp"){setAdminState(uid,"ref_percent","");await sendPrompt(chatId,`Ставка: ${setting("ref_percent","30")}%\n\nВведите новую (0..100):`,"a:cancel_admin");await ans();return;}
  if(data==="a:rmin"){setAdminState(uid,"ref_min_withdraw","");await sendPrompt(chatId,`Мин. вывод: ${setting("ref_min_withdraw","3000")}₽\n\nВведите новую сумму (₽):`,"a:cancel_admin");await ans();return;}

  if(data==="a:find"){setAdminState(uid,"find_user","");await sendPrompt(chatId,"Введите Telegram ID или @username:","a:cancel_admin");await ans();return;}

  // Withdrawal pagination
  if(data.startsWith("a:withdrawals_p:")){
    const pg=Number(data.split(":")[2]||0);
    await render(uid,chatId,msgId,"a_withdrawals",{page:pg}); await ans(); return;
  }
  if(data.startsWith("a:withdrawals")){
    const pg=Number((data.split(":")[2])||0);
    await render(uid,chatId,msgId,"a_withdrawals",{page:pg}); await ans(); return;
  }
  if(data.startsWith("a:wd_done:")){
    const parts=data.split(":"), wid=Number(parts[2]), pg=Number(parts[3]||0);
    const wr=db.prepare("SELECT * FROM withdrawal_requests WHERE id=?").get(wid);
    if(!wr||wr.status!=="pending"){await ans("Уже обработано.",true);return;}
    db.prepare("UPDATE withdrawal_requests SET status='paid',updated_at=? WHERE id=?").run(now(),wid);
    const wu=user(wr.tg_id);
    tg("sendMessage",{chat_id:wr.tg_id,text:`✅ <b>Выплата произведена!</b>\n\nСумма: <b>${rub(wr.amount_rub)}</b>\nМетод: <b>${esc(wr.method)}</b>\nРеквизиты: <b>${esc(wr.details)}</b>`,parse_mode:"HTML"}).catch(()=>{});
    await ans("✅ Выплачено.");
    await render(uid,chatId,msgId,"a_withdrawals",{page:pg}); return;
  }
  if(data.startsWith("a:wd_reject:")){
    const parts=data.split(":"), wid=Number(parts[2]), pg=Number(parts[3]||0);
    const wr=db.prepare("SELECT * FROM withdrawal_requests WHERE id=?").get(wid);
    if(!wr||wr.status!=="pending"){await ans("Уже обработано.",true);return;}
    db.prepare("UPDATE withdrawal_requests SET status='rejected',updated_at=? WHERE id=?").run(now(),wid);
    // Refund to ref_balance
    db.prepare("UPDATE users SET ref_balance_rub=ref_balance_rub+?,updated_at=? WHERE tg_id=?").run(wr.amount_rub,now(),wr.tg_id);
    tg("sendMessage",{chat_id:wr.tg_id,text:`❌ <b>Заявка на вывод отклонена.</b>\n\nСумма <b>${rub(wr.amount_rub)}</b> возвращена на реферальный баланс.\n\nСвяжитесь с поддержкой для уточнения деталей.`,parse_mode:"HTML"}).catch(()=>{});
    await ans("❌ Отклонено, средства возвращены.");
    await render(uid,chatId,msgId,"a_withdrawals",{page:pg}); return;
  }
  // DB
  if(data==="a:db_export"){await ans("Формирую файл...");await exportDbToAdmin(chatId);return;}
  if(data==="a:db_import_start"){setAdminState(uid,"db_import_wait","");await ans("Жду файл .db/.sqlite");await tg("sendMessage",{chat_id:chatId,text:"📤 Отправьте SQLite файл документом.\n⚠️ Бот перезапустится после импорта."});return;}
  // Withdrawal callbacks removed
  // Balance add
  // ── Admin user info: grant sub ────────────────────────────────────────────
  if(data.startsWith("a:user_back:")){
    const targetId=Number(data.split(":")[2]);
    await render(uid,chatId,msgId,"a_user_info",{id:targetId}); await ans(); return;
  }
  if(data.startsWith("a:grant:")){
    const targetId=Number(data.split(":")[2]);
    await render(uid,chatId,msgId,"a_grant",{id:targetId}); await ans(); return;
  }
  if(data.startsWith("a:grant_ok:")){
    const parts=data.split(":"), targetId=Number(parts[2]), tariffCode=parts[3];
    await ans("⏳ Выдаю подписку...");
    try {
      const res = await adminGrantSub(uid, targetId, tariffCode, chatId, msgId);
      await tg("sendMessage",{chat_id:chatId,text:`✅ Подписка «${esc(res.plan)}» выдана ${esc(res.name)}`,parse_mode:"HTML"});
    } catch(e) {
      await tg("sendMessage",{chat_id:chatId,text:`❌ Ошибка: ${e.message}`});
    }
    await render(uid,chatId,user(uid)?.last_menu_id||null,"a_user_info",{id:Number(data.split(":")[2])}); return;
  }

  // ── Admin promo management ────────────────────────────────────────────────
  if(data==="a:promo_add"){
    setAdminState(uid,"promo_add","");
    await sendPrompt(chatId,"Введите промокод в формате:\n<code>КОД СКИДКА% [МАКС_ИСПОЛЬЗОВАНИЙ]</code>\n\nПример: <code>SALE10 10 100</code>\n(0 = без ограничений)","a:cancel_admin");
    await ans(); return;
  }
  if(data==="a:promo_del"){
    setAdminState(uid,"promo_deactivate","");
    await sendPrompt(chatId,"Введите код промокода для деактивации:","a:cancel_admin");
    await ans(); return;
  }

  if(data.startsWith("a:bal_add:")){
    const targetId=data.split(":")[2], tu=user(Number(targetId));
    setAdminState(uid,"bal_add",targetId); await ans();
    await sendPrompt(chatId,`Пополнение для ${esc(tu?.first_name||targetId)}\nБаланс: ${rub(tu?.balance_rub)}\n\nВведите сумму (отрицательная = списание):`,"a:cancel_admin"); return;
  }
  // Sub revoke: deactivate subscription immediately
  if(data.startsWith("a:sub_revoke:")){
    const targetId=Number(data.split(":")[2]);
    const ts=sub(targetId);
    if(!ts||!activeSub(ts)){await ans("Активной подписки нет.",true);return;}
    // Deactivate: set is_active=0 and expires_at=now so it's expired immediately
    db.prepare("UPDATE subscriptions SET is_active=0,expires_at=?,updated_at=? WHERE tg_id=?")
      .run(now()-1,now(),targetId);
    await ans("✅ Подписка отозвана.");
    // Notify the user
    const tu=user(targetId);
    const isRuTarget=getLang(targetId)==="ru";
    tg("sendMessage",{
      chat_id:targetId,
      text:isRuTarget
        ?"<b>Ваша подписка была деактивирована администратором.</b>\n\n<i>Свяжитесь с поддержкой для уточнения деталей.</i>"
        :"<b>Your subscription has been deactivated by the administrator.</b>\n\n<i>Contact support for details.</i>",
      parse_mode:"HTML",
    }).catch(()=>{});
    await render(uid,chatId,msgId,"a_user_info",{id:targetId}); return;
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

function startWebhookServer() {
  const server = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
      if (req.method === "GET" && url.pathname === "/healthz") {
        res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
        res.end(JSON.stringify({ ok: true, service: "bot", freeKassa: isFkEnabled(), ts: Date.now() }));
        return;
      }

      // ── Read body (with size limit) ───────────────────────────────────────
      const chunks = [];
      let size = 0;
      let bodyDestroyed = false;
      await new Promise((resolve, reject) => {
        req.on("data", (chunk) => {
          size += chunk.length;
          if (size > 1024 * 1024) {
            bodyDestroyed = true;
            req.destroy();
            resolve();
            return;
          }
          chunks.push(chunk);
        });
        req.on("end", resolve);
        req.on("error", resolve);
        req.on("close", resolve);
      });
      if (bodyDestroyed) {
        res.writeHead(413, { "Content-Type": "text/plain; charset=utf-8" });
        res.end("Payload too large");
        return;
      }
      const raw = Buffer.concat(chunks).toString("utf8");

      // ── CryptoBot webhook ─────────────────────────────────────────────────
      if (req.method === "POST" && url.pathname === CRYPTOBOT_WEBHOOK_PATH) {
        const headerToken = req.headers["crypto-pay-api-token"] || "";
        if (!CRYPTOBOT_TOKEN || !verifyCryptoBotWebhook(raw, headerToken)) {
          res.writeHead(403, { "Content-Type": "text/plain; charset=utf-8" });
          res.end("Forbidden");
          return;
        }
        let body = {};
        try { body = JSON.parse(raw); } catch {}
        handleCryptoBotWebhookPayload(body).catch(e => console.error("[CryptoBot webhook]", e.message));
        res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
        res.end("OK");
        return;
      }

      // ── FreeKassa webhook ─────────────────────────────────────────────────
      if (req.method !== "POST" || url.pathname !== fkNotifyPath()) {
        res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
        res.end("Not found");
        return;
      }

      const remoteIp = getRequestIp(req);
      if (FK_ENABLE_IP_CHECK && !FK_ALLOWED_IPS.has(remoteIp)) {
        console.warn("[FK webhook] denied ip:", remoteIp);
        res.writeHead(403, { "Content-Type": "text/plain; charset=utf-8" });
        res.end("IP is not allowed");
        return;
      }

      const payload = parseBodyByContentType(raw, req.headers["content-type"]);

      if (!validateFkWebhookSign(payload)) {
        console.warn("[FK webhook] bad sign payload:", payload);
        res.writeHead(400, { "Content-Type": "text/plain; charset=utf-8" });
        res.end("Bad signature");
        return;
      }
      const merchantId = Number(payload.MERCHANT_ID || payload.merchant_id || 0);
      if (merchantId !== fkShopId()) {
        res.writeHead(400, { "Content-Type": "text/plain; charset=utf-8" });
        res.end("Wrong shop id");
        return;
      }

      const paymentId = String(payload.MERCHANT_ORDER_ID || payload.merchant_order_id || "");
      const fkOrderId = payload.intid || payload.INTID || payload.orderId || null;
      const paidAmount = payload.AMOUNT || payload.amount || null;
      const credited = await creditFkPaymentByPaymentId(paymentId, fkOrderId, paidAmount);

      if (!credited.ok && credited.reason !== "ALREADY_PAID") {
        console.warn("[FK webhook] credit failed:", credited.reason, paymentId);
        res.writeHead(409, { "Content-Type": "text/plain; charset=utf-8" });
        res.end("Order not accepted");
        return;
      }

      // If FK payment was linked to a pending order, auto-complete it
      if (credited.ok) {
        const fp = credited.fp;
        if (fp) {
          const po = getPendingOrderByUser(fp.tg_id);
          if (po) {
            closePendingOrder(po.id);
            completePurchaseAfterTopup(fp.tg_id, po).catch(()=>{});
          }
        }
      }

      res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("YES");
    } catch (e) {
      console.error("[webhook]", e);
      res.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("Server error");
    }
  });

  server.listen(FK_PORT, "0.0.0.0", () => {
    const notifyUrl = `https://${FK_DOMAIN}:${FK_PORT}${fkNotifyPath()}`;
    const cryptoUrl = `https://${FK_DOMAIN}:${FK_PORT}${CRYPTOBOT_WEBHOOK_PATH}`;
    console.log(`[Webhook] HTTP server listening on :${FK_PORT}`);
    console.log(`[Webhook] FreeKassa notify URL: ${notifyUrl}`);
    if (CRYPTOBOT_TOKEN) console.log(`[Webhook] CryptoBot webhook URL: ${cryptoUrl}`);
  });
}

async function setMyCommands() {
  const commands = [
    { command: "start", description: "Перезапустить бота" },
    { command: "sub",   description: "Моя подписка" },
  ];
  try {
    await tg("setMyCommands", { commands });
    console.log("[Bot] Commands registered:", commands.map(c=>"/"+c.command).join(", "));
  } catch(e) {
    console.warn("[Bot] setMyCommands failed:", e.message);
  }
}

async function boot() {
  init();
  await setMyCommands();
  await ensureFkServerIp();
  startWebhookServer();
  startFkExpireJob();
  startExpiryNotificationJob();
  poll();
}

boot().catch((e) => {
  console.error("[boot]", e);
  process.exit(1);
});
