require("dotenv").config();

const path = require("path");
const Database = require("better-sqlite3");

const TELEGRAM_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const API_BASE_URL = (process.env.VPN_API_BASE_URL || "").replace(/\/+$/, "");
const APP_SECRET = process.env.APP_SECRET || "";
const ADMIN_TELEGRAM_ID = Number(process.env.ADMIN_TELEGRAM_ID || 0);
const DB_PATH = process.env.SQLITE_PATH || path.join(__dirname, "bot.sqlite");
const NEWS_URL = process.env.BOT_NEWS_URL || "https://t.me/cats_vpn";
const SUPPORT_URL = process.env.BOT_SUPPORT_URL || "https://t.me/Oktsupport";

if (!TELEGRAM_TOKEN || !API_BASE_URL || !APP_SECRET || !ADMIN_TELEGRAM_ID) {
  console.error("Missing env vars: TELEGRAM_BOT_TOKEN, VPN_API_BASE_URL, APP_SECRET, ADMIN_TELEGRAM_ID");
  process.exit(1);
}

const TG_API = `https://api.telegram.org/bot${TELEGRAM_TOKEN}`;
let offset = 0;

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");
db.pragma("foreign_keys = ON");

function initDb() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      tg_id INTEGER PRIMARY KEY,
      username TEXT DEFAULT '',
      first_name TEXT DEFAULT '',
      balance_rub INTEGER NOT NULL DEFAULT 0,
      last_chat_id INTEGER,
      last_menu_message_id INTEGER,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS subscriptions (
      tg_id INTEGER PRIMARY KEY,
      plan_code TEXT DEFAULT '',
      subscription_url TEXT DEFAULT '',
      expires_at INTEGER,
      updated_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS tariffs (
      code TEXT PRIMARY KEY,
      title TEXT NOT NULL,
      duration_days INTEGER NOT NULL,
      price_rub INTEGER NOT NULL,
      sort_order INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL DEFAULT ''
    );

    CREATE TABLE IF NOT EXISTS admin_states (
      admin_tg_id INTEGER PRIMARY KEY,
      state TEXT NOT NULL,
      payload TEXT NOT NULL DEFAULT '',
      updated_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS purchases (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tg_id INTEGER NOT NULL,
      tariff_code TEXT NOT NULL,
      amount_rub INTEGER NOT NULL,
      created_at INTEGER NOT NULL
    );
  `);

  const seedTariffs = db.prepare(`
    INSERT INTO tariffs (code, title, duration_days, price_rub, sort_order)
    VALUES (@code, @title, @duration_days, @price_rub, @sort_order)
    ON CONFLICT(code) DO NOTHING
  `);
  seedTariffs.run({ code: "m1", title: "1 месяц", duration_days: 30, price_rub: 100, sort_order: 1 });
  seedTariffs.run({ code: "m6", title: "6 месяцев", duration_days: 180, price_rub: 600, sort_order: 2 });
  seedTariffs.run({ code: "y1", title: "1 год", duration_days: 365, price_rub: 900, sort_order: 3 });

  ensureSetting("payment_methods", "");
  ensureSetting("gif_main_menu", "");
  ensureSetting("gif_purchase_success", "");
  ensureSetting("gif_broadcast", "");
}

function ensureSetting(key, value) {
  db.prepare(`
    INSERT INTO settings (key, value)
    VALUES (?, ?)
    ON CONFLICT(key) DO NOTHING
  `).run(key, value);
}

function nowTs() {
  return Date.now();
}

function esc(str) {
  return String(str || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function fmtRub(amount) {
  return `${Number(amount || 0)} ₽`;
}

function fmtDate(ts) {
  if (!ts) return "—";
  return new Date(ts).toLocaleDateString("ru-RU");
}

function isAdmin(userId) {
  return Number(userId) === ADMIN_TELEGRAM_ID;
}

function upsertUser(from, chatId) {
  const ts = nowTs();
  db.prepare(`
    INSERT INTO users (tg_id, username, first_name, balance_rub, last_chat_id, created_at, updated_at)
    VALUES (@tg_id, @username, @first_name, 0, @last_chat_id, @created_at, @updated_at)
    ON CONFLICT(tg_id) DO UPDATE SET
      username=excluded.username,
      first_name=excluded.first_name,
      last_chat_id=excluded.last_chat_id,
      updated_at=excluded.updated_at
  `).run({
    tg_id: Number(from.id),
    username: from.username || "",
    first_name: from.first_name || "",
    last_chat_id: Number(chatId),
    created_at: ts,
    updated_at: ts,
  });
}

function getUser(tgId) {
  return db.prepare("SELECT * FROM users WHERE tg_id = ?").get(Number(tgId));
}

function setMenuMessage(tgId, chatId, messageId) {
  db.prepare(`
    UPDATE users
    SET last_chat_id = ?, last_menu_message_id = ?, updated_at = ?
    WHERE tg_id = ?
  `).run(Number(chatId), Number(messageId), nowTs(), Number(tgId));
}

function getTariffs() {
  return db.prepare("SELECT * FROM tariffs ORDER BY sort_order ASC").all();
}

function getTariff(code) {
  return db.prepare("SELECT * FROM tariffs WHERE code = ?").get(code);
}

function updateTariffPrice(code, priceRub) {
  db.prepare("UPDATE tariffs SET price_rub = ? WHERE code = ?").run(Number(priceRub), code);
}

function getSetting(key, fallback = "") {
  const row = db.prepare("SELECT value FROM settings WHERE key = ?").get(key);
  return row ? row.value : fallback;
}

function setSetting(key, value) {
  db.prepare(`
    INSERT INTO settings (key, value)
    VALUES (?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value
  `).run(key, String(value || ""));
}

function getSubscription(tgId) {
  return db.prepare("SELECT * FROM subscriptions WHERE tg_id = ?").get(Number(tgId));
}

function saveSubscription(tgId, planCode, subscriptionUrl, expiresAt) {
  db.prepare(`
    INSERT INTO subscriptions (tg_id, plan_code, subscription_url, expires_at, updated_at)
    VALUES (@tg_id, @plan_code, @subscription_url, @expires_at, @updated_at)
    ON CONFLICT(tg_id) DO UPDATE SET
      plan_code=excluded.plan_code,
      subscription_url=excluded.subscription_url,
      expires_at=excluded.expires_at,
      updated_at=excluded.updated_at
  `).run({
    tg_id: Number(tgId),
    plan_code: planCode,
    subscription_url: subscriptionUrl,
    expires_at: Number(expiresAt || 0),
    updated_at: nowTs(),
  });
}

function addPurchase(tgId, tariffCode, amountRub) {
  db.prepare(`
    INSERT INTO purchases (tg_id, tariff_code, amount_rub, created_at)
    VALUES (?, ?, ?, ?)
  `).run(Number(tgId), tariffCode, Number(amountRub), nowTs());
}

function updateUserBalance(tgId, deltaRub) {
  const tx = db.transaction(() => {
    const user = getUser(tgId);
    if (!user) throw new Error("USER_NOT_FOUND");
    const next = Number(user.balance_rub || 0) + Number(deltaRub || 0);
    if (next < 0) throw new Error("INSUFFICIENT_BALANCE");
    db.prepare("UPDATE users SET balance_rub = ?, updated_at = ? WHERE tg_id = ?")
      .run(next, nowTs(), Number(tgId));
    return next;
  });
  return tx();
}

function setAdminState(adminId, state, payload = "") {
  db.prepare(`
    INSERT INTO admin_states (admin_tg_id, state, payload, updated_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(admin_tg_id) DO UPDATE SET
      state=excluded.state,
      payload=excluded.payload,
      updated_at=excluded.updated_at
  `).run(Number(adminId), state, String(payload), nowTs());
}

function getAdminState(adminId) {
  return db.prepare("SELECT * FROM admin_states WHERE admin_tg_id = ?").get(Number(adminId));
}

function clearAdminState(adminId) {
  db.prepare("DELETE FROM admin_states WHERE admin_tg_id = ?").run(Number(adminId));
}

function getAllUsers() {
  return db.prepare("SELECT tg_id FROM users").all();
}

async function tg(method, payload) {
  const res = await fetch(`${TG_API}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.ok === false) {
    throw new Error(data.description || `Telegram API error ${res.status}`);
  }
  return data.result;
}

async function sendGifIfConfigured(chatId, key, caption = "") {
  const gif = getSetting(key, "");
  if (!gif) return;
  await tg("sendAnimation", {
    chat_id: chatId,
    animation: gif,
    caption: caption || undefined,
    parse_mode: "HTML",
  });
}

function mainMenuKeyboard(userId) {
  const rows = [
    [{ text: "🛍 Тарифы", callback_data: "view:tariffs" }],
    [{ text: "🔗 Моя подписка", callback_data: "view:subscription" }],
    [{ text: "💰 Баланс", callback_data: "view:balance" }],
    [
      { text: "📣 Канал", url: NEWS_URL },
      { text: "🛟 Поддержка", url: SUPPORT_URL },
    ],
    [{ text: "ℹ️ О сервисе", callback_data: "view:about" }],
  ];
  if (isAdmin(userId)) {
    rows.push([{ text: "🛠 Админ панель", callback_data: "admin:main" }]);
  }
  return { inline_keyboard: rows };
}

function tariffsKeyboard() {
  const tariffs = getTariffs();
  const rows = tariffs.map((t) => [{ text: `Купить ${t.title} — ${fmtRub(t.price_rub)}`, callback_data: `buy:${t.code}` }]);
  rows.push([{ text: "⬅️ Назад", callback_data: "view:home" }]);
  return { inline_keyboard: rows };
}

function subscriptionKeyboard(url) {
  const rows = [];
  if (url) rows.push([{ text: "🚀 Открыть subscription-link", url }]);
  rows.push([{ text: "🛍 Купить/продлить", callback_data: "view:tariffs" }]);
  rows.push([{ text: "⬅️ Назад", callback_data: "view:home" }]);
  return { inline_keyboard: rows };
}

function balanceKeyboard() {
  return {
    inline_keyboard: [
      [{ text: "🛍 Купить тариф", callback_data: "view:tariffs" }],
      [{ text: "💳 Способы оплаты", callback_data: "view:payments" }],
      [{ text: "⬅️ Назад", callback_data: "view:home" }],
    ],
  };
}

function paymentsKeyboard() {
  return {
    inline_keyboard: [
      [{ text: "⬅️ Назад", callback_data: "view:balance" }],
    ],
  };
}

function adminMainKeyboard() {
  return {
    inline_keyboard: [
      [{ text: "💸 Цены тарифов", callback_data: "admin:tariffs" }],
      [{ text: "🎞 GIF для сообщений", callback_data: "admin:gifs" }],
      [{ text: "📨 Рассылка", callback_data: "admin:broadcast" }],
      [{ text: "💳 Способы оплаты", callback_data: "admin:payments" }],
      [{ text: "⬅️ Назад", callback_data: "view:home" }],
    ],
  };
}

function adminTariffsKeyboard() {
  const tariffs = getTariffs();
  const rows = tariffs.map((t) => [{ text: `${t.title}: ${fmtRub(t.price_rub)}`, callback_data: `admin:tariff_edit:${t.code}` }]);
  rows.push([{ text: "⬅️ Назад", callback_data: "admin:main" }]);
  return { inline_keyboard: rows };
}

function adminGifsKeyboard() {
  return {
    inline_keyboard: [
      [{ text: "Main menu GIF", callback_data: "admin:set_gif:gif_main_menu" }],
      [{ text: "Purchase success GIF", callback_data: "admin:set_gif:gif_purchase_success" }],
      [{ text: "Broadcast GIF", callback_data: "admin:set_gif:gif_broadcast" }],
      [{ text: "⬅️ Назад", callback_data: "admin:main" }],
    ],
  };
}

function adminPaymentsKeyboard() {
  return {
    inline_keyboard: [
      [{ text: "✏️ Изменить текст", callback_data: "admin:payments_edit" }],
      [{ text: "⬅️ Назад", callback_data: "admin:main" }],
    ],
  };
}

function adminBroadcastKeyboard() {
  return {
    inline_keyboard: [
      [{ text: "✏️ Начать рассылку", callback_data: "admin:broadcast_start" }],
      [{ text: "⬅️ Назад", callback_data: "admin:main" }],
    ],
  };
}

function homeText(user) {
  const sub = getSubscription(user.tg_id);
  const active = sub?.subscription_url ? "есть" : "нет";
  return [
    "<b>ЛИЧНЫЙ КАБИНЕТ</b>",
    "",
    `👋 <b>${esc(user.first_name || "Пользователь")}</b>`,
    `🆔 <code>${user.tg_id}</code>`,
    `💰 Баланс: <b>${fmtRub(user.balance_rub)}</b>`,
    `🔐 Подписка: <b>${active}</b>`,
    "📱 Лимит устройств: <b>3</b>",
    "",
    "Выберите действие в меню ниже.",
  ].join("\n");
}

function tariffsText() {
  const lines = ["<b>ТАРИФЫ VPN</b>", ""];
  for (const t of getTariffs()) {
    lines.push(`• <b>${esc(t.title)}</b> — <b>${fmtRub(t.price_rub)}</b>`);
  }
  lines.push("");
  lines.push("Покупка списывает средства с баланса автоматически.");
  return lines.join("\n");
}

function subscriptionText(user) {
  const sub = getSubscription(user.tg_id);
  if (!sub || !sub.subscription_url) {
    return [
      "<b>МОЯ ПОДПИСКА</b>",
      "",
      "Активная ссылка пока не создана.",
      "Купите тариф, и бот сразу выдаст подписку.",
    ].join("\n");
  }
  return [
    "<b>МОЯ ПОДПИСКА</b>",
    "",
    `Тариф: <b>${esc(sub.plan_code || "—")}</b>`,
    `Действует до: <b>${fmtDate(sub.expires_at)}</b>`,
    "Лимит устройств: <b>3</b>",
    "",
    "Нажмите кнопку ниже, чтобы открыть ссылку.",
  ].join("\n");
}

function balanceText(user) {
  return [
    "<b>БАЛАНС</b>",
    "",
    `Ваш баланс: <b>${fmtRub(user.balance_rub)}</b>`,
    "",
    "Пополнение и способы оплаты настраиваются администратором.",
  ].join("\n");
}

function paymentsText() {
  const methods = getSetting("payment_methods", "").trim();
  return [
    "<b>СПОСОБЫ ОПЛАТЫ</b>",
    "",
    methods || "Пока не настроено.",
  ].join("\n");
}

function aboutText() {
  return [
    "<b>О СЕРВИСЕ</b>",
    "",
    "Надежные VPN-подписки с быстрой выдачей ссылки.",
    "Поддерживаются Happ, v2RayTun и другие совместимые клиенты.",
  ].join("\n");
}

function adminMainText() {
  return [
    "<b>АДМИН ПАНЕЛЬ</b>",
    "",
    "Здесь можно управлять тарифами, GIF, рассылкой и способами оплаты.",
    "Дополнительно доступна команда: <code>/add_balance user_id amount</code>",
  ].join("\n");
}

function adminTariffsText() {
  const lines = ["<b>ЦЕНЫ ТАРИФОВ</b>", ""];
  for (const t of getTariffs()) {
    lines.push(`• ${esc(t.title)}: <b>${fmtRub(t.price_rub)}</b>`);
  }
  lines.push("");
  lines.push("Нажмите на тариф, чтобы изменить цену.");
  return lines.join("\n");
}

function adminGifsText() {
  return [
    "<b>GIF ДЛЯ СООБЩЕНИЙ</b>",
    "",
    "Поддерживаются file_id, URL или Telegram animation-id.",
    "Нажмите кнопку и отправьте GIF/ID следующим сообщением.",
  ].join("\n");
}

function adminPaymentsText() {
  const methods = getSetting("payment_methods", "").trim();
  return [
    "<b>СПОСОБЫ ОПЛАТЫ</b>",
    "",
    methods || "Пока пусто.",
    "",
    "Можно хранить любой текст: реквизиты, ссылки, инструкции.",
  ].join("\n");
}

function adminBroadcastText() {
  return [
    "<b>РАССЫЛКА</b>",
    "",
    "Запустите режим рассылки и отправьте текст одним сообщением.",
    "Бот отправит его всем пользователям из базы.",
  ].join("\n");
}

async function editOrSendMenu(user, chatId, messageId, text, replyMarkup) {
  try {
    if (messageId) {
      await tg("editMessageText", {
        chat_id: chatId,
        message_id: messageId,
        text,
        parse_mode: "HTML",
        disable_web_page_preview: true,
        reply_markup: replyMarkup,
      });
      return Number(messageId);
    }
  } catch (err) {
    if (!String(err.message).includes("message is not modified")) {
      // fallback to send
    } else {
      return Number(messageId);
    }
  }

  const msg = await tg("sendMessage", {
    chat_id: chatId,
    text,
    parse_mode: "HTML",
    disable_web_page_preview: true,
    reply_markup: replyMarkup,
  });
  return Number(msg.message_id);
}

async function renderView(userId, chatId, messageId, view) {
  const user = getUser(userId);
  if (!user) return;

  let text = "";
  let keyboard = mainMenuKeyboard(userId);

  if (view === "home") {
    text = homeText(user);
    keyboard = mainMenuKeyboard(userId);
  } else if (view === "tariffs") {
    text = tariffsText();
    keyboard = tariffsKeyboard();
  } else if (view === "subscription") {
    text = subscriptionText(user);
    keyboard = subscriptionKeyboard(getSubscription(userId)?.subscription_url || "");
  } else if (view === "balance") {
    text = balanceText(user);
    keyboard = balanceKeyboard();
  } else if (view === "payments") {
    text = paymentsText();
    keyboard = paymentsKeyboard();
  } else if (view === "about") {
    text = aboutText();
    keyboard = { inline_keyboard: [[{ text: "⬅️ Назад", callback_data: "view:home" }]] };
  } else if (view === "admin_main") {
    text = adminMainText();
    keyboard = adminMainKeyboard();
  } else if (view === "admin_tariffs") {
    text = adminTariffsText();
    keyboard = adminTariffsKeyboard();
  } else if (view === "admin_gifs") {
    text = adminGifsText();
    keyboard = adminGifsKeyboard();
  } else if (view === "admin_payments") {
    text = adminPaymentsText();
    keyboard = adminPaymentsKeyboard();
  } else if (view === "admin_broadcast") {
    text = adminBroadcastText();
    keyboard = adminBroadcastKeyboard();
  } else {
    text = homeText(user);
    keyboard = mainMenuKeyboard(userId);
  }

  const newMessageId = await editOrSendMenu(user, chatId, messageId, text, keyboard);
  setMenuMessage(userId, chatId, newMessageId);
}

async function createOrUpdateSubscription(user, tariff) {
  const res = await fetch(`${API_BASE_URL}/api/bot-subscription`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-app-secret": APP_SECRET,
    },
    body: JSON.stringify({
      telegramUserId: String(user.tg_id),
      telegramUsername: user.username || "",
      firstName: user.first_name || "",
      durationDays: tariff.duration_days,
      name: `VPN ${tariff.title}`,
    }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `Backend error ${res.status}`);
  return data;
}

async function handleBuy(userId, chatId, messageId, tariffCode, callbackQueryId) {
  const user = getUser(userId);
  const tariff = getTariff(tariffCode);
  if (!user || !tariff) {
    await tg("answerCallbackQuery", { callback_query_id: callbackQueryId, text: "Тариф не найден", show_alert: true });
    return;
  }

  if (Number(user.balance_rub) < Number(tariff.price_rub)) {
    await tg("answerCallbackQuery", {
      callback_query_id: callbackQueryId,
      text: `Недостаточно средств: нужно ${fmtRub(tariff.price_rub)}`,
      show_alert: true,
    });
    await renderView(userId, chatId, messageId, "balance");
    return;
  }

  try {
    const data = await createOrUpdateSubscription(user, tariff);
    updateUserBalance(userId, -Number(tariff.price_rub));
    addPurchase(userId, tariff.code, tariff.price_rub);
    saveSubscription(userId, tariff.code, data.subscriptionUrl, data.subscription?.expiresAt || 0);

    await sendGifIfConfigured(chatId, "gif_purchase_success", "Покупка успешно оформлена");

    const successText = [
      "<b>✅ ПОКУПКА УСПЕШНА</b>",
      "",
      `Тариф: <b>${esc(tariff.title)}</b>`,
      `Списано: <b>${fmtRub(tariff.price_rub)}</b>`,
      `Остаток: <b>${fmtRub(getUser(userId).balance_rub)}</b>`,
      `Действует до: <b>${fmtDate(data.subscription?.expiresAt)}</b>`,
      "Лимит устройств: <b>3</b>",
      "",
      "Ссылка готова к использованию.",
    ].join("\n");

    const keyboard = {
      inline_keyboard: [
        [{ text: "🚀 Открыть подписку", url: data.subscriptionUrl }],
        [{ text: "🔗 Моя подписка", callback_data: "view:subscription" }],
        [{ text: "🏠 В меню", callback_data: "view:home" }],
      ],
    };

    const newMessageId = await editOrSendMenu(getUser(userId), chatId, messageId, successText, keyboard);
    setMenuMessage(userId, chatId, newMessageId);

    await tg("answerCallbackQuery", {
      callback_query_id: callbackQueryId,
      text: "Подписка активирована",
      show_alert: false,
    });
  } catch (err) {
    await tg("answerCallbackQuery", {
      callback_query_id: callbackQueryId,
      text: "Ошибка при покупке",
      show_alert: true,
    });
    await tg("sendMessage", {
      chat_id: chatId,
      text: `Ошибка: ${esc(err.message)}`,
      parse_mode: "HTML",
    });
  }
}

async function runBroadcast(text) {
  const users = getAllUsers();
  let ok = 0;
  let fail = 0;
  for (const u of users) {
    try {
      const gif = getSetting("gif_broadcast", "");
      if (gif) {
        await tg("sendAnimation", {
          chat_id: Number(u.tg_id),
          animation: gif,
          caption: text,
          parse_mode: "HTML",
        });
      } else {
        await tg("sendMessage", {
          chat_id: Number(u.tg_id),
          text,
          parse_mode: "HTML",
          disable_web_page_preview: true,
        });
      }
      ok += 1;
    } catch {
      fail += 1;
    }
  }
  return { ok, fail, total: users.length };
}

async function handleAdminStateMessage(message) {
  const adminId = Number(message.from?.id || 0);
  const chatId = Number(message.chat?.id || 0);
  const stateRow = getAdminState(adminId);
  if (!stateRow) return false;

  const text = String(message.text || "").trim();
  const state = stateRow.state;
  const payload = stateRow.payload || "";

  if (text === "/cancel") {
    clearAdminState(adminId);
    await tg("sendMessage", { chat_id: chatId, text: "Действие отменено." });
    await renderView(adminId, chatId, getUser(adminId)?.last_menu_message_id, "admin_main");
    return true;
  }

  if (state === "await_tariff_price") {
    const price = Number(text);
    if (!Number.isFinite(price) || price <= 0) {
      await tg("sendMessage", { chat_id: chatId, text: "Введите положительное число в рублях." });
      return true;
    }
    updateTariffPrice(payload, Math.round(price));
    clearAdminState(adminId);
    await tg("sendMessage", { chat_id: chatId, text: "Цена обновлена." });
    await renderView(adminId, chatId, getUser(adminId)?.last_menu_message_id, "admin_tariffs");
    return true;
  }

  if (state === "await_payment_methods") {
    setSetting("payment_methods", text);
    clearAdminState(adminId);
    await tg("sendMessage", { chat_id: chatId, text: "Способы оплаты обновлены." });
    await renderView(adminId, chatId, getUser(adminId)?.last_menu_message_id, "admin_payments");
    return true;
  }

  if (state === "await_broadcast_text") {
    clearAdminState(adminId);
    await tg("sendMessage", { chat_id: chatId, text: "Запускаю рассылку..." });
    const result = await runBroadcast(text);
    await tg("sendMessage", {
      chat_id: chatId,
      text: `Рассылка завершена.\nУспешно: ${result.ok}\nОшибок: ${result.fail}\nВсего: ${result.total}`,
    });
    await renderView(adminId, chatId, getUser(adminId)?.last_menu_message_id, "admin_broadcast");
    return true;
  }

  if (state === "await_gif_value") {
    let value = text;
    if (message.animation?.file_id) value = message.animation.file_id;
    if (message.document?.mime_type === "video/mp4" && message.document?.file_id) value = message.document.file_id;

    if (!value) {
      await tg("sendMessage", { chat_id: chatId, text: "Отправьте GIF или file_id текстом." });
      return true;
    }
    setSetting(payload, value);
    clearAdminState(adminId);
    await tg("sendMessage", { chat_id: chatId, text: "GIF сохранен." });
    await renderView(adminId, chatId, getUser(adminId)?.last_menu_message_id, "admin_gifs");
    return true;
  }

  return false;
}

async function handleAddBalanceCommand(message) {
  const userId = Number(message.from?.id || 0);
  const chatId = Number(message.chat?.id || 0);
  if (!isAdmin(userId)) return false;

  const text = String(message.text || "").trim();
  if (!text.startsWith("/add_balance")) return false;

  const parts = text.split(/\s+/);
  if (parts.length !== 3) {
    await tg("sendMessage", {
      chat_id: chatId,
      text: "Формат: /add_balance <telegram_id> <amount_rub>",
    });
    return true;
  }

  const targetId = Number(parts[1]);
  const amount = Number(parts[2]);
  if (!Number.isFinite(targetId) || !Number.isFinite(amount)) {
    await tg("sendMessage", { chat_id: chatId, text: "Неверные параметры." });
    return true;
  }

  const target = getUser(targetId);
  if (!target) {
    await tg("sendMessage", { chat_id: chatId, text: "Пользователь не найден в БД." });
    return true;
  }

  try {
    const newBalance = updateUserBalance(targetId, amount);
    await tg("sendMessage", {
      chat_id: chatId,
      text: `Баланс пользователя ${targetId} изменен на ${fmtRub(amount)}.\nНовый баланс: ${fmtRub(newBalance)}.`,
    });
    await tg("sendMessage", {
      chat_id: targetId,
      text: `Ваш баланс изменен администратором.\nИзменение: ${fmtRub(amount)}\nТекущий баланс: ${fmtRub(newBalance)}.`,
    });
  } catch (err) {
    await tg("sendMessage", { chat_id: chatId, text: `Ошибка: ${err.message}` });
  }
  return true;
}

async function handleMessage(message) {
  const text = String(message.text || "").trim();
  const from = message.from || {};
  const chatId = Number(message.chat?.id || 0);
  if (!chatId || !from.id) return;

  upsertUser(from, chatId);

  if (await handleAddBalanceCommand(message)) return;
  if (isAdmin(from.id) && (await handleAdminStateMessage(message))) return;

  if (text === "/start" || text === "/menu") {
    await sendGifIfConfigured(chatId, "gif_main_menu");
    await renderView(from.id, chatId, getUser(from.id)?.last_menu_message_id, "home");
    return;
  }

  if (text === "/admin") {
    if (!isAdmin(from.id)) {
      await tg("sendMessage", { chat_id: chatId, text: "Недостаточно прав." });
      return;
    }
    await renderView(from.id, chatId, getUser(from.id)?.last_menu_message_id, "admin_main");
    return;
  }

  await tg("sendMessage", {
    chat_id: chatId,
    text: "Используйте /start для открытия меню.",
  });
}

async function handleCallbackQuery(callbackQuery) {
  const data = callbackQuery.data || "";
  const user = callbackQuery.from || {};
  const chatId = Number(callbackQuery.message?.chat?.id || 0);
  const messageId = Number(callbackQuery.message?.message_id || 0);
  if (!chatId || !messageId || !user.id) return;

  upsertUser(user, chatId);

  if (data.startsWith("admin:") && !isAdmin(user.id)) {
    await tg("answerCallbackQuery", {
      callback_query_id: callbackQuery.id,
      text: "Недостаточно прав",
      show_alert: true,
    });
    return;
  }

  if (data.startsWith("view:")) {
    const view = data.split(":")[1];
    await renderView(user.id, chatId, messageId, view);
    await tg("answerCallbackQuery", { callback_query_id: callbackQuery.id });
    return;
  }

  if (data.startsWith("buy:")) {
    const tariffCode = data.split(":")[1];
    await handleBuy(user.id, chatId, messageId, tariffCode, callbackQuery.id);
    return;
  }

  if (data === "admin:main") {
    if (!isAdmin(user.id)) {
      await tg("answerCallbackQuery", { callback_query_id: callbackQuery.id, text: "Недостаточно прав", show_alert: true });
      return;
    }
    await renderView(user.id, chatId, messageId, "admin_main");
    await tg("answerCallbackQuery", { callback_query_id: callbackQuery.id });
    return;
  }

  if (data === "admin:tariffs") {
    await renderView(user.id, chatId, messageId, "admin_tariffs");
    await tg("answerCallbackQuery", { callback_query_id: callbackQuery.id });
    return;
  }

  if (data.startsWith("admin:tariff_edit:")) {
    const code = data.split(":")[2];
    const tariff = getTariff(code);
    if (!tariff) {
      await tg("answerCallbackQuery", { callback_query_id: callbackQuery.id, text: "Тариф не найден", show_alert: true });
      return;
    }
    setAdminState(user.id, "await_tariff_price", code);
    await tg("answerCallbackQuery", { callback_query_id: callbackQuery.id });
    await tg("sendMessage", {
      chat_id: chatId,
      text: `Введите новую цену для "${tariff.title}" в рублях.\nДля отмены: /cancel`,
    });
    return;
  }

  if (data === "admin:gifs") {
    await renderView(user.id, chatId, messageId, "admin_gifs");
    await tg("answerCallbackQuery", { callback_query_id: callbackQuery.id });
    return;
  }

  if (data.startsWith("admin:set_gif:")) {
    const key = data.split(":")[2];
    setAdminState(user.id, "await_gif_value", key);
    await tg("answerCallbackQuery", { callback_query_id: callbackQuery.id });
    await tg("sendMessage", {
      chat_id: chatId,
      text: `Отправьте GIF или file_id для ключа "${key}".\nДля отмены: /cancel`,
    });
    return;
  }

  if (data === "admin:broadcast") {
    await renderView(user.id, chatId, messageId, "admin_broadcast");
    await tg("answerCallbackQuery", { callback_query_id: callbackQuery.id });
    return;
  }

  if (data === "admin:broadcast_start") {
    setAdminState(user.id, "await_broadcast_text", "");
    await tg("answerCallbackQuery", { callback_query_id: callbackQuery.id });
    await tg("sendMessage", {
      chat_id: chatId,
      text: "Отправьте текст рассылки одним сообщением.\nДля отмены: /cancel",
    });
    return;
  }

  if (data === "admin:payments") {
    await renderView(user.id, chatId, messageId, "admin_payments");
    await tg("answerCallbackQuery", { callback_query_id: callbackQuery.id });
    return;
  }

  if (data === "admin:payments_edit") {
    setAdminState(user.id, "await_payment_methods", "");
    await tg("answerCallbackQuery", { callback_query_id: callbackQuery.id });
    await tg("sendMessage", {
      chat_id: chatId,
      text: "Отправьте новый текст для блока способов оплаты.\nДля отмены: /cancel",
    });
    return;
  }

  await tg("answerCallbackQuery", {
    callback_query_id: callbackQuery.id,
    text: "Неизвестная команда",
    show_alert: false,
  });
}

async function poll() {
  while (true) {
    try {
      const updates = await tg("getUpdates", {
        timeout: 30,
        offset,
        allowed_updates: ["message", "callback_query"],
      });

      for (const update of updates) {
        offset = update.update_id + 1;
        if (update.message) {
          await handleMessage(update.message);
        } else if (update.callback_query) {
          await handleCallbackQuery(update.callback_query);
        }
      }
    } catch (err) {
      console.error("Polling error:", err.message);
      await new Promise((resolve) => setTimeout(resolve, 1500));
    }
  }
}

initDb();
console.log(`Bot DB: ${DB_PATH}`);
console.log("Telegram bot started.");
poll();
