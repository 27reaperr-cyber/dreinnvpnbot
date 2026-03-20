const TELEGRAM_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const API_BASE_URL = (process.env.VPN_API_BASE_URL || "").replace(/\/+$/, "");
const APP_SECRET = process.env.APP_SECRET || "";
const DEFAULT_DURATION_DAYS = Number(process.env.DEFAULT_DURATION_DAYS || 30);
const NEWS_URL = process.env.BOT_NEWS_URL || "https://t.me/cats_vpn";
const SUPPORT_URL = process.env.BOT_SUPPORT_URL || "https://t.me/Oktsupport";

if (!TELEGRAM_TOKEN || !API_BASE_URL || !APP_SECRET) {
  console.error("Missing required env vars: TELEGRAM_BOT_TOKEN, VPN_API_BASE_URL, APP_SECRET");
  process.exit(1);
}

const TG_API = `https://api.telegram.org/bot${TELEGRAM_TOKEN}`;
let offset = 0;

function escapeHtml(str) {
  return String(str || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function formatCabinetText(user) {
  const firstName = escapeHtml(user?.first_name || "друг");
  const username = user?.username ? `@${escapeHtml(user.username)}` : "без username";
  return [
    "ЛИЧНЫЙ КАБИНЕТ",
    "",
    `Привет, ${firstName}`,
    `Ваш Telegram: ${username}`,
    `Ваш ID: ${user?.id || "-"}`,
    "",
    "Меню подписки VPN",
    "Лимит устройств на ссылку: 3",
  ].join("\n");
}

function mainMenuKeyboard() {
  return {
    inline_keyboard: [
      [
        { text: "📣 Новостной канал", url: NEWS_URL },
        { text: "👤 Техподдержка", url: SUPPORT_URL }
      ],
      [{ text: "🎁 Моя подписка", callback_data: "my_subscription" }],
      [{ text: "⭐ Генерировать подписку", callback_data: "generate_subscription" }],
      [{ text: "🤝 Партнерская программа", callback_data: "partners_soon" }],
      [{ text: "📘 Инструкции", callback_data: "guide_soon" }],
      [{ text: "💬 О сервисе", callback_data: "about_soon" }]
    ]
  };
}

function subscriptionKeyboard(subscriptionUrl) {
  return {
    inline_keyboard: [
      [{ text: "🔗 Открыть подписку", url: subscriptionUrl }],
      [{ text: "🔄 Перегенерировать", callback_data: "generate_subscription" }],
      [{ text: "🏠 В меню", callback_data: "back_menu" }]
    ]
  };
}

async function tg(method, payload) {
  const res = await fetch(`${TG_API}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.ok === false) {
    const errorText = data.description || `Telegram API error ${res.status}`;
    throw new Error(errorText);
  }
  return data.result;
}

async function sendMenu(chatId, user, messageIdToEdit = null) {
  const payload = {
    chat_id: chatId,
    text: formatCabinetText(user),
    parse_mode: "HTML",
    reply_markup: mainMenuKeyboard()
  };
  if (messageIdToEdit) {
    return tg("editMessageText", { ...payload, message_id: messageIdToEdit });
  }
  return tg("sendMessage", payload);
}

async function ensureSubscription(user) {
  const res = await fetch(`${API_BASE_URL}/api/bot-subscription`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-app-secret": APP_SECRET
    },
    body: JSON.stringify({
      telegramUserId: String(user.id),
      telegramUsername: user.username || "",
      firstName: user.first_name || "",
      durationDays: DEFAULT_DURATION_DAYS
    })
  });

  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error || `Backend error ${res.status}`);
  }
  return data;
}

async function sendSubscriptionMessage(chatId, user, callbackQueryId = null) {
  try {
    const data = await ensureSubscription(user);
    const sub = data.subscription || {};
    const expireDate = sub.expiresAt
      ? new Date(sub.expiresAt).toLocaleDateString("ru-RU")
      : "не указана";
    const text = [
      "Ваша подписка готова",
      "",
      `Имя: ${escapeHtml(sub.name || "VPN Subscription")}`,
      `Истекает: ${escapeHtml(expireDate)}`,
      "Лимит устройств: 3",
      "",
      "Нажмите кнопку ниже, чтобы открыть subscription-link."
    ].join("\n");

    await tg("sendMessage", {
      chat_id: chatId,
      text,
      parse_mode: "HTML",
      reply_markup: subscriptionKeyboard(data.subscriptionUrl)
    });

    if (callbackQueryId) {
      await tg("answerCallbackQuery", {
        callback_query_id: callbackQueryId,
        text: "Ссылка сгенерирована",
        show_alert: false
      });
    }
  } catch (err) {
    await tg("sendMessage", {
      chat_id: chatId,
      text: `Ошибка генерации подписки: ${escapeHtml(err.message)}`,
      parse_mode: "HTML"
    });
    if (callbackQueryId) {
      await tg("answerCallbackQuery", {
        callback_query_id: callbackQueryId,
        text: "Ошибка генерации",
        show_alert: false
      });
    }
  }
}

async function handleMessage(message) {
  const text = String(message.text || "").trim();
  const user = message.from || {};
  const chatId = message.chat?.id;
  if (!chatId) return;

  if (text === "/start" || text === "/menu") {
    await sendMenu(chatId, user);
    return;
  }

  if (text === "/sub") {
    await sendSubscriptionMessage(chatId, user);
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
  const chatId = callbackQuery.message?.chat?.id;
  const messageId = callbackQuery.message?.message_id;
  if (!chatId || !messageId) return;

  if (data === "back_menu") {
    await sendMenu(chatId, user, messageId);
    await tg("answerCallbackQuery", { callback_query_id: callbackQuery.id });
    return;
  }

  if (data === "my_subscription" || data === "generate_subscription") {
    await sendSubscriptionMessage(chatId, user, callbackQuery.id);
    return;
  }

  if (data === "partners_soon" || data === "guide_soon" || data === "about_soon") {
    await tg("answerCallbackQuery", {
      callback_query_id: callbackQuery.id,
      text: "Раздел скоро будет доступен",
      show_alert: false
    });
    return;
  }

  await tg("answerCallbackQuery", {
    callback_query_id: callbackQuery.id,
    text: "Неизвестная команда",
    show_alert: false
  });
}

async function poll() {
  while (true) {
    try {
      const updates = await tg("getUpdates", {
        timeout: 30,
        offset,
        allowed_updates: ["message", "callback_query"]
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
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
  }
}

console.log("Telegram bot is running...");
poll();
