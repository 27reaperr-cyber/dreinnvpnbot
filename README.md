# Telegram VPN Bot (Production)

Полноценный Telegram-бот для продажи VPN с системой тарифов, баланса, рефералкой, подарками, SQLite и админ-панелью через inline-меню.

## Возможности

- Единое редактируемое inline-меню (бот редактирует один экран, а не спамит новыми сообщениями)
- Форматированный UI (`HTML`) для пользовательских и админских экранов
- SQLite-база для пользователей, баланса, подписок, тарифов, настроек, истории покупок
- Реферальная программа (персональная ссылка, начисление бонуса, вывод)
- Подарки VPN через Telegram user picker (выбор пользователя системным меню Telegram)
- Подтверждение покупки/продления перед списанием
- Тарифы по умолчанию:
  - `1 месяц` — `100 ₽`
  - `6 месяцев` — `600 ₽`
  - `1 год` — `900 ₽`
- Покупка тарифа за баланс с автогенерацией subscription-link через сайт (`/api/bot-subscription`)
- Админ-панель:
  - редактирование цен тарифов
  - GIF для сообщений бота
  - рассылка
  - редактирование способов оплаты (текстовый блок)
  - настройки реферальной ставки и минимального вывода
  - экспорт БД
  - импорт БД (после импорта бот перезапускается)
- Команда админа для баланса: `/add_balance <telegram_id> <amount_rub>`

## Переменные окружения

- `TELEGRAM_BOT_TOKEN` — токен Telegram-бота
- `VPN_API_BASE_URL` — URL сайта (например, `https://your-domain.vercel.app`)
- `APP_SECRET` — секрет для доступа к `/api/bot-subscription`
- `ADMIN_TELEGRAM_ID` — Telegram ID администратора
- `SQLITE_PATH` — путь к SQLite файлу (по умолчанию `./bot.sqlite`)
- `BOT_NEWS_URL` — ссылка на канал
- `BOT_SUPPORT_URL` — ссылка на поддержку
- `BOT_FREE_PROXY_URL` — ссылка кнопки «Бесплатные прокси для TG»
- `BOT_USERNAME` — username бота без `@` (для реферальной ссылки)
- `FREEKASSA_SHOP_ID` — ID магазина FreeKassa
- `FREEKASSA_API_KEY` — API ключ FreeKassa (из кабинета)
- `FREEKASSA_SECRET2` — Secret Word 2 (для проверки подписи webhook)
- `FREEKASSA_SERVER_IP` — внешний IP вашего сервера (не `127.0.0.1`)
- `FREEKASSA_DOMAIN` — домен для логов/подсказки URL webhook (например, `dreinn.bothost.tech`)
- `PORT` — порт HTTP сервера webhook (в вашем случае `3000`)
- `FREEKASSA_NOTIFY_PATH` — путь webhook (по умолчанию `/freekassa/notify`)
- `FREEKASSA_MIN_RUB` — минимальная сумма пополнения (по умолчанию `50`)
- `FREEKASSA_CHECK_IPS` — `1` чтобы проверять IP отправителя webhook по whitelist FreeKassa

URL для оповещений FreeKassa (Notification URL):

`https://dreinn.bothost.tech:3000/freekassa/notify`

## Запуск

```bash
cd Бот
npm install
npm run start
```

Проверка синтаксиса:

```bash
npm run check
```

## 2026 Patch Notes

- DB changes are additive and backward-compatible: new columns/tables are created through runtime migrations and do not require wiping the current SQLite database.
- Plan purchase is now simplified: after choosing a tariff the bot creates a checkout session for the exact tariff amount, supports promo codes, and lets the user pay from balance, CryptoBot, or FreeKassa.
- CryptoBot webhook support was added for instant crediting/activation without the `Check` button. FreeKassa instant crediting is also supported through the existing notify endpoint.
- Stale `user_states` and checkout sessions are cleaned up automatically, and pending CryptoBot invoices are expired in background maintenance jobs.
- Subscription expiry reminders are sent automatically for users whose plan ends in 3 days and in 1 day, with a direct `Renew` button.
- Daily database backup delivery to all admins was added. The hour is controlled by `BACKUP_HOUR`.
- Telegram commands are now registered on startup with `setMyCommands`.

## Webhook / Runtime Notes

- Expose the local webhook server only behind an HTTPS reverse proxy. The bot itself listens on plain HTTP for the internal hop, but public webhook URLs should be HTTPS.
- CryptoBot webhook path is controlled by `CRYPTOBOT_WEBHOOK_PATH`. If not set, the bot generates a deterministic secret path and prints it on startup.
- FreeKassa IP allowlist can now be configured through `FREEKASSA_ALLOWED_IPS` or the `fk_allowed_ips` setting stored in SQLite.
- FreeKassa payer email domain can be configured through `FREEKASSA_EMAIL_DOMAIN`.
- Under PM2, systemd, Docker, or similar process managers the bot now exits cleanly instead of spawning an orphan child process on restart/import. Let the process manager perform the actual restart.
