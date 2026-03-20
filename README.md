# Telegram VPN Bot

Минимальный Telegram-бот для генерации VPN subscription-link через сайт (`APP_SECRET`).

## Функции

- Инлайн-меню "личный кабинет" (без пополнения и оплаты)
- Генерация/обновление подписки по Telegram user id
- Лимит устройств на ссылку: `3`

## Переменные окружения

- `TELEGRAM_BOT_TOKEN` — токен бота
- `VPN_API_BASE_URL` — домен сайта (например, `https://your-domain.vercel.app`)
- `APP_SECRET` — тот же секрет, что в Vercel
- `DEFAULT_DURATION_DAYS` — срок подписки (по умолчанию 30)
- `BOT_NEWS_URL` — ссылка на новостной канал
- `BOT_SUPPORT_URL` — ссылка на поддержку

## Запуск

```bash
cd Бот
npm run start
```

Для проверки синтаксиса:

```bash
npm run check
```


