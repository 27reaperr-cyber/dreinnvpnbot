# 🎨 Использование Telegram Premium Emojis в боте

## Быстрый старт

### 1. Для использования в текстовых сообщениях (HTML)

```javascript
// Импорт
const E = require("./emojis.js");

// Функция em() уже доступна в bot.js
// Синтаксис: em(originalEmoji, emojiKey)
text: `${em("🎉", "celebrate")} <b>Успех!</b>`
```

### 2. Для инлайн кнопок

```javascript
// Функция createEmojiBtnInline() уже доступна
// Синтаксис: createEmojiBtnInline(text, callbackOrUrl, emojiKey, isUrl)

// Пример 1: кнопка с callback
[createEmojiBtnInline("Купить", "v:buy", "money")]

// Пример 2: кнопка с URL
[createEmojiBtnInline("Сайт", "https://example.com", "link", true)]

// Пример 3: кнопка без эмодзи
[createEmojiBtnInline("Просто текст", "some_callback")]
```

### 3. Для кнопок в меню

```javascript
// Функция createEmojiButton() для обычных кнопок
button = createEmojiButton("Мой текст", "myEmoji")
```

## Доступные эмодзи

### Архитектура
| Ключ | ID | Эмодзи |
|-----|-----|--------|
| `settings` | 5870982283724328568 | ⚙️ |
| `profile` | 5870994129244131212 | 👤 |
| `users` | 5870772616305839506 | 👥 |
| `userCheck` | 5891207662678317861 | ✓ |
| `userCross` | 5893192487324880883 | ✗ |

### Общие элементы
| Ключ | ID | Эмодзи |
|-----|-----|--------|
| `home` | 5873147866364514353 | 🏘 |
| `settings` | 5870982283724328568 | ⚙️ |
| `money` | 5904462880941545555 | 🪙 |
| `wallet` | 5769126056262898415 | 👛 |
| `gift` | 6032644646587338669 | 🎁 |
| `check` | 5870633910337015697 | ✅ |
| `cross` | 5870657884844462243 | ❌ |

### Действия
| Ключ | ID | Эмодзи |
|-----|-----|--------|
| `send` | 5963103826075456248 | ⬆ |
| `download` | 6039802767931871481 | ⬇ |
| `megaphone` | 6039422865189638057 | 📣 |
| `pencil` | 5870676941614354370 | 🖋 |
| `write` | 5870753782874246579 | ✍ |
| `link` | 5769289093221454192 | 🔗 |

### Статус
| Ключ | ID | Эмодзи |
|-----|-----|--------|
| `lockClosed` | 6037249452824072506 | 🔒 |
| `lockOpen` | 6037496202990194718 | 🔓 |
| `eye` | 6037397706505195857 | 👁 |
| `loading` | 5345906554510012647 | 🔄 |

### Другое
| Ключ | ID | Эмодзи |
|-----|-----|--------|
| `box` | 5884479287171485878 | 📦 |
| `notification` | 6039486778597970865 | 🔔 |
| `calendar` | 5890937706803894250 | 📅 |
| `celebrate` | 6041731551845159060 | 🎉 |
| `stats` | 5870921681735781843 | 📊 |
| `tag` | 5886285355279193209 | 🏷 |

## Примеры использования

### Пример 1: Обновление сообщения с эмодзи

```javascript
await tg("sendMessage", {
  chat_id: chatId,
  text: `${em("⚙️", "settings")} <b>Настройки</b>\n\n${em("✅", "check")} Параметр включен`,
  parse_mode: "HTML"
});
```

### Пример 2: Кнопки с эмодзи

```javascript
const kb = {
  inline_keyboard: [
    [
      createEmojiBtnInline("Купить", "v:buy", "money"),
      createEmojiBtnInline("Подарить", "v:gift", "gift")
    ],
    [
      createEmojiBtnInline("Рефералы", "v:ref", "users")
    ]
  ]
};
```

### Пример 3: Динамический выбор эмодзи

```javascript
const statusEmoji = userActive ? "check" : "cross";
const button = createEmojiBtnInline("Статус", "status", statusEmoji);
```

## API функции

### em(originalEmoji, emojiKey)
Обёртывает текст в тег с Telegram Premium эмодзи

**Параметры:**
- `originalEmoji` (string): Исходный эмодзи для fallback (опционально)
- `emojiKey` (string): Ключ из файла `emojis.js`

**Возврат:** HTML тег с эмодзи или исходный текст

### createEmojiBtnInline(text, callbackOrUrl, emojiKey, isUrl)
Создаёт инлайн кнопку с Telegram Premium эмодзи

**Параметры:**
- `text` (string): Текст кнопки
- `callbackOrUrl` (string): callback_data или URL
- `emojiKey` (string): Ключ из файла `emojis.js`
- `isUrl` (boolean): true если это URL, false если callback (по умолчанию false)

**Возврат:** Объект кнопки

### createEmojiButton(text, emojiKey)
Создаёт обычную кнопку с эмодзи (для ReplyKeyboardMarkup)

**Параметры:**
- `text` (string): Текст кнопки
- `emojiKey` (string): Ключ из файла `emojis.js`

**Возврат:** Объект кнопки

## Советы

1. **Fallback**: Всегда указывайте исходный эмодзи как второй параметр для старых клиентов Telegram
2. **Читаемость**: Используйте эмодзи для визуального разделения секций
3. **Последовательность**: Используйте одни и те же эмодзи для одних и тех же действий
4. **Производительность**: Кеширование эмодзи ID происходит автоматически

## Проблемы?

Если эмодзи не отображаются:
1. Убедитесь, что ID скопирован правильно
2. Проверьте синтаксис HTML
3. Убедитесь, что используется `parse_mode: "HTML"`
4. ID должны быть строками, а не числами

---

**Последнее обновление**: April 8, 2026
