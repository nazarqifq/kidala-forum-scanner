# Kidala Forum Scanner

Микросервис для парсинга жалоб с forum.majestic-rp.ru.
Работает на Railway, пишет данные в ту же MySQL базу что и Discord бот.

## Деплой на Railway (3 минуты)

### Шаг 1: GitHub
1. Зайди на https://github.com/new
2. Создай новый репозиторий: `kidala-forum-scanner` (приватный)
3. Загрузи в него файлы: `index.js`, `package.json`, `railway.toml`

### Шаг 2: Railway
1. Зайди на https://railway.app — залогинься через GitHub
2. Нажми **"New Project"** → **"Deploy from GitHub repo"**
3. Выбери репозиторий `kidala-forum-scanner`
4. Railway автоматически задеплоит

### Шаг 3: Проверка
1. Railway даст URL типа `https://kidala-forum-scanner-xxx.up.railway.app`
2. Открой его в браузере — увидишь статус сканера
3. В логах Railway будет видно как сканер парсит форум

## Как работает

```
Railway сервер (этот код)
  ↓
  Грузит страницы forum.majestic-rp.ru
  (сначала напрямую, если Cloudflare — через ScrapingBee)
  ↓
  Парсит жалобы cheerio
  ↓
  Пишет в MySQL (s626_KidalaDB → forum_complaints)
  ↓
Discord бот на SkailarHost
  ↓
  Читает из MySQL
  ↓
  Показывает в /forumcloud
```

## Мониторинг
- `GET /` — статус + статистика последнего сканирования
- `GET /scan` — запустить сканирование вручную
- `GET /health` — health check

## Настройки (environment variables в Railway)
Все настройки уже прописаны в коде, но можно переопределить:
- `SCAN_INTERVAL` — интервал сканирования в мс (по умолчанию 300000 = 5 мин)
- `SCRAPINGBEE_KEY` — API ключ ScrapingBee
- `FORUM_EMAIL` / `FORUM_PASS` — данные для входа на форум
- `DB_HOST`, `DB_USER`, `DB_PASS`, `DB_NAME` — MySQL (по умолчанию совпадает с ботом)
