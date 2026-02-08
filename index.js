// ===== KIDALA FORUM SCANNER =====
// Отдельный микросервис на Railway
// Парсит forum.majestic-rp.ru → пишет жалобы в MySQL → бот читает из БД
// ===================================

const express = require('express');
const axios = require('axios');
const cheerio = require('cheerio');
const mysql = require('mysql2/promise');

// ===== КОНФИГУРАЦИЯ =====
const CONFIG = {
  // MySQL — та же база что и у бота
  DB: {
    host: process.env.DB_HOST || 'mysql-eu5.skailarhost.com',
    port: parseInt(process.env.DB_PORT || '3306'),
    user: process.env.DB_USER || 'u626_HQJqMYObkl',
    password: process.env.DB_PASS || 'jP8mV6Swf6KRmZM.467NM=.h',
    database: process.env.DB_NAME || 's626_KidalaDB',
    waitForConnections: true,
    connectionLimit: 5,
    queueLimit: 0
  },

  // ScrapingBee
  SCRAPINGBEE_API_KEY: process.env.SCRAPINGBEE_KEY || 'TM3HR6BIP0NTHTTYL0M8JTDEBSMYTQK0NCXHOC80FHK5RCIGJF0UMMKM6XYIK67V4FM58KHAAONAGVYL',

  // Форум
  FORUM_BASE: 'https://forum.majestic-rp.ru',
  FORUM_LOGIN_URL: 'https://forum.majestic-rp.ru/login/login',
  FORUM_EMAIL: process.env.FORUM_EMAIL || 'cloudnaxyi@gmail.com',
  FORUM_PASS: process.env.FORUM_PASS || 'nazar1998TOP!',

  SECTIONS: {
    active: {
      url: 'https://forum.majestic-rp.ru/forums/zhaloby-na-igrokov.1380/',
      status: 'in_progress',
      name: 'Активные жалобы'
    },
    approved: {
      url: 'https://forum.majestic-rp.ru/forums/rassmotrennyye-zhaloby.1381/',
      status: 'approved',
      name: 'Рассмотренные жалобы'
    },
    rejected: {
      url: 'https://forum.majestic-rp.ru/forums/otklonennyye-zhaloby.1382/',
      status: 'rejected',
      name: 'Отклонённые жалобы'
    }
  },

  PAGES_PER_SECTION: { active: 3, approved: 2, rejected: 2 },
  SCAN_INTERVAL: parseInt(process.env.SCAN_INTERVAL || '300000'), // 5 минут
  REQUEST_DELAY: 2500,
  PORT: parseInt(process.env.PORT || '3000')
};

// ===== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ =====
let dbPool = null;
let scanStats = { lastScan: null, total: 0, family: 0, saved: 0, errors: 0, method: 'none' };
let isScanning = false;
const processedComplaints = new Set();

// Для прямого доступа с cookies
let forumCookies = null;
let useScrapingBee = false; // Сначала пробуем напрямую

// ===== ИНИЦИАЛИЗАЦИЯ БД =====
async function initDB() {
  dbPool = mysql.createPool(CONFIG.DB);
  const conn = await dbPool.getConnection();
  console.log('✅ MySQL подключён');
  conn.release();

  // Убеждаемся что таблицы есть
  await dbPool.execute(`
    CREATE TABLE IF NOT EXISTS linked_forum_accounts (
      id INT AUTO_INCREMENT PRIMARY KEY,
      user_id VARCHAR(20) NOT NULL UNIQUE,
      nickname VARCHAR(100) NOT NULL,
      static_id VARCHAR(20) NOT NULL,
      linked_at BIGINT NOT NULL,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);

  await dbPool.execute(`
    CREATE TABLE IF NOT EXISTS forum_complaints (
      id INT AUTO_INCREMENT PRIMARY KEY,
      complaint_id VARCHAR(20) NOT NULL,
      type ENUM('incoming', 'outgoing') NOT NULL,
      user_id VARCHAR(20) NOT NULL,
      target_nickname VARCHAR(100) NOT NULL,
      target_static_id VARCHAR(20) NOT NULL,
      subject VARCHAR(255) NOT NULL,
      status ENUM('waiting', 'in_progress', 'approved', 'rejected') DEFAULT 'waiting',
      forum_url VARCHAR(500),
      created_at BIGINT NOT NULL,
      closed_at BIGINT DEFAULT NULL,
      resolution VARCHAR(255) DEFAULT NULL,
      UNIQUE KEY unique_complaint (complaint_id, user_id, type)
    )
  `);

  console.log('✅ Таблицы проверены');
}

// ===== ПОЛУЧЕНИЕ ПРИВЯЗАННЫХ АККАУНТОВ =====
async function getLinkedAccounts() {
  const [rows] = await dbPool.execute('SELECT * FROM linked_forum_accounts');
  const map = new Map();
  for (const row of rows) {
    map.set(row.static_id, { userId: row.user_id, nickname: row.nickname });
    map.set(row.nickname.toLowerCase(), { userId: row.user_id, nickname: row.nickname });
  }
  return map;
}

// ===== СОХРАНЕНИЕ ЖАЛОБЫ =====
async function saveComplaint(data) {
  try {
    await dbPool.execute(`
      INSERT INTO forum_complaints 
      (complaint_id, type, user_id, target_nickname, target_static_id, subject, status, forum_url, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        status = VALUES(status),
        subject = VALUES(subject)
    `, [
      data.complaintId, data.type, data.userId, data.targetNickname,
      data.targetStaticId, data.subject, data.status || 'waiting',
      data.forumUrl || null, data.createdAt || Date.now()
    ]);
    return true;
  } catch (error) {
    console.error('❌ Ошибка сохранения жалобы:', error.message);
    return false;
  }
}

// ===== HTTP ЗАПРОСЫ К ФОРУМУ =====

// Метод 1: Прямой запрос (бесплатно, работает если Cloudflare не блокирует серверный IP)
async function fetchDirect(url) {
  try {
    const headers = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
      'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
      'Accept-Encoding': 'gzip, deflate, br',
      'Connection': 'keep-alive',
      'Upgrade-Insecure-Requests': '1'
    };
    if (forumCookies) {
      headers['Cookie'] = forumCookies;
    }

    const response = await axios.get(url, {
      headers,
      timeout: 30000,
      maxRedirects: 5,
      validateStatus: (s) => s < 500
    });

    // Проверяем что это не Cloudflare challenge
    const html = response.data;
    if (typeof html === 'string' && (
      html.includes('Just a moment') ||
      html.includes('Checking your browser') ||
      html.includes('cf-browser-verification') ||
      html.includes('Please turn JavaScript on')
    )) {
      console.log('⚠️ Cloudflare challenge — переключаюсь на ScrapingBee');
      return { success: false, cloudflare: true };
    }

    // Проверяем что не редирект на логин
    if (response.status === 403 || (typeof html === 'string' && html.length < 1000 && html.includes('login'))) {
      return { success: false, needsAuth: true };
    }

    return { success: true, html };
  } catch (error) {
    return { success: false, error: error.message };
  }
}

// Метод 2: Через ScrapingBee (платный, но пробивает Cloudflare)
async function fetchWithScrapingBee(url) {
  try {
    const response = await axios.get('https://app.scrapingbee.com/api/v1/', {
      params: {
        api_key: CONFIG.SCRAPINGBEE_API_KEY,
        url: url,
        render_js: 'true',
        premium_proxy: 'true',
        country_code: 'ru',
        block_ads: 'true',
        wait: '5000'
      },
      timeout: 120000
    });

    return { success: true, html: response.data };
  } catch (error) {
    const status = error.response?.status;
    if (status === 402) console.error('❌ ScrapingBee: закончились кредиты!');
    else if (status === 401) console.error('❌ ScrapingBee: невалидный ключ');
    else console.error(`❌ ScrapingBee: ${status || error.message}`);
    return { success: false, error: error.message };
  }
}

// Умный fetch — сначала прямой, потом ScrapingBee
async function fetchPage(url) {
  if (!useScrapingBee) {
    const direct = await fetchDirect(url);
    if (direct.success) return direct;

    if (direct.cloudflare) {
      console.log('🔄 Cloudflare обнаружен — переключаюсь на ScrapingBee для всех запросов');
      useScrapingBee = true;
    }
  }

  // Fallback на ScrapingBee
  return await fetchWithScrapingBee(url);
}

// ===== АВТОРИЗАЦИЯ НА ФОРУМЕ =====
async function loginToForum() {
  console.log('🔐 Попытка авторизации на форуме...');
  try {
    // XenForo login: нужен CSRF token
    // Шаг 1: Загружаем страницу логина
    const loginPageResult = await fetchPage(`${CONFIG.FORUM_BASE}/login/`);
    if (!loginPageResult.success) {
      console.error('❌ Не удалось загрузить страницу логина');
      return false;
    }

    const $ = cheerio.load(loginPageResult.html);
    const csrfToken = $('input[name="_xfToken"]').val() || '';

    if (!csrfToken) {
      console.log('⚠️ CSRF token не найден — возможно уже авторизованы или форум пускает без логина');
      // Проверяем: может контент уже доступен
      const testResult = await fetchPage('https://forum.majestic-rp.ru/threads/sekiro-0115.2883135/');
      if (testResult.success) {
        const $test = cheerio.load(testResult.html);
        if ($test('.message-body').length > 0) {
          console.log('✅ Контент доступен без авторизации!');
          return true;
        }
      }
      return false;
    }

    // Шаг 2: POST логин через ScrapingBee
    const loginResponse = await axios.get('https://app.scrapingbee.com/api/v1/', {
      params: {
        api_key: CONFIG.SCRAPINGBEE_API_KEY,
        url: CONFIG.FORUM_LOGIN_URL,
        render_js: 'true',
        premium_proxy: 'true',
        country_code: 'ru',
        wait: '5000',
        // JS instructions для заполнения формы
        js_scenario: JSON.stringify({
          instructions: [
            { wait: 2000 },
            { fill: ['input[name="login"]', CONFIG.FORUM_EMAIL] },
            { fill: ['input[name="password"]', CONFIG.FORUM_PASS] },
            { click: 'button.button--primary[type="submit"]' },
            { wait: 5000 }
          ]
        })
      },
      timeout: 120000
    });

    const $login = cheerio.load(loginResponse.data);
    // Проверяем успешность — ищем признаки залогиненного юзера
    const loggedIn = $login('.p-navgroup--member').length > 0 ||
                     $login('a[href*="account"]').length > 0 ||
                     loginResponse.data.includes('Cloud Kidala') ||
                     loginResponse.data.includes('cloudnaxyi');

    if (loggedIn) {
      console.log('✅ Авторизация успешна!');
      // Сохраняем cookies из ответа для последующих запросов
      const setCookies = loginResponse.headers['set-cookie'];
      if (setCookies) {
        forumCookies = setCookies.map(c => c.split(';')[0]).join('; ');
        console.log('🍪 Cookies сохранены');
      }
      return true;
    }

    console.log('⚠️ Авторизация не подтверждена — продолжаю без неё');
    return false;
  } catch (error) {
    console.error('❌ Ошибка авторизации:', error.message);
    return false;
  }
}

// ===== ПАРСИНГ =====

function parseThreadList(html, sectionStatus) {
  const $ = cheerio.load(html);
  const threads = [];

  $('.structItem').each((i, el) => {
    try {
      const titleEl = $(el).find('.structItem-title a').last();
      const title = titleEl.text().trim();
      const href = titleEl.attr('href');
      if (!title || !href) return;

      const threadIdMatch = href.match(/\.(\d+)\/?$/);
      const threadId = threadIdMatch ? threadIdMatch[1] : null;
      if (!threadId) return;

      const fullUrl = href.startsWith('http') ? href : `${CONFIG.FORUM_BASE}${href.startsWith('/') ? '' : '/'}${href}`;
      const author = $(el).find('.structItem-cell--main .username').text().trim();
      const dateStr = $(el).find('.structItem-startDate time, .structItem-cell--latest time').attr('datetime') || '';

      threads.push({ threadId, title, url: fullUrl, author, date: dateStr, status: sectionStatus });
    } catch (err) { /* skip */ }
  });

  return threads;
}

function parseComplaintPost(html) {
  const $ = cheerio.load(html);
  const result = {
    title: $('h1.p-title-value').text().trim() || $('title').text().trim(),
    author: '',
    authorNickname: '', // Игровой ник автора жалобы
    authorStaticId: '',
    targetNickname: '',
    targetStaticId: '',
    description: '',
    date: ''
  };

  // Автор первого поста
  result.author = $('.message-userDetails h4 a, .message-name a').first().text().trim();
  result.date = $('.message-attribution time').first().attr('datetime') || '';

  // Парсим структурированные поля жалобы из первого поста
  const firstPost = $('.message-body .bbWrapper').first();
  const postHtml = firstPost.html() || '';
  const postText = firstPost.text().trim();

  // Ищем поля формы жалобы (формат XenForo)
  // Ваш игровой никнейм: Cloud Kidala
  // Ваш статический ID #: 20485
  // Статический #ID нарушителя: 63167, 99072

  const fieldPatterns = {
    authorNickname: [
      /(?:ваш\s+)?игровой\s+ник(?:нейм)?\s*[:\-]?\s*(.+)/i,
      /your\s+(?:game\s+)?nickname\s*[:\-]?\s*(.+)/i
    ],
    authorStaticId: [
      /ваш\s+статический\s+(?:ID|#ID|id)\s*[#:]?\s*(\d[\d\s,]*)/i,
      /your\s+static\s*(?:ID|#)\s*[:\-]?\s*(\d[\d\s,]*)/i
    ],
    targetStaticId: [
      /статический\s+(?:#ID|ID|id)\s+нарушител[яь]\s*[:\-]?\s*(\d[\d\s,]*)/i,
      /(?:#ID|ID)\s+(?:нарушител[яь]|violator)\s*[:\-]?\s*(\d[\d\s,]*)/i,
      /нарушител[яь].*?(?:ID|id)\s*[#:]?\s*(\d[\d\s,]*)/i
    ],
    description: [
      /краткое\s+описание\s+ситуации\s*[:\-]?\s*(.+)/i,
      /описание\s*[:\-]?\s*(.+)/i
    ]
  };

  for (const [field, patterns] of Object.entries(fieldPatterns)) {
    for (const pattern of patterns) {
      const match = postText.match(pattern);
      if (match) {
        result[field] = match[1].trim();
        break;
      }
    }
  }

  // Fallback: ищем никнейм из заголовка
  if (!result.targetNickname) {
    const titlePatterns = [
      /жалоба\s+на\s+(.+?)(?:\s*[\[\|]|\s*$)/i,
      /^(.+?)(?:\s*[\[\|]|\s*-\s*\d)/i
    ];
    for (const pattern of titlePatterns) {
      const match = result.title.match(pattern);
      if (match) {
        result.targetNickname = match[1].trim().replace(/\s*\d+\s*$/, '').trim();
        break;
      }
    }
  }

  // Из targetStaticId берём первый ID (может быть "63167, 99072")
  if (result.targetStaticId) {
    const ids = result.targetStaticId.replace(/\s/g, '').split(',').filter(Boolean);
    result.targetStaticId = ids[0] || '';
    result.allTargetIds = ids;
  }

  return result;
}

// ===== ГЛАВНОЕ СКАНИРОВАНИЕ =====

async function scanForum() {
  if (isScanning) {
    console.log('⏳ Сканирование уже идёт, пропускаю');
    return scanStats;
  }
  isScanning = true;
  const stats = { total: 0, family: 0, saved: 0, errors: 0, method: useScrapingBee ? 'scrapingbee' : 'direct' };

  try {
    const linkedAccounts = await getLinkedAccounts();
    if (linkedAccounts.size === 0) {
      console.log('⚠️ Нет привязанных аккаунтов — пропуск');
      isScanning = false;
      stats.method = 'skipped';
      scanStats = { ...stats, lastScan: new Date().toISOString() };
      return stats;
    }
    console.log(`🔍 Сканирование... (${linkedAccounts.size / 2} аккаунтов)`);

    for (const [sectionKey, section] of Object.entries(CONFIG.SECTIONS)) {
      const pages = CONFIG.PAGES_PER_SECTION[sectionKey] || 1;

      for (let page = 1; page <= pages; page++) {
        const url = page === 1 ? section.url : `${section.url}page-${page}`;
        const result = await fetchPage(url);

        if (!result.success) {
          stats.errors++;
          console.error(`  ❌ ${section.name} стр.${page}: ${result.error || 'failed'}`);
          await delay(CONFIG.REQUEST_DELAY);
          continue;
        }

        const threads = parseThreadList(result.html, section.status);
        stats.total += threads.length;
        console.log(`  📋 ${section.name} стр.${page}: ${threads.length} тредов`);

        for (const thread of threads) {
          if (processedComplaints.has(thread.threadId)) continue;

          // Быстрая проверка: заголовок/автор содержит ник семьи?
          const titleLower = (thread.title + ' ' + thread.author).toLowerCase();
          let matchesFamily = false;
          for (const [key] of linkedAccounts) {
            if (typeof key === 'string' && titleLower.includes(key.toLowerCase())) {
              matchesFamily = true;
              break;
            }
          }

          if (!matchesFamily) {
            processedComplaints.add(thread.threadId);
            continue;
          }

          // Загружаем детали треда
          await delay(CONFIG.REQUEST_DELAY);
          const threadResult = await fetchPage(thread.url);

          if (!threadResult.success) {
            stats.errors++;
            continue;
          }

          const complaint = parseComplaintPost(threadResult.html);
          processedComplaints.add(thread.threadId);

          // Проверяем все target static IDs
          const allTargetIds = complaint.allTargetIds || (complaint.targetStaticId ? [complaint.targetStaticId] : []);

          // Входящие: кто-то жалуется на нашего участника
          for (const targetId of allTargetIds) {
            const match = linkedAccounts.get(targetId);
            if (match) {
              stats.family++;
              const saved = await saveComplaint({
                complaintId: thread.threadId,
                type: 'incoming',
                userId: match.userId,
                targetNickname: complaint.authorNickname || complaint.author || thread.author || 'Неизвестно',
                targetStaticId: targetId,
                subject: complaint.title || thread.title,
                status: thread.status,
                forumUrl: thread.url,
                createdAt: complaint.date ? new Date(complaint.date).getTime() : Date.now()
              });
              if (saved) stats.saved++;
              console.log(`    ⚠️ Входящая на ${match.nickname}: ${thread.title}`);
            }
          }

          // По нику тоже
          const nickMatch = complaint.targetNickname ? linkedAccounts.get(complaint.targetNickname.toLowerCase()) : null;
          if (nickMatch && !allTargetIds.some(id => linkedAccounts.has(id))) {
            stats.family++;
            const saved = await saveComplaint({
              complaintId: thread.threadId,
              type: 'incoming',
              userId: nickMatch.userId,
              targetNickname: complaint.authorNickname || complaint.author || 'Неизвестно',
              targetStaticId: complaint.targetStaticId || '0',
              subject: complaint.title || thread.title,
              status: thread.status,
              forumUrl: thread.url,
              createdAt: complaint.date ? new Date(complaint.date).getTime() : Date.now()
            });
            if (saved) stats.saved++;
            console.log(`    ⚠️ Входящая (по нику) на ${nickMatch.nickname}: ${thread.title}`);
          }

          // Исходящие: наш участник подал жалобу
          const authorMatch = complaint.authorNickname
            ? linkedAccounts.get(complaint.authorNickname.toLowerCase())
            : (complaint.author ? linkedAccounts.get(complaint.author.toLowerCase()) : null);

          if (authorMatch) {
            stats.family++;
            const saved = await saveComplaint({
              complaintId: thread.threadId,
              type: 'outgoing',
              userId: authorMatch.userId,
              targetNickname: complaint.targetNickname || 'Неизвестно',
              targetStaticId: complaint.targetStaticId || '0',
              subject: complaint.title || thread.title,
              status: thread.status,
              forumUrl: thread.url,
              createdAt: complaint.date ? new Date(complaint.date).getTime() : Date.now()
            });
            if (saved) stats.saved++;
            console.log(`    📤 Исходящая от ${authorMatch.nickname}: ${thread.title}`);
          }
        }

        await delay(CONFIG.REQUEST_DELAY);
      }
    }
  } catch (error) {
    console.error('❌ Ошибка сканирования:', error.message);
    stats.errors++;
  } finally {
    isScanning = false;
  }

  stats.method = useScrapingBee ? 'scrapingbee' : 'direct';
  scanStats = { ...stats, lastScan: new Date().toISOString() };
  console.log(`✅ Итого: ${stats.total} тредов, ${stats.family} семья, ${stats.saved} сохранено, ${stats.errors} ошибок (${stats.method})`);
  return stats;
}

function delay(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// ===== EXPRESS СЕРВЕР (для мониторинга + Railway health check) =====
const app = express();

app.get('/', (req, res) => {
  res.json({
    service: 'Kidala Forum Scanner',
    status: 'running',
    scanning: isScanning,
    lastScan: scanStats,
    uptime: Math.round(process.uptime()) + 's'
  });
});

app.get('/scan', async (req, res) => {
  if (isScanning) {
    return res.json({ status: 'already_scanning', lastScan: scanStats });
  }
  const result = await scanForum();
  res.json({ status: 'done', ...result });
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', uptime: Math.round(process.uptime()) });
});

// ===== ЗАПУСК =====
async function start() {
  console.log('🚀 Kidala Forum Scanner запускается...');

  try {
    await initDB();
  } catch (error) {
    console.error('❌ Не удалось подключиться к MySQL:', error.message);
    process.exit(1);
  }

  // Запускаем HTTP сервер (Railway требует)
  app.listen(CONFIG.PORT, () => {
    console.log(`🌐 HTTP сервер на порту ${CONFIG.PORT}`);
  });

  // Пробуем авторизоваться
  await loginToForum();

  // Первое сканирование через 10 сек
  setTimeout(async () => {
    console.log('🔄 Первое сканирование...');
    await scanForum();
  }, 10000);

  // Автосканирование
  setInterval(async () => {
    console.log('🔄 Автосканирование...');
    await scanForum();
  }, CONFIG.SCAN_INTERVAL);

  console.log(`⏰ Автосканирование каждые ${CONFIG.SCAN_INTERVAL / 60000} мин`);
}

start().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
