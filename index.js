const express = require('express');
const axios = require('axios');
const cheerio = require('cheerio');
const mysql = require('mysql2/promise');

const CONFIG = {
  DB: { host:'mysql-eu5.skailarhost.com', port:3306, user:'u626_HQJqMYObkl', password:'jP8mV6Swf6KRmZM.467NM=.h', database:'s626_KidalaDB', waitForConnections:true, connectionLimit:5, queueLimit:0 },
  SCRAPINGBEE_KEY: process.env.SCRAPINGBEE_KEY || 'TBJEE0HHIX3BR113920RW1X4W0P2U0HE6337KQFPB2WY24F7M44GWNPFUBU98Y4OL1LMR5B0DL4WIAPL',
  FORUM_BASE: 'https://forum.majestic-rp.ru',
  SECTIONS: {
    active: { url:'https://forum.majestic-rp.ru/forums/zhaloby-na-igrokov.1380/', status:'in_progress', name:'Активные жалобы' },
    approved: { url:'https://forum.majestic-rp.ru/forums/rassmotrennyye-zhaloby.1381/', status:'approved', name:'Рассмотренные жалобы' },
    rejected: { url:'https://forum.majestic-rp.ru/forums/otklonennyye-zhaloby.1382/', status:'rejected', name:'Отклонённые жалобы' }
  },
  PAGES: { active:3, approved:2, rejected:2 },
  SCAN_INTERVAL: 300000, REQUEST_DELAY: 3000,
  PORT: parseInt(process.env.PORT || '3000')
};

let dbPool=null, scanStats={lastScan:null,total:0,family:0,saved:0,errors:0,method:'none'}, isScanning=false, fetchMethod='unknown';
const processedComplaints = new Set();

// ===== DB =====
async function initDB() {
  dbPool = mysql.createPool(CONFIG.DB);
  const c = await dbPool.getConnection(); console.log('✅ MySQL подключён'); c.release();
  await dbPool.execute(`CREATE TABLE IF NOT EXISTS linked_forum_accounts (id INT AUTO_INCREMENT PRIMARY KEY, user_id VARCHAR(20) NOT NULL UNIQUE, nickname VARCHAR(100) NOT NULL, static_id VARCHAR(20) NOT NULL, linked_at BIGINT NOT NULL, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)`);
  await dbPool.execute(`CREATE TABLE IF NOT EXISTS forum_complaints (id INT AUTO_INCREMENT PRIMARY KEY, complaint_id VARCHAR(20) NOT NULL, type ENUM('incoming','outgoing') NOT NULL, user_id VARCHAR(20) NOT NULL, target_nickname VARCHAR(100) NOT NULL, target_static_id VARCHAR(20) NOT NULL, subject VARCHAR(255) NOT NULL, status ENUM('waiting','in_progress','approved','rejected') DEFAULT 'waiting', forum_url VARCHAR(500), created_at BIGINT NOT NULL, closed_at BIGINT DEFAULT NULL, resolution VARCHAR(255) DEFAULT NULL, UNIQUE KEY unique_complaint (complaint_id,user_id,type))`);
}

async function getLinkedAccounts() {
  const [rows] = await dbPool.execute('SELECT * FROM linked_forum_accounts');
  const m = new Map();
  for (const r of rows) { m.set(r.static_id, {userId:r.user_id,nickname:r.nickname}); m.set(r.nickname.toLowerCase(), {userId:r.user_id,nickname:r.nickname}); }
  return m;
}

async function saveComplaint(d) {
  try {
    await dbPool.execute(`INSERT INTO forum_complaints (complaint_id,type,user_id,target_nickname,target_static_id,subject,status,forum_url,created_at) VALUES (?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE status=VALUES(status),subject=VALUES(subject)`,
      [d.complaintId,d.type,d.userId,d.targetNickname,d.targetStaticId,d.subject,d.status||'waiting',d.forumUrl||null,d.createdAt||Date.now()]);
    return true;
  } catch(e) { console.error('❌ DB:', e.message); return false; }
}

// ===== FETCH =====
async function fetchDirect(url) {
  try {
    const r = await axios.get(url, { timeout:30000, headers:{'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36','Accept':'text/html,application/xhtml+xml','Accept-Language':'ru-RU,ru;q=0.9'}, maxRedirects:5, validateStatus:s=>s<500 });
    return { ok:true, html:typeof r.data==='string'?r.data:String(r.data), status:r.status };
  } catch(e) { return { ok:false, err:e.message }; }
}

async function fetchSB(url, stealth=false) {
  try {
    const params = { api_key:CONFIG.SCRAPINGBEE_KEY, url, render_js:'true', country_code:'ru', wait:'8000', block_ads:'true' };
    if (stealth) params.stealth_proxy = 'true';
    else params.premium_proxy = 'true';
    const r = await axios.get('https://app.scrapingbee.com/api/v1/', { params, timeout:120000 });
    return { ok:true, html:typeof r.data==='string'?r.data:String(r.data) };
  } catch(e) { return { ok:false, err:`${e.response?.status||''} ${e.message}`.trim() }; }
}

function isReal(html) {
  if (!html || html.length < 2000) return false;
  if (html.includes('Just a moment') || html.includes('Checking your browser') || html.includes('Please turn JavaScript on')) return false;
  return html.includes('structItem') || html.includes('message-body') || html.includes('p-title') || html.includes('p-body') || html.includes('XenForo');
}

async function fetchPage(url) {
  if (fetchMethod==='direct'||fetchMethod==='unknown') { const r=await fetchDirect(url); if(r.ok&&isReal(r.html)) return {success:true,html:r.html}; }
  if (fetchMethod==='sb_stealth'||fetchMethod==='unknown') { const r=await fetchSB(url,true); if(r.ok&&isReal(r.html)) return {success:true,html:r.html}; }
  if (fetchMethod==='sb_premium'||fetchMethod==='unknown') { const r=await fetchSB(url,false); if(r.ok&&isReal(r.html)) return {success:true,html:r.html}; }
  return {success:false};
}

// ===== DIAGNOSTICS =====
async function runDiag() {
  console.log('\n🔬 === ДИАГНОСТИКА ===');
  const testUrl = CONFIG.SECTIONS.active.url;

  console.log('1️⃣ Прямой запрос...');
  const d = await fetchDirect(testUrl);
  if (d.ok) {
    const real = isReal(d.html);
    console.log(`   HTTP ${d.status}, ${d.html.length} bytes, реальный: ${real}`);
    if (real) { const $=cheerio.load(d.html); console.log(`   Тредов: ${$('.structItem').length}`); fetchMethod='direct'; console.log('   ✅ ПРЯМОЙ ДОСТУП РАБОТАЕТ!\n'); return; }
    else console.log(`   Начало: ${d.html.substring(0,120).replace(/\n/g,' ')}`);
  } else console.log(`   Ошибка: ${d.err}`);

  console.log('2️⃣ ScrapingBee Stealth...');
  const s = await fetchSB(testUrl, true);
  if (s.ok) {
    const real = isReal(s.html);
    console.log(`   ${s.html.length} bytes, реальный: ${real}`);
    if (real) { const $=cheerio.load(s.html); console.log(`   Тредов: ${$('.structItem').length}`); fetchMethod='sb_stealth'; console.log('   ✅ STEALTH РАБОТАЕТ!\n'); return; }
    else console.log(`   Начало: ${s.html.substring(0,120).replace(/\n/g,' ')}`);
  } else console.log(`   Ошибка: ${s.err}`);

  console.log('3️⃣ ScrapingBee Premium...');
  const p = await fetchSB(testUrl, false);
  if (p.ok) {
    const real = isReal(p.html);
    console.log(`   ${p.html.length} bytes, реальный: ${real}`);
    if (real) { const $=cheerio.load(p.html); console.log(`   Тредов: ${$('.structItem').length}`); fetchMethod='sb_premium'; console.log('   ✅ PREMIUM РАБОТАЕТ!\n'); return; }
    else console.log(`   Начало: ${p.html.substring(0,120).replace(/\n/g,' ')}`);
  } else console.log(`   Ошибка: ${p.err}`);

  console.log('❌ Ни один метод не сработал\n');
}

// ===== PARSE =====
function parseThreads(html, status) {
  const $=cheerio.load(html), threads=[];
  $('.structItem').each((i,el)=>{
    const a=$(el).find('.structItem-title a').last(), title=a.text().trim(), href=a.attr('href');
    if(!title||!href) return;
    const m=href.match(/\.(\d+)\/?$/); if(!m) return;
    threads.push({ threadId:m[1], title, url:href.startsWith('http')?href:`${CONFIG.FORUM_BASE}${href.startsWith('/')?'':'/'}${href}`, author:$(el).find('.username').first().text().trim(), date:$(el).find('time').first().attr('datetime')||'', status });
  });
  return threads;
}

function parsePost(html) {
  const $=cheerio.load(html), txt=$('.message-body .bbWrapper').first().text().trim();
  const r = { title:$('h1.p-title-value').text().trim(), author:$('.message-name a, .message-userDetails h4 a').first().text().trim(), authorNick:'', targetNick:'', targetId:'', allIds:[], date:$('.message-attribution time').first().attr('datetime')||'' };
  let m;
  if (m=txt.match(/игровой\s+ник(?:нейм)?\s*[:\-]?\s*(.+)/i)) r.authorNick=m[1].trim();
  if (m=txt.match(/статический\s+(?:#?ID)\s+нарушител[яь]\s*[:\-]?\s*(\d[\d\s,]*)/i)) { r.targetId=m[1].trim(); r.allIds=r.targetId.replace(/\s/g,'').split(',').filter(Boolean); r.targetId=r.allIds[0]||''; }
  else if (m=txt.match(/нарушител[яь].*?(?:ID|id)\s*[#:]?\s*(\d[\d\s,]*)/i)) { r.targetId=m[1].trim(); r.allIds=r.targetId.replace(/\s/g,'').split(',').filter(Boolean); r.targetId=r.allIds[0]||''; }
  if (m=r.title.match(/жалоба\s+на\s+(.+?)(?:\s*[\[\|]|\s*$)/i)) r.targetNick=m[1].trim();
  return r;
}

// ===== SCAN =====
async function scan() {
  if (isScanning||fetchMethod==='unknown') return scanStats;
  isScanning=true;
  const st={total:0,family:0,saved:0,errors:0,method:fetchMethod};
  try {
    const linked=await getLinkedAccounts();
    if (!linked.size) { console.log('⚠️ Нет привязанных'); isScanning=false; return st; }
    console.log(`🔍 Скан (${linked.size/2} акк, ${fetchMethod})`);
    for (const [sk,sec] of Object.entries(CONFIG.SECTIONS)) {
      for (let p=1;p<=(CONFIG.PAGES[sk]||1);p++) {
        const url=p===1?sec.url:`${sec.url}page-${p}`;
        const r=await fetchPage(url);
        if(!r.success){st.errors++;await delay(CONFIG.REQUEST_DELAY);continue;}
        const threads=parseThreads(r.html,sec.status);
        st.total+=threads.length;
        console.log(`  📋 ${sec.name} [${p}]: ${threads.length}`);
        for (const t of threads) {
          if(processedComplaints.has(t.threadId)) continue;
          const low=(t.title+' '+t.author).toLowerCase();
          let hit=false; for(const[k]of linked){if(typeof k==='string'&&low.includes(k.toLowerCase())){hit=true;break;}}
          if(!hit){processedComplaints.add(t.threadId);continue;}
          await delay(CONFIG.REQUEST_DELAY);
          const tr=await fetchPage(t.url); if(!tr.success){st.errors++;continue;}
          const c=parsePost(tr.html); processedComplaints.add(t.threadId);
          const ids=c.allIds.length?c.allIds:(c.targetId?[c.targetId]:[]);
          for(const id of ids){const m=linked.get(id);if(m){st.family++;if(await saveComplaint({complaintId:t.threadId,type:'incoming',userId:m.userId,targetNickname:c.authorNick||c.author||'?',targetStaticId:id,subject:c.title||t.title,status:t.status,forumUrl:t.url,createdAt:c.date?new Date(c.date).getTime():Date.now()}))st.saved++;console.log(`    ⚠️ На ${m.nickname}: ${t.title}`);}}
          const nm=c.targetNick?linked.get(c.targetNick.toLowerCase()):null;
          if(nm&&!ids.some(id=>linked.has(id))){st.family++;if(await saveComplaint({complaintId:t.threadId,type:'incoming',userId:nm.userId,targetNickname:c.authorNick||c.author||'?',targetStaticId:c.targetId||'0',subject:c.title||t.title,status:t.status,forumUrl:t.url,createdAt:c.date?new Date(c.date).getTime():Date.now()}))st.saved++;}
          const am=c.authorNick?linked.get(c.authorNick.toLowerCase()):(c.author?linked.get(c.author.toLowerCase()):null);
          if(am){st.family++;if(await saveComplaint({complaintId:t.threadId,type:'outgoing',userId:am.userId,targetNickname:c.targetNick||'?',targetStaticId:c.targetId||'0',subject:c.title||t.title,status:t.status,forumUrl:t.url,createdAt:c.date?new Date(c.date).getTime():Date.now()}))st.saved++;}
        }
        await delay(CONFIG.REQUEST_DELAY);
      }
    }
  } catch(e){console.error('❌',e.message);st.errors++;}
  finally{isScanning=false;}
  scanStats={...st,lastScan:new Date().toISOString()};
  console.log(`✅ ${st.total} тредов, ${st.family} семья, ${st.saved} новых, ${st.errors} ошибок`);
  return st;
}

function delay(ms){return new Promise(r=>setTimeout(r,ms));}

// ===== HTTP =====
const app=express();
app.get('/',(q,r)=>r.json({service:'Kidala Forum Scanner v2',fetchMethod,scanning:isScanning,lastScan:scanStats,uptime:Math.round(process.uptime())+'s'}));
app.get('/scan',async(q,r)=>{if(isScanning)return r.json({status:'busy'});r.json(await scan());});
app.get('/diag',async(q,r)=>{fetchMethod='unknown';await runDiag();r.json({method:fetchMethod});});
app.get('/health',(q,r)=>r.json({ok:true}));

// ===== START =====
(async()=>{
  console.log('🚀 Kidala Forum Scanner v2');
  await initDB();
  app.listen(CONFIG.PORT,()=>console.log(`🌐 :${CONFIG.PORT}`));
  await runDiag();
  if(fetchMethod!=='unknown'){
    setTimeout(()=>scan(),5000);
    setInterval(()=>scan(),CONFIG.SCAN_INTERVAL);
    console.log(`⏰ Авто каждые ${CONFIG.SCAN_INTERVAL/60000} мин`);
  } else {
    console.log('⚠️ Методы не работают — ретрай каждые 5 мин');
    setInterval(async()=>{fetchMethod='unknown';await runDiag();if(fetchMethod!=='unknown')await scan();},CONFIG.SCAN_INTERVAL);
  }
})().catch(e=>{console.error('FATAL:',e);process.exit(1);});
