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
const TOKEN        = process.env.TELEGRAM_BOT_TOKEN || "";
const API          = (process.env.VPN_API_BASE_URL  || "").replace(/\/+$/, "");
const APP_SECRET   = process.env.APP_SECRET         || "";
const ADMIN_ID     = Number(process.env.ADMIN_TELEGRAM_ID || 0);
const DB_FILE      = process.env.SQLITE_PATH        || path.join(__dirname, "bot.db");
const NEWS_URL     = process.env.BOT_NEWS_URL       || "";
const SUPPORT_URL  = process.env.BOT_SUPPORT_URL    || "";
const FREE_PROXY   = process.env.BOT_FREE_PROXY_URL || "";
const BOT_USERNAME = process.env.BOT_USERNAME       || "";

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
// Helpers
// ─────────────────────────────────────────────────────────────────────────────
const now     = () => Date.now();
const esc     = (s) => String(s || "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
const rub     = (n) => `${Number(n||0).toLocaleString("ru-RU")} ₽`;
const dt      = (ts) => ts ? new Date(ts).toLocaleDateString("ru-RU")  : "—";
const dts     = (ts) => ts ? new Date(ts).toLocaleString("ru-RU")      : "—";
const isAdmin = (id) => Number(id) === ADMIN_ID;
const refLink = (code) => BOT_USERNAME
  ? `https://t.me/${BOT_USERNAME}?start=partner_${code}`
  : `https://t.me/?start=partner_${code}`;

// ─────────────────────────────────────────────────────────────────────────────
// Settings
// ─────────────────────────────────────────────────────────────────────────────
function setting(k, f = "") { return db.prepare("SELECT value v FROM settings WHERE key=?").get(k)?.v ?? f; }
function setSetting(k, v)   { db.prepare("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").run(k, String(v ?? "")); }

// ─────────────────────────────────────────────────────────────────────────────
// DB helpers
// ─────────────────────────────────────────────────────────────────────────────
function user(id)     { return db.prepare("SELECT * FROM users WHERE tg_id=?").get(Number(id)); }
function sub(id)      { return db.prepare("SELECT * FROM subscriptions WHERE tg_id=?").get(Number(id)); }
function activeSub(s) { return !!(s && s.is_active===1 && s.expires_at>now() && s.sub_url); }
function tariffs()    { return db.prepare("SELECT * FROM tariffs ORDER BY sort_order").all(); }
function tariff(c)    { return db.prepare("SELECT * FROM tariffs WHERE code=?").get(c); }

function updateBalance(uid, delta) {
  const u = user(uid); if (!u) throw new Error("NO_USER");
  const n = Number(u.balance_rub) + Number(delta);
  if (n < 0) throw new Error("NO_MONEY");
  db.prepare("UPDATE users SET balance_rub=?,updated_at=? WHERE tg_id=?").run(n, now(), Number(uid));
  return n;
}
function updateRefBalance(uid, delta) {
  const u = user(uid); if (!u) throw new Error("NO_USER");
  const n = Number(u.ref_balance_rub||0) + Number(delta);
  if (n < 0) throw new Error("NO_MONEY");
  db.prepare("UPDATE users SET ref_balance_rub=?,updated_at=? WHERE tg_id=?").run(n, now(), Number(uid));
  return n;
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
  updateRefBalance(r.tg_id, reward);
  db.prepare("UPDATE users SET ref_earned=ref_earned+?,updated_at=? WHERE tg_id=?").run(reward,now(),Number(r.tg_id));
  db.prepare("INSERT INTO referrals(referrer_tg_id,invited_tg_id,amount_rub,percent,reward_rub,created_at) VALUES(?,?,?,?,?,?)").run(Number(r.tg_id),Number(buyerId),Number(amount),pct,reward,now());
  tg("sendMessage",{chat_id:r.tg_id,text:`💰 <b>Начислено вознаграждение!</b>\n\nВаш реферал совершил покупку.\n+<b>${rub(reward)}</b> на реферальный баланс.`,parse_mode:"HTML"}).catch(()=>{});
}

// withdrawal requests
function getWithdrawal(id)          { return db.prepare("SELECT * FROM withdrawal_requests WHERE id=?").get(Number(id)); }
function userPendingWithdrawal(uid) { return db.prepare("SELECT * FROM withdrawal_requests WHERE tg_id=? AND status='pending'").get(Number(uid)); }
function pendingWithdrawals()       { return db.prepare("SELECT wr.*,u.first_name,u.username FROM withdrawal_requests wr JOIN users u ON wr.tg_id=u.tg_id WHERE wr.status='pending' ORDER BY wr.created_at ASC").all(); }
function withdrawalHistory(lim=20)  { return db.prepare("SELECT wr.*,u.first_name,u.username FROM withdrawal_requests wr JOIN users u ON wr.tg_id=u.tg_id ORDER BY wr.updated_at DESC LIMIT ?").all(lim); }

// states
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
  `);

  // Migrations — idempotent
  for (const m of ["ALTER TABLE users ADD COLUMN ref_balance_rub INTEGER NOT NULL DEFAULT 0"]) {
    try { db.exec(m); } catch {}
  }

  // Seed tariffs
  const st = db.prepare("INSERT INTO tariffs(code,title,duration_days,price_rub,sort_order) VALUES(?,?,?,?,?) ON CONFLICT(code) DO NOTHING");
  [["m1","1 месяц",30,100,1],["m6","6 месяцев",180,600,2],["y1","1 год",365,900,3]].forEach(r=>st.run(...r));

  // Seed settings
  const ss = db.prepare("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO NOTHING");
  [["payment_methods",""],["gif_main_menu",""],["gif_purchase_success",""],["gif_gift_success",""],["gif_broadcast",""],["ref_percent","30"],["ref_withdraw_min","500"]].forEach(([k,v])=>ss.run(k,v));
}

// ─────────────────────────────────────────────────────────────────────────────
// Telegram API
// ─────────────────────────────────────────────────────────────────────────────
async function tg(method, params) {
  const r = await fetch(`${TG_BASE}/${method}`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(params)});
  const j = await r.json().catch(()=>({}));
  if(!r.ok||j.ok===false) throw new Error(j.description||`TG HTTP ${r.status}`);
  return j.result;
}

// FIX: send local file via multipart/form-data (JSON.stringify can't handle file streams)
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

async function gif(chatId, key) {
  const g = setting(key,""); if(g) await tg("sendAnimation",{chat_id:chatId,animation:g}).catch(()=>{});
}

async function editOrSend(chatId, msgId, text, kb) {
  try {
    if(msgId){await tg("editMessageText",{chat_id:chatId,message_id:msgId,text,parse_mode:"HTML",disable_web_page_preview:true,reply_markup:kb});return Number(msgId);}
  } catch(e){ if(String(e.message).includes("message is not modified")) return Number(msgId); }
  const m = await tg("sendMessage",{chat_id:chatId,text,parse_mode:"HTML",disable_web_page_preview:true,reply_markup:kb});
  return Number(m.message_id);
}

function restartBot() {
  try{db.close();}catch{}
  const child=spawn(process.execPath,[path.join(__dirname,"bot.js")],{cwd:__dirname,detached:true,stdio:"ignore",env:process.env});
  child.unref(); process.exit(0);
}

async function exportDbToAdmin(chatId) {
  await tgSendFile("sendDocument", chatId, "document", DB_FILE, {caption:"📦 База данных бота"});
}

async function importDbFromDocument(fileId) {
  const f = await tg("getFile",{file_id:fileId});
  if(!f?.file_path) throw new Error("file_path not found");
  const resp = await fetch(`https://api.telegram.org/file/bot${TOKEN}/${f.file_path}`);
  if(!resp.ok) throw new Error(`Скачивание: HTTP ${resp.status}`);
  const tmp = `${DB_FILE}.import.tmp`;
  await fsp.writeFile(tmp, Buffer.from(await resp.arrayBuffer()));
  try{db.close();}catch{}
  await fsp.copyFile(DB_FILE,`${DB_FILE}.backup.${Date.now()}`);
  await fsp.rename(tmp, DB_FILE);
}

// ─────────────────────────────────────────────────────────────────────────────
// User helpers
// ─────────────────────────────────────────────────────────────────────────────
function upsertUser(from, chatId) {
  const cur = user(from.id);
  const ref = cur?.ref_code || crypto.randomBytes(5).toString("hex");
  db.prepare(`INSERT INTO users(tg_id,username,first_name,balance_rub,ref_balance_rub,referred_by,ref_code,ref_earned,payout_method,payout_details,last_chat_id,created_at,updated_at)
    VALUES(@id,@u,@f,0,0,NULL,@r,0,'','',@c,@t,@t)
    ON CONFLICT(tg_id) DO UPDATE SET username=excluded.username,first_name=excluded.first_name,last_chat_id=excluded.last_chat_id,updated_at=excluded.updated_at`)
    .run({id:Number(from.id),u:from.username||"",f:from.first_name||"",r:ref,c:Number(chatId),t:now()});
}
function setMenu(uid,chatId,mid){ db.prepare("UPDATE users SET last_chat_id=?,last_menu_id=?,updated_at=? WHERE tg_id=?").run(Number(chatId),Number(mid),now(),Number(uid)); }
function findRef(code)          { return db.prepare("SELECT * FROM users WHERE ref_code=?").get(String(code||"").trim()); }
function setRef(uid,rid)        { const u=user(uid); if(!u||u.referred_by||Number(uid)===Number(rid)) return; db.prepare("UPDATE users SET referred_by=?,updated_at=? WHERE tg_id=?").run(Number(rid),now(),Number(uid)); }

// ─────────────────────────────────────────────────────────────────────────────
// Purchase logic
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
  const payer=user(payerId),receiver=user(receiverId),tr=tariff(code);
  if(!payer||!receiver||!tr) throw new Error("INVALID");
  const s=sub(receiverId), act=activeSub(s);
  if(kind==="new"  &&act)  throw new Error("ACTIVE");
  if(kind==="renew"&&!act) throw new Error("NO_ACTIVE");
  if(Number(payer.balance_rub)<Number(tr.price_rub)) throw new Error("NO_MONEY");
  const api = await createSubViaApi(receiver,tr,kind==="gift");
  db.transaction(()=>{
    updateBalance(payerId,-Number(tr.price_rub));
    if(payerId===receiverId) addReferralReward(receiverId,tr.price_rub);
    db.prepare("INSERT INTO subscriptions(tg_id,plan_code,plan_title,sub_url,expires_at,is_active,created_at,updated_at) VALUES(?,?,?,?,?,1,?,?) ON CONFLICT(tg_id) DO UPDATE SET plan_code=excluded.plan_code,plan_title=excluded.plan_title,sub_url=excluded.sub_url,expires_at=excluded.expires_at,is_active=1,updated_at=excluded.updated_at")
      .run(Number(receiverId),tr.code,tr.title,api.subscriptionUrl,Number(api.subscription?.expiresAt||0),now(),now());
    db.prepare("INSERT INTO purchases(tg_id,tariff_code,tariff_title,amount_rub,kind,created_at) VALUES(?,?,?,?,?,?)").run(Number(payerId),tr.code,tr.title,Number(tr.price_rub),kind,now());
    if(kind==="gift") db.prepare("INSERT INTO gifts(from_tg_id,to_tg_id,tariff_code,tariff_title,amount_rub,created_at) VALUES(?,?,?,?,?,?)").run(Number(payerId),Number(receiverId),tr.code,tr.title,Number(tr.price_rub),now());
  })();
  return {tr, url:api.subscriptionUrl, exp:Number(api.subscription?.expiresAt||0)};
}

async function buySelf(uid, chatId, msgId, code, mode, cbid) {
  try {
    const res=await doPurchase(uid,uid,code,mode);
    await gif(chatId,"gif_purchase_success");
    const me=user(uid);
    const text=["✅ <b>Оплата прошла успешно!</b>","",`📦 Тариф:    <b>${esc(res.tr.title)}</b>`,`💸 Списано:   <b>${rub(res.tr.price_rub)}</b>`,`💵 Баланс:    <b>${rub(me.balance_rub)}</b>`,`📅 Истекает:  <b>${dt(res.exp)}</b>`,"",`🔗 <code>${esc(res.url)}</code>`].join("\n");
    const kb={inline_keyboard:[[{text:"📲 Установить подписку",url:res.url}],[{text:"🔐 Моя подписка",callback_data:"v:sub"},{text:"👤 Главная",callback_data:"v:home"}]]};
    const nm=await editOrSend(chatId,msgId,text,kb); setMenu(uid,chatId,nm);
    await tg("answerCallbackQuery",{callback_query_id:cbid,text:"Готово ✅"});
  } catch(e) {
    const msg=e.message==="ACTIVE"?"Подписка уже активна. Выберите «Продлить».":e.message==="NO_ACTIVE"?"Нет активной подписки для продления.":e.message==="NO_MONEY"?"Недостаточно средств на балансе.":"Ошибка при оплате. Попробуйте позже.";
    await tg("answerCallbackQuery",{callback_query_id:cbid,text:msg,show_alert:true});
    if(e.message==="NO_MONEY") await render(uid,chatId,msgId,"bal");
  }
}

async function askBuyConfirm(uid, chatId, msgId, code, mode, cbid) {
  const tr=tariff(code);
  if(!tr){await tg("answerCallbackQuery",{callback_query_id:cbid,text:"Тариф не найден",show_alert:true});return;}
  const u=user(uid), diff=Number(u.balance_rub)-Number(tr.price_rub);
  const text=[`🧾 <b>${mode==="renew"?"Продление подписки":"Покупка подписки"}</b>`,"",`📦 Тариф:     <b>${esc(tr.title)}</b>`,`💸 Стоимость:  <b>${rub(tr.price_rub)}</b>`,`💵 Баланс:     <b>${rub(u.balance_rub)}</b>`,`📊 После:      <b>${rub(Math.max(0,diff))}</b>`,"",diff<0?"⚠️ Недостаточно средств. Пополните баланс.":"Подтвердите оплату ↓"].join("\n");
  const kb=diff<0?{inline_keyboard:[[{text:"💳 Способы оплаты",callback_data:"v:pay"}],[{text:"⬅️ Назад",callback_data:"v:home"}]]}:{inline_keyboard:[[{text:"✅ Подтвердить оплату",callback_data:`pay:c:${mode}:${code}`}],[{text:"⬅️ Отмена",callback_data:"v:home"}]]};
  const nm=await editOrSend(chatId,msgId,text,kb); setMenu(uid,chatId,nm);
  await tg("answerCallbackQuery",{callback_query_id:cbid});
}

async function giftToUser(fromId, toId, code, chatId, msgId, cbid) {
  try {
    const res=await doPurchase(fromId,toId,code,"gift");
    await gif(chatId,"gif_gift_success");
    const to=user(toId), me=user(fromId);
    const text=["🎁 <b>Подарок отправлен!</b>","",`👤 Получатель: <b>${esc(to?.first_name||to?.username||String(toId))}</b>`,`📦 Тариф:      <b>${esc(res.tr.title)}</b>`,`💸 Списано:    <b>${rub(res.tr.price_rub)}</b>`,`💵 Баланс:     <b>${rub(me.balance_rub)}</b>`].join("\n");
    const nm=await editOrSend(chatId,msgId,text,{inline_keyboard:[[{text:"🎁 Ещё подарок",callback_data:"v:gift"},{text:"👤 Главная",callback_data:"v:home"}]]});
    setMenu(fromId,chatId,nm);
    if(to) tg("sendMessage",{chat_id:to.tg_id,text:`🎁 <b>Вам подарили подписку!</b>\n\n📦 Тариф: <b>${esc(res.tr.title)}</b>\n📅 Истекает: <b>${dt(res.exp)}</b>\n\n🔗 <code>${esc(res.url)}</code>`,parse_mode:"HTML",reply_markup:{inline_keyboard:[[{text:"📲 Установить",url:res.url}]]}}).catch(()=>{});
    if(cbid) await tg("answerCallbackQuery",{callback_query_id:cbid,text:"Подарок отправлен 🎁"});
  } catch(e) {
    const msg=e.message==="NO_MONEY"?"Недостаточно средств на балансе.":e.message==="ACTIVE"?"У получателя уже активна подписка.":"Ошибка отправки. Попробуйте позже.";
    if(cbid) await tg("answerCallbackQuery",{callback_query_id:cbid,text:msg,show_alert:true});
    else tg("sendMessage",{chat_id:chatId,text:`❌ ${msg}`,parse_mode:"HTML"}).catch(()=>{});
    await render(fromId,chatId,msgId||null,"home");
  }
}

async function requestGiftRecipient(uid, chatId, code) {
  setUserState(uid,"gift_pick",code);
  await tg("sendMessage",{chat_id:chatId,text:"Выберите пользователя.",reply_markup:{keyboard:[[{text:"Выбрать пользователя",request_user:{request_id:1,user_is_bot:false}}],[{text:"Отмена"}]],resize_keyboard:true,one_time_keyboard:true}});
}

// ─────────────────────────────────────────────────────────────────────────────
// Text builders
// ─────────────────────────────────────────────────────────────────────────────
function homeText(u) {
  const s=sub(u.tg_id), hasSub=activeSub(s);
  const lines=[`👋 Привет, <b>${esc(u.first_name||"друг")}</b>!`,"",`🆔 ID: <code>${u.tg_id}</code>`,`💵 Баланс: <b>${rub(u.balance_rub)}</b>`];
  if(hasSub){const dd=Math.floor(Math.max(0,s.expires_at-now())/86400000);lines.push(`🔐 Подписка: активна ещё <b>${dd} дн.</b>`);}
  lines.push("");
  if(NEWS_URL)   lines.push(`📣 Канал — ${esc(NEWS_URL)}`);
  if(SUPPORT_URL)lines.push(`👤 Поддержка — ${esc(SUPPORT_URL)}`);
  if(FREE_PROXY) lines.push(`🆓 Прокси — ${esc(FREE_PROXY)}`);
  return lines.join("\n");
}

function subText(uid) {
  const s=sub(uid);
  if(!activeSub(s)) return "🔑 <b>Моя подписка</b>\n\nАктивная подписка не найдена.\nОформите тариф в разделе «Купить подписку».";
  const ms=Math.max(0,s.expires_at-now()), dd=Math.floor(ms/86400000), hh=Math.floor((ms%86400000)/3600000), mm=Math.floor((ms%3600000)/60000);
  return ["🔑 <b>Моя подписка</b>","",`📦 Тариф: <b>${esc(s.plan_title||s.plan_code||"—")}</b>`,`📅 Истекает: <b>${dt(s.expires_at)}</b>`,`⌛ Осталось: <b>${dd} дн. ${hh} ч. ${mm} мин.</b>`,`📱 Устройств: <b>до 3</b>`,"","🔗 Ссылка подписки:",`<code>${esc(s.sub_url)}</code>`,"","Нажмите кнопку для подключения 👇"].join("\n");
}

function buyText(uid) {
  const u=user(uid), act=activeSub(sub(uid));
  const lines=["⭐ <b>Тарифы VPN</b>",""];
  tariffs().forEach(t=>lines.push(`• ${t.title} — <b>${rub(t.price_rub)}</b>`));
  lines.push("",`💵 Ваш баланс: <b>${rub(u.balance_rub)}</b>`,"",act?"✅ Подписка активна. Можно продлить.":"👆 Выберите тариф для оформления.");
  return lines.join("\n");
}

function balText(u) {
  return ["💵 <b>Мой баланс</b>","",`Основной баланс: <b>${rub(u.balance_rub)}</b>`,"","Пополните баланс через «Способы оплаты»,","затем купите или продлите подписку."].join("\n");
}

function refText(u) {
  const st=db.prepare("SELECT COUNT(*) c, COALESCE(SUM(reward_rub),0) s FROM referrals WHERE referrer_tg_id=?").get(Number(u.tg_id));
  const pct=Number(setting("ref_percent","30"))||30;
  const min=Number(setting("ref_withdraw_min","500"))||500;
  const refBal=Number(u.ref_balance_rub||0);
  const pending=userPendingWithdrawal(u.tg_id);
  const link=refLink(u.ref_code);
  return [
    "🤝 <b>Партнёрская программа</b>","",
    `Приглашайте друзей — получайте <b>${pct}%</b>`,
    "с каждой их покупки на реф. баланс.","",
    "📊 <b>Статистика</b>",
    `├ Приглашено:          <b>${st.c||0} чел.</b>`,
    `├ Всего заработано:    <b>${rub(st.s||0)}</b>`,
    `└ Реферальный баланс: <b>${rub(refBal)}</b>`,"",
    "💳 <b>Вывод</b>",
    `├ Минимум: <b>${rub(min)}</b>`,
    `├ Метод:   <b>${esc(u.payout_method||"не задан")}</b>`,
    `└ Статус:  <b>${pending?"⏳ заявка в обработке":"—"}</b>`,"",
    "🔗 <b>Ваша ссылка</b>",
    `<code>${link}</code>`,
  ].join("\n");
}

function purchasesText(uid, page=0) {
  const size=5, off=page*size;
  const rows=db.prepare("SELECT * FROM purchases WHERE tg_id=? ORDER BY created_at DESC LIMIT ? OFFSET ?").all(Number(uid),size,off);
  const total=Number(db.prepare("SELECT COUNT(*) c FROM purchases WHERE tg_id=?").get(Number(uid)).c||0);
  if(!rows.length) return{text:"🛒 <b>История покупок</b>\n\nПокупок пока нет.",total,page,size};
  const lines=["🛒 <b>История покупок</b>",""];
  for(const p of rows){
    const icon=p.kind==="gift"?"🎁":p.kind==="renew"?"⏳":"🛍";
    lines.push(`${icon} <b>${esc(p.tariff_title)}</b> — ${rub(p.amount_rub)}`);
    lines.push(`   📅 ${dt(p.created_at)}`);
  }
  lines.push("",`Страница ${page+1} из ${Math.max(1,Math.ceil(total/size))} (всего ${total})`);
  return{text:lines.join("\n"),total,page,size};
}

function refHistoryText(uid, page=0) {
  const size=5, off=page*size;
  const rows=db.prepare("SELECT * FROM referrals WHERE referrer_tg_id=? ORDER BY created_at DESC LIMIT ? OFFSET ?").all(Number(uid),size,off);
  const total=Number(db.prepare("SELECT COUNT(*) c FROM referrals WHERE referrer_tg_id=?").get(Number(uid)).c||0);
  if(!rows.length) return{text:"📋 <b>Реф. начисления</b>\n\nНачислений пока нет.",total,page,size};
  const lines=["📋 <b>Реф. начисления</b>",""];
  for(const r of rows){
    lines.push(`+<b>${rub(r.reward_rub)}</b>  (${r.percent}% от ${rub(r.amount_rub)})`);
    lines.push(`   📅 ${dt(r.created_at)}`);
  }
  lines.push("",`Страница ${page+1} из ${Math.max(1,Math.ceil(total/size))} (всего ${total})`);
  return{text:lines.join("\n"),total,page,size};
}

// ─────────────────────────────────────────────────────────────────────────────
// Keyboard builders
// ─────────────────────────────────────────────────────────────────────────────
function homeKb(uid) {
  const rows=[
    [{text:"🔐 Моя подписка",callback_data:"v:sub"},{text:"⭐ Купить",callback_data:"v:buy"}],
    [{text:"💵 Мой баланс",callback_data:"v:bal"},{text:"🎁 Подарить",callback_data:"v:gift"}],
    [{text:"🤝 Партнёрская программа",callback_data:"v:ref"}],
    [{text:"📘 Инструкции",callback_data:"v:guide"},{text:"💬 О сервисе",callback_data:"v:about"}],
  ];
  if(isAdmin(uid)) rows.push([{text:"🛠 Панель администратора",callback_data:"a:main"}]);
  return{inline_keyboard:rows};
}
function buyKb(uid) {
  const act=activeSub(sub(uid));
  const rows=tariffs().map(t=>[{text:`${act?"⏳":"🛍"} ${t.title} — ${rub(t.price_rub)}`,callback_data:`${act?"pay:r:":"pay:n:"}${t.code}`}]);
  rows.push([{text:"⬅️ Назад",callback_data:"v:home"}]);
  return{inline_keyboard:rows};
}
function subKb(uid) {
  const s=sub(uid), rows=[];
  if(activeSub(s)){
    rows.push([{text:"📲 Подключить устройство",url:s.sub_url}]);
    rows.push([{text:"📋 Скопировать ссылку",callback_data:"sub:copy"},{text:"📺 TV",callback_data:"sub:tv"}]);
  }
  rows.push([{text:"⏳ Продлить",callback_data:"v:buy"},{text:"♻️ Сбросить привязку",callback_data:"sub:reset"}]);
  rows.push([{text:"❌ Скрыть",callback_data:"sub:del"},{text:"⬅️ Назад",callback_data:"v:home"}]);
  return{inline_keyboard:rows};
}
function balKb() { return{inline_keyboard:[[{text:"💳 Способы оплаты",callback_data:"v:pay"}],[{text:"⭐ Купить подписку",callback_data:"v:buy"}],[{text:"🛒 История покупок",callback_data:"ph:0"}],[{text:"⬅️ Назад",callback_data:"v:home"}]]}; }
function refKb(u) {
  const pending=!!userPendingWithdrawal(u.tg_id);
  return{inline_keyboard:[
    pending?[{text:"⏳ Заявка в обработке",callback_data:"noop"}]:[{text:"💰 Вывести средства",callback_data:"ref:w"}],
    [{text:"📋 История начислений",callback_data:"ref:hist:0"}],
    [{text:"🧾 Способ вывода",callback_data:"ref:p"},{text:"🔄 Сменить код",callback_data:"ref:r"}],
    [{text:"✉️ Пригласить друзей",callback_data:"ref:i"}],
    [{text:"⬅️ Назад",callback_data:"v:home"}],
  ]};
}
const payoutMethodKb=()=>({inline_keyboard:[[{text:"💳 Карта (RU)",callback_data:"ref:pm:card"}],[{text:"🪙 USDT (TRC20)",callback_data:"ref:pm:usdt_trc20"}],[{text:"🏦 СБП",callback_data:"ref:pm:sbp"}],[{text:"⬅️ Назад",callback_data:"v:ref"}]]});
const back=(t="v:home")=>({inline_keyboard:[[{text:"⬅️ Назад",callback_data:t}]]});
function giftKb() { return{inline_keyboard:[...tariffs().map(t=>[{text:`🎁 ${t.title} — ${rub(t.price_rub)}`,callback_data:`g:p:${t.code}`}]),[{text:"⬅️ Назад",callback_data:"v:home"}]]}; }
function giftUsersKb(sender, code, page) {
  const{items,total,page:p,size}=usersPage(page,sender,8);
  const max=Math.max(0,Math.ceil(total/size)-1);
  const rows=items.map(u=>[{text:`${u.first_name||u.username||u.tg_id} (${u.username?`@${u.username}`:`id:${u.tg_id}`})`,callback_data:`g:u:${code}:${u.tg_id}`}]);
  const nav=[];
  if(p>0)   nav.push({text:"◀️",callback_data:`g:l:${code}:${p-1}`});
  nav.push({text:`${p+1}/${max+1}`,callback_data:"noop"});
  if(p<max) nav.push({text:"▶️",callback_data:`g:l:${code}:${p+1}`});
  rows.push(nav,[{text:"🔍 Выбрать через Telegram",callback_data:`g:sys:${code}`},{text:"⬅️ Назад",callback_data:"v:gift"}]);
  return{inline_keyboard:rows};
}
function pagingKb(prefix, page, total, size, backTarget) {
  const max=Math.max(0,Math.ceil(total/size)-1);
  const nav=[];
  if(page>0)   nav.push({text:"◀️",callback_data:`${prefix}:${page-1}`});
  nav.push({text:`${page+1}/${max+1}`,callback_data:"noop"});
  if(page<max) nav.push({text:"▶️",callback_data:`${prefix}:${page+1}`});
  return{inline_keyboard:[nav,[{text:"⬅️ Назад",callback_data:backTarget}]]};
}

// ─────────────────────────────────────────────────────────────────────────────
// Admin panels
// ─────────────────────────────────────────────────────────────────────────────
function adminStatsText() {
  const uCount  = Number(db.prepare("SELECT COUNT(*) c FROM users").get().c);
  const aCount  = Number(db.prepare("SELECT COUNT(*) c FROM subscriptions WHERE is_active=1").get().c);
  const revenue = Number(db.prepare("SELECT COALESCE(SUM(amount_rub),0) s FROM purchases").get().s||0);
  const today   = new Date(); today.setHours(0,0,0,0);
  const newDay  = Number(db.prepare("SELECT COUNT(*) c FROM users WHERE created_at>=?").get(today.getTime()).c);
  const refPaid = Number(db.prepare("SELECT COALESCE(SUM(reward_rub),0) s FROM referrals").get().s||0);
  const pending = pendingWithdrawals().length;
  return ["📊 <b>Статистика бота</b>","",`👥 Пользователей:    <b>${uCount}</b>  (+${newDay} сегодня)`,`🔐 Активных подп.:   <b>${aCount}</b>`,`💰 Общая выручка:    <b>${rub(revenue)}</b>`,`🤝 Выплачено реф.:   <b>${rub(refPaid)}</b>`,`⏳ Заявок на вывод:  <b>${pending}</b>`].join("\n");
}

function withdrawalPanelView() {
  const items=pendingWithdrawals();
  if(!items.length) return{t:"💸 <b>Заявки на вывод</b>\n\nОчередь пуста. 🎉",kb:{inline_keyboard:[[{text:"📜 История выводов",callback_data:"a:wr_hist"}],[{text:"⬅️ Назад",callback_data:"a:main"}]]}};
  const shown=items.slice(0,5);
  const t=`💸 <b>Заявки на вывод</b> (${items.length} в очереди)\n\n`+shown.map((wr,i)=>[`<b>${i+1}. ${esc(wr.first_name||wr.username||String(wr.tg_id))}</b> (ID: <code>${wr.tg_id}</code>)`,`   💰 ${rub(wr.amount_rub)}  🏦 ${esc(wr.method)}`,`   📋 <code>${esc(wr.details)}</code>`,`   🕐 ${dts(wr.created_at)}`].join("\n")).join("\n\n");
  const kb={inline_keyboard:[...shown.map(wr=>[{text:`✅ #${wr.id} ${rub(wr.amount_rub)}`,callback_data:`a:wr_a:${wr.id}`},{text:`❌ Отклонить #${wr.id}`,callback_data:`a:wr_r:${wr.id}`}]),[{text:"🔄 Обновить",callback_data:"a:wr"},{text:"📜 История",callback_data:"a:wr_hist"},{text:"⬅️ Назад",callback_data:"a:main"}]]};
  return{t,kb};
}

function withdrawalHistoryText() {
  const rows=withdrawalHistory(20);
  if(!rows.length) return "📜 <b>История выводов</b>\n\nПусто.";
  const lines=["📜 <b>История выводов</b> (посл. 20)",""];
  for(const wr of rows){
    const icon=wr.status==="approved"?"✅":wr.status==="rejected"?"❌":"⏳";
    lines.push(`${icon} <b>${rub(wr.amount_rub)}</b> — ${esc(wr.first_name||wr.username||String(wr.tg_id))}`);
    lines.push(`   ${esc(wr.method)} | ${dts(wr.updated_at)}`);
    if(wr.admin_note) lines.push(`   Прим.: ${esc(wr.admin_note)}`);
  }
  return lines.join("\n");
}

function adminUserInfoText(tu) {
  const ts=sub(tu.tg_id), hasSub=activeSub(ts);
  return ["👤 <b>Пользователь</b>","",`ID:              <code>${tu.tg_id}</code>`,`Имя:             ${esc(tu.first_name)}`,`Username:        ${tu.username?`@${esc(tu.username)}`:"—"}`,`Баланс:          <b>${rub(tu.balance_rub)}</b>`,`Реф. баланс:     <b>${rub(tu.ref_balance_rub||0)}</b>`,`Реф. заработано: <b>${rub(tu.ref_earned||0)}</b>`,`Подписка:        ${hasSub?`✅ до ${dt(ts.expires_at)}`:"❌ нет"}`,`Зарегистрирован: ${dt(tu.created_at)}`].join("\n");
}

// ─────────────────────────────────────────────────────────────────────────────
// Render
// ─────────────────────────────────────────────────────────────────────────────
async function render(uid, chatId, msgId, view, data={}) {
  const u=user(uid); if(!u) return;
  let t=homeText(u), kb=homeKb(uid);
  switch(view){
    case "home":      t=homeText(u);  kb=homeKb(uid);  break;
    case "sub":       t=subText(uid); kb=subKb(uid);   break;
    case "buy":       t=buyText(uid); kb=buyKb(uid);   break;
    case "bal":       t=balText(u);   kb=balKb();       break;
    case "pay":       t=`💳 <b>Способы оплаты</b>\n\n${esc(setting("payment_methods","Пока не настроено.")).replace(/\\n/g,"\n")}`; kb=back("v:bal"); break;
    case "guide":     t=["📘 <b>Инструкция по подключению</b>","","1️⃣ Купите подписку.","2️⃣ Откройте «Моя подписка» → скопируйте ссылку.","3️⃣ Установите клиент (v2rayNG, Hiddify и др.).","4️⃣ Импортируйте ссылку в клиент.","5️⃣ Подключитесь — готово!"].join("\n"); kb=SUPPORT_URL?{inline_keyboard:[[{text:"💬 Поддержка",url:SUPPORT_URL}],[{text:"⬅️ Назад",callback_data:"v:home"}]]}:back(); break;
    case "about":     t="💬 <b>О сервисе</b>\n\nНадёжная VPN-подписка с быстрой выдачей ссылки, продлением через Telegram и реферальной программой."; kb=back(); break;
    case "ref":       t=refText(u); kb=refKb(u); break;
    case "ref_payout":t=["🧾 <b>Способ вывода</b>","",`Текущий: <b>${esc(u.payout_method||"не задан")}</b>`,`Реквизиты: <b>${esc(u.payout_details||"не заданы")}</b>`,"","Выберите метод:"].join("\n"); kb=payoutMethodKb(); break;
    case "purchases": { const{text:ht,total,size}=purchasesText(uid,Number(data.page||0)); t=ht; kb=pagingKb("ph",Number(data.page||0),total,size,"v:home"); break; }
    case "ref_hist":  { const{text:ht,total,size}=refHistoryText(uid,Number(data.page||0)); t=ht; kb=pagingKb("ref:hist",Number(data.page||0),total,size,"v:ref"); break; }
    case "gift":      t="🎁 <b>Подарить подписку</b>\n\nВыберите тариф:"; kb=giftKb(); break;
    case "gift_users":{ const tr=tariff(data.code); t=tr?`🎁 <b>Подарок: ${esc(tr.title)}</b>\n\nВыберите получателя из списка или через Telegram:`:"Тариф не найден."; kb=tr?giftUsersKb(uid,tr.code,data.page||0):back("v:gift"); break; }
    case "a_main":    { const p=pendingWithdrawals().length; t=adminStatsText(); kb={inline_keyboard:[[{text:"💸 Тарифы",callback_data:"a:t"},{text:"🎞 GIF",callback_data:"a:g"}],[{text:"📨 Рассылка",callback_data:"a:b"},{text:"💳 Оплата",callback_data:"a:p"}],[{text:"🤝 Реф. настройки",callback_data:"a:r"},{text:"🔍 Поиск юзера",callback_data:"a:find"}],[{text:p>0?`💸 Заявки (${p} !)`:  "💸 Заявки на вывод",callback_data:"a:wr"}],[{text:"🗄 База данных",callback_data:"a:db"}],[{text:"⬅️ Назад",callback_data:"v:home"}]]}; break; }
    case "a_wr":      { const{t:wt,kb:wkb}=withdrawalPanelView(); t=wt; kb=wkb; break; }
    case "a_wr_hist": t=withdrawalHistoryText(); kb=back("a:wr"); break;
    case "a_tariffs": t=`💸 <b>Цены тарифов</b>\n\n${tariffs().map(x=>`• ${x.title}: <b>${rub(x.price_rub)}</b>`).join("\n")}`; kb={inline_keyboard:[...tariffs().map(x=>[{text:`✏️ ${x.title} — ${rub(x.price_rub)}`,callback_data:`a:te:${x.code}`}]),[{text:"⬅️ Назад",callback_data:"a:main"}]]}; break;
    case "a_gif":     t="🎞 <b>GIF-анимации</b>\n\nНастройте анимации для каждого события:"; kb={inline_keyboard:[[{text:`🏠 Главное меню${setting("gif_main_menu")?" ✅":""}`,callback_data:"a:ge:gif_main_menu"}],[{text:`✅ Покупка${setting("gif_purchase_success")?" ✅":""}`,callback_data:"a:ge:gif_purchase_success"}],[{text:`🎁 Подарок${setting("gif_gift_success")?" ✅":""}`,callback_data:"a:ge:gif_gift_success"}],[{text:`📨 Рассылка${setting("gif_broadcast")?" ✅":""}`,callback_data:"a:ge:gif_broadcast"}],[{text:"⬅️ Назад",callback_data:"a:main"}]]}; break;
    case "a_bcast":   t="📨 <b>Рассылка</b>\n\nОтправьте текст. Поддерживается HTML."; kb={inline_keyboard:[[{text:"✏️ Начать рассылку",callback_data:"a:bs"}],[{text:"⬅️ Назад",callback_data:"a:main"}]]}; break;
    case "a_pay":     t=`💳 <b>Способы оплаты</b>\n\n${esc(setting("payment_methods","Пока пусто."))}`; kb={inline_keyboard:[[{text:"✏️ Изменить",callback_data:"a:pe"}],[{text:"⬅️ Назад",callback_data:"a:main"}]]}; break;
    case "a_ref":     t=["🤝 <b>Реф. настройки</b>","",`Ставка:      <b>${setting("ref_percent","30")}%</b>`,`Мин. вывод:  <b>${rub(setting("ref_withdraw_min","500"))}</b>`].join("\n"); kb={inline_keyboard:[[{text:"✏️ Ставка",callback_data:"a:rp"},{text:"✏️ Мин. вывод",callback_data:"a:rm"}],[{text:"⬅️ Назад",callback_data:"a:main"}]]}; break;
    case "a_db":      t="🗄 <b>База данных</b>\n\nСкачайте или импортируйте БД (после импорта бот перезапустится)."; kb={inline_keyboard:[[{text:"⬇️ Скачать БД",callback_data:"a:db_export"}],[{text:"⬆️ Импорт БД",callback_data:"a:db_import_start"}],[{text:"⬅️ Назад",callback_data:"a:main"}]]}; break;
    case "a_user_info":{ const tu=user(data.id); if(!tu){t="Пользователь не найден.";kb=back("a:main");break;} t=adminUserInfoText(tu); kb={inline_keyboard:[[{text:"➕ Пополнить баланс",callback_data:`a:bal_add:${tu.tg_id}`}],[{text:"⬅️ Назад",callback_data:"a:main"}]]}; break; }
  }
  const nm=await editOrSend(chatId,msgId,t,kb);
  setMenu(uid,chatId,nm);
}

// ─────────────────────────────────────────────────────────────────────────────
// Admin state handler
// ─────────────────────────────────────────────────────────────────────────────
async function handleAdminState(msg) {
  const aid=Number(msg.from?.id||0); if(!isAdmin(aid)) return false;
  const row=getAdminState(aid); if(!row) return false;
  const text=String(msg.text||"").trim(), chatId=Number(msg.chat?.id||0);

  if(text==="/cancel"){clearAdminState(aid); await render(aid,chatId,user(aid)?.last_menu_id||null,"home"); return true;}

  switch(row.state){
    case "db_import_wait":
      if(!msg.document?.file_id){await tg("sendMessage",{chat_id:chatId,text:"Ожидаю файл SQLite (.db/.sqlite)."});return true;}
      try{await tg("sendMessage",{chat_id:chatId,text:"⏳ Импортирую базу данных..."});await importDbFromDocument(msg.document.file_id);clearAdminState(aid);await tg("sendMessage",{chat_id:chatId,text:"✅ Импорт завершён. Перезапускаю бота..."});setTimeout(()=>restartBot(),500);}
      catch(e){await tg("sendMessage",{chat_id:chatId,text:`❌ Ошибка импорта: ${e.message}`});}
      return true;

    case "tariff_price":{
      const n=Number(text); if(!Number.isFinite(n)||n<=0){await tg("sendMessage",{chat_id:chatId,text:"Введите цену > 0."});return true;}
      db.prepare("UPDATE tariffs SET price_rub=? WHERE code=?").run(Math.round(n),row.payload);
      clearAdminState(aid); await tg("sendMessage",{chat_id:chatId,text:`✅ Цена обновлена: ${rub(Math.round(n))}`});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_tariffs"); return true;
    }
    case "gif":{
      const v=msg.animation?.file_id||text; if(!v){await tg("sendMessage",{chat_id:chatId,text:"Отправьте GIF или file_id."});return true;}
      setSetting(row.payload,v); clearAdminState(aid); await tg("sendMessage",{chat_id:chatId,text:"✅ GIF сохранён."});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_gif"); return true;
    }
    case "pay_methods":
      setSetting("payment_methods",text); clearAdminState(aid); await tg("sendMessage",{chat_id:chatId,text:"✅ Способы оплаты обновлены."});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_pay"); return true;

    case "broadcast":{
      clearAdminState(aid);
      const ids=db.prepare("SELECT tg_id FROM users").all(); let ok=0,fail=0;
      for(const u of ids){try{const g=setting("gif_broadcast","");if(g)await tg("sendAnimation",{chat_id:u.tg_id,animation:g,caption:text,parse_mode:"HTML"});else await tg("sendMessage",{chat_id:u.tg_id,text,parse_mode:"HTML"});ok++;}catch{fail++;}}
      await tg("sendMessage",{chat_id:chatId,text:`📨 Рассылка завершена.\n✅ Доставлено: ${ok}\n❌ Ошибок: ${fail}`});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_bcast"); return true;
    }
    case "ref_percent":{
      const n=Number(text); if(!Number.isFinite(n)||n<0||n>100){await tg("sendMessage",{chat_id:chatId,text:"Введите 0..100."});return true;}
      setSetting("ref_percent",Math.round(n)); clearAdminState(aid); await tg("sendMessage",{chat_id:chatId,text:`✅ Ставка: ${Math.round(n)}%`});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_ref"); return true;
    }
    case "ref_min":{
      const n=Number(text); if(!Number.isFinite(n)||n<0){await tg("sendMessage",{chat_id:chatId,text:"Введите сумму ≥ 0."});return true;}
      setSetting("ref_withdraw_min",Math.round(n)); clearAdminState(aid); await tg("sendMessage",{chat_id:chatId,text:`✅ Минимум: ${rub(Math.round(n))}`});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_ref"); return true;
    }
    case "wr_reject":{
      const wrId=Number(row.payload), wr=getWithdrawal(wrId);
      clearAdminState(aid);
      if(!wr||wr.status!=="pending"){await render(aid,chatId,user(aid)?.last_menu_id||null,"a_wr");return true;}
      db.prepare("UPDATE withdrawal_requests SET status='rejected',admin_note=?,updated_at=? WHERE id=?").run(text,now(),wrId);
      await tg("sendMessage",{chat_id:chatId,text:`❌ Заявка #${wrId} отклонена.`});
      tg("sendMessage",{chat_id:wr.tg_id,text:`❌ <b>Заявка на вывод отклонена.</b>\n\nСумма: <b>${rub(wr.amount_rub)}</b>\nПричина: ${esc(text)}`,parse_mode:"HTML"}).catch(()=>{});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_wr"); return true;
    }
    case "bal_add":{
      const targetId=Number(row.payload), n=Number(text);
      if(!Number.isFinite(n)){await tg("sendMessage",{chat_id:chatId,text:"Введите сумму числом."});return true;}
      const nb=updateBalance(targetId,n); clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:`✅ Баланс ${targetId} изменён на ${n>0?"+":""}${rub(n)}.\nНовый баланс: <b>${rub(nb)}</b>`,parse_mode:"HTML"});
      if(n>0) tg("sendMessage",{chat_id:targetId,text:`💵 <b>Баланс пополнен на ${rub(n)}!</b>\nТекущий баланс: <b>${rub(nb)}</b>`,parse_mode:"HTML"}).catch(()=>{});
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_user_info",{id:targetId}); return true;
    }
    case "find_user":{
      clearAdminState(aid);
      let found=null;
      if(/^\d+$/.test(text)) found=user(Number(text));
      if(!found) found=db.prepare("SELECT * FROM users WHERE username=?").get(text.replace(/^@/,""));
      if(!found){await tg("sendMessage",{chat_id:chatId,text:"Пользователь не найден."});return true;}
      await render(aid,chatId,user(aid)?.last_menu_id||null,"a_user_info",{id:found.tg_id}); return true;
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

  // Universal cancel → home
  if((text==="Отмена"||text==="Отмена выбора")&&ustate){
    clearUserState(from.id);
    await tg("sendMessage",{chat_id:chatId,text:"Отменено.",reply_markup:{remove_keyboard:true}});
    await render(from.id,chatId,user(from.id)?.last_menu_id||null,"home");
    return;
  }

  // Gift: system picker result
  if(msg.user_shared&&ustate?.state==="gift_pick"){
    const recipientId=Number(msg.user_shared.user_id||0), code=ustate.payload||"";
    clearUserState(from.id);
    await tg("sendMessage",{chat_id:chatId,text:"Получатель выбран.",reply_markup:{remove_keyboard:true}});
    if(!user(recipientId)){
      await tg("sendMessage",{chat_id:chatId,text:"❌ Пользователь не зарегистрирован в боте. Попросите его нажать /start."});
      await render(from.id,chatId,user(from.id)?.last_menu_id||null,"home"); return;
    }
    await giftToUser(from.id,recipientId,code,chatId,user(from.id)?.last_menu_id||null,null); return;
  }

  // Payout details
  if(ustate?.state==="ref_payout_details"&&msg.text&&!msg.text.startsWith("/")){
    const method=ustate.payload||"Неизвестно";
    db.prepare("UPDATE users SET payout_method=?,payout_details=?,updated_at=? WHERE tg_id=?").run(method,text,now(),Number(from.id));
    clearUserState(from.id);
    await tg("sendMessage",{chat_id:chatId,text:"✅ Реквизиты сохранены.",reply_markup:{remove_keyboard:true}});
    await render(from.id,chatId,user(from.id)?.last_menu_id||null,"ref"); return;
  }

  // Withdrawal amount
  if(ustate?.state==="ref_withdraw_amount"){
    const amount=Number(text.replace(/[^\d.]/g,""));
    const u=user(from.id);
    const min=Number(setting("ref_withdraw_min","500"))||500;
    const refBal=Number(u?.ref_balance_rub||0);
    if(!Number.isFinite(amount)||amount<=0){await tg("sendMessage",{chat_id:chatId,text:"Введите сумму числом, например: 500"});return;}
    if(amount<min){await tg("sendMessage",{chat_id:chatId,text:`❌ Минимальная сумма: ${rub(min)}\nРеф. баланс: ${rub(refBal)}`});return;}
    if(amount>refBal){await tg("sendMessage",{chat_id:chatId,text:`❌ Недостаточно средств.\nРеф. баланс: ${rub(refBal)}`});return;}
    const amt=Math.round(amount);
    clearUserState(from.id);
    db.prepare("INSERT INTO withdrawal_requests(tg_id,amount_rub,method,details,status,admin_note,created_at,updated_at) VALUES(?,?,?,?,'pending','',?,?)").run(Number(from.id),amt,u.payout_method,u.payout_details,now(),now());
    await tg("sendMessage",{chat_id:chatId,text:`✅ <b>Заявка на вывод создана!</b>\n\nСумма: <b>${rub(amt)}</b>\nМетод: ${esc(u.payout_method)}\n\nОжидайте рассмотрения.`,parse_mode:"HTML",reply_markup:{remove_keyboard:true}});
    const pCount=pendingWithdrawals().length;
    tg("sendMessage",{chat_id:ADMIN_ID,text:[`💸 <b>Новая заявка на вывод</b>`,"",`👤 ${esc(u.first_name||u.username||String(from.id))} (ID: <code>${from.id}</code>)`,`💰 Сумма: <b>${rub(amt)}</b>`,`🏦 Метод: ${esc(u.payout_method)}`,`📋 Реквизиты: <code>${esc(u.payout_details)}</code>`,"",`В очереди: <b>${pCount}</b>`].join("\n"),parse_mode:"HTML",reply_markup:{inline_keyboard:[[{text:"📋 Открыть заявки",callback_data:"a:wr"}]]}}).catch(()=>{});
    await render(from.id,chatId,user(from.id)?.last_menu_id||null,"ref"); return;
  }

  // Admin states
  if(await handleAdminState(msg)) return;

  // Admin commands
  if(isAdmin(from.id)){
    if(text.startsWith("/add_balance")){
      const p=text.split(/\s+/); if(p.length!==3){await tg("sendMessage",{chat_id:chatId,text:"Формат: /add_balance <id> <amount>"});return;}
      if(!user(Number(p[1]))||!Number.isFinite(Number(p[2]))){await tg("sendMessage",{chat_id:chatId,text:"Неверные параметры."});return;}
      const nb=updateBalance(Number(p[1]),Number(p[2])); await tg("sendMessage",{chat_id:chatId,text:`✅ Баланс ${p[1]}: ${rub(nb)}`}); return;
    }
    if(text.startsWith("/add_ref_balance")){
      const p=text.split(/\s+/); if(p.length!==3){await tg("sendMessage",{chat_id:chatId,text:"Формат: /add_ref_balance <id> <amount>"});return;}
      if(!user(Number(p[1]))||!Number.isFinite(Number(p[2]))){await tg("sendMessage",{chat_id:chatId,text:"Неверные параметры."});return;}
      const nb=updateRefBalance(Number(p[1]),Number(p[2])); await tg("sendMessage",{chat_id:chatId,text:`✅ Реф-баланс ${p[1]}: ${rub(nb)}`}); return;
    }
  }

  // Standard commands
  if(text.startsWith("/start")){
    const m=text.match(/^\/start\s+partner_([a-zA-Z0-9]+)$/);
    if(m){const r=findRef(m[1]); if(r) setRef(from.id,r.tg_id);}
    await gif(chatId,"gif_main_menu");
    await render(from.id,chatId,null,"home"); return;
  }
  if(text==="/menu") {await render(from.id,chatId,null,"home"); return;}
  if(text==="/admin"&&isAdmin(from.id)){await render(from.id,chatId,user(from.id)?.last_menu_id,"a_main"); return;}

  await tg("sendMessage",{chat_id:chatId,text:"Используйте /start для открытия меню."});
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
  if(data.startsWith("a:")&&!isAdmin(uid)){await ans("Недостаточно прав",true);return;}

  // Navigation
  if(data.startsWith("v:")){
    const map={home:"home",sub:"sub",buy:"buy",bal:"bal",gift:"gift",ref:"ref",guide:"guide",about:"about",pay:"pay"};
    await render(uid,chatId,msgId,map[data.slice(2)]||"home"); await ans(); return;
  }
  // Purchase
  if(data.startsWith("pay:n:")){await askBuyConfirm(uid,chatId,msgId,data.split(":")[2],"new",q.id);return;}
  if(data.startsWith("pay:r:")){await askBuyConfirm(uid,chatId,msgId,data.split(":")[2],"renew",q.id);return;}
  if(data.startsWith("pay:c:")){const[,,mode,code]=data.split(":");await buySelf(uid,chatId,msgId,code,mode,q.id);return;}
  // Purchase history
  if(data.startsWith("ph:")){await render(uid,chatId,msgId,"purchases",{page:Number(data.split(":")[1]||0)});await ans();return;}
  // Subscription
  if(data==="sub:del"){db.prepare("UPDATE subscriptions SET is_active=0,updated_at=? WHERE tg_id=?").run(now(),uid);await ans("Подписка скрыта");await render(uid,chatId,msgId,"sub");return;}
  if(data==="sub:reset"){await ans("Сброс применится при следующей покупке.",true);return;}
  if(data==="sub:tv"){await ans("Скопируйте ссылку и импортируйте в клиент на TV.",true);return;}
  if(data==="sub:copy"){const s=sub(uid);if(activeSub(s)){tg("sendMessage",{chat_id:chatId,text:`🔗 Ссылка подписки:\n<code>${esc(s.sub_url)}</code>`,parse_mode:"HTML"}).catch(()=>{});await ans("Ссылка отправлена ↑");}else{await ans("Активной подписки нет.",true);}return;}
  // Referral
  if(data==="ref:i"){const u=user(uid);tg("sendMessage",{chat_id:chatId,text:`🔗 <b>Партнёрская ссылка:</b>\n<code>${refLink(u.ref_code)}</code>`,parse_mode:"HTML"}).catch(()=>{});await ans("Ссылка отправлена ↑");return;}
  if(data==="ref:r"){db.prepare("UPDATE users SET ref_code=?,updated_at=? WHERE tg_id=?").run(crypto.randomBytes(5).toString("hex"),now(),uid);await render(uid,chatId,msgId,"ref");await ans("Код обновлён ✅");return;}
  if(data==="ref:p"){await render(uid,chatId,msgId,"ref_payout");await ans();return;}
  if(data.startsWith("ref:pm:")){
    const map={card:"Карта (RU)",usdt_trc20:"USDT (TRC20)",sbp:"СБП"};
    const method=map[data.split(":")[2]]||"Неизвестно";
    setUserState(uid,"ref_payout_details",method); await ans();
    await tg("sendMessage",{chat_id:chatId,text:`Выбран метод: <b>${esc(method)}</b>\n\nВведите реквизиты одним сообщением.\n(«Отмена» — для отмены)`,parse_mode:"HTML",reply_markup:{keyboard:[[{text:"Отмена"}]],resize_keyboard:true,one_time_keyboard:true}});
    return;
  }
  if(data.startsWith("ref:hist:")){await render(uid,chatId,msgId,"ref_hist",{page:Number(data.split(":")[2]||0)});await ans();return;}
  if(data==="ref:w"){
    const u=user(uid), min=Number(setting("ref_withdraw_min","500"))||500, refBal=Number(u.ref_balance_rub||0);
    if(!u.payout_method||!u.payout_details){await ans("Сначала укажите способ вывода и реквизиты.",true);await render(uid,chatId,msgId,"ref_payout");return;}
    if(userPendingWithdrawal(uid)){await ans("У вас уже есть заявка в обработке.",true);return;}
    if(refBal<min){await ans(`Минимум: ${rub(min)}. Баланс: ${rub(refBal)}`,true);return;}
    setUserState(uid,"ref_withdraw_amount",""); await ans();
    await tg("sendMessage",{chat_id:chatId,text:`💸 <b>Вывод реферальных средств</b>\n\nРеф. баланс: <b>${rub(refBal)}</b>\nМинимум: <b>${rub(min)}</b>\n\nВведите сумму:`,parse_mode:"HTML",reply_markup:{keyboard:[[{text:String(refBal)},{text:"Отмена"}]],resize_keyboard:true,one_time_keyboard:true}});
    return;
  }
  // Gifts
  if(data.startsWith("g:p:")){
    const code=data.split(":")[2], tr=tariff(code), u=user(uid);
    if(!tr){await ans("Тариф не найден.",true);return;}
    if(Number(u.balance_rub)<Number(tr.price_rub)){await ans(`Нужно ${rub(tr.price_rub)}, у вас ${rub(u.balance_rub)}`,true);return;}
    await render(uid,chatId,msgId,"gift_users",{code,page:0}); await ans(); return;
  }
  if(data.startsWith("g:sys:")){await requestGiftRecipient(uid,chatId,data.split(":")[2]);await ans();return;}
  if(data.startsWith("g:l:")){const[,,code,page]=data.split(":");await render(uid,chatId,msgId,"gift_users",{code,page:Number(page||0)});await ans();return;}
  if(data.startsWith("g:u:")){const[,,code,rid]=data.split(":");await giftToUser(uid,Number(rid),code,chatId,msgId,q.id);return;}
  // Admin nav
  const adminNav={"a:main":"a_main","a:t":"a_tariffs","a:g":"a_gif","a:b":"a_bcast","a:p":"a_pay","a:r":"a_ref","a:wr":"a_wr","a:wr_hist":"a_wr_hist","a:db":"a_db"};
  if(adminNav[data]){await render(uid,chatId,msgId,adminNav[data]);await ans();return;}
  // Admin edit triggers
  if(data.startsWith("a:te:")){const code=data.split(":")[2],tr=tariff(code);setAdminState(uid,"tariff_price",code);await tg("sendMessage",{chat_id:chatId,text:`«${esc(tr?.title||code)}»: текущая цена ${rub(tr?.price_rub||0)}\n\nВведите новую цену (₽):\n/cancel — отмена.`});await ans();return;}
  if(data.startsWith("a:ge:")){setAdminState(uid,"gif",data.split(":")[2]);await tg("sendMessage",{chat_id:chatId,text:"Отправьте GIF-файл или file_id.\n/cancel — отмена."});await ans();return;}
  if(data==="a:bs"){setAdminState(uid,"broadcast","");await tg("sendMessage",{chat_id:chatId,text:"Отправьте текст рассылки (HTML).\n/cancel — отмена."});await ans();return;}
  if(data==="a:pe"){setAdminState(uid,"pay_methods","");await tg("sendMessage",{chat_id:chatId,text:"Отправьте новый текст способов оплаты.\n/cancel — отмена."});await ans();return;}
  if(data==="a:rp"){setAdminState(uid,"ref_percent","");await tg("sendMessage",{chat_id:chatId,text:`Текущая ставка: ${setting("ref_percent","30")}%\n\nВведите новую (0..100):\n/cancel — отмена.`});await ans();return;}
  if(data==="a:rm"){setAdminState(uid,"ref_min","");await tg("sendMessage",{chat_id:chatId,text:`Текущий минимум: ${rub(setting("ref_withdraw_min","500"))}\n\nВведите новый (₽):\n/cancel — отмена.`});await ans();return;}
  if(data==="a:find"){setAdminState(uid,"find_user","");await tg("sendMessage",{chat_id:chatId,text:"Введите Telegram ID или @username:\n/cancel — отмена."});await ans();return;}
  // DB
  if(data==="a:db_export"){await ans("Формирую файл...");await exportDbToAdmin(chatId);return;}
  if(data==="a:db_import_start"){setAdminState(uid,"db_import_wait","");await ans("Жду файл .db/.sqlite");await tg("sendMessage",{chat_id:chatId,text:"📤 Отправьте файл SQLite документом.\n⚠️ Бот перезапустится после импорта."});return;}
  // Withdrawal approve
  if(data.startsWith("a:wr_a:")){
    const wrId=Number(data.split(":")[2]), wr=getWithdrawal(wrId);
    if(!wr||wr.status!=="pending"){await ans("Заявка уже обработана.",true);await render(uid,chatId,msgId,"a_wr");return;}
    const rec=user(wr.tg_id);
    if(!rec||Number(rec.ref_balance_rub||0)<wr.amount_rub){await ans("Недостаточно реф-средств у пользователя.",true);return;}
    db.transaction(()=>{updateRefBalance(wr.tg_id,-wr.amount_rub);db.prepare("UPDATE withdrawal_requests SET status='approved',updated_at=? WHERE id=?").run(now(),wrId);})();
    await ans("✅ Одобрено");
    tg("sendMessage",{chat_id:wr.tg_id,text:`✅ <b>Вывод одобрен!</b>\n\nСумма: <b>${rub(wr.amount_rub)}</b>\nМетод: ${esc(wr.method)}\nРеквизиты: <code>${esc(wr.details)}</code>`,parse_mode:"HTML"}).catch(()=>{});
    await render(uid,chatId,msgId,"a_wr"); return;
  }
  // Withdrawal reject
  if(data.startsWith("a:wr_r:")){
    const wrId=data.split(":")[2], wr=getWithdrawal(Number(wrId));
    if(!wr||wr.status!=="pending"){await ans("Заявка уже обработана.",true);await render(uid,chatId,msgId,"a_wr");return;}
    setAdminState(uid,"wr_reject",wrId); await ans();
    await tg("sendMessage",{chat_id:chatId,text:`Отклонение заявки #${wrId} на ${rub(wr.amount_rub)}\n\nВведите причину:\n/cancel — отмена.`}); return;
  }
  // Balance add shortcut
  if(data.startsWith("a:bal_add:")){
    const targetId=data.split(":")[2], tu=user(Number(targetId));
    setAdminState(uid,"bal_add",targetId); await ans();
    await tg("sendMessage",{chat_id:chatId,text:`Пополнение для ${esc(tu?.first_name||targetId)}\nТекущий баланс: ${rub(tu?.balance_rub)}\n\nВведите сумму (отрицательная — списание):\n/cancel — отмена.`}); return;
  }

  await ans("Неизвестная команда");
}

// ─────────────────────────────────────────────────────────────────────────────
// Long-poll
// ─────────────────────────────────────────────────────────────────────────────
async function poll() {
  console.log("🤖 Бот запущен. Ожидаю обновления...");
  while(true){
    try{
      const ups=await tg("getUpdates",{timeout:30,offset,allowed_updates:["message","callback_query"]});
      for(const u of ups){
        offset=u.update_id+1;
        if(u.message)        handleMessage(u.message).catch(e=>console.error("[msg]",e.message));
        else if(u.callback_query) handleCallback(u.callback_query).catch(e=>console.error("[cb]",e.message));
      }
    } catch(e){ console.error("[poll]",e.message); await new Promise(r=>setTimeout(r,2000)); }
  }
}

init();
poll();
