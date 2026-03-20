require("dotenv").config();
const path=require("path");
const crypto=require("crypto");
const fs=require("fs");
const fsp=fs.promises;
const { spawn }=require("child_process");
const Database=require("better-sqlite3");

const TOKEN=process.env.TELEGRAM_BOT_TOKEN||"";
const API=(process.env.VPN_API_BASE_URL||"").replace(/\/+$/,"");
const APP_SECRET=process.env.APP_SECRET||"";
const ADMIN_ID=Number(process.env.ADMIN_TELEGRAM_ID||0);
const DB_FILE=process.env.SQLITE_PATH||path.join(__dirname,"bot.sqlite");
const NEWS_URL=process.env.BOT_NEWS_URL||"https://t.me/cats_vpn";
const SUPPORT_URL=process.env.BOT_SUPPORT_URL||"https://t.me/Oktsupport";
const FREE_PROXY_URL=process.env.BOT_FREE_PROXY_URL||"https://t.me/cats_vpn";
const BOT_USERNAME=process.env.BOT_USERNAME||"";
if(!TOKEN||!API||!APP_SECRET||!ADMIN_ID)process.exit(1);

const TG=`https://api.telegram.org/bot${TOKEN}`;
let offset=0;
const db=new Database(DB_FILE);
db.pragma("journal_mode = WAL");
const now=()=>Date.now();
const esc=(s)=>String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
const rub=(n)=>`${Number(n||0)} ₽`;
const d=(ts)=>ts?new Date(ts).toLocaleDateString("ru-RU"):"—";
const isAdmin=(id)=>Number(id)===ADMIN_ID;

function setting(k,f=""){return db.prepare("SELECT value v FROM settings WHERE key=?").get(k)?.v??f;}
function setSetting(k,v){db.prepare("INSERT INTO settings(key,value)VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").run(k,String(v||""));}
function user(id){return db.prepare("SELECT * FROM users WHERE tg_id=?").get(Number(id));}
function sub(id){return db.prepare("SELECT * FROM subscriptions WHERE tg_id=?").get(Number(id));}
function activeSub(s){return !!(s&&s.is_active===1&&s.expires_at>now()&&s.sub_url);}
function tariffs(){return db.prepare("SELECT * FROM tariffs ORDER BY sort_order").all();}
function tariff(c){return db.prepare("SELECT * FROM tariffs WHERE code=?").get(c);}

function init(){
db.exec(`
CREATE TABLE IF NOT EXISTS users(tg_id INTEGER PRIMARY KEY,username TEXT DEFAULT '',first_name TEXT DEFAULT '',balance_rub INTEGER NOT NULL DEFAULT 0,referred_by INTEGER,ref_code TEXT,ref_earned INTEGER NOT NULL DEFAULT 0,payout_method TEXT DEFAULT '',payout_details TEXT DEFAULT '',last_chat_id INTEGER,last_menu_id INTEGER,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ref_code ON users(ref_code);
CREATE TABLE IF NOT EXISTS subscriptions(tg_id INTEGER PRIMARY KEY,plan_code TEXT DEFAULT '',plan_title TEXT DEFAULT '',sub_url TEXT DEFAULT '',expires_at INTEGER,is_active INTEGER NOT NULL DEFAULT 0,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS tariffs(code TEXT PRIMARY KEY,title TEXT NOT NULL,duration_days INTEGER NOT NULL,price_rub INTEGER NOT NULL,sort_order INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS referrals(id INTEGER PRIMARY KEY AUTOINCREMENT,referrer_tg_id INTEGER,invited_tg_id INTEGER,amount_rub INTEGER,percent INTEGER,reward_rub INTEGER,created_at INTEGER);
CREATE TABLE IF NOT EXISTS gifts(id INTEGER PRIMARY KEY AUTOINCREMENT,from_tg_id INTEGER,to_tg_id INTEGER,tariff_code TEXT,tariff_title TEXT,amount_rub INTEGER,created_at INTEGER);
CREATE TABLE IF NOT EXISTS purchases(id INTEGER PRIMARY KEY AUTOINCREMENT,tg_id INTEGER,tariff_code TEXT,tariff_title TEXT,amount_rub INTEGER,kind TEXT,created_at INTEGER);
CREATE TABLE IF NOT EXISTS admin_states(admin_tg_id INTEGER PRIMARY KEY,state TEXT,payload TEXT,updated_at INTEGER);
CREATE TABLE IF NOT EXISTS user_states(tg_id INTEGER PRIMARY KEY,state TEXT,payload TEXT,updated_at INTEGER);
`);
db.prepare("INSERT INTO tariffs(code,title,duration_days,price_rub,sort_order) VALUES('m1','1 месяц',30,100,1) ON CONFLICT(code) DO NOTHING").run();
db.prepare("INSERT INTO tariffs(code,title,duration_days,price_rub,sort_order) VALUES('m6','6 месяцев',180,600,2) ON CONFLICT(code) DO NOTHING").run();
db.prepare("INSERT INTO tariffs(code,title,duration_days,price_rub,sort_order) VALUES('y1','1 год',365,900,3) ON CONFLICT(code) DO NOTHING").run();
["payment_methods","gif_main_menu","gif_purchase_success","gif_gift_success","gif_broadcast"].forEach(k=>db.prepare("INSERT INTO settings(key,value)VALUES(?,?) ON CONFLICT(key) DO NOTHING").run(k,""));
db.prepare("INSERT INTO settings(key,value) VALUES('ref_percent','30') ON CONFLICT(key) DO NOTHING").run();
db.prepare("INSERT INTO settings(key,value) VALUES('ref_withdraw_min','1000') ON CONFLICT(key) DO NOTHING").run();
}

async function tg(m,p){const r=await fetch(`${TG}/${m}`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(p)});const j=await r.json().catch(()=>({}));if(!r.ok||j.ok===false)throw new Error(j.description||`TG ${r.status}`);return j.result;}
async function gif(chatId,key){const g=setting(key,"");if(g)await tg("sendAnimation",{chat_id:chatId,animation:g});}
function restartBot(){try{db.close();}catch{}const child=spawn(process.execPath,[path.join(__dirname,"bot.js")],{cwd:__dirname,detached:true,stdio:"ignore",env:process.env});child.unref();process.exit(0);}
async function exportDbToAdmin(chatId){await tg("sendDocument",{chat_id:chatId,document:fs.createReadStream(DB_FILE),caption:"Текущая база данных"});}
async function importDbFromDocument(fileId){
  const f=await tg("getFile",{file_id:fileId});
  if(!f?.file_path) throw new Error("file_path not found");
  const url=`https://api.telegram.org/file/bot${TOKEN}/${f.file_path}`;
  const resp=await fetch(url);
  if(!resp.ok) throw new Error(`Download error ${resp.status}`);
  const buf=Buffer.from(await resp.arrayBuffer());
  const tmp=`${DB_FILE}.import.tmp`;
  await fsp.writeFile(tmp,buf);
  try{db.close();}catch{}
  await fsp.copyFile(DB_FILE,`${DB_FILE}.backup.${Date.now()}`);
  await fsp.rename(tmp,DB_FILE);
}

function upsertUser(from,chatId){const cur=user(from.id);const ref=cur?.ref_code||crypto.randomBytes(4).toString("hex");db.prepare(`INSERT INTO users(tg_id,username,first_name,balance_rub,referred_by,ref_code,ref_earned,payout_method,payout_details,last_chat_id,created_at,updated_at)
VALUES(@id,@u,@f,0,NULL,@r,0,'','',@c,@t,@t)
ON CONFLICT(tg_id) DO UPDATE SET username=excluded.username,first_name=excluded.first_name,last_chat_id=excluded.last_chat_id,updated_at=excluded.updated_at`).run({id:Number(from.id),u:from.username||"",f:from.first_name||"",r:ref,c:Number(chatId),t:now()});}
function setMenu(id,chatId,mid){db.prepare("UPDATE users SET last_chat_id=?,last_menu_id=?,updated_at=? WHERE tg_id=?").run(Number(chatId),Number(mid),now(),Number(id));}
function findRef(code){return db.prepare("SELECT * FROM users WHERE ref_code=?").get(String(code||"").trim());}
function setRef(uid,rid){const u=user(uid);if(!u||u.referred_by||Number(uid)===Number(rid))return;db.prepare("UPDATE users SET referred_by=?,updated_at=? WHERE tg_id=?").run(Number(rid),now(),Number(uid));}
function updateBalance(uid,delta){const u=user(uid);if(!u)throw new Error("NO_USER");const n=Number(u.balance_rub)+Number(delta);if(n<0)throw new Error("NO_MONEY");db.prepare("UPDATE users SET balance_rub=?,updated_at=? WHERE tg_id=?").run(n,now(),Number(uid));return n;}
function usersPage(page,me,size=8){const p=Math.max(0,Number(page||0)),off=p*size;const items=db.prepare("SELECT tg_id,username,first_name FROM users WHERE tg_id!=? ORDER BY updated_at DESC LIMIT ? OFFSET ?").all(Number(me),size,off);const total=db.prepare("SELECT COUNT(*) c FROM users WHERE tg_id!=?").get(Number(me)).c;return{items,total:Number(total||0),page:p,size};}
function addReferralReward(buyerId,amount){const b=user(buyerId);if(!b||!b.referred_by)return;const r=user(b.referred_by);if(!r)return;const pct=Math.max(0,Math.min(100,Number(setting("ref_percent","30"))||30));const reward=Math.floor((Number(amount)*pct)/100);if(reward<=0)return;updateBalance(r.tg_id,reward);db.prepare("UPDATE users SET ref_earned=ref_earned+?,updated_at=? WHERE tg_id=?").run(reward,now(),Number(r.tg_id));db.prepare("INSERT INTO referrals(referrer_tg_id,invited_tg_id,amount_rub,percent,reward_rub,created_at) VALUES(?,?,?,?,?,?)").run(Number(r.tg_id),Number(buyerId),Number(amount),pct,reward,now());}
function setAdminState(id,state,payload=""){db.prepare("INSERT INTO admin_states(admin_tg_id,state,payload,updated_at) VALUES(?,?,?,?) ON CONFLICT(admin_tg_id) DO UPDATE SET state=excluded.state,payload=excluded.payload,updated_at=excluded.updated_at").run(Number(id),String(state),String(payload),now());}
function getAdminState(id){return db.prepare("SELECT * FROM admin_states WHERE admin_tg_id=?").get(Number(id));}
function clearAdminState(id){db.prepare("DELETE FROM admin_states WHERE admin_tg_id=?").run(Number(id));}
function setUserState(id,state,payload=""){db.prepare("INSERT INTO user_states(tg_id,state,payload,updated_at) VALUES(?,?,?,?) ON CONFLICT(tg_id) DO UPDATE SET state=excluded.state,payload=excluded.payload,updated_at=excluded.updated_at").run(Number(id),String(state),String(payload),now());}
function getUserState(id){return db.prepare("SELECT * FROM user_states WHERE tg_id=?").get(Number(id));}
function clearUserState(id){db.prepare("DELETE FROM user_states WHERE tg_id=?").run(Number(id));}

async function editOrSend(chatId,msgId,text,kb){try{if(msgId){await tg("editMessageText",{chat_id:chatId,message_id:msgId,text,parse_mode:"HTML",disable_web_page_preview:true,reply_markup:kb});return Number(msgId);}}catch(e){if(String(e.message).includes("message is not modified"))return Number(msgId);}const m=await tg("sendMessage",{chat_id:chatId,text,parse_mode:"HTML",disable_web_page_preview:true,reply_markup:kb});return Number(m.message_id);} 

function homeText(u){return[`👋 Привет, <b>${esc(u.first_name||"друг")}</b>`,``,`— Ваш ID: <code>${u.tg_id}</code>`,`— Ваш баланс: <b>${rub(u.balance_rub)}</b>`,``,`📣 Новостной канал — ${esc(NEWS_URL)}`,`👤 Техническая поддержка — ${esc(SUPPORT_URL)}`].join("\n");}
function homeKb(uid){const rows=[[{text:"🔐 Моя подписка",callback_data:"v:sub"}],[{text:"⭐ Купить подписку",callback_data:"v:buy"}],[{text:"💵 Мой баланс",callback_data:"v:bal"}],[{text:"🎁 Подарить подписку",callback_data:"v:gift"}],[{text:"🤝 Партнёрская программа",callback_data:"v:ref"}],[{text:"📘 Инструкции",callback_data:"v:guide"}]];if(isAdmin(uid))rows.push([{text:"🛠 Админ панель",callback_data:"a:main"}]);return{inline_keyboard:rows};}
function buyText(uid){const lines=["⭐ Тарифы",""];tariffs().forEach(t=>lines.push(`• ${t.title} — <b>${rub(t.price_rub)}</b>`));lines.push("",activeSub(sub(uid))?"У вас уже есть активная подписка. Можно только продлить.":"Выберите тариф для покупки.");return lines.join("\n");}
function buyKb(uid){const act=activeSub(sub(uid));const rows=tariffs().map(t=>[{text:`${act?"⏳ Продлить":"🛍 Купить"} ${t.title} — ${rub(t.price_rub)}`,callback_data:`${act?"pay:r:":"pay:n:"}${t.code}`}]);rows.push([{text:"⬅️ Назад",callback_data:"v:home"}]);return{inline_keyboard:rows};}
function subText(uid){const s=sub(uid);if(!activeSub(s))return["🔑 Информация о подписке","","Активная подписка не найдена.","Оформите тариф в разделе «Купить подписку»."].join("\n");const mins=Math.max(0,Math.floor((s.expires_at-now())/60000)),dd=Math.floor(mins/1440),hh=Math.floor((mins%1440)/60),mm=mins%60;return["🔑 Информация о подписке","",`Ссылка: ${esc(s.sub_url)}`,"",`⌛ Осталось: <b>${dd} дн. ${hh} ч. ${mm} мин.</b>`,`Истекает: <b>${d(s.expires_at)}</b>`,``,`📦 Тариф: <b>${esc(s.plan_title||s.plan_code||"—")}</b>`,`📱 Лимит устройств: <b>3</b>`,``,`Подключите устройство по кнопкам ниже 👇`].join("\n");}
function subKb(uid){const s=sub(uid),rows=[];if(activeSub(s))rows.push([{text:"📲 Установить подписку",url:s.sub_url}]);rows.push([{text:"⏳ Продлить подписку",callback_data:"v:buy"}],[{text:"♻️ Сбросить привязку",callback_data:"sub:reset"}],[{text:"📺 Подключить TV",callback_data:"sub:tv"}],[{text:"❌ Удалить",callback_data:"sub:del"}],[{text:"👤 Личный кабинет",callback_data:"v:home"}]);return{inline_keyboard:rows};}
function refText(u){const st=db.prepare("SELECT COUNT(*) c, COALESCE(SUM(reward_rub),0) s FROM referrals WHERE referrer_tg_id=?").get(Number(u.tg_id));const pct=Number(setting("ref_percent","30"))||30;const min=Number(setting("ref_withdraw_min","1000"))||1000;const link=`${BOT_USERNAME?`https://t.me/${BOT_USERNAME}`:"https://t.me/<бот>"}?start=partner_${u.ref_code}`;return["👥 Партнёрская программа","","🎁 Зарабатывайте вместе с нами!",`1) Приглашайте друзей и получайте <b>${pct}%</b> с каждой оплаты.`,`2) Используйте бонусы для продления и подарков.`,``,`🔗 Ваша ссылка:\n${link}`,``,`📊 Ваша статистика:`,`• Приглашено: <b>${st.c||0}</b>`,`• Заработано: <b>${rub(st.s||0)}</b>`,`• Баланс: <b>${rub(u.balance_rub)}</b>`,`• Способ вывода: <b>${esc(u.payout_method||"не задан")}</b>`,``,`💸 Вывод доступен от ${rub(min)}`].join("\n");}
const refKb=()=>({inline_keyboard:[[{text:"💰 Вывести средства",callback_data:"ref:w"}],[{text:"🧾 Способ вывода",callback_data:"ref:p"}],[{text:"✉️ Пригласить друзей",callback_data:"ref:i"}],[{text:"🔄 Сменить код ссылки",callback_data:"ref:r"}],[{text:"⬅️ Назад",callback_data:"v:home"}]]});
const payoutMethodKb=()=>({inline_keyboard:[[{text:"💳 Карта (RU)",callback_data:"ref:pm:card"}],[{text:"🪙 USDT (TRC20)",callback_data:"ref:pm:usdt_trc20"}],[{text:"🏦 СБП",callback_data:"ref:pm:sbp"}],[{text:"⬅️ Вернуться в партнёрку",callback_data:"v:ref"}]]});
const back=()=>({inline_keyboard:[[{text:"⬅️ Назад",callback_data:"v:home"}]]});
function giftUsersKb(sender,code,page){const{items,total,page:p,size}=usersPage(page,sender,8),max=Math.max(0,Math.ceil(total/size)-1);const rows=items.map(u=>[{text:`${u.first_name||u.username||u.tg_id} (${u.username?`@${u.username}`:`id:${u.tg_id}`})`,callback_data:`g:u:${code}:${u.tg_id}`}]);const nav=[];if(p>0)nav.push({text:"⬅️",callback_data:`g:l:${code}:${p-1}`});nav.push({text:`${p+1}/${max+1}`,callback_data:"noop"});if(p<max)nav.push({text:"➡️",callback_data:`g:l:${code}:${p+1}`});rows.push(nav,[{text:"⬅️ Назад",callback_data:"v:gift"}]);return{inline_keyboard:rows};}
async function requestGiftRecipient(uid,chatId,code){
  setUserState(uid,"gift_pick",code);
  await tg("sendMessage",{
    chat_id:chatId,
    text:"Выберите пользователя в системном меню Telegram.",
    reply_markup:{
      keyboard:[
        [{text:"Выбрать пользователя",request_user:{request_id:1,user_is_bot:false}}],
        [{text:"Отмена выбора"}]
      ],
      resize_keyboard:true,
      one_time_keyboard:true
    }
  });
}

async function render(uid,chatId,msgId,view,data={}){const u=user(uid);if(!u)return;let t=homeText(u),kb=homeKb(uid);
if(view==="home"){t=homeText(u);kb=homeKb(uid);}else if(view==="buy"){t=buyText(uid);kb=buyKb(uid);}else if(view==="sub"){t=subText(uid);kb=subKb(uid);}else if(view==="bal"){t=`💵 Мой баланс\n\nТекущий баланс: <b>${rub(u.balance_rub)}</b>`;kb={inline_keyboard:[[{text:"⭐ Купить подписку",callback_data:"v:buy"}],[{text:"💳 Способы оплаты",callback_data:"v:pay"}],[{text:"⬅️ Назад",callback_data:"v:home"}]]};}
else if(view==="pay"){t=`💳 Способы оплаты\n\n${esc(setting("payment_methods","Пока не настроено."))}`;kb={inline_keyboard:[[{text:"⬅️ Назад",callback_data:"v:bal"}]]};}
else if(view==="guide"){t="📘 Инструкции\n\n1) Купите подписку.\n2) Откройте «Моя подписка».\n3) Нажмите «Установить подписку».\n4) Импортируйте ссылку в клиент.";kb=back();}
else if(view==="about"){t="💬 О сервисе\n\nНадёжная VPN подписка с быстрой выдачей ссылки и продлением через Telegram.";kb=back();}
else if(view==="ref"){t=refText(u);kb=refKb();}
else if(view==="ref_payout"){t="Выберите способ вывода. Текущий метод будет обновлён после проверки реквизитов.";kb=payoutMethodKb();}
else if(view==="gift"){t="🎁 Подарить подписку\n\nВыберите тариф:";kb={inline_keyboard:[...tariffs().map(x=>[{text:`🎁 ${x.title} — ${rub(x.price_rub)}`,callback_data:`g:p:${x.code}`}]),[{text:"⬅️ Назад",callback_data:"v:home"}]]};}
else if(view==="gift_users"){const tr=tariff(data.code);t=tr?`🎁 Подарок: <b>${esc(tr.title)}</b>\nВыберите получателя:`:"Тариф не найден.";kb=tr?giftUsersKb(uid,tr.code,data.page||0):back();}
else if(view==="a_main"){t="🛠 Админ панель";kb={inline_keyboard:[[{text:"💸 Цены тарифов",callback_data:"a:t"}],[{text:"🎞 GIF",callback_data:"a:g"}],[{text:"📨 Рассылка",callback_data:"a:b"}],[{text:"💳 Способы оплаты",callback_data:"a:p"}],[{text:"🤝 Партнёрские настройки",callback_data:"a:r"}],[{text:"🗄 База данных",callback_data:"a:db"}],[{text:"⬅️ Назад",callback_data:"v:home"}]]};}
else if(view==="a_db"){t="🗄 База данных\n\nМожно скачать текущую БД или импортировать новую SQLite базу.";kb={inline_keyboard:[[{text:"⬇️ Скачать БД",callback_data:"a:db_export"}],[{text:"⬆️ Импорт БД",callback_data:"a:db_import_start"}],[{text:"⬅️ Назад",callback_data:"a:main"}]]};}
else if(view==="a_tariffs"){t=`💸 Цены тарифов\n\n${tariffs().map(x=>`• ${x.title}: <b>${rub(x.price_rub)}</b>`).join("\n")}`;kb={inline_keyboard:[...tariffs().map(x=>[{text:`${x.title}: ${rub(x.price_rub)}`,callback_data:`a:te:${x.code}`}]),[{text:"⬅️ Назад",callback_data:"a:main"}]]};}
else if(view==="a_gif"){t="🎞 GIF для сообщений";kb={inline_keyboard:[[{text:"Главное меню",callback_data:"a:ge:gif_main_menu"}],[{text:"Успешная покупка",callback_data:"a:ge:gif_purchase_success"}],[{text:"Успешный подарок",callback_data:"a:ge:gif_gift_success"}],[{text:"Рассылка",callback_data:"a:ge:gif_broadcast"}],[{text:"⬅️ Назад",callback_data:"a:main"}]]};}
else if(view==="a_bcast"){t="📨 Рассылка\n\nНажмите кнопку и отправьте текст следующим сообщением.";kb={inline_keyboard:[[{text:"✏️ Начать",callback_data:"a:bs"}],[{text:"⬅️ Назад",callback_data:"a:main"}]]};}
else if(view==="a_pay"){t=`💳 Способы оплаты\n\n${esc(setting("payment_methods","Пока пусто."))}`;kb={inline_keyboard:[[{text:"✏️ Изменить",callback_data:"a:pe"}],[{text:"⬅️ Назад",callback_data:"a:main"}]]};}
else if(view==="a_ref"){t=`🤝 Партнёрские настройки\n\nСтавка: <b>${setting("ref_percent","30")}%</b>\nМин. вывод: <b>${rub(setting("ref_withdraw_min","1000"))}</b>`;kb={inline_keyboard:[[{text:"✏️ Ставка",callback_data:"a:rp"}],[{text:"✏️ Мин. вывод",callback_data:"a:rm"}],[{text:"⬅️ Назад",callback_data:"a:main"}]]};}
const nm=await editOrSend(chatId,msgId,t,kb);setMenu(uid,chatId,nm);} 
async function createSub(target,tr,giftMode){
  const r=await fetch(`${API}/api/bot-subscription`,{method:"POST",headers:{"Content-Type":"application/json","x-app-secret":APP_SECRET},body:JSON.stringify({telegramUserId:String(target.tg_id),telegramUsername:target.username||"",firstName:target.first_name||"",durationDays:tr.duration_days,name:`VPN ${tr.title}`,description:giftMode?`Подарок: ${tr.title}`:`Тариф: ${tr.title}`})});
  const j=await r.json().catch(()=>({}));if(!r.ok)throw new Error(j.error||`API ${r.status}`);return j;
}

async function doPurchase(payerId,receiverId,code,kind){
  const payer=user(payerId),receiver=user(receiverId),tr=tariff(code);if(!payer||!receiver||!tr)throw new Error("INVALID");
  const s=sub(receiverId),act=activeSub(s);
  if(kind==="new"&&act)throw new Error("ACTIVE");
  if(kind==="renew"&&!act)throw new Error("NO_ACTIVE");
  if(Number(payer.balance_rub)<Number(tr.price_rub))throw new Error("NO_MONEY");
  const api=await createSub(receiver,tr,kind==="gift");
  const tx=db.transaction(()=>{
    updateBalance(payerId,-Number(tr.price_rub));
    if(payerId===receiverId)addReferralReward(receiverId,tr.price_rub);
    db.prepare("INSERT INTO subscriptions(tg_id,plan_code,plan_title,sub_url,expires_at,is_active,created_at,updated_at) VALUES(?,?,?,?,?,1,?,?) ON CONFLICT(tg_id) DO UPDATE SET plan_code=excluded.plan_code,plan_title=excluded.plan_title,sub_url=excluded.sub_url,expires_at=excluded.expires_at,is_active=1,updated_at=excluded.updated_at").run(Number(receiverId),tr.code,tr.title,api.subscriptionUrl,Number(api.subscription?.expiresAt||0),now(),now());
    db.prepare("INSERT INTO purchases(tg_id,tariff_code,tariff_title,amount_rub,kind,created_at) VALUES(?,?,?,?,?,?)").run(Number(payerId),tr.code,tr.title,Number(tr.price_rub),kind,now());
    if(kind==="gift")db.prepare("INSERT INTO gifts(from_tg_id,to_tg_id,tariff_code,tariff_title,amount_rub,created_at) VALUES(?,?,?,?,?,?)").run(Number(payerId),Number(receiverId),tr.code,tr.title,Number(tr.price_rub),now());
  });tx();
  return{tr,url:api.subscriptionUrl,exp:Number(api.subscription?.expiresAt||0)};
}

async function buySelf(uid,chatId,msgId,code,mode,cbid){
  try{
    const res=await doPurchase(uid,uid,code,mode);
    await gif(chatId,"gif_purchase_success");
    const me=user(uid);
    const text=["✅ Оплата успешна","",`Тариф: <b>${esc(res.tr.title)}</b>`,`Списано: <b>${rub(res.tr.price_rub)}</b>`,`Баланс: <b>${rub(me.balance_rub)}</b>`,`Истекает: <b>${d(res.exp)}</b>`,``,`Ссылка: ${esc(res.url)}`].join("\n");
    const kb={inline_keyboard:[[{text:"📲 Установить подписку",url:res.url}],[{text:"🔐 Моя подписка",callback_data:"v:sub"}],[{text:"👤 Личный кабинет",callback_data:"v:home"}]]};
    const nm=await editOrSend(chatId,msgId,text,kb);setMenu(uid,chatId,nm);
    await tg("answerCallbackQuery",{callback_query_id:cbid,text:"Готово",show_alert:false});
  }catch(e){
    const msg=e.message==="ACTIVE"?"Подписка уже активна. Можно только продлить.":e.message==="NO_ACTIVE"?"Нет активной подписки для продления.":e.message==="NO_MONEY"?"Недостаточно средств.":"Ошибка оплаты.";
    await tg("answerCallbackQuery",{callback_query_id:cbid,text:msg,show_alert:true});
    if(e.message==="NO_MONEY")await render(uid,chatId,msgId,"bal");
  }
}

async function askBuyConfirm(uid,chatId,msgId,code,mode,cbid){
  const tr=tariff(code);
  if(!tr){await tg("answerCallbackQuery",{callback_query_id:cbid,text:"Тариф не найден",show_alert:true});return;}
  const title=mode==="renew"?"подтверждение продления":"подтверждение покупки";
  const text=[`🧾 ${title}`,"",`Тариф: <b>${esc(tr.title)}</b>`,`Стоимость: <b>${rub(tr.price_rub)}</b>`,"","Подтвердите оплату."].join("\n");
  const kb={inline_keyboard:[[{text:"✅ Подтвердить",callback_data:`pay:c:${mode}:${code}`}],[{text:"⬅️ Отмена",callback_data:"v:buy"}]]};
  const nm=await editOrSend(chatId,msgId,text,kb);setMenu(uid,chatId,nm);
  await tg("answerCallbackQuery",{callback_query_id:cbid,text:"Проверьте детали",show_alert:false});
}

async function giftToUser(fromId,toId,code,chatId,msgId,cbid){
  try{
    const res=await doPurchase(fromId,toId,code,"gift");
    await gif(chatId,"gif_gift_success");
    const to=user(toId),me=user(fromId);
    const text=["🎁 Подарок отправлен","",`Получатель: <b>${esc(to?.first_name||to?.username||toId)}</b>`,`Тариф: <b>${esc(res.tr.title)}</b>`,`Списано: <b>${rub(res.tr.price_rub)}</b>`,`Ваш баланс: <b>${rub(me.balance_rub)}</b>`].join("\n");
    const nm=await editOrSend(chatId,msgId,text,{inline_keyboard:[[{text:"🎁 Новый подарок",callback_data:"v:gift"}],[{text:"👤 Личный кабинет",callback_data:"v:home"}]]});setMenu(fromId,chatId,nm);
    if(to)await tg("sendMessage",{chat_id:to.tg_id,text:`🎁 Вам подарили подписку ${res.tr.title}\nИстекает: ${d(res.exp)}\nСсылка: ${res.url}`});
    if(cbid) await tg("answerCallbackQuery",{callback_query_id:cbid,text:"Подарок отправлен",show_alert:false});
  }catch(e){
    const msg=e.message==="NO_MONEY"?"Недостаточно средств.":e.message==="ACTIVE"?"У получателя уже активна подписка.":"Ошибка отправки подарка.";
    if(cbid) await tg("answerCallbackQuery",{callback_query_id:cbid,text:msg,show_alert:true});
    else await tg("sendMessage",{chat_id:chatId,text:msg});
  }
}

async function handleAdminState(msg){
  const aid=Number(msg.from?.id||0);if(!isAdmin(aid))return false;
  const row=getAdminState(aid);if(!row)return false;
  const text=String(msg.text||"").trim(),chatId=Number(msg.chat?.id||0);
  if(text==="/cancel"){clearAdminState(aid);await tg("sendMessage",{chat_id:chatId,text:"Отменено."});return true;}
  if(row.state==="db_import_wait"){
    if(!msg.document?.file_id){await tg("sendMessage",{chat_id:chatId,text:"Ожидаю документ SQLite (.db/.sqlite)."});return true;}
    try{
      await tg("sendMessage",{chat_id:chatId,text:"Импортирую базу данных..."});
      await importDbFromDocument(msg.document.file_id);
      clearAdminState(aid);
      await tg("sendMessage",{chat_id:chatId,text:"Импорт завершён. Перезапускаю бота..."});
      setTimeout(()=>restartBot(),500);
    }catch(e){
      await tg("sendMessage",{chat_id:chatId,text:`Ошибка импорта: ${e.message}`});
    }
    return true;
  }
  if(row.state==="tariff_price"){const n=Number(text);if(!Number.isFinite(n)||n<=0){await tg("sendMessage",{chat_id:chatId,text:"Введите корректную цену."});return true;}db.prepare("UPDATE tariffs SET price_rub=? WHERE code=?").run(Math.round(n),row.payload);clearAdminState(aid);await tg("sendMessage",{chat_id:chatId,text:"Цена обновлена."});return true;}
  if(row.state==="gif"){let v=text;if(msg.animation?.file_id)v=msg.animation.file_id;if(!v){await tg("sendMessage",{chat_id:chatId,text:"Отправьте GIF или file_id."});return true;}setSetting(row.payload,v);clearAdminState(aid);await tg("sendMessage",{chat_id:chatId,text:"Сохранено."});return true;}
  if(row.state==="pay_methods"){setSetting("payment_methods",text);clearAdminState(aid);await tg("sendMessage",{chat_id:chatId,text:"Обновлено."});return true;}
  if(row.state==="broadcast"){clearAdminState(aid);const ids=db.prepare("SELECT tg_id FROM users").all();let ok=0,fail=0;for(const u of ids){try{const g=setting("gif_broadcast","");if(g)await tg("sendAnimation",{chat_id:u.tg_id,animation:g,caption:text,parse_mode:"HTML"});else await tg("sendMessage",{chat_id:u.tg_id,text,parse_mode:"HTML"});ok++;}catch{fail++;}}await tg("sendMessage",{chat_id:chatId,text:`Рассылка завершена. Успешно: ${ok}, ошибок: ${fail}`});return true;}
  if(row.state==="ref_percent"){const n=Number(text);if(!Number.isFinite(n)||n<0||n>100){await tg("sendMessage",{chat_id:chatId,text:"Введите 0..100"});return true;}setSetting("ref_percent",Math.round(n));clearAdminState(aid);await tg("sendMessage",{chat_id:chatId,text:"Ставка обновлена."});return true;}
  if(row.state==="ref_min"){const n=Number(text);if(!Number.isFinite(n)||n<0){await tg("sendMessage",{chat_id:chatId,text:"Введите сумму в рублях."});return true;}setSetting("ref_withdraw_min",Math.round(n));clearAdminState(aid);await tg("sendMessage",{chat_id:chatId,text:"Минимум обновлён."});return true;}
  return false;
}
async function handleMessage(msg){
  const from=msg.from||{},chatId=Number(msg.chat?.id||0);if(!chatId||!from.id)return;
  upsertUser(from,chatId);
  const ustate=getUserState(from.id);

  if(msg.text==="Отмена выбора"&&ustate?.state==="gift_pick"){
    clearUserState(from.id);
    await tg("sendMessage",{chat_id:chatId,text:"Выбор получателя отменён.",reply_markup:{remove_keyboard:true}});
    await render(from.id,chatId,user(from.id)?.last_menu_id,"gift");
    return;
  }

  if(msg.user_shared&&ustate?.state==="gift_pick"){
    const recipientId=Number(msg.user_shared.user_id||0);
    const code=ustate.payload||"";
    clearUserState(from.id);
    await tg("sendMessage",{chat_id:chatId,text:"Получатель выбран.",reply_markup:{remove_keyboard:true}});
    if(!user(recipientId)){
      await tg("sendMessage",{chat_id:chatId,text:"Пользователь не зарегистрирован в боте. Попросите его нажать /start."});
      await render(from.id,chatId,user(from.id)?.last_menu_id,"gift");
      return;
    }
    await giftToUser(from.id,recipientId,code,chatId,user(from.id)?.last_menu_id||null,null);
    return;
  }

  if(ustate?.state==="ref_payout_details"&&msg.text&&!msg.text.startsWith("/")){
    const method=ustate.payload||"Неизвестно";
    db.prepare("UPDATE users SET payout_method=?, payout_details=?, updated_at=? WHERE tg_id=?")
      .run(method,String(msg.text).trim(),now(),Number(from.id));
    clearUserState(from.id);
    await tg("sendMessage",{chat_id:chatId,text:"Реквизиты сохранены. Метод будет применён после проверки."});
    await render(from.id,chatId,user(from.id)?.last_menu_id||null,"ref");
    return;
  }

  if(await handleAdminState(msg))return;

  const text=String(msg.text||"").trim();
  if(isAdmin(from.id)&&text.startsWith("/add_balance")){
    const p=text.split(/\s+/);if(p.length!==3){await tg("sendMessage",{chat_id:chatId,text:"Формат: /add_balance <id> <amount>"});return;}
    const id=Number(p[1]),amount=Number(p[2]);if(!user(id)||!Number.isFinite(amount)){await tg("sendMessage",{chat_id:chatId,text:"Неверные параметры."});return;}
    const nb=updateBalance(id,amount);await tg("sendMessage",{chat_id:chatId,text:`Новый баланс: ${rub(nb)}`});return;
  }

  if(text.startsWith("/start")){
    const m=text.match(/^\/start\s+partner_([a-zA-Z0-9]+)$/);if(m){const r=findRef(m[1]);if(r)setRef(from.id,r.tg_id);}
    await gif(chatId,"gif_main_menu");
    await render(from.id,chatId,null,"home");
    return;
  }
  if(text==="/menu"){await render(from.id,chatId,null,"home");return;}
  if(text==="/admin"&&isAdmin(from.id)){await render(from.id,chatId,user(from.id)?.last_menu_id,"a_main");return;}
  await tg("sendMessage",{chat_id:chatId,text:"Используйте /start для открытия меню."});
}

async function handleCallback(q){
  const data=q.data||"",uid=Number(q.from?.id||0),chatId=Number(q.message?.chat?.id||0),msgId=Number(q.message?.message_id||0);
  if(!uid||!chatId||!msgId)return;upsertUser(q.from,chatId);
  if(data==="noop"){await tg("answerCallbackQuery",{callback_query_id:q.id});return;}
  if(data.startsWith("a:")&&!isAdmin(uid)){await tg("answerCallbackQuery",{callback_query_id:q.id,text:"Недостаточно прав",show_alert:true});return;}

  if(data.startsWith("v:")){const map={home:"home",sub:"sub",buy:"buy",bal:"bal",gift:"gift",ref:"ref",guide:"guide",about:"about",pay:"pay"};await render(uid,chatId,msgId,map[data.slice(2)]||"home");await tg("answerCallbackQuery",{callback_query_id:q.id});return;}
  if(data.startsWith("pay:n:")){await askBuyConfirm(uid,chatId,msgId,data.split(":")[2],"new",q.id);return;}
  if(data.startsWith("pay:r:")){await askBuyConfirm(uid,chatId,msgId,data.split(":")[2],"renew",q.id);return;}
  if(data.startsWith("pay:c:")){const[,,mode,code]=data.split(":");await buySelf(uid,chatId,msgId,code,mode,q.id);return;}
  if(data==="sub:del"){db.prepare("UPDATE subscriptions SET is_active=0,updated_at=? WHERE tg_id=?").run(now(),uid);await tg("answerCallbackQuery",{callback_query_id:q.id,text:"Подписка скрыта",show_alert:false});await render(uid,chatId,msgId,"sub");return;}
  if(data==="sub:reset"){await tg("answerCallbackQuery",{callback_query_id:q.id,text:"Сброс применится при новом импорте.",show_alert:true});return;}
  if(data==="sub:tv"){await tg("answerCallbackQuery",{callback_query_id:q.id,text:"Откройте ссылку подписки на TV и импортируйте в клиент.",show_alert:true});return;}

  if(data==="ref:i"){const u=user(uid);const link=`${BOT_USERNAME?`https://t.me/${BOT_USERNAME}`:"https://t.me/<бот>"}?start=partner_${u.ref_code}`;await tg("sendMessage",{chat_id:chatId,text:`Ваша ссылка:\n${link}`});await tg("answerCallbackQuery",{callback_query_id:q.id,text:"Отправлено",show_alert:false});return;}
  if(data==="ref:r"){db.prepare("UPDATE users SET ref_code=?,updated_at=? WHERE tg_id=?").run(crypto.randomBytes(4).toString("hex"),now(),uid);await render(uid,chatId,msgId,"ref");await tg("answerCallbackQuery",{callback_query_id:q.id,text:"Код обновлён",show_alert:false});return;}
  if(data==="ref:p"){await render(uid,chatId,msgId,"ref_payout");await tg("answerCallbackQuery",{callback_query_id:q.id});return;}
  if(data.startsWith("ref:pm:")){
    const methodMap={card:"Карта (RU)",usdt_trc20:"USDT (TRC20)",sbp:"СБП"};
    const key=data.split(":")[2];
    const method=methodMap[key]||"Неизвестно";
    setUserState(uid,"ref_payout_details",method);
    await tg("answerCallbackQuery",{callback_query_id:q.id,text:`Метод: ${method}`,show_alert:false});
    await tg("sendMessage",{chat_id:chatId,text:`Вы выбрали: ${method}\nТеперь отправьте реквизиты одним сообщением.`});
    return;
  }
  if(data==="ref:w"){const min=Number(setting("ref_withdraw_min","1000"))||1000,u=user(uid);if(Number(u.balance_rub)<min){await tg("answerCallbackQuery",{callback_query_id:q.id,text:`Минимум: ${rub(min)}`,show_alert:true});return;}if(!u.payout_method||!u.payout_details){await tg("answerCallbackQuery",{callback_query_id:q.id,text:"Сначала укажите способ вывода и реквизиты.",show_alert:true});await render(uid,chatId,msgId,"ref_payout");return;}await tg("sendMessage",{chat_id:chatId,text:"Заявка на вывод отправлена администратору."});await tg("sendMessage",{chat_id:ADMIN_ID,text:`Запрос на вывод от ${uid}. Баланс: ${rub(u.balance_rub)}.\nМетод: ${u.payout_method}\nРеквизиты: ${u.payout_details}`});await tg("answerCallbackQuery",{callback_query_id:q.id,text:"Заявка отправлена",show_alert:false});return;}

  if(data.startsWith("g:p:")){await requestGiftRecipient(uid,chatId,data.split(":")[2]);await tg("answerCallbackQuery",{callback_query_id:q.id,text:"Открыл выбор получателя",show_alert:false});return;}
  if(data.startsWith("g:l:")){const[,,code,page]=data.split(":");await render(uid,chatId,msgId,"gift_users",{code,page:Number(page||0)});await tg("answerCallbackQuery",{callback_query_id:q.id});return;}
  if(data.startsWith("g:u:")){const[,,code,rid]=data.split(":");await giftToUser(uid,Number(rid),code,chatId,msgId,q.id);return;}

  if(data==="a:main"){await render(uid,chatId,msgId,"a_main");await tg("answerCallbackQuery",{callback_query_id:q.id});return;}
  if(data==="a:t"){await render(uid,chatId,msgId,"a_tariffs");await tg("answerCallbackQuery",{callback_query_id:q.id});return;}
  if(data.startsWith("a:te:")){setAdminState(uid,"tariff_price",data.split(":")[2]);await tg("sendMessage",{chat_id:chatId,text:"Введите новую цену тарифа.\n/cancel для отмены."});await tg("answerCallbackQuery",{callback_query_id:q.id});return;}
  if(data==="a:g"){await render(uid,chatId,msgId,"a_gif");await tg("answerCallbackQuery",{callback_query_id:q.id});return;}
  if(data.startsWith("a:ge:")){setAdminState(uid,"gif",data.split(":")[2]);await tg("sendMessage",{chat_id:chatId,text:"Отправьте GIF или file_id.\n/cancel для отмены."});await tg("answerCallbackQuery",{callback_query_id:q.id});return;}
  if(data==="a:b"){await render(uid,chatId,msgId,"a_bcast");await tg("answerCallbackQuery",{callback_query_id:q.id});return;}
  if(data==="a:bs"){setAdminState(uid,"broadcast","");await tg("sendMessage",{chat_id:chatId,text:"Отправьте текст рассылки.\n/cancel для отмены."});await tg("answerCallbackQuery",{callback_query_id:q.id});return;}
  if(data==="a:p"){await render(uid,chatId,msgId,"a_pay");await tg("answerCallbackQuery",{callback_query_id:q.id});return;}
  if(data==="a:pe"){setAdminState(uid,"pay_methods","");await tg("sendMessage",{chat_id:chatId,text:"Отправьте новый текст способов оплаты.\n/cancel для отмены."});await tg("answerCallbackQuery",{callback_query_id:q.id});return;}
  if(data==="a:r"){await render(uid,chatId,msgId,"a_ref");await tg("answerCallbackQuery",{callback_query_id:q.id});return;}
  if(data==="a:db"){await render(uid,chatId,msgId,"a_db");await tg("answerCallbackQuery",{callback_query_id:q.id});return;}
  if(data==="a:db_export"){await tg("answerCallbackQuery",{callback_query_id:q.id,text:"Отправляю файл",show_alert:false});await exportDbToAdmin(chatId);return;}
  if(data==="a:db_import_start"){setAdminState(uid,"db_import_wait","");await tg("answerCallbackQuery",{callback_query_id:q.id,text:"Жду файл .db/.sqlite",show_alert:false});await tg("sendMessage",{chat_id:chatId,text:"Отправьте файл базы данных SQLite документом.\nПосле импорта бот перезапустится."});return;}
  if(data==="a:rp"){setAdminState(uid,"ref_percent","");await tg("sendMessage",{chat_id:chatId,text:"Введите новую ставку в процентах (0..100)."});await tg("answerCallbackQuery",{callback_query_id:q.id});return;}
  if(data==="a:rm"){setAdminState(uid,"ref_min","");await tg("sendMessage",{chat_id:chatId,text:"Введите минимальную сумму вывода."});await tg("answerCallbackQuery",{callback_query_id:q.id});return;}

  await tg("answerCallbackQuery",{callback_query_id:q.id,text:"Неизвестная команда",show_alert:false});
}

async function poll(){while(true){try{const ups=await tg("getUpdates",{timeout:30,offset,allowed_updates:["message","callback_query"]});for(const u of ups){offset=u.update_id+1;if(u.message)await handleMessage(u.message);else if(u.callback_query)await handleCallback(u.callback_query);}}catch{await new Promise(r=>setTimeout(r,1500));}}}

init();
poll();
