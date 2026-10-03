// 演示用“订阅端”（聚合商通知的接收方）。独立进程、零依赖。
//
// 职责：
//  - POST /validate ：订阅校验握手——回源聚合商 JWKS 验签后回显 challenge；
//  - POST /         ：接收通知，先验签（含轮换后的旧公钥），再按幂等键去重，
//                    再依据“故障模式”决定如何应答，用于演示各种投递场景；
//  - GET  /admin    ：故障模式控制台 + 已收通知（含验签证据）。
//
// 故障模式（通过 PUT /admin/mode 切换）：
//  ok        一切正常（200）
//  fail      持续 503（直到切回 ok）——用于触发重试耗尽 -> 死信
//  failOnce  对“首次见到的信封”失败一次，随后恢复——暂时失败后顺序恢复
//  timeout   挂起不响应，逼聚合商超时，用【同一幂等密钥】再次请求
//  ackloss   内部已处理并按幂等键去重，但对第一次应答 500——模拟 2xx 确认丢失，
//            重试时回 200 且 duplicate=true，证明幂等重投安全
import { createServer } from 'node:http';
import { appendFileSync } from 'node:fs';
import { verifySignature } from '../src/notify/crypto.js';

const PORT = Number(process.env.SUB_PORT || 9100);
const NAME = process.env.SUB_NAME || `订阅端-${PORT}`;
const AGGREGATOR = process.env.AGGREGATOR_BASE || 'http://localhost:8080';
const DEBUG_LOG = process.env.SUB_DEBUG || null;
function dbg(msg) {
  if (!DEBUG_LOG) return;
  try { appendFileSync(DEBUG_LOG, `${new Date().toISOString()} pid=${process.pid} port=${PORT} ${msg}\n`); } catch { /* 忽略 */ }
}

// ---- 状态 ----
let mode = process.env.SUB_MODE || 'ok';
let modeNote = '';
const inbox = [];                 // 已【处理】的通知（按幂等键去重）
const seenKeys = new Map();       // idempotencyKey -> 首次处理信息
const attempts = [];              // 每次收到的投递（含验签结果）
let jwksCache = null;
let jwksFetchedAt = 0;
const bootedAt = Date.now();

async function fetchJwks(force = false) {
  // 短 TTL 缓存：轮换演示中退避重投（≥2s）后即可看到旧密钥已变 RETIRED；
  // kid 未知（轮换后首次见更老的信封）时 authenticate 会强制回源。
  if (!force && jwksCache && Date.now() - jwksFetchedAt < 3000) return jwksCache;
  const res = await fetch(AGGREGATOR + '/api/notify/jwks');
  if (!res.ok) throw new Error('JWKS 回源失败 ' + res.status);
  const data = await res.json();
  jwksCache = data.jwks || data;
  jwksFetchedAt = Date.now();
  return jwksCache;
}

function send(res, status, obj, headers = {}) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', ...headers });
  res.end(body);
}

function readRaw(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (c) => { data += c; if (data.length > 5_000_000) reject(new Error('too large')); });
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}

// 验签：kid 未知（比如轮换后第一次见旧信封）时强制回源 JWKS 再验。
async function authenticate(req, rawBody) {
  const kid = req.headers['x-dr-key-id'];
  const timestamp = req.headers['x-dr-timestamp'];
  const signature = req.headers['x-dr-signature'];
  let jwks = await fetchJwks(false);
  let result = await verifySignature({ jwks, kid, timestamp, body: rawBody, signature });
  if (!result.ok && result.reason === 'UNKNOWN_SIGNING_KEY') {
    jwks = await fetchJwks(true);
    result = await verifySignature({ jwks, kid, timestamp, body: rawBody, signature });
  }
  return { result, key: jwks.find((k) => k.kid === kid) || null };
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const path = url.pathname;
  try {
    // ---------- 订阅校验 ----------
    if (req.method === 'POST' && path === '/validate') {
      const raw = await readRaw(req);
      let msg;
      try { msg = JSON.parse(raw); } catch { return send(res, 400, { error: 'BAD_JSON' }); }
      const { result, key } = await authenticate(req, raw);
      attempts.push({ at: Date.now(), kind: 'VALIDATE', kid: req.headers['x-dr-key-id'], verify: result });
      if (!result.ok) return send(res, 401, { error: 'SIGNATURE_INVALID', reason: result.reason });
      if (msg.type !== 'SUBSCRIPTION_VALIDATION') return send(res, 400, { error: 'WRONG_TYPE' });
      return send(res, 200, { challenge: msg.challenge, verified: true, signedByKid: result.kid, keyStatus: key?.status });
    }

    // ---------- 通知接收 ----------
    if (req.method === 'POST' && path === '/') {
      const raw = await readRaw(req);
      const idem = req.headers['idempotency-key'] || null;
      let msg;
      try { msg = JSON.parse(raw); } catch { return send(res, 400, { error: 'BAD_JSON' }); }

      const { result, key } = await authenticate(req, raw);
      const dup = seenKeys.has(idem);
      attempts.push({
        at: Date.now(), kind: 'NOTIFY', envelopeId: msg.envelopeId, idempotencyKey: idem,
        duplicate: dup, verify: result, mode, httpReply: null,
      });
      const lastAttempt = attempts[attempts.length - 1];

      if (!result.ok) {
        lastAttempt.httpReply = 401;
        return send(res, 401, { error: 'SIGNATURE_INVALID', reason: result.reason });
      }

      // 幂等：重复投递不重复处理业务，但根据模式决定应答。
      if (!dup) {
        seenKeys.set(idem, { firstAt: Date.now(), envelopeId: msg.envelopeId });
        inbox.push({
          envelopeId: msg.envelopeId, idempotencyKey: idem, subSeq: msg.subSeq,
          type: msg.type, eventId: msg.eventId, content: msg.content,
          receivedAt: Date.now(), signedByKid: result.kid, signKeyStatus: key?.status,
        });
      }

      switch (mode) {
        case 'fail':
          lastAttempt.httpReply = 503;
          dbg(`NOTIFY env=${msg.envelopeId} idem=${idem} dup=${dup} mode=${mode} -> 503`);
          return send(res, 503, { error: 'SUBSCRIBER_TEMPORARILY_UNAVAILABLE', mode });
        case 'failOnce':
          if (!dup) {
            lastAttempt.httpReply = 503;
            dbg(`NOTIFY env=${msg.envelopeId} idem=${idem} dup=${dup} mode=${mode} -> 503(failOnce)`);
            return send(res, 503, { error: 'FAIL_ONCE', note: '订阅端暂时失败，稍后请用同一幂等键重试' });
          }
          lastAttempt.httpReply = 200;
          dbg(`NOTIFY env=${msg.envelopeId} idem=${idem} dup=${dup} mode=${mode} -> 200(failOnce dup)`);
          return send(res, 200, { ack: 'ok', duplicate: true, envelopeId: msg.envelopeId });
        case 'timeout':
          lastAttempt.httpReply = 0;
          dbg(`NOTIFY env=${msg.envelopeId} idem=${idem} mode=${mode} -> HANG`);
          return; // 故意挂起：聚合商超时中断，之后用同一幂等键再次请求
        case 'ackloss':
          // 已处理（inbox 已记录），但第一次应答失败，模拟 2xx 确认在回程丢失。
          if (!dup) {
            lastAttempt.httpReply = 500;
            dbg(`NOTIFY env=${msg.envelopeId} idem=${idem} dup=${dup} mode=${mode} -> 500(acklost)`);
            return send(res, 500, { error: 'ACK_LOST_ON_WIRE', note: '已处理但应答丢失' });
          }
          lastAttempt.httpReply = 200;
          dbg(`NOTIFY env=${msg.envelopeId} idem=${idem} dup=${dup} mode=${mode} -> 200(ackloss dup)`);
          return send(res, 200, { ack: 'ok', duplicate: true, recovered: true, envelopeId: msg.envelopeId });
        case 'ok':
        default:
          lastAttempt.httpReply = 200;
          dbg(`NOTIFY env=${msg.envelopeId} idem=${idem} dup=${dup} mode=${mode} -> 200(ok)`);
          return send(res, 200, { ack: 'ok', duplicate: dup, envelopeId: msg.envelopeId });
      }
    }

    // ---------- 管理 ----------
    if (req.method === 'PUT' && path === '/admin/mode') {
      let body = {};
      try { body = JSON.parse(await readRaw(req)); } catch { /* 空 */ }
      mode = body.mode || mode;
      modeNote = body.note || '';
      dbg(`MODE -> ${mode}`);
      return send(res, 200, { mode, note: modeNote });
    }

    if (req.method === 'GET' && path === '/admin/state') {
      return send(res, 200, {
        name: NAME, port: PORT, pid: process.pid, bootedAt, aggregator: AGGREGATOR, mode, modeNote,
        processedCount: inbox.length, inbox: [...inbox].reverse(),
        attempts: [...attempts].reverse().slice(0, 60),
      });
    }

    if (req.method === 'GET' && path === '/') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      return res.end(PAGE);
    }

    send(res, 404, { error: 'not found' });
  } catch (err) {
    send(res, 500, { error: err.message });
  }
});

server.listen(PORT, () => {
  console.log(`${NAME} 已启动: http://localhost:${PORT}  （聚合商 ${AGGREGATOR}）  初始模式=${mode}`);
});

export { server };

const PAGE = `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"/>
<title>${NAME} · 订阅端控制台</title>
<style>
body{margin:0;font:14px/1.5 -apple-system,"Segoe UI",Roboto,"PingFang SC",sans-serif;background:#0e1420;color:#e6ecf5}
header{padding:14px 22px;background:linear-gradient(90deg,#12203a,#0e1420);border-bottom:1px solid #28354a;font-weight:700}
.wrap{padding:16px 22px;display:grid;grid-template-columns:320px 1fr;gap:14px}
.panel{background:#161f2e;border:1px solid #28354a;border-radius:10px;padding:14px}
button{cursor:pointer;padding:8px 10px;margin:3px 0;width:100%;background:#1c2738;color:#e6ecf5;border:1px solid #28354a;border-radius:7px;font-weight:600}
button.on{background:#4f9cff;color:#06101f}
table{width:100%;border-collapse:collapse;font-size:12.5px}
th,td{text-align:left;padding:5px 7px;border-bottom:1px solid #28354a;vertical-align:top}
th{color:#8fa0b8}
.tag{padding:1px 7px;border-radius:99px;font-size:11px;font-weight:700;background:#13315c;color:#8fc2ff}
.bad{color:#f06a6a}.ok{color:#34c78a}.mut{color:#8fa0b8;font-size:12px}
pre{white-space:pre-wrap;word-break:break-all;font-size:11px;background:#0a101b;border:1px solid #28354a;border-radius:7px;padding:8px;max-height:160px;overflow:auto}
</style></head><body>
<header>📥 ${NAME} <span class="mut">订阅端 · 故障模式控制台</span></header>
<div class="wrap">
  <div class="panel">
    <b>故障模式</b>
    <div style="margin:8px 0" id="modes"></div>
    <div class="mut">ok 正常 / fail 持续失败(死信) / failOnce 暂时失败 / timeout 超时 / ackloss 确认丢失</div>
    <hr style="border-color:#28354a">
    <div class="mut">已处理通知：<b id="cnt">0</b>（按幂等键去重）</div>
  </div>
  <div class="panel">
    <b>投递与验签记录</b>
    <table><thead><tr><th>#</th><th>信封/序号</th><th>类型</th><th>验签</th><th>密钥</th><th>重复</th><th>应答</th></tr></thead>
    <tbody id="rows"></tbody></table>
    <b style="display:block;margin-top:12px">已处理通知内容</b>
    <div id="inbox"></div>
  </div>
</div>
<script>
const MODES=[['ok','正常 200'],['fail','持续失败 503'],['failOnce','暂时失败一次'],['timeout','超时不响应'],['ackloss','确认丢失']];
async function api(p,o){const r=await fetch(p,o);return r.json()}
async function setMode(m){await api('/admin/mode',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({mode:m})});load()}
function modeButtons(m){document.getElementById('modes').innerHTML=MODES.map(([v,l])=>'<button class="'+(v===m?'on':'')+'" onclick="setMode(\\''+v+'\\')">'+l+'</button>').join('')}
function esc(s){return String(s??'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]))}
async function load(){
  const s=await api('/admin/state');
  modeButtons(s.mode);document.getElementById('cnt').textContent=s.processedCount;
  document.getElementById('rows').innerHTML=s.attempts.map((a,i)=>'<tr><td>'+(s.attempts.length-i)+'</td><td>'+esc((a.envelopeId||a.kind||'').slice(-10))+'</td><td>'+esc(a.kind)+'</td><td class="'+(a.verify.ok?'ok':'bad')+'">'+(a.verify.ok?'✓ '+esc(a.verify.keyStatus):esc(a.verify.reason))+'</td><td class="mut">'+esc(a.kid?.slice(-8))+'</td><td>'+(a.duplicate?'是':'否')+'</td><td class="'+(a.httpReply===200?'ok':a.httpReply===0?'mut':'bad')+'">'+(a.httpReply===0?'超时':a.httpReply)+'</td></tr>').join('');
  document.getElementById('inbox').innerHTML=s.inbox.map(x=>'<div style="margin:8px 0"><span class="tag">'+esc(x.type)+'</span> <b>'+esc(x.envelopeId.slice(-10))+'</b> seq='+x.subSeq+' 事件='+esc(x.eventId)+' <span class="mut">签名密钥 '+esc(x.signedByKid.slice(-8))+' ('+esc(x.signKeyStatus)+')</span><pre>'+esc(JSON.stringify(x.content,null,1))+'</pre></div>').join('');
}
load();setInterval(load,1200);
</script></body></html>`;
