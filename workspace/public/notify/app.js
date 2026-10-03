// 投递台前端：轮询 /api/notify/view，提供订阅、密钥轮换、死信重入队、证据复验等操作。
const $ = (id) => document.getElementById(id);
const api = async (p, body, method = 'POST') => {
  const res = await fetch(p, {
    method,
    headers: body !== undefined ? { 'Content-Type': 'application/json' } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw Object.assign(new Error(data.error || res.statusText), { code: data.code, details: data.details });
  return data;
};
const esc = (s) => String(s ?? '').replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
const short = (s, n = 8) => (s ? String(s).slice(-n) : '—');

let VIEW = null;
let selectedEnv = null;

function toast(msg, kind = '') {
  const el = document.createElement('div');
  el.className = 'toast ' + kind;
  el.textContent = msg;
  $('toasts').appendChild(el);
  setTimeout(() => el.remove(), 3800);
}

async function addSubscriber() {
  try {
    const data = await api('/api/notify/subscribers', { name: $('subName').value, callbackUrl: $('subUrl').value });
    if (data.validation?.active) toast('订阅已校验并激活：' + data.subscriber.subId, 'ok');
    else toast('订阅已登记但校验未通过：' + (data.validation?.error || '稍后可重试'), 'err');
  } catch (e) { toast('登记失败：' + e.message, 'err'); }
  load();
}

async function verifySub(subId) {
  try { await api(`/api/notify/subscribers/${subId}/verify`, {}); toast('订阅校验通过', 'ok'); }
  catch (e) { toast('校验失败：' + e.message, 'err'); }
  load();
}

async function rotateKey() {
  try { const r = await api('/api/notify/keys/rotate', { label: '页面轮换' }); toast('已轮换到新密钥 ' + r.kid.slice(-8) + '，旧公钥保留', 'ok'); }
  catch (e) { toast('轮换失败：' + e.message, 'err'); }
  load();
}

async function manualAck(envId) {
  try {
    const r = await api(`/api/notify/envelopes/${envId}/ack`, { note: '页面手动确认' });
    toast(r.duplicate ? '重复确认：游标不重复推进（idempotent）' : '已手动确认，游标推进', r.duplicate ? '' : 'ok');
  } catch (e) { toast('确认失败：' + e.message, 'err'); }
  load();
}

async function requeue(envId) {
  try { await api(`/api/notify/dlq/${envId}/requeue`, { note: '页面人工重新入队' }); toast('已重新入队（排回队尾，重置重试）', 'ok'); }
  catch (e) { toast('重入队失败：' + e.message, 'err'); }
  load();
}

async function showEvidence(envId) {
  selectedEnv = envId;
  try {
    const { envelope, evidence } = await api(`/api/notify/envelopes/${envId}`, undefined, 'GET');
    const keyRow = VIEW.jwks.find((k) => k.kid === evidence.signedByKid);
    $('evidence').innerHTML = `
      <div class="card">
        <div class="kv">
          <dt>信封</dt><dd class="mono">${esc(envelope.envelopeId)}</dd>
          <dt>幂等键</dt><dd class="mono" style="font-size:10.5px">${esc(envelope.idempotencyKey)}</dd>
          <dt>类型 / 序号</dt><dd>${esc(envelope.type)} / seq ${envelope.subSeq}</dd>
          <dt>签名密钥</dt><dd class="mono">…${short(evidence.signedByKid, 12)}
            <span class="tag t-${evidence.signKeyStatus}">${evidence.signKeyStatus || '缺失'}</span></dd>
          <dt>密钥仍在 JWKS</dt><dd class="${evidence.signKeyPresentInJwks ? 'ok' : 'bad'}">${evidence.signKeyPresentInJwks ? '是（轮换后旧在途可验）' : '否'}</dd>
          <dt>信封体哈希一致</dt><dd class="${evidence.bodyHashMatches ? 'ok' : 'bad'}">${evidence.bodyHashMatches ? '✓ 一致' : '✗ 被篡改'}</dd>
        </div>
        <div style="margin-top:8px"><button class="ghost" onclick="reVerify('${envId}')">用 JWKS 复验签名/哈希证据</button></div>
        <details><summary>信封内容（投递体）</summary><pre class="log">${esc(JSON.stringify(envelope.content, null, 2))}</pre></details>
        <details><summary>尝试历史（${envelope.attempts.length}）</summary><pre class="log">${esc(envelope.attempts.map((a) => `#${a.attemptNo} ${a.ok ? 'OK ' + (a.httpStatus || '') : 'FAIL ' + (a.reason || '') + ' ' + (a.httpStatus || '')} @${new Date(a.at).toLocaleTimeString()}`).join('\n'))}</pre></details>
      </div>`;
  } catch (e) { $('evidence').textContent = '证据加载失败：' + e.message; }
}

async function reVerify(envId) {
  try {
    const r = await api(`/api/notify/envelopes/${envId}/verify-signature`, undefined, 'GET');
    toast(r.ok ? '复验通过：信封体哈希一致，签名密钥仍受信' : '复验失败', r.ok ? 'ok' : 'err');
  } catch (e) { toast('复验失败：' + e.message, 'err'); }
}

async function resetAll() {
  if (!confirm('将清空通知链（订阅、信封、游标、死信）并重新生成密钥。继续？')) return;
  try { await api('/api/notify/reset', {}); toast('通知模块已复位', 'ok'); selectedEnv = null; }
  catch (e) { toast('复位失败：' + e.message, 'err'); }
  load();
}

function renderKeys() {
  $('keys').innerHTML = VIEW.jwks.map((k) => `
    <div class="card" style="margin-bottom:6px">
      <div class="row" style="justify-content:space-between">
        <span class="mono">…${short(k.kid, 12)}</span>
        <span class="tag t-${k.status}">${k.status}${k.kid === VIEW.activeKid ? ' · 当前' : ''}</span>
      </div>
      <div class="muted mono" style="font-size:10.5px;margin-top:3px">fp ${esc(k.fingerprint)} · ${new Date(k.createdAt).toLocaleTimeString()}</div>
    </div>`).join('');
}

function renderSubs() {
  if (!VIEW.subscribers.length) { $('subs').innerHTML = '<div class="muted">尚无订阅方。</div>'; return; }
  $('subs').innerHTML = VIEW.subscribers.map((s) => {
    const pend = s.pending.map((p) => `<tr><td class="mono">#${p.subSeq}</td><td><span class="tag t-PENDING">${esc(p.type)}</span></td><td>${p.attempts}</td></tr>`).join('');
    return `<div class="card ${s.status === 'ACTIVE' ? '' : 'dead'}">
      <div class="row" style="justify-content:space-between">
        <b>${esc(s.name)}</b>
        <span class="tag t-${s.status === 'ACTIVE' ? 'ACTIVE' : 'PENDING_SUB'}">${s.status}</span>
      </div>
      <div class="muted mono" style="font-size:11px;word-break:break-all">${esc(s.callbackUrl)}</div>
      <div class="row" style="margin-top:5px">
        <span class="pill">subId …${short(s.subId)}</span>
        <span class="pill">游标 cursor=${s.cursor}</span>
        <span class="pill">待发 ${s.pendingCount}</span>
        ${s.status !== 'ACTIVE' ? `<button class="warn" style="margin-left:auto" onclick="verifySub('${s.subId}')">重新校验</button>` : ''}
      </div>
      ${pend ? `<table style="margin-top:6px"><thead><tr><th>队头序号</th><th>类型</th><th>已尝试</th></tr></thead>${pend}</table>` : '<div class="muted" style="margin-top:5px">队列已清空（全部确认或在死信）。</div>'}
      ${s.dlq.length ? `<div class="bad muted" style="margin-top:4px">死信 ${s.dlq.length} 封（见右下）</div>` : ''}
    </div>`;
  }).join('');
}

function renderEnvs() {
  $('envs').innerHTML = VIEW.envelopes.map((e) => {
    const sub = VIEW.subscribers.find((s) => s.subId === e.subId);
    const key = VIEW.jwks.find((k) => k.kid === e.signKid);
    return `<tr>
      <td class="mono">#${e.subSeq}</td>
      <td>${esc(sub?.name || short(e.subId))}</td>
      <td><span class="tag t-PENDING">${esc(e.type)}</span></td>
      <td class="mono muted">${short(e.eventId, 10)}</td>
      <td>${e.attemptCount}</td>
      <td class="mono muted">…${short(e.signKid, 6)} <span class="tag t-${key?.status || 'RETIRED'}">${key?.status || '?'}</span></td>
      <td><span class="tag t-${e.status}">${e.status}</span></td>
      <td class="btns">
        <button class="ghost" onclick="showEvidence('${e.envelopeId}')">证据</button>
        ${e.status !== 'ACKED' ? `<button class="good" onclick="manualAck('${e.envelopeId}')">手动确认</button>` : ''}
      </td>
    </tr>`;
  }).join('') || '<tr><td colspan="8" class="muted">尚无信封（在运营台发布事件即生成）。</td></tr>';
}

function renderDlq() {
  if (!VIEW.dlq.length) { $('dlq').innerHTML = '<div class="muted">无死信。</div>'; return; }
  $('dlq').innerHTML = VIEW.dlq.map((d) => `
    <div class="card dead">
      <div class="row" style="justify-content:space-between">
        <b class="mono">…${short(d.envelopeId, 10)}</b>
        <span class="tag t-DEAD">DEAD</span>
      </div>
      <div class="kv" style="margin-top:5px">
        <dt>序号</dt><dd>#${d.subSeq}</dd>
        <dt>尝试</dt><dd>${d.attempts}</dd>
        <dt>末次原因</dt><dd class="bad">${esc(d.lastReason)} ${d.lastHttpStatus ?? ''}</dd>
      </div>
      <button class="warn" style="margin-top:7px" onclick="requeue('${d.envelopeId}')">人工重新入队</button>
    </div>`).join('');
}

function renderAudit() {
  $('audit').textContent = VIEW.attempts.slice(0, 40).map((a) =>
    `${new Date(a.at).toLocaleTimeString()}  ${a.ok ? '✓' : '✗'}  #${a.subSeq}  ${a.type}  ${a.ok ? 'HTTP ' + a.httpStatus : (a.reason || '') + ' ' + (a.httpStatus ?? '')}`
  ).join('\n') || '（暂无尝试）';
}

async function load() {
  try {
    VIEW = await api('/api/notify/view', undefined, 'GET');
    $('clock').textContent = new Date(VIEW.now).toLocaleString();
    $('wm').textContent = '内核消费水位 seq=' + VIEW.bridgeWatermark;
    renderKeys(); renderSubs(); renderEnvs(); renderDlq(); renderAudit();
    if (selectedEnv) {
      const still = VIEW.envelopes.find((e) => e.envelopeId === selectedEnv);
      if (still) showEvidence(selectedEnv); else selectedEnv = null;
    }
  } catch (e) {
    // 静默处理轮询错误
  }
}

load();
setInterval(load, 1200);
