// 通知投递端到端演示脚本（真实 HTTP + 真实进程重启）。
// 用法：
//   1) node scripts/notify-demo.mjs            # 脚本自行拉起聚合商 + 两个订阅端
//   2) 也可在已启动的服务上运行：BASE=http://localhost:8080 node scripts/notify-demo.mjs
import { spawn } from 'node:child_process';
import { rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

// 自包含演示：BASE/PORT 缺省即随机选端口并全新拉起聚合商与两个订阅端，
// 从根本上避免与历史残留进程撞端口（撞端口会把请求打到状态不一致的旧实例）。
const AGG_PORT = Number(process.env.PORT || 8600 + Math.floor(Math.random() * 300));
const BASE = process.env.BASE || `http://localhost:${AGG_PORT}`;
// 默认用随机高端口，避免与历史运行残留的订阅端/聚合商实例撞端口（撞端口会让请求
// 打到状态不一致的旧进程）。需要固定端口时显式设置 SUB1_PORT/SUB2_PORT/PORT。
const SUB1 = Number(process.env.SUB1_PORT || 9400 + Math.floor(Math.random() * 300));
const SUB2 = Number(process.env.SUB2_PORT || 9700 + Math.floor(Math.random() * 300));
const DATA_DIR = process.env.DEMO_DIR || join(tmpdir(), 'dr-notify-demo');
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

let pass = 0, fail = 0;
function check(name, cond, extra = '') {
  if (cond) { pass++; console.log('  ✅', name, extra); }
  else { fail++; console.log('  ❌', name, extra); }
}

async function api(path, body, method = 'POST') {
  const res = await fetch(BASE + path, {
    method,
    headers: body !== undefined ? { 'Content-Type': 'application/json' } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw Object.assign(new Error(data.error || res.statusText), { data });
  return data;
}
async function sub(port, path, body, method = 'POST') {
  const res = await fetch(`http://localhost:${port}${path}`, {
    method,
    headers: body !== undefined ? { 'Content-Type': 'application/json' } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  return res.json();
}
const subState = (port) => sub(port, '/admin/state', undefined, 'GET');

// 取某事件在某订阅方的“权威信封”（含聚合商侧完整尝试时间线，不受订阅端 60 条截断影响）。
async function envForEvent(eventId, subId) {
  const v = await api('/api/notify/view', undefined, 'GET');
  const row = v.envelopes.find((e) => e.eventId === eventId && e.subId === subId && e.type === 'EVENT_PUBLISHED');
  if (!row) return null;
  const detail = await api(`/api/notify/envelopes/${row.envelopeId}`, undefined, 'GET');
  return detail.envelope;
}


const children = [];
function start(cmd, args, env, label, tag = label) {
  const p = spawn(cmd, args, { env: { ...process.env, ...env }, stdio: ['ignore', 'ignore', 'pipe'] });
  p._tag = tag;
  let errTail = '';
  p.stderr?.on('data', (c) => { errTail += c; if (errTail.length > 400) errTail = errTail.slice(-400); });
  p.on('exit', (code) => { if (code && code !== 0) console.error(`  ! ${label} 退出 code=${code}: ${errTail.trim()}`); });
  children.push(p);
  console.log(`  …启动 ${label} (pid ${p.pid})`);
  return p;
}

// 轮询直到某 HTTP 端点就绪
async function waitReady(probe, timeoutMs = 6000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try { if (await probe()) return true; } catch { /* 未就绪 */ }
    await wait(120);
  }
  return false;
}

(async () => {
  let ownServer = false;

  // 默认自起聚合商（随机端口）。仅当显式设置 BASE 指向一个已在运行的服务时才复用。
  if (process.env.BASE) {
    try {
      await fetch(BASE + '/api/notify/jwks');
      console.log('使用已运行的聚合商：', BASE);
    } catch {
      throw new Error(`BASE=${BASE} 不可达`);
    }
  } else {
    ownServer = true;
    rmSync(DATA_DIR, { recursive: true, force: true });
    start(process.execPath, ['src/server.js'], {
      PORT: String(AGG_PORT),
      DR_LOG: join(DATA_DIR, 'events.log'),
      DR_NOTIFY_LOG: join(DATA_DIR, 'notify.log'),
    }, '聚合商', 'agg');
    const ok = await waitReady(() => fetch(BASE + '/api/notify/jwks').then((r) => r.ok));
    if (!ok) throw new Error('聚合商启动失败（端口可能被占用）');
  }

  // 复位通知演示（保证幂等可重复）
  await api('/api/notify/reset', {});

  // 总是全新拉起订阅端到（随机）端口，并等待其就绪；不复用任何已有实例。
  const subPids = new Map();
  const subDebugFile = new Map();
  async function ensureSub(port, name, mode) {
    const dbgFile = join(DATA_DIR, `sub-${port}.debug.log`);
    subDebugFile.set(port, dbgFile);
    const p = start(process.execPath, ['subscriber/server.js'], {
      SUB_PORT: String(port), SUB_NAME: name, SUB_MODE: mode, AGGREGATOR_BASE: BASE, SUB_DEBUG: dbgFile,
    }, `${name}(${port})`);
    const ok = await waitReady(async () => {
      const s = await subState(port);
      if (s.port !== port) return false;
      subPids.set(port, s.pid);
      return true;
    });
    if (!ok) throw new Error(`订阅端 ${name}(${port}) 启动失败（端口可能被占用）`);
    return true;
  }
  // 两个订阅端都以“正常”模式启动：避免故障方在场景 1 就积压信封、毒化队列，
  // 干扰后面场景 4/5 的在途与重入队判断。需要失败时在具体场景内再切换。
  await ensureSub(SUB1, '稳定聚合商', 'ok');
  await ensureSub(SUB2, '故障聚合商', 'ok');
  await wait(300);

  // 切换故障模式并回读确认（订阅端 /admin/mode 只接受 PUT）。
  async function setMode(port, mode) {
    await sub(port, '/admin/mode', { mode }, 'PUT');
    for (let i = 0; i < 10; i++) {
      const s = await subState(port);
      if (s.mode === mode) return true;
      await wait(100);
    }
    throw new Error(`无法把订阅端 ${port} 切到模式 ${mode}（模式切换端点未生效）`);
  }

  console.log('\n场景 1：订阅校验 + 发布/开始/结束/订正 四类通知按序送达');
  const reg1 = await api('/api/notify/subscribers', { name: '稳定聚合商', callbackUrl: `http://localhost:${SUB1}/` });
  check('稳定订阅方校验通过', reg1.subscriber.status === 'ACTIVE');
  const reg2 = await api('/api/notify/subscribers', { name: '故障聚合商', callbackUrl: `http://localhost:${SUB2}/` });
  check('故障订阅方也通过校验（校验端点不受投递故障模式影响）', reg2.subscriber.status === 'ACTIVE');

  const st = await (await fetch(BASE + '/api/state')).json();
  const I = 15 * 60000;
  const startMs = Math.ceil((st.now + 30 * 60000) / I) * I;
  const endMs = startMs + 4 * I;
  const pub = await api('/api/events', { meterId: 'MP-1', targetKw: 40, startMs, endMs, algorithm: 'MID_8_OF_10' });
  const evId = pub.event.id;

  await api('/api/sim/tick', { minutes: (endMs - st.now) / 60000 + 1 });
  await api('/api/late-history', { meterId: 'MP-1', dayOffset: 4 });
  // 轮询等待该事件的 4 封通知全部按序到达（桥接 + 异步投递有极小延迟）。
  let inbox1 = [];
  for (let i = 0; i < 20; i++) {
    inbox1 = [...(await subState(SUB1)).inbox].sort((a, b) => a.subSeq - b.subSeq);
    if (inbox1.filter((x) => x.eventId === evId).length >= 4) break;
    await wait(300);
  }
  const mine = inbox1.filter((x) => x.eventId === evId);
  check('稳定方收到 EVENT_PUBLISHED', mine.some((x) => x.type === 'EVENT_PUBLISHED'));
  const seq = mine.map((x) => x.subSeq);
  const types = mine.map((x) => x.type);
  check('按序收到 发布→开始→结束→订正', JSON.stringify(types) === JSON.stringify(['EVENT_PUBLISHED', 'EVENT_STARTED', 'EVENT_ENDED', 'REPORT_CORRECTED']), types.join(','));
  check('subSeq 严格递增', JSON.stringify(seq) === JSON.stringify([1, 2, 3, 4]), seq.join(','));
  check('订阅端每条都验签通过', mine.every((x) => x.signedByKid), '');

  console.log('\n场景 2：订阅端暂时失败（failOnce）后用同一幂等键恢复，顺序不变');
  await setMode(SUB1, 'failOnce');
  const foPub = await api('/api/events', { meterId: 'MP-2', targetKw: 20, startInMinutes: 60, durationIntervals: 4, algorithm: 'LAST_10_AVG' });
  let foEnv = null;
  for (let i = 0; i < 12; i++) {
    foEnv = await envForEvent(foPub.event.id, reg1.subscriber.subId);
    if (foEnv && foEnv.status === 'ACKED' && foEnv.attempts.length >= 2) break;
    await wait(500);
  }
  const foFails = (foEnv?.attempts || []).filter((a) => !a.ok);
  check('暂时失败后重试成功（信封最终 ACKED）', foEnv && foEnv.status === 'ACKED');
  check('首投失败 1 次后用同一幂等键恢复', foFails.length === 1, `失败×${foFails.length}`);

  console.log('\n场景 3：确认丢失（ackloss）——处理一次，重投 duplicate=true，游标只推进一次');
  await setMode(SUB1, 'ackloss');
  const ackPub = await api('/api/events', { meterId: 'MP-3', targetKw: 10, startInMinutes: 60, durationIntervals: 4, algorithm: 'LAST_5_AVG' });
  let lastEnvRow = null;
  for (let i = 0; i < 12; i++) {
    const v = await api('/api/notify/view', undefined, 'GET');
    lastEnvRow = v.envelopes.find((e) => e.subId === reg1.subscriber.subId && e.type === 'EVENT_PUBLISHED' && e.eventId === ackPub.event.id);
    if (lastEnvRow && lastEnvRow.status === 'ACKED' && lastEnvRow.attemptCount >= 2) break;
    await wait(500);
  }
  const lastEnv = lastEnvRow;
  const st3 = await subState(SUB1);
  check('确认丢失的信封最终 ACKED，尝试 2 次', lastEnv && lastEnv.status === 'ACKED' && lastEnv.attemptCount === 2, lastEnv && `${lastEnv.status}/${lastEnv.attemptCount}`);
  check('重投携带同一稳定幂等键', st3.attempts.some((a) => a.idempotencyKey === lastEnv?.idempotencyKey));
  const dupAck = await api(`/api/notify/envelopes/${lastEnv.envelopeId}/ack`, { note: '重复确认' });
  check('重复确认返回 duplicate 且游标不重复推进', dupAck.duplicate === true);

  console.log('\n场景 4：密钥轮换期间，旧在途信封靠 RETIRED 旧公钥仍验签通过');
  await setMode(SUB2, 'fail'); // 故障方持续失败
  const jwksBefore = (await (await fetch(BASE + '/api/notify/jwks')).json());
  const oldKid = jwksBefore.activeKid;
  // 发一封给故障方，保持在途；按事件 id 精确定位其 PENDING 信封。
  const rotPub = await api('/api/events', { meterId: 'MP-1', targetKw: 9, startInMinutes: 200, durationIntervals: 4, algorithm: 'LAST_5_AVG' });
  let inflight = null;
  for (let i = 0; i < 10; i++) {
    const vB = await api('/api/notify/view', undefined, 'GET');
    inflight = vB.envelopes.find((e) => e.eventId === rotPub.event.id && e.subId === reg2.subscriber.subId && e.status === 'PENDING');
    if (inflight) break;
    await wait(300);
  }
  check('存在未确认在途信封且由旧密钥签名', inflight && inflight.signKid === oldKid);
  await api('/api/notify/keys/rotate', { label: 'e2e 轮换' });
  const jwksAfter = (await (await fetch(BASE + '/api/notify/jwks')).json());
  check('轮换后旧公钥仍在 JWKS 且为 RETIRED', jwksAfter.jwks.some((k) => k.kid === oldKid && k.status === 'RETIRED'));
  check('新活动密钥不同于旧密钥', jwksAfter.activeKid !== oldKid);
  await setMode(SUB2, 'ok');
  let got = null;
  if (inflight) {
    for (let i = 0; i < 12; i++) {
      await wait(2000);
      got = (await subState(SUB2)).inbox.find((x) => x.envelopeId === inflight.envelopeId);
      if (got && got.signKeyStatus === 'RETIRED') break;
    }
  }
  check('旧在途信封轮换后重投仍被处理', !!got);
  check('旧信封由 RETIRED 旧公钥验签', got && got.signKeyStatus === 'RETIRED', got && got.signKeyStatus);
  if (inflight) {
    const evidence = await api(`/api/notify/envelopes/${inflight.envelopeId}/verify-signature`, undefined, 'GET');
    check('证据端点：体哈希一致且签名密钥仍受信', evidence.ok && evidence.evidence.bodyHashMatches);
  }

  console.log('\n场景 5：持续失败耗尽重试 → 死信 → 人工重新入队');
  await setMode(SUB2, 'fail');
  await api('/api/events', { meterId: 'MP-2', targetKw: 7, startInMinutes: 120, durationIntervals: 4, algorithm: 'LAST_10_AVG' });
  console.log('    等待退避重试耗尽（约 30s）…');
  let dlqEnv = null;
  for (let i = 0; i < 20; i++) {
    await wait(2000);
    const dlq = (await api('/api/notify/dlq', undefined, 'GET')).dlq;
    if (dlq.length) { dlqEnv = dlq[dlq.length - 1]; break; }
  }
  check('达到上限进入死信队列', !!dlqEnv, dlqEnv && `attempts=${dlqEnv.attempts}`);
  if (dlqEnv) {
    await setMode(SUB2, 'ok');
    await api(`/api/notify/dlq/${dlqEnv.envelopeId}/requeue`, { note: 'e2e 人工重入队' });
    let after = null;
    for (let i = 0; i < 8; i++) {
      await wait(1500);
      after = (await api('/api/notify/view', undefined, 'GET')).envelopes.find((e) => e.envelopeId === dlqEnv.envelopeId);
      if (after.status === 'ACKED') break;
    }
    check('重入队后成功 ACKED', after && after.status === 'ACKED', after && after.status);
    const got2 = [...(await subState(SUB2)).inbox].find((x) => x.envelopeId === dlqEnv.envelopeId);
    check('重入队信封验签仍通过', !!got2 && !!got2.signedByKid);
  }

  console.log('\n场景 6：进程重启——未确认投递继续，游标不重复推进，通知链完整');
  if (ownServer) {
    // 制造一封超时未确认信封
    await setMode(SUB1, 'timeout');
    await api('/api/events', { meterId: 'MP-3', targetKw: 6, startInMinutes: 150, durationIntervals: 4, algorithm: 'LAST_5_AVG' });
    await wait(2000);
    const v = await api('/api/notify/view', undefined, 'GET');
    const pendSub = v.subscribers.find((s) => s.subId === reg1.subscriber.subId);
    const pendingBefore = pendSub.pendingCount;
    const cursorBefore = pendSub.cursor;
    check('重启前存在未确认在途信封', pendingBefore >= 1);

    // kill 聚合商（只杀聚合商，保留两个订阅端）并重启（同一数据文件）
    for (const p of children.filter((c) => c._tag === 'agg')) { try { p.kill('SIGKILL'); } catch { } }
    await wait(800);
    await setMode(SUB1, 'ok');
    start(process.execPath, ['src/server.js'], {
      PORT: String(AGG_PORT),
      DR_LOG: join(DATA_DIR, 'events.log'),
      DR_NOTIFY_LOG: join(DATA_DIR, 'notify.log'),
    }, '聚合商(重启)', 'agg');
    await waitReady(() => fetch(BASE + '/api/notify/jwks').then((r) => r.ok));
    await wait(2500);

    const v2 = await api('/api/notify/view', undefined, 'GET');
    const pendSub2 = v2.subscribers.find((s) => s.subId === reg1.subscriber.subId);
    check('重启后通知哈希链完整', v2.chain.ok === true, `len=${v2.chain.length}`);
    check('未确认信封重启后继续投递直至清空', pendSub2.pendingCount === 0, `pending=${pendSub2.pendingCount}`);
    check('游标从 ' + cursorBefore + ' 单调前进（不回退/不重复）', pendSub2.cursor >= cursorBefore, `→ ${pendSub2.cursor}`);
    const processedAfter = (await subState(SUB1)).processedCount;
    // 再等一拍确认没有重复补发
    await wait(1500);
    const processedAfter2 = (await subState(SUB1)).processedCount;
    check('订阅端按幂等键去重，重启不产生重复业务处理', processedAfter === processedAfter2, `processed=${processedAfter}`);
  } else {
    console.log('    （连接的是外部已运行服务，跳过“自动重启进程”子场景）');
  }

  console.log(`\n==== 通知投递演示结果：${pass} 通过 / ${fail} 失败 ====`);
  // 脚本自身拉起的进程：保留运行供查看（也可 Ctrl-C）。延迟退出以免误杀外部服务。
  if (process.env.KEEP) console.log('（KEEP 模式：保留演示进程）');
  else {
    await wait(500);
    for (const p of children) { try { p.kill('SIGTERM'); } catch { } }
  }
  process.exit(fail ? 1 : 0);
})().catch((e) => {
  console.error('演示脚本异常：', e);
  for (const p of children) { try { p.kill('SIGTERM'); } catch { } }
  process.exit(1);
});
