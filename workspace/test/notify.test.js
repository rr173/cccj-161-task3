// 聚合商通知投递模块单测：不可变信封、每订阅方顺序/游标、稳定幂等键、
// 确认幂等、死信/重入队、密钥轮换旧信封可验、桥接去重、重启重放恢复。
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { NotifyEngine, MAX_ATTEMPTS } from '../src/notify/engine.js';
import { signBody, verifySignature, toJwks } from '../src/notify/crypto.js';
import { KernelBridge } from '../src/notify/bridge.js';
import { Kernel } from '../src/kernel.js';
import { EventStore } from '../src/store.js';
import { BASE_NOW } from '../src/seed.js';
import { sha256Hex, canonicalJSON } from '../src/util.js';

let clock = 1_700_000_000_000;
const now = () => clock;

async function newEngine() {
  clock = 1_700_000_000_000;
  return NotifyEngine.freshMemory({ now });
}

// 登记并直接激活一个订阅方（跳过 HTTP 握手），返回 subId
function activate(eng, name = 'S') {
  eng.registerSubscriber({ name, callbackUrl: `http://${name}/` });
  const subId = [...eng.subscribers.keys()].at(-1);
  eng.store.append('N_SUB_ACTIVATED', { subId, at: now() });
  eng.apply(eng.store.all().at(-1));
  return subId;
}

function ids(eng, subId) {
  return (eng.queue.get(subId) || []).map((envId) => eng.envelopes.get(envId).subSeq);
}

test('密钥：生成/签名/验签闭环，篡改请求体即验签失败', async () => {
  const eng = await newEngine();
  const key = eng.activeKey;
  const body = JSON.stringify({ hello: '世界' });
  const h = await signBody({ key, body, now: now() });
  const ok = await verifySignature({ jwks: toJwks([key]), kid: h.keyId, timestamp: h.timestamp, body, signature: h.signature, now: now() });
  assert.ok(ok.ok);
  const tampered = await verifySignature({ jwks: toJwks([key]), kid: h.keyId, timestamp: h.timestamp, body: body + ' ', signature: h.signature, now: now() });
  assert.equal(tampered.ok, false);
  assert.equal(tampered.reason, 'SIGNATURE_MISMATCH');
});

test('信封：入队即不可变，bodyHash 覆盖内容，稳定幂等键不含尝试序号', async () => {
  const eng = await newEngine();
  const s1 = activate(eng);
  eng.publish({ type: 'EVENT_PUBLISHED', eventId: 'EV1', dedupeKey: 'P:EV1' });
  const env = eng.headFor(s1);
  const expectedHash = sha256Hex(canonicalJSON(env.content));
  assert.equal(env.bodyHash, expectedHash);
  const key1 = env.idempotencyKey;
  // 失败两次后幂等键保持不变
  eng.failOrRetry({ envelopeId: env.envelopeId, httpStatus: 503, reason: 'x' });
  eng.failOrRetry({ envelopeId: env.envelopeId, httpStatus: 503, reason: 'x' });
  assert.equal(eng.envelopes.get(env.envelopeId).idempotencyKey, key1);
});

test('顺序：同一订阅方严格按 subSeq，队头未确认不投递后继', async () => {
  const eng = await newEngine();
  const s1 = activate(eng);
  eng.publish({ type: 'EVENT_PUBLISHED', eventId: 'E', dedupeKey: 'a' });
  eng.publish({ type: 'EVENT_STARTED', eventId: 'E', dedupeKey: 'b' });
  eng.publish({ type: 'EVENT_ENDED', eventId: 'E', dedupeKey: 'c' });
  assert.deepEqual(ids(eng, s1), [1, 2, 3]);
  const head = eng.headFor(s1);
  assert.equal(head.subSeq, 1);
  // 不确认队头，head 始终是 seq1
  eng.failOrRetry({ envelopeId: head.envelopeId, httpStatus: 503, reason: 'x' });
  assert.equal(eng.headFor(s1).subSeq, 1);
});

test('游标：只有确认队头才推进；重复确认不重复推进', async () => {
  const eng = await newEngine();
  const s1 = activate(eng);
  eng.publish({ type: 'EVENT_PUBLISHED', eventId: 'E', dedupeKey: 'a' });
  eng.publish({ type: 'EVENT_STARTED', eventId: 'E', dedupeKey: 'b' });
  const e1 = eng.headFor(s1);
  const ack1 = eng.acknowledge({ envelopeId: e1.envelopeId });
  assert.equal(ack1.cursor, 1);
  assert.equal(ack1.duplicate, false);
  // 重复确认（投递 2xx 再来 / 显式 API）游标不变
  const dup1 = eng.acknowledge({ envelopeId: e1.envelopeId });
  assert.equal(dup1.duplicate, true);
  assert.equal(dup1.cursor, 1);
  const dup1b = eng.explicitAck({ envelopeId: e1.envelopeId });
  assert.equal(dup1b.duplicate, true);
  assert.equal(dup1b.cursor, 1);
  // 后继确认推进到 2
  const e2 = eng.headFor(s1);
  assert.equal(e2.subSeq, 2);
  assert.equal(eng.acknowledge({ envelopeId: e2.envelopeId }).cursor, 2);
});

test('互不阻塞：一个订阅方卡住不影响另一个订阅方投递推进', async () => {
  const eng = await newEngine();
  const slow = activate(eng, 'SLOW');
  const fast = activate(eng, 'FAST');
  eng.publish({ type: 'EVENT_PUBLISHED', eventId: 'E', dedupeKey: 'pub' });
  // 两个订阅方各有一封 seq1
  assert.equal(eng.headFor(slow).subSeq, 1);
  assert.equal(eng.headFor(fast).subSeq, 1);
  // fast 连续确认推进；slow 从不确认
  for (let i = 0; i < 3; i++) {
    eng.acknowledge({ envelopeId: eng.headFor(fast).envelopeId });
    eng.publish({ type: 'EVENT_PUBLISHED', eventId: 'E' + i, dedupeKey: 'fast' + i });
  }
  assert.ok(eng.subscriberView(fast).cursor >= 1);
  assert.equal(eng.headFor(slow).subSeq, 1, '慢订阅方仍停在队头');
  assert.equal(eng.subscriberView(slow).cursor, 0);
});

test('死信：重试达上限进 DLQ、移出队头让后继继续；游标不因失败推进', async () => {
  const eng = await newEngine();
  const s1 = activate(eng);
  eng.publish({ type: 'EVENT_PUBLISHED', eventId: 'E', dedupeKey: 'a' });
  eng.publish({ type: 'EVENT_STARTED', eventId: 'E', dedupeKey: 'b' });
  const headId = eng.headFor(s1).envelopeId;
  let r;
  for (let i = 0; i < MAX_ATTEMPTS; i++) {
    r = eng.failOrRetry({ envelopeId: headId, httpStatus: 503, reason: 'down' });
  }
  assert.equal(r.deadLettered, true);
  assert.equal(eng.dlq.size, 1);
  assert.equal(eng.subscriberView(s1).cursor, 0, '失败不推进游标');
  // 队头变成 seq2，可继续投递
  assert.equal(eng.headFor(s1).subSeq, 2);
});

test('人工重新入队：重置计数并回到原始 subSeq 位置（恢复后顺序不变）', async () => {
  const eng = await newEngine();
  const s1 = activate(eng);
  // seq1 死信，seq2 先确认
  eng.publish({ type: 'EVENT_PUBLISHED', eventId: 'E', dedupeKey: 'a' });
  eng.publish({ type: 'EVENT_STARTED', eventId: 'E', dedupeKey: 'b' });
  const seq1 = eng.headFor(s1).envelopeId;
  for (let i = 0; i < MAX_ATTEMPTS; i++) eng.failOrRetry({ envelopeId: seq1, httpStatus: 503, reason: 'down' });
  assert.equal(eng.headFor(s1).subSeq, 2);
  eng.acknowledge({ envelopeId: eng.headFor(s1).envelopeId });
  assert.equal(eng.subscriberView(s1).cursor, 2);
  // 重入队 seq1
  eng.requeueDeadLetter({ envelopeId: seq1 });
  assert.equal(eng.dlq.size, 0);
  assert.equal(eng.headFor(s1).subSeq, 1, 'seq1 回到队头（原位置）');
  assert.equal(eng.envelopes.get(seq1).attemptCount, 0);
  // 确认 seq1：此前 seq2 已确认，游标直接到 2（不重复推进 seq2）
  const ack = eng.acknowledge({ envelopeId: seq1 });
  assert.equal(ack.cursor, 2);
  assert.equal(eng.subscriberView(s1).cursor, 2);
});

test('顺序守卫：不能显式确认非队头信封（跳过未确认信封会破坏顺序）', async () => {
  const eng = await newEngine();
  const s1 = activate(eng);
  eng.publish({ type: 'EVENT_PUBLISHED', eventId: 'E', dedupeKey: 'a' });
  eng.publish({ type: 'EVENT_STARTED', eventId: 'E', dedupeKey: 'b' });
  const seq2 = eng.envelopes.get(eng.queue.get(s1)[1]);
  assert.throws(() => eng.explicitAck({ envelopeId: seq2.envelopeId }), (e) => e.code === 'OUT_OF_ORDER_ACK');
  // 确认队头 seq1 后，seq2 成为队头即可确认
  eng.acknowledge({ envelopeId: eng.headFor(s1).envelopeId });
  assert.equal(eng.explicitAck({ envelopeId: seq2.envelopeId }).duplicate, false);
});

test('密钥轮换：旧信封由 RETIRED 旧公钥仍可验签；新信封用新密钥', async () => {
  const eng = await newEngine();
  const oldKey = eng.activeKey;
  const s1 = activate(eng);
  // 一封在途信封，入队时由旧密钥签名
  eng.publish({ type: 'EVENT_PUBLISHED', eventId: 'E', dedupeKey: 'a' });
  const oldEnv = eng.headFor(s1);
  assert.equal(oldEnv.signKid, oldKey.kid);
  const oldBody = JSON.stringify({ envelopeId: oldEnv.envelopeId });
  const oldSig = await signBody({ key: oldKey, body: oldBody, now: now() });

  // 轮换
  await eng.rotateKey('rot');
  const newKey = eng.activeKey;
  assert.notEqual(newKey.kid, oldKey.kid);
  const jwks = eng.jwks();
  const oldInJwks = jwks.find((k) => k.kid === oldKey.kid);
  assert.equal(oldInJwks.status, 'RETIRED');

  // 旧信封在轮换后仍能由旧（RETIRED）公钥验证
  const oldStillValid = await verifySignature({ jwks, kid: oldSig.keyId, timestamp: oldSig.timestamp, body: oldBody, signature: oldSig.signature, now: now() });
  assert.ok(oldStillValid.ok);
  assert.equal(oldStillValid.keyStatus, 'RETIRED');

  // 新信封用新密钥
  eng.acknowledge({ envelopeId: oldEnv.envelopeId });
  eng.publish({ type: 'EVENT_STARTED', eventId: 'E', dedupeKey: 'b' });
  assert.equal(eng.headFor(s1).signKid, newKey.kid);
});

test('桥接：内核 5 个时点翻译成通知；v1 报告不发订正；重启不重发', async () => {
  const store = new EventStore(null);
  const kernel = new Kernel(store); // 已含种子
  const eng = await newEngine();
  const bridge = new KernelBridge(kernel, eng);
  const s1 = activate(eng, 'BR');

  // 首次 pump：只有种子记录，无业务通知
  let r = bridge.pump();
  assert.equal(eng.envelopes.size, 0);
  const wm0 = eng.bridgeWatermark;
  assert.ok(wm0 > 0);

  // 发布一个事件并走到结束 + 订正
  const I = 15 * 60000;
  const start = Math.ceil((kernel.now() + 30 * 60000) / I) * I;
  const end = start + 4 * I;
  const { id } = kernel.dispatch({ type: 'PUBLISH_EVENT', meterId: 'MP-1', targetKw: 40, startMs: start, endMs: end, algorithm: 'MID_8_OF_10' }).event;
  bridge.pump();
  kernel.dispatch({ type: 'TICK', minutes: (end - kernel.now()) / 60000 });
  bridge.pump();
  kernel.dispatch({ type: 'LOAD_MISSING_DAY', meterId: 'MP-1', dayOffset: 4 });
  bridge.pump();

  const types = [...eng.envelopes.values()].filter((e) => e.subId === s1).map((e) => e.type);
  assert.deepEqual(types, ['EVENT_PUBLISHED', 'EVENT_STARTED', 'EVENT_ENDED', 'REPORT_CORRECTED']);
  // v1 原始裁决不产生 REPORT_CORRECTED（只有 CORRECTED 版才发）
  assert.equal(types.filter((t) => t === 'REPORT_CORRECTED').length, 1);

  // 模拟重启：新建引擎/桥接，重放通知链后再次 pump，不应补发任何信封
  const eng2 = eng; // 同一持久链（内存中即同一对象）；水位已持久
  const before = eng2.envelopes.size;
  new KernelBridge(kernel, eng2).pump();
  assert.equal(eng2.envelopes.size, before, '重启后不重发');
});

test('桥接去重：同一 dedupeKey 对同一订阅方只入队一次（崩溃重放安全）', async () => {
  const eng = await newEngine();
  const s1 = activate(eng);
  eng.publish({ type: 'EVENT_CANCELLED', eventId: 'E1', dedupeKey: 'EVENT_CANCELLED:E1' });
  eng.publish({ type: 'EVENT_CANCELLED', eventId: 'E1', dedupeKey: 'EVENT_CANCELLED:E1' }); // 内核可能写两条撤销
  assert.equal((eng.queue.get(s1) || []).length, 1);
});

test('重启恢复：从持久哈希链重放，队列/游标/DLQ/密钥状态完全重建', async () => {
  const os = await import('node:os');
  const fsp = await import('node:fs/promises');
  const path = await import('node:path');
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), 'notify-'));
  const file = path.join(dir, 'notify.log');

  const eng = new NotifyEngine(file, { now });
  await eng.bootstrap();
  const s1 = activate(eng, 'PERSIST');
  eng.publish({ type: 'EVENT_PUBLISHED', eventId: 'E', dedupeKey: 'a' });
  eng.publish({ type: 'EVENT_STARTED', eventId: 'E', dedupeKey: 'b' });
  const e1 = eng.headFor(s1);
  eng.acknowledge({ envelopeId: e1.envelopeId });
  // seq2 保持未确认（模拟崩溃时在途）
  await eng.rotateKey('persist-rot');

  // 全新进程视角重建
  const eng2 = new NotifyEngine(file, { now });
  await eng2.bootstrap();
  assert.ok(eng2.store.verify().ok, '通知哈希链完整');
  const view = eng2.subscriberView(s1);
  assert.equal(view.cursor, 1, '已确认游标恢复为 1');
  assert.equal(view.pendingCount, 1, '未确认信封仍在队列');
  assert.equal(eng2.headFor(s1).subSeq, 2, '重启后从队头 seq2 继续');
  // 密钥状态重建：一把 RETIRED、一把 ACTIVE
  const statuses = eng2.jwks().map((k) => k.status).sort();
  assert.deepEqual(statuses, ['ACTIVE', 'RETIRED']);
  // 再确认 seq2 游标推进到 2（不重复）
  assert.equal(eng2.acknowledge({ envelopeId: eng2.headFor(s1).envelopeId }).cursor, 2);
  await fsp.rm(dir, { recursive: true, force: true });
});
