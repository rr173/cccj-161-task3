// 聚合商通知投递引擎（独立模块）。
//
// 与领域内核（src/kernel.js）完全解耦：内核产生需求响应事件，桥接器（bridge.js）
// 只负责把内核记录翻译成 5 类通知喂给本引擎。本引擎自己的事实来源是
// 一条独立的只增 + SHA-256 哈希链日志（默认 data/notify.log）：
//
//   N_KEY            签名密钥登记/轮换（私钥也随链保存，本地演示；链外分发只给 JWKS 公钥）
//   N_SUBSCRIBER     订阅方登记（callbackUrl、待校验 challenge）
//   N_SUB_ACTIVATED  订阅校验通过 -> ACTIVE
//   N_ENVELOPE       不可变通知信封（每条只写一次，永不更新/删除）
//   N_ATTEMPT        一次投递尝试（成功/失败/超时；尝试本身也是审计证据）
//   N_ACK            投递确认（携带订阅方回传的 envelopeId，幂等）
//   N_DLQ            超过最大重试 -> 死信
//   N_REQUEUE        人工把死信重新入队
//   N_BRIDGE_WATERMARK  桥接器已消费到的内核事件 seq（重启续传，不漏不重）
//
// 顺序与游标：每个订阅方维护一条单调 subSeq（信封进入该订阅方队列时分配），
// 只有“队头信封确认落链”才把 cursor 前进一格；队头未确认时绝不投递后继信封，
// 因此同一订阅方严格按事件顺序、且游标永远不会重复推进。
// 不同订阅方使用各自独立的投递循环与等待计时，互不阻塞。
import { generateSigningKey, toJwks } from './crypto.js';
import { EventStore } from '../store.js';
import { sha256Hex, canonicalJSON, uid, iso } from '../util.js';

export const EVENT_TYPES = ['EVENT_PUBLISHED', 'EVENT_CANCELLED', 'EVENT_STARTED', 'EVENT_ENDED', 'REPORT_CORRECTED'];
export const MAX_ATTEMPTS = 5;

export class NotifyError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.code = code;
    this.details = details;
    this.status = ({
      BAD_REQUEST: 400, NOT_FOUND: 404, SUBSCRIPTION_NOT_ACTIVE: 409,
      ALREADY_EXISTS: 409, NOT_DELIVERED: 409, NOT_IN_DLQ: 409,
      OUT_OF_ORDER_ACK: 409,
      LOG_QUARANTINED: 503,
    })[code] || 400;
  }
}

export class NotifyEngine {
  constructor(file = null, { now = () => Date.now(), maxAttempts = MAX_ATTEMPTS } = {}) {
    this.store = new EventStore(file);
    this.clock = now; // 投递/签名时间默认真实时钟（通知是真实 HTTP，不用仿真时钟）
    this.maxAttempts = maxAttempts;
    this.pendingHook = null; // dispatcher 注册：有新待发/可重试时被唤醒
  }

  async bootstrap() {
    await this.store.load();
    this.replay(); // 先建投影，空库登记首密钥时也能 apply
    if (this.store.all().length === 0 && this.store.writable) {
      // 空库：登记第一把签名密钥
      await this.rotateKey('初始签名密钥');
    }
  }

  static async freshMemory(opts = {}) {
    const eng = new NotifyEngine(null, opts);
    await eng.bootstrap();
    return eng;
  }

  // ---------- 重放投影 ----------

  replay() {
    this.keys = new Map();          // kid -> key（含 privateJwk）
    this.keyOrder = [];             // kid 登记顺序
    this.activeKid = null;
    this.subscribers = new Map();   // subId -> subscriber
    this.envelopes = new Map();     // envelopeId -> envelope
    this.queue = new Map();         // subId -> [envelopeId]（按 subSeq 顺序，未确认）
    this.attempts = [];             // 全部尝试（审计）
    this.acks = new Map();          // envelopeId -> ack 记录（每个信封至多一条有效确认）
    this.dlq = new Map();           // envelopeId -> dlq 记录
    this.bridgeWatermark = 0;

    for (const rec of this.store.all()) this.apply(rec);
  }

  apply(rec) {
    const p = rec.payload;
    switch (rec.type) {
      case 'N_KEY':
        this.keys.set(p.kid, { ...p });
        if (!this.keyOrder.includes(p.kid)) this.keyOrder.push(p.kid);
        if (p.status === 'ACTIVE') this.activeKid = p.kid;
        break;
      case 'N_KEY_RETIRED': {
        const k = this.keys.get(p.kid);
        if (k) { k.status = 'RETIRED'; k.retiredAt = p.retiredAt; }
        if (this.activeKid === p.kid) this.activeKid = null;
        break;
      }
      case 'N_SUBSCRIBER':
        this.subscribers.set(p.subId, {
          ...p,
          status: p.status || 'PENDING',
          createdAt: p.createdAt,
        });
        break;
      case 'N_SUB_ACTIVATED': {
        const s = this.subscribers.get(p.subId);
        if (s) { s.status = 'ACTIVE'; s.activatedAt = p.at; s.challenge = null; }
        break;
      }
      case 'N_ENVELOPE': {
        this.envelopes.set(p.envelopeId, { ...p });
        if (!this.queue.has(p.subId)) this.queue.set(p.subId, []);
        this.queue.get(p.subId).push(p.envelopeId);
        break;
      }
      case 'N_ATTEMPT':
        this.attempts.push({ ...p });
        break;
      case 'N_ACK': {
        // 幂等：同一信封的有效确认只记录一次
        if (!this.acks.has(p.envelopeId)) {
          this.acks.set(p.envelopeId, { ...p });
          this.#trimFront(p.subId); // 只有队头确认后游标才前进；剪枝保证队首恒为未决信封
        }
        break;
      }
      case 'N_DLQ': {
        this.dlq.set(p.envelopeId, { ...p });
        this.#trimFront(p.subId); // 死信移出队头，让后继信封可以继续投递
        break;
      }
      case 'N_REQUEUE': {
        this.dlq.delete(p.envelopeId);
        if (!this.queue.has(p.subId)) this.queue.set(p.subId, []);
        const q = this.queue.get(p.subId);
        if (!q.includes(p.envelopeId)) q.push(p.envelopeId);
        // 回到原始 subSeq 位置：人工重入队后仍在所有更高序号信封之前投递，
        // 保证该订阅方“恢复后顺序”不变。
        q.sort((a, b) => this.envelopes.get(a).subSeq - this.envelopes.get(b).subSeq);
        const env = this.envelopes.get(p.envelopeId);
        if (env) env.attemptCount = 0;
        break;
      }
      case 'N_BRIDGE_WATERMARK':
        this.bridgeWatermark = Math.max(this.bridgeWatermark, p.seq);
        break;
      default:
        break;
    }
  }

  #append(type, payload) {
    if (!this.store.writable) {
      const e = new Error('通知日志已隔离（只读）');
      e.code = 'LOG_QUARANTINED';
      e.status = 503;
      throw e;
    }
    const rec = this.store.append(type, payload, this.clock());
    this.apply(rec);
    return rec;
  }

  // 队首剪枝：移除队列开头所有“已确认或已死信”的信封。
  // 顺序不变量：游标只在队头被确认/死信时前进；任何时刻队首都是当前待决信封。
  #trimFront(subId) {
    const q = this.queue.get(subId);
    if (!q) return;
    while (q.length) {
      const id = q[0];
      if (this.acks.has(id) || this.dlq.has(id)) q.shift();
      else break;
    }
  }

  #wake(subId) {
    if (this.pendingHook) this.pendingHook(subId);
  }

  // ---------- 密钥 ----------

  get activeKey() {
    return this.keys.get(this.activeKid);
  }

  jwks() {
    return toJwks(this.keyOrder.map((kid) => this.keys.get(kid)));
  }

  async rotateKey(label = '密钥轮换') {
    const key = await generateSigningKey(this.clock());
    // 旧活动密钥先显式落链“退役”（重放时状态确定，不能只改内存）。
    if (this.activeKid) {
      this.#append('N_KEY_RETIRED', { kid: this.activeKid, retiredAt: this.clock() });
    }
    this.#append('N_KEY', { ...key, label, rotatedInAt: this.clock() });
    return { kid: key.kid, fingerprint: key.fingerprint, label };
  }

  // ---------- 订阅方 + 订阅校验 ----------

  registerSubscriber({ name, callbackUrl }) {
    if (!/^https?:\/\/.+/.test(callbackUrl || '')) {
      throw new NotifyError('BAD_REQUEST', 'callbackUrl 必须是 http(s) 地址', { callbackUrl });
    }
    for (const s of this.subscribers.values()) {
      if (s.callbackUrl === callbackUrl && s.status !== 'REMOVED') {
        throw new NotifyError('ALREADY_EXISTS', '该回调地址已有有效订阅', { subId: s.subId });
      }
    }
    const subId = uid('SUB');
    const challenge = uid('CHAL');
    const payload = {
      subId,
      name: name || subId,
      callbackUrl,
      status: 'PENDING',
      challenge,
      createdAt: this.clock(),
      createdIso: iso(this.clock()),
    };
    this.#append('N_SUBSCRIBER', payload);
    return this.subscriberView(subId);
  }

  // 订阅校验：聚合商向订阅端的 validate 端点发一个带签名的 challenge，
  // 订阅端必须原样回 challenge 并自证验签通过，订阅才被激活。
  async verifySubscription(subId, transport) {
    const s = this.requireSubscriber(subId);
    if (s.status === 'ACTIVE') return { active: true, idempotent: true, subId };
    const key = this.activeKey;
    const bodyObj = { type: 'SUBSCRIPTION_VALIDATION', challenge: s.challenge, subId, keyId: key.kid };
    const body = JSON.stringify(bodyObj);
    const headers = await this.#signHeaders(body);
    let result;
    try {
      result = await transport({
        method: 'POST',
        url: joinUrl(s.callbackUrl, '/validate'),
        headers,
        body,
        timeoutMs: 8000,
      });
    } catch (e) {
      throw new NotifyError('SUBSCRIPTION_NOT_ACTIVE', `无法完成订阅校验：${e.message}`, { reason: e.code || 'TRANSPORT' });
    }
    if (result.status < 200 || result.status >= 300 || result.json?.challenge !== s.challenge) {
      throw new NotifyError('SUBSCRIPTION_NOT_ACTIVE', '订阅端未通过校验（challenge/状态不符）', {
        httpStatus: result.status, returned: result.json,
      });
    }
    this.#append('N_SUB_ACTIVATED', { subId, at: this.clock(), httpStatus: result.status });
    // 可能已有信封在 PENDING 时排队（允许先登记后校验）
    this.#wake(subId);
    return { active: true, subId };
  }

  requireSubscriber(subId) {
    const s = this.subscribers.get(subId);
    if (!s) throw new NotifyError('NOT_FOUND', `订阅方 ${subId} 不存在`);
    return s;
  }

  activeSubscribers() {
    return [...this.subscribers.values()].filter((s) => s.status === 'ACTIVE');
  }

  // ---------- 不可变信封 ----------

  // 业务侧（桥接器）调用：为某个需求响应事件向所有 ACTIVE 订阅方生成信封。
  // 返回生成的信封列表。dedupeKey 保证“同一业务事件 + 类型”对某订阅方只入队一次
  // （重启续传桥接器时靠它防重）。
  publish({ type, eventId, event, dedupeKey, payload = {} }) {
    if (!EVENT_TYPES.includes(type)) throw new NotifyError('BAD_REQUEST', `未知通知类型 ${type}`);
    const subs = this.activeSubscribers();
    if (!subs.length) return { envelopes: [] };
    const out = [];
    for (const s of subs) {
      const dkey = dedupeKey || `${s.subId}:${type}:${eventId}:${payload.version || 1}`;
      if (this.#dedupeHas(s.subId, dkey)) continue;
      out.push(this.#enqueue(s, { type, eventId, event, dedupeKey: dkey, payload }));
      this.#wake(s.subId);
    }
    return { envelopes: out };
  }

  #dedupeHas(subId, dedupeKey) {
    for (const env of this.envelopes.values()) {
      if (env.subId === subId && env.dedupeKey === dedupeKey) return true;
    }
    return false;
  }

  #enqueue(s, { type, eventId, event, dedupeKey, payload }) {
    const subSeq = this.#nextSubSeq(s.subId);
    const envelopeId = uid('ENV');
    const createdAt = this.clock();
    // 信封主体一旦写入永不改变；bodyHash 让订阅端能证明“投递体 == 链上信封”。
    const content = {
      type,
      eventId,
      event: event || null,
      payload,
      issuedAt: createdAt,
      issuedIso: iso(createdAt),
      aggregator: 'DR-ORCHESTRATOR',
    };
    const bodyHash = sha256Hex(canonicalJSON(content));
    // 稳定幂等键：超时后再次请求时完全不变（订阅端据此去重），不随尝试序号变化。
    const idempotencyKey = `dr-${s.subId}-${subSeq}-${sha256Hex(dedupeKey).slice(0, 16)}`;
    const rec = this.#append('N_ENVELOPE', {
      envelopeId,
      subId: s.subId,
      subSeq,
      type,
      eventId,
      dedupeKey,
      idempotencyKey,
      content,
      bodyHash,
      signKid: this.activeKid, // 入队时由当前活动密钥签名（轮换演示旧信封）
      attemptCount: 0,
    });
    return rec.payload;
  }

  #nextSubSeq(subId) {
    let max = 0;
    for (const env of this.envelopes.values()) if (env.subId === subId) max = Math.max(max, env.subSeq);
    return max + 1;
  }

  // ---------- 投递 / 确认 / 死信 ----------

  // 该订阅方现在应当投递的信封：队头（严格顺序），且不在 DLQ。
  headFor(subId) {
    const q = this.queue.get(subId) || [];
    const id = q[0];
    if (!id) return null;
    if (this.dlq.has(id)) return null;
    return this.envelopes.get(id);
  }

  // dispatcher 调用：把信封序列化为真实投递请求体（签名）。
  async buildDelivery(envelope) {
    const key = this.keys.get(envelope.signKid) || this.activeKey;
    const message = {
      envelopeId: envelope.envelopeId,
      idempotencyKey: envelope.idempotencyKey,
      subSeq: envelope.subSeq,
      eventId: envelope.eventId,
      type: envelope.type,
      content: envelope.content,
    };
    const body = JSON.stringify(message);
    const headers = await this.#signHeaders(body, key);
    return { body, headers };
  }

  async #signHeaders(body, key = this.activeKey) {
    const { signBody } = await import('./crypto.js');
    const sig = await signBody({ key, body, now: this.clock() });
    return {
      'Content-Type': 'application/json',
      'X-DR-Key-Id': sig.keyId,
      'X-DR-Timestamp': String(sig.timestamp),
      'X-DR-Signature': sig.signature,
      'X-DR-Aggregator': 'DR-ORCHESTRATOR',
    };
  }

  // 记录一次尝试。ok=true 表示订阅端 HTTP 2xx（即已确认投递，由其回执体驱动）。
  recordAttempt({ envelopeId, ok, httpStatus, reason, response }) {
    const env = this.envelopes.get(envelopeId);
    if (!env) throw new NotifyError('NOT_FOUND', `信封 ${envelopeId} 不存在`);
    env.attemptCount = (env.attemptCount || 0) + 1;
    this.#append('N_ATTEMPT', {
      attemptId: uid('ATT'),
      envelopeId,
      subId: env.subId,
      subSeq: env.subSeq,
      attemptNo: env.attemptCount,
      ok: !!ok,
      httpStatus: httpStatus ?? null,
      reason: reason || null,
      response: response ?? null,
      at: this.clock(),
      iso: iso(this.clock()),
    });
    return env.attemptCount;
  }

  // 投递成功确认。必须是该订阅方队头才能推进游标；重复确认安全（返回 duplicate）。
  acknowledge({ envelopeId, httpStatus, response }) {
    const env = this.envelopes.get(envelopeId);
    if (!env) throw new NotifyError('NOT_FOUND', `信封 ${envelopeId} 不存在`);
    if (this.acks.has(envelopeId)) {
      return { acknowledged: true, duplicate: true, subSeq: env.subSeq, cursor: this.#cursor(env.subId) };
    }
    this.recordAttempt({ envelopeId, ok: true, httpStatus: httpStatus ?? 200, response });
    this.#append('N_ACK', {
      ackId: uid('ACK'),
      envelopeId,
      subId: env.subId,
      subSeq: env.subSeq,
      idempotencyKey: env.idempotencyKey,
      at: this.clock(),
      iso: iso(this.clock()),
      source: 'DELIVERY_2XX',
    });
    this.#wake(env.subId);
    return { acknowledged: true, duplicate: false, subSeq: env.subSeq, cursor: this.#cursor(env.subId) };
  }

  // 显式确认 API（HTTP 投递成功但确认“丢失”后，订阅端/运维补确认）。
  // 只允许确认“当前在途队头”：确认更早/更晚的信封会破坏同订阅方严格投递顺序。
  // 重复确认安全（返回 duplicate，游标不重复推进）。
  explicitAck({ envelopeId, note }) {
    const env = this.envelopes.get(envelopeId);
    if (!env) throw new NotifyError('NOT_FOUND', `信封 ${envelopeId} 不存在`);
    if (this.dlq.has(envelopeId)) throw new NotifyError('NOT_DELIVERED', '信封已在死信队列，不能确认；请先人工重新入队');
    if (this.acks.has(envelopeId)) {
      return { acknowledged: true, duplicate: true, subSeq: env.subSeq, cursor: this.#cursor(env.subId) };
    }
    const head = this.headFor(env.subId);
    if (!head || head.envelopeId !== envelopeId) {
      throw new NotifyError('OUT_OF_ORDER_ACK', '只能确认当前在途队头信封（前面尚有未确认信封，严格顺序投递不可跳过）', {
        headSubSeq: head?.subSeq ?? null, requestedSubSeq: env.subSeq,
      });
    }
    this.#append('N_ACK', {
      ackId: uid('ACK'),
      envelopeId,
      subId: env.subId,
      subSeq: env.subSeq,
      idempotencyKey: env.idempotencyKey,
      at: this.clock(),
      iso: iso(this.clock()),
      source: 'EXPLICIT_API',
      note: note || null,
    });
    this.#wake(env.subId);
    return { acknowledged: true, duplicate: false, subSeq: env.subSeq, cursor: this.#cursor(env.subId) };
  }

  // 一次失败尝试后判断：达到上限进死信，否则等待退避后重试（同一幂等键）。
  failOrRetry({ envelopeId, httpStatus, reason }) {
    const env = this.envelopes.get(envelopeId);
    if (!env) throw new NotifyError('NOT_FOUND', `信封 ${envelopeId} 不存在`);
    const attemptNo = this.recordAttempt({ envelopeId, ok: false, httpStatus, reason });
    if (attemptNo >= this.maxAttempts) {
      this.#append('N_DLQ', {
        dlqId: uid('DLQ'),
        envelopeId,
        subId: env.subId,
        subSeq: env.subSeq,
        attempts: attemptNo,
        lastReason: reason,
        lastHttpStatus: httpStatus ?? null,
        at: this.clock(),
        iso: iso(this.clock()),
      });
      this.#wake(env.subId); // 队头移到 DLQ 后唤醒投递后继
      return { deadLettered: true, attempts: attemptNo };
    }
    // 不改变队列位置：仍是队头，dispatcher 按指数退避用同一幂等键再次请求。
    return { deadLettered: false, attempts: attemptNo, retryAfterMs: backoffMs(attemptNo) };
  }

  // 死信后人工重新入队：重置尝试计数，信封回到其原始 subSeq 位置
  // （排在更高序号信封之前），恢复后该订阅方仍严格按事件顺序投递。
  requeueDeadLetter({ envelopeId, note }) {
    const dlq = this.dlq.get(envelopeId);
    if (!dlq) throw new NotifyError('NOT_IN_DLQ', '该信封不在死信队列');
    this.#append('N_REQUEUE', {
      requeueId: uid('RQ'),
      envelopeId,
      subId: dlq.subId,
      subSeq: dlq.subSeq,
      note: note || '人工重新入队',
      at: this.clock(),
      iso: iso(this.clock()),
    });
    this.#wake(dlq.subId);
    return { requeued: true, envelopeId };
  }

  #cursor(subId) {
    // cursor = 最近确认的 subSeq（队列里剩下的最小序号 - 1）
    let maxAck = 0;
    for (const ack of this.acks.values()) if (ack.subId === subId) maxAck = Math.max(maxAck, ack.subSeq);
    return maxAck;
  }

  // 演示复位：清空通知链并重新登记初始密钥（独立于内核日志）。
  resetDemo() {
    this.store.truncate();
    this.replay();
    return this.rotateKey('复位后的初始签名密钥');
  }

  // ---------- 桥接水位 ----------

  setBridgeWatermark(seq) {
    if (seq > this.bridgeWatermark) {
      this.#append('N_BRIDGE_WATERMARK', { seq });
    }
    return this.bridgeWatermark;
  }

  // ---------- 视图 ----------

  subscriberView(subId) {
    const s = this.requireSubscriber(subId);
    const pending = (this.queue.get(subId) || [])
      .filter((id) => !this.dlq.has(id) && !this.acks.has(id))
      .map((id) => this.envelopes.get(id));
    const cursor = this.#cursor(subId);
    return {
      subId: s.subId,
      name: s.name,
      callbackUrl: s.callbackUrl,
      status: s.status,
      createdAt: s.createdAt,
      createdIso: s.createdIso,
      activatedAt: s.activatedAt || null,
      cursor,
      pendingCount: pending.length,
      pending: pending.map((e) => ({ envelopeId: e.envelopeId, subSeq: e.subSeq, type: e.type, eventId: e.eventId, attempts: e.attemptCount })),
      dlq: [...this.dlq.values()].filter((d) => d.subId === subId).map((d) => ({
        envelopeId: d.envelopeId, subSeq: d.subSeq, attempts: d.attempts, lastReason: d.lastReason, at: d.at,
      })),
    };
  }

  envelopeView(envelopeId) {
    const e = this.envelopes.get(envelopeId);
    if (!e) throw new NotifyError('NOT_FOUND', `信封 ${envelopeId} 不存在`);
    return {
      ...e,
      status: this.acks.has(envelopeId) ? 'ACKED' : this.dlq.has(envelopeId) ? 'DEAD' : 'PENDING',
      ack: this.acks.get(envelopeId) || null,
      dlq: this.dlq.get(envelopeId) || null,
      attempts: this.attempts.filter((a) => a.envelopeId === envelopeId),
    };
  }

  // 投递页面完整视图
  view() {
    const chain = this.store.verify();
    return {
      now: this.clock(),
      nowIso: iso(this.clock()),
      mode: chain.mode,
      readOnly: !chain.writable,
      recovery: chain.recovery,
      activeKid: this.activeKid,
      jwks: this.jwks(),
      bridgeWatermark: this.bridgeWatermark,
      maxAttempts: this.maxAttempts,
      subscribers: [...this.subscribers.keys()].map((id) => this.subscriberView(id)),
      envelopes: [...this.envelopes.values()].map((e) => ({
        envelopeId: e.envelopeId,
        subId: e.subId,
        subSeq: e.subSeq,
        type: e.type,
        eventId: e.eventId,
        idempotencyKey: e.idempotencyKey,
        bodyHash: e.bodyHash,
        signKid: e.signKid,
        attemptCount: e.attemptCount,
        status: this.acks.has(e.envelopeId) ? 'ACKED' : this.dlq.has(e.envelopeId) ? 'DEAD' : 'PENDING',
        ackAt: this.acks.get(e.envelopeId)?.at || null,
        dlqAt: this.dlq.get(e.envelopeId)?.at || null,
        issuedAt: e.content.issuedAt,
      })).sort((a, b) => b.issuedAt - a.issuedAt),
      attempts: this.attempts.slice(-80).reverse(),
      dlq: [...this.dlq.values()],
      chain,
    };
  }
}

function joinUrl(base, path) {
  return base.replace(/\/+$/, '') + path;
}

// 指数退避：2s,4s,8s,16s（演示足够快；上限 30s）
export function backoffMs(attemptNo) {
  return Math.min(2000 * 2 ** Math.max(0, attemptNo - 1), 30000);
}
