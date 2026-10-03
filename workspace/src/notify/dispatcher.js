// 投递调度器：为每个订阅方运行一个【独立】投递循环。
//
// 互不阻塞：每个订阅方有自己的等待计时器与在途标记；慢订阅方（超时/长退避）
// 只会延迟它自己的后继信封，绝不影响其它订阅方。
//
// 顺序：循环只取该订阅方的队头（engine.headFor），未确认不投递后继。
// 稳定幂等键：超时/失败后再次请求完全复用信封的 idempotencyKey（入队时一次确定）。
//
// 进程重启：在途尝试没有任何“内存中的成功”状态——成功只有在 2xx 回执落链（N_ACK）
// 后才算数。重启后 dispatcher 重新从持久队列的队头开始，因此未确认的投递会继续；
// 游标只在确认落链后由引擎推进，天然不会重复推进。
import http from 'node:http';
import https from 'node:https';
import { backoffMs } from './engine.js';

export const DELIVERY_TIMEOUT_MS = 6000;

// 最小 HTTP 传输（零依赖），返回 { status, json, text }。超时即中断底层 socket。
export function httpRequest({ method = 'POST', url, headers = {}, body = '', timeoutMs = DELIVERY_TIMEOUT_MS }) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const u = new URL(url);
    const lib = u.protocol === 'https:' ? https : http;
    const req = lib.request(
      {
        method,
        hostname: u.hostname,
        port: u.port || (u.protocol === 'https:' ? 443 : 80),
        path: u.pathname + u.search,
        headers: { ...headers, 'Content-Length': Buffer.byteLength(body) },
      },
      (res) => {
        let chunks = '';
        res.setEncoding('utf8');
        res.on('data', (c) => { chunks += c; });
        res.on('end', () => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          let json = null;
          try { json = JSON.parse(chunks); } catch { /* 非 JSON 回执 */ }
          resolve({ status: res.statusCode, text: chunks, json, headers: res.headers });
        });
      }
    );
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      req.destroy(Object.assign(new Error(`请求超时（>${timeoutMs}ms）`), { code: 'DELIVERY_TIMEOUT' }));
    }, timeoutMs);
    req.on('error', (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(err);
    });
    req.end(body);
  });
}

export class Dispatcher {
  constructor(engine, { transport = httpRequest, deliveryTimeoutMs = DELIVERY_TIMEOUT_MS } = {}) {
    this.engine = engine;
    this.transport = transport;
    this.deliveryTimeoutMs = deliveryTimeoutMs;
    this.busy = new Set();     // 正在投递/退避等待的订阅方
    this.sleepers = new Map(); // subId -> timeout handle（用于停机）
    this.stopped = true;
    engine.pendingHook = (subId) => this.kick(subId);
  }

  start() {
    this.stopped = false;
    // 重启恢复：为每个订阅方都启动循环（未确认投递继续）。
    for (const subId of this.engine.subscribers.keys()) this.kick(subId);
  }

  stop() {
    this.stopped = true;
    for (const h of this.sleepers.values()) clearTimeout(h);
    this.sleepers.clear();
  }

  kick(subId) {
    if (this.stopped || !subId) return;
    if (this.busy.has(subId)) return;
    void this.#loop(subId);
  }

  #schedule(subId, ms) {
    const h = setTimeout(() => {
      this.sleepers.delete(subId);
      this.busy.delete(subId);
      this.kick(subId);
    }, ms);
    this.sleepers.set(subId, h);
    if (h.unref) h.unref(); // 不阻止进程退出
  }

  async #loop(subId) {
    if (this.stopped) return;
    const sub = this.engine.subscribers.get(subId);
    if (!sub || sub.status !== 'ACTIVE') { this.busy.delete(subId); return; }

    const head = this.engine.headFor(subId);
    if (!head) { this.busy.delete(subId); return; } // 队列空（或队头已进 DLQ 移出）

    this.busy.add(subId);
    let delivery;
    try {
      delivery = await this.engine.buildDelivery(head);
    } catch (e) {
      this.busy.delete(subId);
      return;
    }

    try {
      const res = await this.transport({
        method: 'POST',
        url: sub.callbackUrl,
        headers: { ...delivery.headers, 'Idempotency-Key': head.idempotencyKey },
        body: delivery.body,
        timeoutMs: this.deliveryTimeoutMs,
      });
      if (res.status >= 200 && res.status < 300) {
        // 2xx 即确认；引擎按信封幂等，重复投递拿到 2xx 也只推进一次游标。
        this.engine.acknowledge({
          envelopeId: head.envelopeId,
          httpStatus: res.status,
          response: res.json?.ack || res.text?.slice(0, 120) || null,
        });
        this.busy.delete(subId);
        this.kick(subId); // 立即投递后继
        return;
      }
      const r = this.engine.failOrRetry({
        envelopeId: head.envelopeId,
        httpStatus: res.status,
        reason: res.json?.error || `HTTP_${res.status}`,
      });
      this.#afterFailure(subId, r);
    } catch (err) {
      // 超时 / 连接失败：信封留在队头，稍后用【同一幂等键】再次请求。
      const r = this.engine.failOrRetry({
        envelopeId: head.envelopeId,
        httpStatus: null,
        reason: err.code || 'TRANSPORT_ERROR',
      });
      this.#afterFailure(subId, r);
    }
  }

  #afterFailure(subId, r) {
    if (r.deadLettered) {
      this.busy.delete(subId);
      this.kick(subId); // 队头进 DLQ 后，后继可以继续
      return;
    }
    this.#schedule(subId, backoffMs(r.attempts));
  }
}
