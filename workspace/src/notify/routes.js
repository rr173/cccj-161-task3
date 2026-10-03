// 聚合商通知模块 HTTP 路由（挂载在主编排服务的 /api/notify/* 之下）。
// 纯函数式挂载：把若干路由交给主 server 的请求循环处理，返回是否命中。
import { NotifyError } from './engine.js';
import { httpRequest } from './dispatcher.js';
import { verifySignature } from './crypto.js';

// 根据引擎当前快照独立重算证据：信封体哈希、尝试签名是否可由 JWKS 验证。
async function buildEvidence(engine, envelopeView) {
  const { canonicalJSON } = await import('../util.js');
  // 信封内容指纹复算（证明链上内容 == 投递内容）
  const { sha256Hex } = await import('../util.js');
  const recomputedBodyHash = sha256Hex(canonicalJSON(envelopeView.content));
  const jwks = engine.jwks();
  // 抽验最近一次投递的签名：用当时密钥重新签一份并比对是不现实的（时间戳每次不同），
  // 证据改为：签名所用 kid 仍在 JWKS（即使已 RETIRED），且其状态与记录一致。
  const signKey = jwks.find((k) => k.kid === envelopeView.signKid) || null;
  return {
    envelopeId: envelopeView.envelopeId,
    bodyHashMatches: recomputedBodyHash === envelopeView.bodyHash,
    bodyHash: envelopeView.bodyHash,
    recomputedBodyHash,
    signedByKid: envelopeView.signKid,
    signKeyPresentInJwks: !!signKey,
    signKeyStatus: signKey?.status || null,
  };
}

// 处理一个请求；命中返回 true（已写响应），未命中返回 false。
export async function handleNotifyRoute({ req, res, path, body, send, engine, dispatcher, bridge, pumpNow }) {
  const P = '/api/notify';

  // ---- 视图 / JWKS ----
  if (path === `${P}/view` && req.method === 'GET') {
    return send(res, 200, engine.view()), true;
  }
  if (path === `${P}/jwks` && req.method === 'GET') {
    return send(res, 200, { jwks: engine.jwks(), activeKid: engine.activeKid }), true;
  }

  // ---- 订阅方 ----
  if (path === `${P}/subscribers` && req.method === 'GET') {
    return send(res, 200, { subscribers: [...engine.subscribers.keys()].map((id) => engine.subscriberView(id)) }), true;
  }
  if (path === `${P}/subscribers` && req.method === 'POST') {
    const view = engine.registerSubscriber({ name: body.name, callbackUrl: body.callbackUrl });
    // 登记后立即尝试订阅校验；失败也保留 PENDING，可稍后重试。
    let validation = null;
    try {
      validation = await engine.verifySubscription(view.subId, httpRequest);
    } catch (e) {
      validation = { active: false, error: e.message, code: e.code };
    }
    if (pumpNow) pumpNow();
    return send(res, 201, { subscriber: engine.subscriberView(view.subId), validation }), true;
  }
  const subVerify = path.match(new RegExp(`^${P}/subscribers/([^/]+)/verify$`));
  if (subVerify && req.method === 'POST') {
    const out = await engine.verifySubscription(decodeURIComponent(subVerify[1]), httpRequest);
    if (pumpNow) pumpNow();
    return send(res, 200, out), true;
  }

  // ---- 信封 / 显式确认 / 证据 ----
  const envMatch = path.match(new RegExp(`^${P}/envelopes/([^/]+)$`));
  if (envMatch && req.method === 'GET') {
    const view = engine.envelopeView(decodeURIComponent(envMatch[1]));
    const evidence = await buildEvidence(engine, view);
    return send(res, 200, { envelope: view, evidence }), true;
  }
  const ackMatch = path.match(new RegExp(`^${P}/envelopes/([^/]+)/ack$`));
  if (ackMatch && req.method === 'POST') {
    const out = engine.explicitAck({ envelopeId: decodeURIComponent(ackMatch[1]), note: body.note });
    return send(res, 200, out), true;
  }
  const evSignMatch = path.match(new RegExp(`^${P}/envelopes/([^/]+)/verify-signature$`));
  if (evSignMatch && req.method === 'GET') {
    // 独立证据端点：用 JWKS 重算信封体哈希 + 报告签名密钥仍在信任集。
    const view = engine.envelopeView(decodeURIComponent(evSignMatch[1]));
    const evidence = await buildEvidence(engine, view);
    return send(res, 200, { ok: evidence.bodyHashMatches && evidence.signKeyPresentInJwks, evidence }), true;
  }

  // ---- 死信 ----
  if (path === `${P}/dlq` && req.method === 'GET') {
    return send(res, 200, { dlq: [...engine.dlq.values()] }), true;
  }
  const requeueMatch = path.match(new RegExp(`^${P}/dlq/([^/]+)/requeue$`));
  if (requeueMatch && req.method === 'POST') {
    const out = engine.requeueDeadLetter({ envelopeId: decodeURIComponent(requeueMatch[1]), note: body.note });
    return send(res, 200, out), true;
  }

  // ---- 密钥轮换 ----
  if (path === `${P}/keys/rotate` && req.method === 'POST') {
    const out = await engine.rotateKey(body.label || '手动轮换');
    return send(res, 200, out), true;
  }

  // ---- 即时桥接（演示用：内核操作后立刻同步到通知）----
  if (path === `${P}/pump` && req.method === 'POST') {
    const out = pumpNow ? pumpNow() : { watermark: engine.bridgeWatermark, emitted: [] };
    return send(res, 200, out), true;
  }

  // ---- 演示复位 ----
  if (path === `${P}/reset` && req.method === 'POST') {
    const key = engine.resetDemo();
    return send(res, 200, { reset: true, newKey: key }), true;
  }

  // ---- 在线验签工具（用当前 JWKS 校验一段通知原文，订阅端视角）----
  if (path === `${P}/verify` && req.method === 'POST') {
    const result = await verifySignature({
      jwks: engine.jwks(),
      kid: body.keyId,
      timestamp: body.timestamp,
      body: typeof body.body === 'string' ? body.body : JSON.stringify(body.body),
      signature: body.signature,
    });
    return send(res, result.ok ? 200 : 422, { ...result }), true;
  }

  return false;
}

export { NotifyError };
