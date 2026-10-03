// 通知信封的非对称签名（Ed25519）。
// 为什么用非对称而不是共享 HMAC：聚合商（签名方）持有私钥，所有订阅端只信任
// 公开的 JWKS 即可独立验签；密钥轮换后旧公钥仍长期保留在 JWKS 中，
// 因此“轮换前已经在途、轮换后才重投/补验”的旧信封依然能被验证——这是
// 演示“密钥轮换期间旧在途通知校验”的关键。
//
// 签名内容覆盖：keyId + 时间戳 + 请求体精确字节的 SHA-256，
// 与真实 HTTP 投递逐字节一致，任何篡改都会被订阅端检出。
import { webcrypto as crypto } from 'node:crypto';
import { sha256Hex, uid } from '../util.js';

export const SIGN_ALG = 'Ed25519';
const SIG_WINDOW_MS = 5 * 60 * 1000; // 验签允许的时间戳偏移（防重放窗口）

// 生成一把新签名密钥。返回持久化所需的完整材料（私钥仅聚合商本地保存）。
export async function generateSigningKey(now = Date.now()) {
  const kp = await crypto.subtle.generateKey({ name: SIGN_ALG }, true, ['sign', 'verify']);
  const [publicJwk, privateJwk] = await Promise.all([
    crypto.subtle.exportKey('jwk', kp.publicKey),
    crypto.subtle.exportKey('jwk', kp.privateKey),
  ]);
  const kid = `key-${uid('').slice(0, 12)}`;
  return {
    kid,
    alg: SIGN_ALG,
    status: 'ACTIVE', // ACTIVE | RETIRED
    publicJwk,
    privateJwk,
    fingerprint: await publicKeyFingerprint(publicJwk),
    createdAt: now,
  };
}

// 公钥指纹（便于 UI/日志展示与人工比对）
export async function publicKeyFingerprint(publicJwk) {
  return sha256Hex(JSON.stringify(publicJwk)).slice(0, 24);
}

async function importPrivate(jwk) {
  return crypto.subtle.importKey('jwk', jwk, { name: SIGN_ALG }, false, ['sign']);
}
async function importPublic(jwk) {
  return crypto.subtle.importKey('jwk', jwk, { name: SIGN_ALG }, false, ['verify']);
}

function b64url(buf) {
  const bytes = new Uint8Array(buf);
  let s = '';
  for (const b of bytes) s += String.fromCharCode(b);
  return Buffer.from(s, 'binary').toString('base64url');
}
function b64urlToBytes(s) {
  return new Uint8Array(Buffer.from(s, 'base64url'));
}

// 计算签名基底（聚合商与订阅端必须完全一致）
export function signingContent({ kid, timestamp, body }) {
  return Buffer.from(`${kid}\n${timestamp}\n${sha256Hex(body)}`, 'utf8');
}

// 对“要投递的请求体字符串”签名，返回可放进 HTTP 头的字段集合。
export async function signBody({ key, body, now = Date.now() }) {
  if (!key?.privateJwk) throw new Error('缺少签名私钥');
  const timestamp = now;
  const priv = await importPrivate(key.privateJwk);
  const content = signingContent({ kid: key.kid, timestamp, body });
  const sig = await crypto.subtle.sign({ name: SIGN_ALG }, priv, content);
  return {
    keyId: key.kid,
    timestamp,
    signature: b64url(sig),
  };
}

// 订阅端独立验签：按 kid 从其信任的 JWKS 取公钥，校验签名与新鲜度。
// 返回 { ok, reason }。unknownKey 时订阅端可回源 JWKS（演示轮换期旧信封）。
export async function verifySignature({ jwks, kid, timestamp, body, signature, now = Date.now() }) {
  const key = jwks.find((k) => k.kid === kid);
  if (!key) return { ok: false, reason: 'UNKNOWN_SIGNING_KEY' };
  if (!Number.isFinite(Number(timestamp))) return { ok: false, reason: 'BAD_TIMESTAMP' };
  if (Math.abs(now - Number(timestamp)) > SIG_WINDOW_MS) {
    return { ok: false, reason: 'STALE_TIMESTAMP', keyStatus: key.status };
  }
  let sigBuf;
  try {
    sigBuf = b64urlToBytes(signature);
  } catch {
    return { ok: false, reason: 'BAD_SIGNATURE_ENCODING', keyStatus: key.status };
  }
  let pub;
  try {
    pub = await importPublic(key.publicJwk);
  } catch {
    return { ok: false, reason: 'BAD_PUBLIC_KEY', keyStatus: key.status };
  }
  const content = signingContent({ kid, timestamp, body });
  const valid = await crypto.subtle.verify({ name: SIGN_ALG }, pub, sigBuf, content);
  if (!valid) return { ok: false, reason: 'SIGNATURE_MISMATCH', keyStatus: key.status };
  return { ok: true, keyStatus: key.status, kid };
}

// JWKS 公开视图：绝不包含私钥；轮换后 RETIRED 公钥依旧公布。
export function toJwks(keys) {
  return keys.map((k) => ({
    kid: k.kid,
    alg: k.alg,
    status: k.status,
    publicJwk: k.publicJwk,
    fingerprint: k.fingerprint,
    createdAt: k.createdAt,
    retiredAt: k.retiredAt || null,
  }));
}
