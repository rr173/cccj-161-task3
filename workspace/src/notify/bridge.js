// 内核 -> 通知引擎 桥接器（单向、只读消费内核哈希链）。
//
// 不侵入领域内核：按内核记录 seq 的“消费水位”（持久化在通知链 N_BRIDGE_WATERMARK）
// 增量扫描内核只增日志，把 5 个业务时点翻译成通知：
//   EVENT_PUBLISHED  -> EVENT_PUBLISHED
//   EVENT_CANCELLED  -> EVENT_CANCELLED   （开始后撤销内核会写两条，靠 dedupeKey 只发一次）
//   EVENT_ACTIVATED  -> EVENT_STARTED
//   EVENT_CLOSED     -> EVENT_ENDED
//   REPORT_ISSUED(kind=CORRECTED) -> REPORT_CORRECTED（v1 原始裁决不发“订正”）
//
// 每条内核记录处理完即把水位推进到该 seq；崩溃后从旧水位重放，
// 配合信封的 per-subscriber dedupeKey，做到不漏发、不重发。
export class KernelBridge {
  constructor(kernel, notifyEngine) {
    this.kernel = kernel;
    this.engine = notifyEngine;
  }

  // 处理所有 seq > 水位的内核记录；返回本次新发信封概要。
  pump() {
    const emitted = [];
    const records = this.kernel.store.all();
    let wm = this.engine.bridgeWatermark;
    for (const rec of records) {
      if (rec.seq <= wm) continue;
      const notice = this.#translate(rec);
      if (notice) {
        const { envelopes } = this.engine.publish(notice);
        for (const e of envelopes) emitted.push({ seq: rec.seq, type: notice.type, envelopeId: e.envelopeId, subId: e.subId });
      }
      // 无论是否产生通知，都把水位推进到该记录（逐条，崩溃可续）
      wm = rec.seq;
      this.engine.setBridgeWatermark(rec.seq);
    }
    return { watermark: wm, emitted };
  }

  #translate(rec) {
    const p = rec.payload;
    switch (rec.type) {
      case 'EVENT_PUBLISHED':
        return {
          type: 'EVENT_PUBLISHED',
          eventId: p.id,
          dedupeKey: `EVENT_PUBLISHED:${p.id}`,
          event: {
            meterId: p.meterId, meterName: p.meterName, aggregatorId: p.aggregatorId,
            targetKw: p.targetKw, algorithm: p.algorithm,
            startMs: p.startMs, endMs: p.endMs,
            startIso: new Date(p.startMs).toISOString(), endIso: new Date(p.endMs).toISOString(),
          },
        };
      case 'EVENT_CANCELLED':
        return {
          type: 'EVENT_CANCELLED',
          eventId: p.eventId,
          dedupeKey: `EVENT_CANCELLED:${p.eventId}`,
          event: { at: p.at, reason: p.reason },
        };
      case 'EVENT_ACTIVATED':
        return {
          type: 'EVENT_STARTED',
          eventId: p.eventId,
          dedupeKey: `EVENT_STARTED:${p.eventId}`,
          event: { activatedAt: p.at },
        };
      case 'EVENT_CLOSED':
        return {
          type: 'EVENT_ENDED',
          eventId: p.eventId,
          dedupeKey: `EVENT_ENDED:${p.eventId}`,
          event: { closedAt: p.at },
        };
      case 'REPORT_ISSUED': {
        if (p.report.kind !== 'CORRECTED') return null;
        const r = p.report;
        return {
          type: 'REPORT_CORRECTED',
          eventId: r.eventId,
          dedupeKey: `REPORT_CORRECTED:${r.eventId}:v${r.version}`,
          event: {
            reportId: r.reportId,
            version: r.version,
            verdict: r.verdict,
            baselineVersion: r.baselineVersion,
            reductionKwh: r.reductionKwh,
            deliveryRatio: r.deliveryRatio,
            hash: r.hash,
            issuedAt: r.issuedAt,
          },
        };
      }
      default:
        return null;
    }
  }
}
