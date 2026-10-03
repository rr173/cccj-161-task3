# 电力需求响应（Demand Response）事件编排服务

运营方按 **计量点 / 基线算法 / 目标削减量 / 事件时窗** 发布指令；参与设备回报 **接受 / 执行 / 恢复** 状态。
基线在事件开始时冻结；迟到的电表读数**只能形成订正版**，永不改写原始执行裁决。多个聚合商声明同一设备、
指令撤销与设备执行并发、回执乱序、通信中断等场景下，**一台设备同一时刻只归属一个有效事件，并保持明确水位**。

- 零依赖、纯 Node.js（≥ 20），无数据库、无构建步骤。
- **事件溯源 + SHA-256 哈希链**：所有裁决（发布、冻结、声明、回执、撤销、读数、报告）都是只增记录，可独立核验；任何历史篡改都会被检出。
- 仿真时钟：时间只由 `/api/sim/tick` 推进，裁决、迟到数据、订正版本完全可重复演示。

## 快速开始

```bash
npm start                 # http://localhost:8080
# 可选：PORT=8080 DR_MEMORY=1 npm start   # 纯内存，不落盘
```

- 设备执行页面（运营台）：http://localhost:8080/
- 自动化测试：`npm test`（38 个领域/恢复/通知单测；`npm run test:notify` 可单跑通知模块 11 项）
- 端到端场景：先 `npm start`，再 `npm run test:e2e`（24 项 HTTP 断言）

## 重启恢复契约（日志损坏 / 断电残尾）

服务重启时逐行重放 `data/events.log`，并对每条记录独立校验**序号连续性、prevHash 衔接、内容哈希**：

- **任何完整记录异常（序号被改写、中间断链、内容与哈希不符、结构性损坏）→ 只读隔离（QUARANTINED）**。
  - 检出**首个损坏位置**（seq、物理行号、字节偏移、原因）后立即停止重放，内存只保留最后一条验证通过记录之前的一致状态；
  - 服务**照常启动**：`GET /api/state`、`/api/log`、`/api/chain/verify`、报告查询与运营台页面均可只读访问，
    状态接口与页面横幅显示首个损坏位置；
  - **所有业务写入（含 `/api/sim/reset`）返回 HTTP 503（`LOG_QUARANTINED`），日志绝不追加、文件原样保留**；
  - 隔离不会因再次重启而自愈，需人工核验。
- **仅文件末尾的单条不完整记录可作为断电残尾恢复**：残尾必须是“未写完且无终止换行”的最后一个物理行。
  - 残尾按**原始字节**原样复制到 `data/quarantine/torn-tail-<时间戳>-<pid>.log`，
    同目录 `.meta.json` 留存字节长度、**残尾 sha256**、截断位置、最后一条完好记录的 seq/hash；
  - 主日志截断到残尾之前，服务从最后一条验证通过的记录恢复，**新记录延续原哈希链**（序号与 prevHash 接续）；
  - 残尾之后还有任何内容（多条坏行、坏行后仍有记录、带换行的坏物理行）一律按损坏隔离。
- 恢复后再次重启即为正常重启；`/api/state` 顶层暴露 `mode`（`OK`/`QUARANTINED`）、`readOnly`、`recovery`。

## 一页走查（运营台）

1. **发布指令**：左侧选择计量点、基线算法、目标削减 kW、时窗长度，发布。事件为 `SCHEDULED`，基线尚未冻结。
2. **设备声明**：中间表格为每个设备选择聚合商并“声明”。同一设备：
   - 同事件换聚合商 → `DEVICE_ALREADY_CLAIMED (409)`；
   - 声明到另一个仍有效的事件 → `DEVICE_BUSY (409)`。
3. 顶栏 **+15 分钟 / +1 小时** 推进时间。时间跨过事件开始点时自动 **冻结基线 v1** 并激活事件。
4. 对已声明设备发送 `ACCEPTED → EXECUTING → RECOVERED` 回执；可点 **↕ 乱序执行** 演示跳序号：
   高序号先到进入缓冲，缺口补齐后自动排水，过期重放返回 `STALE_SEQ`，每个设备都有单调 **watermark（wm）**。
5. 事件进行中点 **撤销指令**：设备立即释放（可被新事件声明），之后迟到的 `EXECUTING` 被拒绝，
   但迟到的 `RECOVERED` 仍会被记录为终态水位（不改变归属、不改写裁决）。
6. 在计量点面板 **中断通信** 后跨过事件结束：缺失区间形成 `DATA_GAP`，缺口过多时裁决为 `INCONCLUSIVE`。
7. 事件结束自动出具 **v1 原始裁决**（锚定冻结基线 v1 与结束时数据水位）。
8. 左下 **补送 MP-1 的 D-4 历史读数**：该历史日在种子数据中缺失。迟到数据
   - 产生 **基线 v2（BASELINE_CORRECTED）** 与 **报告 v2（CORRECTED）**；
   - **v1 字节级不变**（UI 中指纹前后一致，接口 `/api/reports/:id/versions/1` 哈希恒定）。
9. **补传选中事件窗口内缺失的实际读数**（通信恢复后的电表补传）：有缺口才产生新订正版；无新证据不产生新版本。
10. **验证事件链 + 报告指纹**：重新计算整条哈希链与每份报告的内容指纹。

## 关键不变量（如何落地需求）

| 需求 | 实现 |
| --- | --- |
| 基线在事件开始时冻结 | 时间跨过 `startMs` 时写入 `BASELINE_FROZEN(version=1)`，此后基线对象只读 |
| 迟到数据只能订正，不能改写原始裁决 | 新读数只能追加；重算有差异才追加 `BASELINE_CORRECTED` / 新 `REPORT_ISSUED(version=n)`，v1 永不更新 |
| 同一区间读数不可变 | 已存在读数再次写入 → `CONFLICTING_READING(409)`，幂等重复写入安全忽略 |
| 一设备只属一个有效事件 | 声明时检查归属：重叠有效事件 `DEVICE_BUSY`；撤销/结束时 `DEVICE_RELEASED` |
| 撤销与执行并发 | 命令严格串行、按到达顺序裁决；撤销即释放归属，迟到 `RECOVERED` 作为终态水位留痕 |
| 回执乱序 | 每设备单调 `clientSeq` 水位；超前缓冲、缺口自动排水、过期拒绝（`STALE_SEQ`/`PHASE_CONFLICT`），状态机禁止非法跃迁 |
| 通信中断 | 表计离线期间无读数 → 区间缺口；裁决按缺口比例给 `INCONCLUSIVE`；恢复后补传只产生订正版 |
| 可验证的削减报告 | 报告含逐区间基线/实际/削减、设备履约行、裁决等级与 **内容指纹**；链上锚定，`/verify` 可独立重算 |

报告裁决等级：`MET`（交付率 ≥ 100%）、`PARTIAL`（≥ 50%）、`MISSED`、`INCONCLUSIVE`（缺口 ≥ 40%）、
`NO_PARTICIPATION`、`RECALLED`。设备履约：`PERFORMED / UNDERPERFORMED / FAILED / OPT_OUT`。

## 聚合商通知投递模块（独立模块）

需求响应事件在 **发布 / 撤销 / 开始 / 结束 / 报告订正** 五个时点，向各订阅聚合商投递
**带 Ed25519 签名的不可变通知信封**。模块与领域内核解耦，拥有独立的只增 + SHA-256
哈希链 `data/notify.log`（复用同一套残尾恢复 / 篡改只读隔离契约）。

- **不可变信封**：入队即写 `N_ENVELOPE`（永不更新/删除），含每订阅方单调 `subSeq`、
  稳定 `idempotencyKey`、信封内容指纹 `bodyHash`、入队时签名密钥 `signKid`。
- **按订阅方独立持久待发队列**：**只有队头信封确认落链才推进游标**；队头未确认绝不投递后继。
  不同订阅方各跑独立投递循环与退避计时，**互不阻塞**。
- **稳定幂等键**：`dr-<subId>-<subSeq>-<dedupeKey哈希>`，入队时一次确定；超时/失败后再次请求
  完全不变，订阅端据此去重（确认丢失后重投返回 `duplicate=true`，业务只执行一次）。
- **进程重启续投**：成功只在 2xx 回执落链（`N_ACK`）后才算数。重启从 `notify.log` 重放重建
  队列/游标/死信/密钥，未确认信封继续投递，**游标不回退、不重复推进**。桥接器以持久消费水位
  （`N_BRIDGE_WATERMARK`）增量读内核链，重启不漏发、不重发。
- **订阅校验**：登记后聚合商向订阅端 `/validate` 发带签名 challenge，订阅端回源 JWKS 验签并
  原样回 challenge，订阅才置 ACTIVE。
- **签名通知与密钥轮换**：非对称 Ed25519，私钥签名、订阅端只信公开 JWKS；轮换把旧密钥显式
  落链为 `RETIRED` 且公钥长期保留，因此**轮换前已在途、轮换后重投/复验的旧信封仍可验签**。
- **死信与人工重入队**：连续失败达上限（默认 5 次，指数退避 2/4/8/16/30s）进 DLQ 并移出队头让
  后继继续；人工重入队后信封**回到原始 subSeq 位置**，恢复后顺序不变、签名证据仍可验证。

快速体验：

```bash
npm start                 # 聚合商/编排服务 http://localhost:8080
npm run subscriber        # 演示订阅端（默认 9100，自带故障模式控制台）
# 投递台：http://localhost:8080/notify/   订阅端控制台：http://localhost:9100/
npm run demo:notify       # 全自动端到端：六类故障/轮换/死信/重启场景（自起进程，约 100s）
npm run test:notify       # 11 个通知模块单测
```

订阅端故障模式：`ok` / `fail`（持续 503→死信）/ `failOnce`（暂时失败一次）/ `timeout`
（超时不响应，逼聚合商用同一幂等键再请求）/ `ackloss`（已处理但首次应答丢失）。
多开订阅端：`SUB_PORT=9101 SUB_NAME=名称 npm run subscriber`。

通知模块 HTTP API（均在 `/api/notify` 下）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/notify/view` | 投递台完整视图（订阅方队列/游标、信封、尝试、DLQ、链校验） |
| GET | `/api/notify/jwks` | 公开 JWKS（含 ACTIVE 与 RETIRED 公钥，绝不含私钥） |
| POST | `/api/notify/subscribers` | 登记订阅方 `{name,callbackUrl}` 并立即做订阅校验 |
| POST | `/api/notify/subscribers/:id/verify` | 重试订阅校验握手 |
| POST | `/api/notify/keys/rotate` | 轮换签名密钥（旧密钥落链 RETIRED，公钥保留） |
| GET | `/api/notify/envelopes/:id` | 信封详情 + 逐条尝试 + 签名证据 |
| POST | `/api/notify/envelopes/:id/ack` | 显式确认 API（重复确认安全，游标不重复推进） |
| GET | `/api/notify/envelopes/:id/verify-signature` | 独立复验信封体哈希与签名密钥受信状态 |
| GET | `/api/notify/dlq` | 死信队列 |
| POST | `/api/notify/dlq/:id/requeue` | 人工把死信重新入队（回到原 subSeq 位置） |
| POST | `/api/notify/pump` | 立即增量桥接内核事件 |
| POST | `/api/notify/reset` | 演示复位（清空通知链并重新生成密钥） |

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/state` | 运营台完整视图（含哈希链校验结果） |
| POST | `/api/events` | 发布事件 `{meterId,targetKw,algorithm,startInMinutes,durationIntervals}` 或显式 `startMs/endMs` |
| POST | `/api/events/cancel` | `{eventId,reason}` 撤销 |
| POST | `/api/claims` | 聚合商声明设备 `{eventId,deviceId,aggregatorId}` |
| POST | `/api/receipts` | 设备回执 `{eventId,deviceId,status,requestedKw?,clientSeq?}`（缓冲返回 202） |
| POST | `/api/readings` | 通用读数上报 `{items:[{meterId,ts,value}]}` |
| POST | `/api/late-history` | 迟到历史日 `{meterId,dayOffset}` |
| POST | `/api/backfill` | 补传事件窗口内缺失实际读数 `{eventId}` |
| POST | `/api/meters/offline` | 模拟通信中断/恢复 `{meterId,offline}` |
| POST | `/api/sim/tick` | 推进仿真时钟 `{minutes}` |
| POST | `/api/sim/reset` | 截断日志并重建种子数据 |
| GET | `/api/reports/:eventId/versions/:v` | 完整报告（逐区间 + 设备行 + 指纹） |
| GET | `/api/reports/:eventId/versions/:v/verify` | 独立重算报告指纹 |
| GET | `/api/chain/verify` | 哈希链 + 全部报告指纹校验 |
| GET | `/api/log?type=&limit=` | 原始裁决日志（JSONL 记录） |

## 数据模型（事件类型）

事件日志默认落盘到 `data/events.log`（每行一条 JSON，UTF-8 JSONL）：

```
SEED_CLOCK / SEED_AGGREGATOR / SEED_METER / SEED_DEVICE / SEED_DAY_READINGS
EVENT_PUBLISHED · BASELINE_FROZEN · EVENT_ACTIVATED · EVENT_CANCELLED · EVENT_CLOSED
DEVICE_CLAIMED · DEVICE_RELEASED
DEVICE_RECEIPT · RECEIPT_BUFFERED · RECEIPT_DROPPED
READINGS_BATCH · DAY_READINGS_LOADED · SIM_METER_OFFLINE · SIM_TICK
BASELINE_CORRECTED · REPORT_ISSUED
```

每条记录：`{seq,ts,type,payload,prevHash,hash}`，
`hash = sha256(规范化JSON({seq,ts,type,payload,prevHash}))`，规范化时键名排序，保证任意实现可复算。

## 目录

```
src/
  server.js       HTTP API + 静态页面
  kernel.js       领域内核（命令校验 → 追加记录 → 投影/水位）
  baseline.js     基线算法（近10均值 / 居中8日 / 近5均值，工作日同类日筛选）
  adjudication.js 削减报告纯函数（区间能量、裁决等级、内容指纹）
  store.js        只增日志 + SHA-256 哈希链（重启恢复：残尾归档续链 / 损坏只读隔离）
  seed.js         确定性种子（2 聚合商 / 3 计量点 / 5 设备 / 21 天 15 分钟曲线，含缺失日）
  clock.js        仿真时钟
  util.js         时间区间、规范化 JSON、哈希
  notify/         聚合商通知投递模块（独立持久链 data/notify.log）
    engine.js       不可变信封/每订阅方队列与游标/确认幂等/死信重入队/密钥轮换/订阅校验
    dispatcher.js   每订阅方独立投递循环（互不阻塞）、超时中断、指数退避、稳定幂等键
    bridge.js       内核链 → 通知（按持久消费水位增量翻译 5 时点，重启不漏不重）
    crypto.js       Ed25519 签名/验签 + JWKS（轮换后旧 RETIRED 公钥保留）
    routes.js       /api/notify/* HTTP 路由
subscriber/       演示订阅端（server.js：订阅校验、独立验签、5 种故障模式、证据控制台）
public/           运营台（index.html + app.js，含隔离/残尾恢复横幅）
public/notify/    通知投递台（index.html + app.js：订阅/轮换/队列/证据/死信）
test/kernel.test.js   14 个领域单测（node:test）
test/recovery.test.js 13 个恢复契约单测（残尾/篡改/断链/503 边界/重启续链，含真实 HTTP）
test/notify.test.js   11 个通知模块单测（顺序/游标幂等/死信/轮换/桥接/重启重放）
scripts/e2e.mjs       24 项 HTTP 端到端断言
scripts/notify-demo.mjs  通知模块全自动端到端（订阅校验/暂时失败/确认丢失/重复确认/
                         密钥轮换旧在途/死信重入队/进程重启续投，约 100s）
```
