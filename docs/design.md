# 设计与语义说明

## 1. 版本兼容规则

版本为语义化 `major.minor.patch`。每个上游适配器声明：

- `APIVersion()`：该上游实现的 API 版本；
- `Capabilities()`：提供的业务能力及每个能力的**语义代际**（`Semantic`）。

调用方请求携带：调用方 API 版本 + 所需能力列表（能力名 + 最低/精确语义代际）。

### 协商两关（任何一关失败都拒绝，绝不静默转发）

**第一关：网关支持范围**（`version.Policy.Accept`）

| 情况 | 拒绝原因 `RejectReason` |
|---|---|
| 版本号非法或不在 `Known` 列表 | `unknown_version` |
| 版本已知但低于 `MinSupported` | `version_too_old` |

**第二关：上游能力匹配**（`upstream.Registry.Select`）

上游同时满足以下条件才进入候选集合：

1. `major` 与调用方相等；
2. 上游 `minor` **不高于**调用方 `minor`（只在同一 major 内向旧兼容，
   不把调用方转发给它不认识的更新 minor）；
3. 调用方要求的每项能力，上游都提供，且语义代际兼容：
   - `ExactSemantic=true`：必须同代际；
   - 否则上游代际 `>= MinSemantic`（新代际向后兼容旧调用方）。

无候选时按下列**可区分原因**拒绝：

| 情况 | 拒绝原因 |
|---|---|
| 没有任何上游提供所需能力名 | `capability_missing` |
| 提供了能力但语义代际不兼容 | `semantic_conflict` |
| 存在语义兼容的提供者，但都被版本门控排除（如只部署了 2.0 上游） | `semantic_conflict` |

### 重试的版本边界

重试只在**本次协商出的同一兼容候选集合内**轮换候选，
**永远不会**因为重试而转发到 major 不同、minor 更高或语义不兼容的上游。

## 2. 限流语义

两层令牌桶，**两层都放行才放行**：

- 调用方桶（`callerID`）：单调用方总配额；
- 路由桶（`route`）：被所有调用方**共享**的路由突发配额。

每个桶：容量 `Burst`，按 `Rate` 令牌/秒惰性补充。

并发正确性：

- 两次扣减在同一把锁内原子完成，因此 200 并发、突发 20 时恰好放行 20，
  **不会超额放行**；
- 调用方桶先预扣 1 个令牌；若路由桶不足而拒绝，调用方桶立即**回补**，
  因此路由级拒绝**不会泄漏调用方配额**；
- 拒绝原因区分 `caller_quota_exhausted` 与 `route_quota_exhausted`，
  决策记录带两层桶的剩余令牌。

## 3. 熔断与降级语义

每个上游实例独立一个熔断器（`circuit.Breaker`），三态：

| 状态 | 语义 | 请求处理 |
|---|---|---|
| `closed` | 正常 | 放行；累计连续失败，达到 `FailureThreshold` → `open` |
| `open` | 熔断中 | 一律拒绝（`open_cooldown`），不打上游；冷却到期后转入 `half_open` |
| `half_open` | 探测恢复 | **只放行 1 个探测请求**，其余并发请求拒绝（`half_open_probe_in_flight`） |

半开探测规则（避免恢复瞬间惊群）：

- 冷却到期时**第一个**请求独占探测权；
- 探测成功数达到 `SuccessThreshold` → 关闭并清零计数；
- 探测期间任何失败 → 立即重新打开，冷却时间从失败时刻重新计起；
- 200 并发同时撞上冷却到期点，也只有 1 个请求能成为探测（有测试覆盖）。

降级：全部尝试因**可重试的上游故障**（超时/连接失败/过载）或熔断拒绝而失败，
且该路由配置了兜底响应时，返回兜底内容，结果状态 `degraded`；
原始故障类别（`upstream_timeout` / `upstream_connection_error` /
`upstream_overloaded` / `circuit_open`）完整保留在决策记录与错误中，可区分。
未配置兜底则直接失败，错误类别同样可区分，且 `Retryable` 标注是否可安全重放。

状态迁移通过 `Breaker.Listen` 产生事件，`Gateway.CircuitSnapshot()`
可随时观测全部上游的当前状态。

## 4. 重试与幂等语义

重试必须同时满足：

1. 操作声明为**幂等**（`In.Idempotent`）并携带业务幂等键；
2. 错误属于可安全重放类别：上游超时、连接失败、过载，或熔断打开；
3. 未超过有界次数 `Retry.MaxAttempts`（指数退避 + 抖动，等待可被取消）。

非幂等操作**只执行一次**：失败也不重试（返回原因
`non_idempotent_operation_not_retried`），保证同一业务操作不被重复执行。

进程内幂等存储（`idempotency.Store`）：

- 相同业务键的并发请求只有一个真正执行，其余等待并共享同一结果；
- 成功结果被保存，后续同键请求直接复用（`idempotent_replay`）；
- 明确失败的记录被移除，允许后续携带同键的新请求作为安全重放重新执行；
- 对"超时"这类**不确定上游是否已执行**的错误，网关只在操作幂等时重放，
  由业务幂等键在上游侧保证不重复入账。

## 5. 回调投递保证

回调链路采用 **at-least-once + 幂等消费** 模型（`pkg/callback`）。

存储：`FileOutbox` 把每条通知以 JSON 文件存于
`pending/`、`delivered/`、`dead/` 三个子目录，状态更新为目录间原子改名，
进程重启后从磁盘恢复。

规则：

1. **先落盘后投递**：通知先进入 outbox 再调用投递器；
2. **未确认不伪装成功**：投递器返回 error 即视为未送达，保持 `pending`；
   只有对端确认（nil）才置 `delivered`；
3. **有界重放、避免无限重试**：每次尝试前先持久化自增的 `Attempts`，
   崩溃后重放尝试次数不清零；达到 `MaxAttempts` 仍未确认 → 进入 `dead`
   死信并返回 `DeadLetteredError`；
4. **重复投递幂等**：同键同序号（或更旧序号）的重复登记被吸收，
   已送达通知的重复投递不会产生第二次发送；
5. **乱序返回保护**：同键以单调 `Seq` 区分新旧；迟到的旧序号确认不会
   把更新序号的已送达状态回退；
6. **保留原始关联信息**：通知携带 `Correlation`（如请求 ID/业务 ID），
   重放与死信记录中始终保留，保证可追溯；
7. **重启重放**：`Manager.ReplayPending` 扫描全部 pending 并发受控重放，
   返回 `{Scanned, Delivered, StillPending, Dead}` 报告；
8. 同键并发投递由管理器互斥，同一时刻只有一个真实投递在途。

> 语义边界：回调保证"已确认才报告成功、未确认保持可重放/可介入"，
> 不承诺精确一次（exactly-once）；接收方需按通知键/序号幂等。

## 6. 错误可区分性

`errors.GatewayError` 携带：`Kind`（version_rejected / rate_limited /
upstream_timeout / upstream_connection_error / upstream_overloaded /
circuit_open / degraded / callback_delivery_failed 等）、
人类可读 `Reason`、`RequestID`、`Retryable`、`Attempts`。
版本拒绝另带 `version.RejectReason`，上游失败另带 `upstream.ErrorKind`，
回调死信另带 `DeadLetteredError` 与 Correlation。

## 7. 本地验证

```bash
go test -race -count=1 ./...
go run ./cmd/demo
```

测试覆盖（均带断言与决策日志）：

- 并发配额耗尽：200 并发突发 50/20，成功数精确等突发量，无配额泄漏；
- 版本不兼容：未知版本、旧版本、能力缺失、语义冲突、只存在跨 major 上游；
- 上游抖动：超时后在兼容集合内重试成功；非幂等不重试；
- 熔断：连续失败打开、半开单探测（200 并发惊群）、探测失败重开、探测成功恢复；
- 回调：重复投递只发一次、失败后确认不伪装成功、耗尽次数入死信、
  乱序序号保护、模拟崩溃后重启重放保留尝试次数与 Correlation。
