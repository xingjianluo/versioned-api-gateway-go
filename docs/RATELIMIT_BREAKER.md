# 限流与熔断状态语义

## 1. 分层令牌桶（`ratelimit`）

每个请求必须同时拿到两层令牌：

| 层 | 键 | 作用 |
| --- | --- | --- | --- |
| 调用方层 | caller | 限制该调用方横跨所有路由的总量（防止单租户横扫） |
| 路由层 | route | 限制该路由被所有调用方共享的总量 |

令牌补充公式（惰性计算，不依赖后台 goroutine）：

```
tokens = min(burst, tokens + elapsed_seconds * rate)
```

### 1.1 原子双扣，不超额、不泄漏

“按时间补充令牌 → 检查两层是否各有 ≥1 → 同时扣减”全程持有**同一把互斥锁**：

- 并发再多，放行数也恰好等于两层中较小的可用量，绝不会超额放行；
- 任一层不足时**两层都不扣减**，因此被拒请求不会白白消耗另一层的令牌
  （配额不泄漏）。

### 1.2 未配置即零配额

未在配置中声明的调用方/路由按零配额拒绝（`caller has no rate-limit
config` / `route has no rate-limit config`），避免“漏配即无限放行”。
可在构造时区分原因：`quota_layer=caller` → `caller_quota_exhausted`；
`quota_layer=route` → `route_quota_exhausted`。

对应测试：

- `ratelimit.TestConcurrentQuotaExactBurst`（500 并发 / burst=50，放行恰为 50）；
- `ratelimit.TestRouteDenyDoesNotLeakCallerTokens`（路由耗尽后调用方令牌完好）；
- `gateway.TestConcurrentQuotaExhaustionAtGateway`（网关层并发精确耗尽）。

## 2. 熔断器（`breaker`，按上游隔离）

三态及其迁移：

```
        连续失败 >= FailureThreshold
 closed ───────────────────────────────► open
   ▲                                      │
   │ 半开探测成功                           │ 经过 OpenCoolDown
   │                                      ▼
   └──────────────────────────────── half-open
                ▲                       │
                └── 半开探测失败：立即回到 open，并重新计时冷却
```

- **closed（关闭）**：正常放行；成功会清零连续失败计数。
- **open（打开）**：`Allow` 立即拒绝（reason=`circuit open, fast fail`），
  完全不触达上游；网关映射为 `degrade_circuit_open`。
- **half-open（半开）**：冷却到期后进入。锁内用一个 `probeInFlight`
  标志放行**恰好一个**探测请求；并发的其他请求看到探测在途即拒绝
  （reason=`half-open probe already in flight`），从根上避免恢复瞬间惊群。
  - 探测成功 → closed，失败计数清零；
  - 探测失败 → 立即 open，冷却从头开始（给上游更长恢复时间）。

### 2.1 可观测性

状态迁移通过同步回调 `StateListener(upstreamID, from, to, reason)` 发出，
网关把它写成 `breaker_transition from=... to=... reason=...` 日志。
对应测试：`breaker.TestHalfOpenSingleProbeThunderingHerd`（100 并发恰 1 个
探测）、`gateway.TestCircuitOpensThenHalfOpenProbe`（网关链路验证）。

## 3. 重试与降级原因（可区分）

上游故障分四类，网关结论一一对应：

| `error_kind` | 可重试 | 全部尝试失败后的结论 |
| --- | --- | --- |
| `timeout` | 是（退避前立即重试） | `degrade_upstream_timeout` |
| `connection` | 任 | `degrade_upstream_connection` |
| `overload` | 是 | `degrade_upstream_overload` |
| `semantic` | **否**，立即结束 | `upstream_semantic_error` |
| 熔断打开 / 半开探测被占 | —（不触达上游） | `degrade_circuit_open` |

重试次数记录为“首次之外的尝试数”，日志字段为 `retry` / `retry_count`。
每次失败都会计入该上游熔断器；熔断打开后剩余重试立即短路到
`degrade_circuit_open`，不再打上游。
