# 请求处理链路与决策原因表

## 处理顺序

```
Serve(caller, requestID, clientVersion, route, idemKey, payload)
  │
  ├─ 1. 解析客户端版本（失败 -> unknown_client_version，rejected）
  ├─ 2. 查路由规则（缺失 -> route_not_found，rejected）
  ├─ 3. 版本协商：兼容窗口 + 同 major 能力匹配
  │      （任一不满足 -> version_* 原因，rejected，不触达上游）
  ├─ 4. 分层限流：调用方层 + 路由层原子双扣
  │      （不足 -> caller_quota_exhausted / route_quota_exhausted，rejected）
  ├─ 5. 以业务幂等键去重执行（idempotency.Do）
  │      └─ invokeWithRetry（固定使用第 3 步选定的同一个上游）：
  │           a. 熔断器 Allow？ 否 -> degrade_circuit_open（不触达上游）
  │           b. 带超时调用上游
  │           c. 成功 -> 上报熔断成功，缓存成功结果，返回 ok
  │           d. 失败 -> 按错误类别上报熔断；
  │              semantic 立即失败；其余可重试类别在次数内重试
  │           e. 次数耗尽 -> degrade_upstream_{timeout|connection|overload}
  └─ 6. 每个环节打印结构化日志（见下）
```

## 决策原因总表

| Outcome.Status | Reason | 含义 |
| --- | --- | --- |
| rejected | `unknown_client_version` | 客户端版本串无法解析 |
| rejected | `route_not_found` | 路由无规则 |
| rejected | `version_client_too_old` | 低于路由最小支持版本 |
| rejected | `version_client_too_new` | 高于路由最大支持版本 |
| rejected | `version_no_upstream_for_major` | 无同 major 上游 |
| rejected | `version_capability_missing` | 所需能力缺失（从未提供/版本太早） |
| rejected | `version_semantic_incompatible_capability_removed` | 所需能力已被移除 |
| rejected | `caller_quota_exhausted` | 调用方层令牌不足 / 未配置 |
| rejected | `route_quota_exhausted` | 路由共享层令牌不足 / 未配置 |
| degraded | `degrade_circuit_open` | 熔断打开或半开探测名额已被占用 |
| degraded | `degrade_upstream_timeout` | 重试耗尽：上游超时 |
| degraded | `degrade_upstream_connection` | 重试耗尽：连接失败 |
| degraded | `degrade_upstream_overload` | 重试耗尽：上游过载 |
| rejected | `upstream_semantic_error` | 语义错误，不可重试 |
| ok | `ok` | 成功（可能来自上游或幂等缓存重放） |

## 日志字段

所有日志为单行 `key=value`，常见字段：

`request_id`、`caller`、`route`、`client_version`、`idem_key`、
`stage`（version/ratelimit/breaker/invoke/complete）、`decision`、
`version_basis`/`version_reason`、`quota_layer`/`*_remaining`、
`breaker_state`、`from`/`to`（熔断迁移）、`attempt`、`retry`/`retry_count`、
`error_kind`、`retryable`、`business_executions`/`replayed`/`shared`。

## 本地验证入口

- 单元/集成测试：`go test ./... -race -count=3`
- 单进程演示：`go run ./cmd/demo`（覆盖正常调用、版本拒绝、
  并发限流、抖动重试、幂等重放、回调重放、熔断打开/探测恢复）
