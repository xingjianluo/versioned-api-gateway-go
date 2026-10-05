# 灰度分流：配置、粘性、回滚与故障回退

同一个兼容版本范围里同时存在多个可用上游时，网关按**路由级分流计划**
在兼容候选之间按比例投放流量，而不是永远只走版本最高的那个。

## 配置与生效规则

分流计划在 `gateway.Config.SplitPlans` 中按路由声明，也可在运行时用
`Gateway.SetSplitPlan(route, entries)` 调整、`Gateway.RollbackSplitPlan(route)` 整体回滚：

```go
SplitPlans: map[string][]graysplit.Entry{
    "/pay": {
        {UpstreamID: "orders-v1-4", Weight: 8}, // 8 份
        {UpstreamID: "orders-v1-3", Weight: 2}, // 2 份
    },
}
```

- **只在兼容候选集内投放**：计划条目先与版本协商产出的候选集
  （同 major 且能力满足，见 [COMPATIBILITY.md](COMPATIBILITY.md)）求交，
  交集中的条目按权重分桶；计划里指向不兼容上游的权重对该请求不生效，
  请求**绝不会**被送出兼容集（不跨 major、不跨能力边界）。
- **原子生效**：每份计划整体校验通过后以单次原子指针替换发布，
  并带单调递增的 `Revision`。进行中的请求要么看到完整的旧计划、
  要么看到完整的新计划，不存在半更新，也不存在"既没算进旧配置
  也没算进新配置"的请求。
- **非法配置明确拒绝**（返回错误，旧计划原样保留，绝不静默按默认值放行）：
  空计划、空/重复上游 ID、负权重、权重和为 0、权重和超过
  `graysplit.MaxTotalWeight`（10000）、引用不存在的上游或路由。
  启动期配置非法时 `New` 直接返回错误。
- **整体回滚**：`RollbackSplitPlan` 恢复上一份计划，但以**新的 Revision**
  发布（避免与历史 Revision 混淆）；没有历史计划时返回错误，当前计划不变。
- 路由未配置计划时（`no_plan`），沿用"版本最高者优先"的协商默认顺序，
  行为与引入分流前一致。

## 业务级粘性

- 首次为某业务幂等键分流时，按 `hash(route, idemKey)` 在权重桶中**确定性**
  选择上游并粘性固定（`weighted`）；此后该键命中 `sticky` 规则，
  **比例调整、整体回滚、重试、回调重放、并发重复请求都不会让它漂移**。
- 并发重复请求由幂等层 singleflight 合并、成功结果被缓存重放，
  同一业务操作在整个集群视图下只执行一次（见 README 幂等一节）。
- 重试仍只在已选定的同一个上游内发生，不跨上游、不跨版本。
- 唯一的重新固定场景：被固定的上游**被新计划整体移出**（或权重回收为 0），
  此时按当前计划重新选择并重新固定（`pin_withdrawn_weighted`），
  日志中规则名明确可辨。已完成执行的业务不受影响（重放命中幂等缓存）。

## 与熔断 / 限流 / 降级的关系

处理顺序：版本协商 → 分层限流 → **灰度分流** → 熔断 → 幂等去重执行（含重试）。

- 分流只决定"偏好顺序"，熔断决定"单个上游此刻能不能打"。
- 首选上游不可用（熔断打开 / 半开探测名额被占 / 重试耗尽仍
  timeout、connection、overload）时，按分流给出的**确定性顺序回退**
  到下一个仍然健康的兼容候选，并打印 `split_fallback` 日志
  （`from` / `to` / `cause` / 当时比例与 Revision），而不是把失败直接
  甩给调用方。回退目标仍是兼容候选，绝不跨版本。
- `semantic` 错误说明请求本身不合法：**立即失败，绝不回退**（避免把
  非法请求放大到全部上游）。
- 全部候选都不可用时才进入既有的降级结论
  （`degrade_circuit_open` / `degrade_upstream_*`）。
- 恢复瞬间不会形成集中冲击：熔断半开期只放行单探测；故障期间流量已
  按确定性顺序摊到健康候选上，恢复后粘性键逐步回到原上游。
- 限流在分流之前完成，比例调整不影响配额扣减语义。

## 决策日志字段

分流相关的结构化日志（单行 key=value）：

| 消息 | 关键字段 |
| --- | --- |
| `split_plan_published` / `split_plan_rollback` | `route`、`split_revision`、`split_weights` |
| `split_decided` | `decision`（首选上游）、`split_rule`、`split_revision`、`split_weights`、`split_pinned`、`candidates` |
| `split_fallback` | `from`、`to`、`cause`、`split_revision`、`split_weights` |
| `gateway_ok` / `gateway_degrade` | `upstream`、`split_rule`、`split_revision`、`split_fallback` |

`split_rule` 取值：`weighted`（按权重分桶）、`sticky`（命中粘性固定）、
`pin_withdrawn_weighted`（固定上游被移出计划后重选）、
`no_plan`（无计划，走协商默认顺序）、`no_plan_entry`（计划与候选集无交集，
退化为协商默认顺序）。

`Outcome` 同步暴露 `SplitRule` / `SplitRevision` / `SplitWeights` / `Fallback`，
便于调用方与测试断言。

## 本地验证

```bash
# 全部测试（含竞态，多跑几次确认稳定）
go test ./... -race -count=3

# 只看灰度分流场景
go test ./gateway/ -race -v -run 'Split|Fallback|Sticky|Rollback'
go test ./graysplit/ -race -v

# 单进程演示（第 8 节为灰度分流）
go run ./cmd/demo
```

覆盖的关键场景：

- 比例并发调整 + 周期性回滚下，同一业务键不被重复执行、不漂移
  （`TestSplitPlanAdjustConcurrentSameKeyExecutesOnce`）；
- 首选上游持续超时 / 熔断打开后的回退分流
  （`TestFallbackWhenPreferredUpstreamUnhealthy`）；
- 非法比例配置在启动期与运行期都被拒绝且旧计划不变
  （`TestNewRejectsInvalidSplitPlan`、`TestSetSplitPlanInvalidRejectedAndOldPlanKept`）；
- 比例翻转与整体回滚下粘性保持（`TestStickyAcrossRatioChangeRetryAndReplay`）；
- 分流绝不跨 major（`TestSplitNeverCrossesIncompatibleMajor`）；
- 计划发布的原子性（`TestConcurrentPickAndSetPlanNoHalfUpdate`）。
