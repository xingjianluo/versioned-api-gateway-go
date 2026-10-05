# 灰度分流：配置、粘性、回滚与故障联动

同一兼容版本范围内同时存在多个可用上游时，网关按可配置的比例在**兼容候选
集合内**灰度放量，比例可随时调整与整体回滚，并对同一业务操作提供上游粘性。

## 配置模型

```go
split.RouteConfig{
    Route:      "/pay",                 // 必填：按路由匹配
    MinVersion: v("1.0.0"),             // 可选：客户端版本范围下限（零值不限）
    MaxVersion: v("1.9.9"),             // 可选：客户端版本范围上限（零值不限）
    Weights: []split.Weight{
        {UpstreamID: "va", Weight: 20},
        {UpstreamID: "vb", Weight: 80},
    },
}
```

- 初始配置放在 `gateway.Config.Split`，构造期全量校验，非法则**拒绝启动**；
- 运行期通过 `Gateway.UpdateSplitConfigs(cfgs)` 调整；**整体回滚 = 用旧配置
  再调一次** `UpdateSplitConfigs`；
- 每次 `UpdateSplitConfigs` 都是全量替换：先整体校验，通过后**原子换入**
  （`atomic.Pointer` 快照），任何请求要么看到完整的旧配置、要么看到完整的
  新配置，不存在半更新；配置代次（`split_generation`）单调递增并写入日志。

### 非法配置（一律整体拒绝，绝不静默按默认放行）

| 非法情形 | 结论 |
| --- | --- |
| 权重为负 / 为零（含全部为零） | 拒绝：`weight must be positive` |
| 权重和超过上限（2^20） | 拒绝：`weight sum exceeds` |
| 引用不存在的上游 ID | 拒绝：`unknown upstream` |
| 同一配置内上游重复 | 拒绝：`duplicate upstream` |
| 版本范围 Min > Max | 拒绝：`min version > max version` |
| 同一路由的版本范围互相重叠 | 拒绝：`version range overlaps` |
| 路由为空 / 权重列表为空 | 拒绝 |

运行期拒绝时旧配置继续完整生效，代次不变，并记录
`split_config_rejected` 日志。

## 生效规则

1. **兼容优先**：分流只在版本协商得到的兼容候选集合内进行（同 major、
   能力匹配、在路由窗口内），权重不会把请求送出兼容范围；配置中引用了
   不兼容的上游时，该上游自然被候选过滤掉。
2. **确定性加权**：新业务键按 `fnv32(idemKey) % Σweight` 落桶选型——同一
   键在相同配置下结果确定，重试、回调重放、并发重复请求天然一致。
3. **无配置路由**：保持既有语义，选版本最高的候选（`default_highest_version`）。
4. **健康过滤**：选型时熔断器非 closed 的上游被视为不健康；有健康候选时
   只在健康候选内按权重归一化选择。

## 粘性与回滚语义

- 命中显式分流配置的业务键在首次选型后被**钉住**（pin），钉住关系保存在
  配置快照**之外**：比例调整、整体回滚、配置反复换入换出都不影响既有钉住；
- 同一业务键在比例调整、重试、回调重放、并发重复请求下稳定落到同一上游
  （`sticky_pin`），配合幂等层保证同一业务操作不会被两个后端各执行一次；
- 回滚不丢请求：在途请求继续走旧快照完成；新请求用新快照；钉住关系不变，
  因此回滚前后同一业务键的上游不变；
- 默认模式（无显式配置）不钉住，避免配置移除后残留影响。

## 与熔断 / 降级的联动（故障回退）

- **回退只在“请求根本不会发往该上游”时发生**：被钉住/应选的上游熔断打开
  （或 half-open）时，在仍然健康的兼容候选内按当前权重重选并**改钉**到新
  上游（`fallback_healthy`，日志带 `pinned_from`）；已实际发往上游的请求
  绝不因分流换上游——重试仍只在原上游内进行，因此不会出现同一操作在两个
  后端各执行一次；
- 改钉后旧上游恢复也**不回跳**，避免抖动与恢复瞬间的集中冲击
  （half-open 单探测由熔断器保证）；
- 所有候选都不健康时不在选择层腾挪：保留原选择交由熔断器快速失败，
  走既有 `degrade_circuit_open` 降级路径；
- 持续超时/过载的上游会先由熔断器打开，随后分流自然把新键与受影响键
  回退到健康候选，恢复过程同样受 half-open 单探测保护。

## 可观测日志

每个请求的分流结论都记录在 `split_decision`：

```
split_decision ... upstream=vb split_reason=fallback_healthy split_rule=/pay[*..*] \
  split_weights=va=50,vb=50 split_generation=3 fallback=true pinned_from=va
```

- `upstream`：最终分给哪个上游；
- `split_rule` / `split_generation` / `split_weights`：命中哪条规则、当时的
  配置代次与权重快照；
- `split_reason`：`weighted_new` / `sticky_pin` / `fallback_healthy` /
  `default_highest_version` / `all_unhealthy`；
- `fallback` / `pinned_from`：是否发生回退、回退前钉在哪个上游。

配置变更记录 `split_config_applied` / `split_config_rejected`。

## 本地验证

```bash
# 全部测试（含竞态，多跑几次确认稳定）
go test ./... -race -count=3

# 只看灰度分流相关用例（日志可见最终上游/命中规则/当时权重/是否回退）
go test ./gateway/ -race -v -run 'TestSplit'
go test ./split/ -race -v

# 单进程演示第 8 节：比例投放、粘性、故障回退、非法配置拒绝
go run ./cmd/demo
```
