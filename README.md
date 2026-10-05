# 版本化 API 网关核心（本地可验证版，Go）

一个**无外部依赖、只能在本地验证**的版本化 API 网关核心。它把业务能力通过不同的
上游适配器暴露给调用方，并在一条请求链路中一致地处理：

- 版本协商与上游能力匹配（拒绝原因可区分，绝不静默转发）；
- 调用方层 / 路由层共享的分层速率限制与突发配额（并发下不超额、不泄漏）；
- 按上游隔离的熔断器（关闭 / 打开 / 半开三态，半开单探测防惊群）；
- 兼容候选内的灰度分流：按路由（可带版本范围）配置比例、原子换入、整体回滚，
  业务键粘性钉住不漂移，上游不健康时按可解释规则回退到健康候选；
- 仅在兼容上游内进行的受限重试，以及与故障类别一一对应的降级路径；
- 业务幂等键去重（singleflight + 成功结果缓存），重试/重放不重复执行业务；
- 回调/通知投递链路：先持久化后投递、确认才算成功、退避有上限、重启可重放。

## 包结构

| 包 | 职责 |
| --- | --- |
| `versionx` | 语义化版本解析、路由兼容窗口、同 major 能力匹配 |
| `ratelimit` | 调用方层 + 路由层原子双扣的分层令牌桶 |
| `breaker` | 每上游三态熔断器，半开期单探测 |
| `split` | 灰度分流：权重配置校验与原子快照、业务键粘性、健康回退 |
| `upstream` | 适配器接口、故障四分类、脚本化故障注入适配器 |
| `idempotency` | 幂等键去重执行（并发合并、成功缓存、失败不缓存） |
| `callback` | 事件 JSON 文件持久化、退避重放、dead 终态 |
| `callback/receiver` | 按 EventID 去重的回调接收方参考实现 |
| `gateway` | 组合以上组件的网关核心 |
| `internal/logging` | 结构化决策日志（请求标识/版本/配额/熔断/重试） |
| `cmd/demo` | 单进程演示程序 |

## 本地验证

要求 Go 1.25+（开发环境为 Go 1.26）。

```bash
# 全部单元/集成测试，带竞态检测（多跑几次确认稳定）
go test ./... -race -count=3

# 只看某个场景的详细决策日志
go test ./gateway/ -race -v -run TestRetryOnJitterThenSuccessExecutesBusinessOnce

# 单进程演示所有场景（不启动任何网络服务）
go run ./cmd/demo
```

测试日志中会打印每个请求的：

- `request_id`：请求标识；
- `version_basis` / `version_reason`：版本判定依据与拒绝原因；
- `quota_layer` / `caller_remaining` / `route_remaining`：配额决策依据；
- `breaker_state` / `from->to` 状态迁移：熔断决策依据；
- `attempt` / `retry` / `retry_count`：重试次数；
- `error_kind`：上游故障类别（timeout / connection / overload / semantic）；
- `split_rule` / `split_weights` / `split_generation`：分流命中规则、当时权重快照与配置代次；
- `split_reason` / `fallback` / `pinned_from`：分流结论原因、是否健康回退、回退前钉住的上游。

## 文档

- [版本兼容规则](docs/COMPATIBILITY.md)
- [限流与熔断状态语义](docs/RATELIMIT_BREAKER.md)
- [灰度分流：配置、粘性、回滚与故障联动](docs/GRAY_SPLIT.md)
- [回调投递保证](docs/CALLBACK_DELIVERY.md)
- [请求处理链路与决策原因表](docs/REQUEST_FLOW.md)
