# 版本兼容规则

## 1. 版本号

采用 `MAJOR.MINOR.PATCH`（可选 `v` 前缀，minor/patch 缺省为 0），
按语义化版本逐段比较。无法解析的客户端版本一律以
`unknown_client_version` 拒绝，不猜测、不转发。

## 2. 路由兼容窗口

每条路由声明一个**客户端版本窗口** `[MinSupported, MaxSupported]`：

- 客户端版本低于下限：`version_client_too_old`；
- 高于上限：`version_client_too_new`；
- 路由不存在：`route_not_found`。

这些拒绝发生在挑选上游**之前**，因此不会触达任何上游。

## 3. 同 MAJOR 与能力匹配（核心不变量）

窗口通过后，网关在上游集合中选择：

1. **只考虑与客户端同 MAJOR 的上游**。不同 MAJOR 代表存在破坏性变更，
   绝不会把 v1 调用转发给 v2 上游（反之亦然）。没有同 MAJOR 上游时：
   `version_no_upstream_for_major`。
2. 在同 MAJOR 候选中，要求上游提供路由所需的**全部**能力，选其中版本最高者。
3. 能力按版本区间声明（`Since` 起提供，`Until` 后或 `Removed=true` 终止）：
   - 能力从未声明 / 上游版本早于 `Since`：`version_capability_missing`
     （能力缺失）；
   - 能力曾提供但已被移除：
     `version_semantic_incompatible_capability_removed`
     （语义不兼容）。
   这两种结论刻意区分，便于调用方识别“升级即可用”与“语义已破坏”。

## 4. 重试不得跨越不兼容版本

重试发生在版本协商**之后**，且固定使用协商选定的那一个上游：

- 重试循环不重新协商、不切换上游、不改变 `UpstreamVersion`；
- 即使重试期间出现其他错误，也不会落到另一个 MAJOR 或能力不满足的上游；
- `semantic` 类故障不可重试（见下游文档），立即返回
  `upstream_semantic_error`。

测试：`gateway.TestRetryNeverCrossesIncompatibleVersion` 在注册了一个 v2
上游的情况下，让 v1 调用的重试全部失败，并断言 v2 上游调用次数为 0。

## 5. 幂等：重试不重复执行业务

进入调用阶段前，以业务幂等键（`idemKey`）去重：

- 同键并发只执行业务一次，其余请求共享同一结果（singleflight）；
- 成功结果按幂等键缓存，客户端重试/重放直接返回缓存结果，不再触达上游；
- 失败（含“结果未知”）**不写成功缓存**，允许后续重新尝试——
  由于失败的尝试没有产生业务结果，重试不会造成同一业务操作被执行两次。

> 说明：本地适配器以脚本方式在**失败**后恢复，成功的业务效果只可能由
> 第一次成功调用产生；真实上游应同时支持服务端幂等键去重。
