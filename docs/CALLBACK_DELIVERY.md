# 回调投递保证

网关在业务处理成功后可以产生回调/通知（`callback` 包）。本地版用 JSON 文件
做持久化、用幂等接收方参考实现模拟下游，保证语义可以被确定性地验证。

## 1. 事件模型与关联信息

每个事件携带：

- `EventID`：回调事件唯一标识（接收方按它去重）；
- `Target`：投递目标；
- `RequestID`、`IdemKey`、`Payload`：**原始关联信息**，重放/重启后保持不变；
- `Attempts`、`MaxAttempts`、`Status`、时间戳。

## 2. 状态机

```
                 Deliver 返回确认(nil)
 pending ├──────────────────────────────► delivered（终态，重复重放为空操作）
   │
   ├─ Deliver 返回错误（无法确认）：保持 pending，持久化 Attempts+1，
   │   按指数退避等待（base * 2^(attempt-1)，封顶 8 倍）
   │
   └─ Attempts >= MaxAttempts：dead（终态，停止重试，保留现场）
```

关键规则：

1. **先持久化，后投递**：`Emit` 先把 pending 事件原子写入文件
   （临时文件 + rename），任何投递尝试之前落盘；
2. **确认才算成功**：只有 `Deliverer.Deliver` 返回 nil（接收方确认）
   才标记 delivered。超时、连接重置、进程终止等“无法确认是否送达”的
   情况一律保持 pending，**绝不伪装成功**；
3. **幂等登记**：同一 `EventID` 重复 `Emit` 不产生第二条事件；
4. **受限重放**：指数退避且总次数受 `MaxAttempts` 限制，耗尽转 `dead`，
   避免无限重试；
5. **并发去重**：同一事件同一时刻只允许一个投递协程（in-flight 集合）。

## 3. 进程重启后的重放

`FileStore.ListPending` 从事件目录读回所有 pending 事件（按创建时间稳定
排序）。用同一目录构造一个新的 `Dispatcher` 即等价于“进程重启”：

- pending 事件（含已消耗的 Attempts）被重新加载，在**既有次数基础上**继续
  投递，而不是从 0 静默重来；
- 已 delivered 的事件不会再出现；dead 事件保持终态；
- 关联信息完整保留。

对应测试：`callback.TestRestartReplaysPendingPreservingCorrelation`
（在退避中取消 ctx 模拟进程终止，随后用新 store/dispatcher 重放成功）。

## 4. 重复投递与乱序返回

接收方参考实现（`callback/receiver`）按 `EventID` 去重：

- 首次见到事件才真正处理一次，记录首达负载与到达次序；
- 重复 `EventID`（即使负载不同）返回 `duplicate=true` 且**不覆盖**首达结果；
- 因此“投递侧重试导致的重复投递”和“乱序到达”都不会让业务被重复处理。

对应测试：`receiver.TestDuplicateAndOutOfOrder`、
`callback.TestUnconfirmedRetriedThenConfirmed`（尝试 3 次但业务只处理 1 次）。

## 5. 边界与非目标（本地版）

- 文件存储面向单机本地验证，不是分布式队列；多机部署需要替换 `Store`/
  `Deliverer` 实现（接口已预留）；
- 退避等待期间若 ctx 被取消，事件保持 pending，交给下一轮/重启重放；
- `dead` 事件需要人工介入或外部改回 pending，系统不会自动复活它。
