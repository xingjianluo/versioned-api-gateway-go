// Package idempotency 提供业务幂等键去重。
//
// 保证：
//   - 同一 idemKey 的并发调用只执行业务函数一次，其余调用等待并共享同一结果
//     （singleflight），避免重试/并发导致同一业务操作被重复执行；
//   - 成功结果按 idemKey 缓存，后续重放/重试直接返回原结果（Replayed）；
//   - 失败不缓存（尤其“结果未知”的错误），允许后续调用重新尝试业务；
//   - 结果未知（Unknown=true 的错误）同样不缓存，调用方绝不能把它当成功。
package idempotency

import (
	"context"
	"sync"
)

// AmbiguousError 包装“无法确认业务是否已执行”的错误（如超时）。
// 这种错误下幂等层不写成功缓存，调用方必须按未知处理，不得伪装成功。
type AmbiguousError struct {
	Cause error
}

// Error 实现 error。
func (e *AmbiguousError) Error() string {
	if e.Cause == nil {
		return "ambiguous: business outcome unknown"
	}
	return "ambiguous: " + e.Cause.Error()
}

// Unwrap 支持 errors.Is/As。
func (e *AmbiguousError) Unwrap() error { return e.Cause }

type call struct {
	done chan struct{}
	val  any
	err  error
}

type entry struct {
	cached   bool
	val      any
	inflight *call
	executed int // 该键真正执行 fn 的次数
	shared   int // 并发共享在途结果的次数
	replayed int // 命中成功缓存的次数
}

// Executor 按业务幂等键去重执行。
type Executor struct {
	mu      sync.Mutex
	entries map[string]*entry
}

// NewExecutor 构造去重执行器。
func NewExecutor() *Executor {
	return &Executor{entries: make(map[string]*entry)}
}

// Do 以 idemKey 去重执行 fn。
func (e *Executor) Do(ctx context.Context, idemKey string, fn func() (any, error)) (any, error) {
	e.mu.Lock()
	ent := e.entries[idemKey]
	if ent == nil {
		ent = &entry{}
		e.entries[idemKey] = ent
	}
	// 命中成功缓存：直接重放原结果。
	if ent.cached {
		ent.replayed++
		v := ent.val
		e.mu.Unlock()
		return v, nil
	}
	// 已有同键调用在途：共享其结果，绝不重复执行业务。
	if ent.inflight != nil {
		c := ent.inflight
		ent.shared++
		e.mu.Unlock()
		select {
		case <-c.done:
			return c.val, c.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	// 首次调用：执行业务。
	c := &call{done: make(chan struct{})}
	ent.inflight = c
	ent.executed++
	e.mu.Unlock()

	val, err := fn()

	// 先写结果再关闭通道：等待者从 done 收到信号时，结果已可见
	// （close 与 <-done 构成 happens-before）。
	c.val, c.err = val, err
	close(c.done)

	e.mu.Lock()
	defer e.mu.Unlock()
	ent.inflight = nil
	if err == nil {
		// 仅成功才缓存；失败/未知都允许后续重试。
		ent.cached = true
		ent.val = val
	}
	return val, err
}

// Stats 单个幂等键的去重统计。
type Stats struct {
	Executed int
	Shared   int
	Replayed int
}

// StatsOf 返回某键的统计（测试与可观测用）。
func (e *Executor) StatsOf(idemKey string) Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	ent := e.entries[idemKey]
	if ent == nil {
		return Stats{}
	}
	return Stats{Executed: ent.executed, Shared: ent.shared, Replayed: ent.replayed}
}
