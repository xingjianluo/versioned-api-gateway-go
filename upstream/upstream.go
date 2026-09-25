// Package upstream 定义上游适配器接口与可在本地制造故障的内存适配器。
//
// 故障被分为可区分的四类（ErrorKind），供网关做差异化重试/降级：
//   - timeout（上游超时）、connection（连接失败）、overload（上游过载）：
//     可重试，但只在兼容版本/同一上游选择结论下重试；
//   - semantic（语义不兼容/业务拒绝）：不可重试，立即失败。
package upstream

import (
	"context"
	"sync"
	"time"
)

// Request 是发往上游的业务请求。
type Request struct {
	RequestID string
	Route     string
	Version   string
	IdemKey   string // 业务幂等键
	Payload   string
}

// Response 是上游返回。
type Response struct {
	UpstreamID string
	Version    string
	Result     string
}

// ErrorKind 区分上游故障类别，供重试/降级做可区分决策。
type ErrorKind int

// 上游故障类别。
const (
	KindNone ErrorKind = iota
	KindTimeout
	KindConnection
	KindOverload
	KindSemantic
)

// String 返回故障类别名。
func (k ErrorKind) String() string {
	switch k {
	case KindNone:
		return "none"
	case KindTimeout:
		return "timeout"
	case KindConnection:
		return "connection"
	case KindOverload:
		return "overload"
	case KindSemantic:
		return "semantic"
	default:
		return "unknown"
	}
}

// Retryable 报告该故障类别是否允许重试。
func (k ErrorKind) Retryable() bool {
	return k == KindTimeout || k == KindConnection || k == KindOverload
}

// UpstreamError 携带故障类别的上游错误。
type UpstreamError struct {
	Kind    ErrorKind
	Message string
}

// Error 实现 error。
func (e *UpstreamError) Error() string {
	return e.Kind.String() + ": " + e.Message
}

// Adapter 上游适配器接口。
type Adapter interface {
	Desc() StaticDesc
	Invoke(ctx context.Context, req Request) (Response, error)
}

// StaticDesc 适配器静态描述（版本与能力）。
type StaticDesc struct {
	ID      string
	Version string
	Caps    []string
}

// ScriptedBehavior 脚本化故障注入（本地验证用）。
type ScriptedBehavior struct {
	mu sync.Mutex
	// FailNextKinds 按调用次序返回的故障类别；KindNone 表示成功；
	// 脚本耗尽后一律成功。
	FailNextKinds []ErrorKind
	Latency       time.Duration
	InvokeCount   int
}

// Count 返回累计调用次数。
func (b *ScriptedBehavior) Count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.InvokeCount
}

// next 取下一次调用的故障类别。
func (b *ScriptedBehavior) next() ErrorKind {
	b.mu.Lock()
	defer b.mu.Unlock()
	idx := b.InvokeCount
	b.InvokeCount++
	if idx < len(b.FailNextKinds) {
		return b.FailNextKinds[idx]
	}
	return KindNone
}

// ScriptedAdapter 按脚本返回成功/各类故障的内存适配器。
type ScriptedAdapter struct {
	StaticDesc
	Behavior *ScriptedBehavior
}

// Desc 返回静态描述。
func (a *ScriptedAdapter) Desc() StaticDesc { return a.StaticDesc }

// Invoke 执行一次脚本化调用。
func (a *ScriptedAdapter) Invoke(ctx context.Context, req Request) (Response, error) {
	kind := KindNone
	if a.Behavior != nil {
		kind = a.Behavior.next()
	}

	// 用定时器模拟延迟，并尊重 ctx 取消（真实超时由网关侧 ctx 触发，
	// 这里模拟“处理时间”，便于演示）。
	if a.Behavior != nil && a.Behavior.Latency > 0 {
		t := time.NewTimer(a.Behavior.Latency)
		select {
		case <-ctx.Done():
			return Response{}, &UpstreamError{Kind: KindTimeout, Message: "client deadline exceeded"}
		case <-t.C:
		}
	}

	switch kind {
	case KindNone:
		return Response{
			UpstreamID: a.StaticDesc.ID,
			Version:    a.StaticDesc.Version,
			Result:     "processed:" + req.Payload,
		}, nil
	default:
		return Response{}, &UpstreamError{Kind: kind, Message: "scripted failure at " + a.StaticDesc.ID}
	}
}
