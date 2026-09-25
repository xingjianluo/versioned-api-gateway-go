// Package upstream 定义上游适配器契约与注册表。
package upstream

import (
	"context"
	"fmt"

	"github.com/xingjianluo/versioned-api-gateway-go/pkg/version"
)

// ErrorKind 区分上游故障类别，驱动重试与降级。
type ErrorKind string

const (
	ErrTimeout  ErrorKind = "timeout"
	ErrConn     ErrorKind = "connection_error"
	ErrOverload ErrorKind = "overloaded"
	ErrRejected ErrorKind = "rejected"
)

// Error 是适配器返回的可分类错误。
type Error struct {
	Kind   ErrorKind
	Reason string
}

func (e *Error) Error() string {
	return fmt.Sprintf("upstream error %s: %s", e.Kind, e.Reason)
}

// Retryable 判断该类上游故障是否可安全重试（语义上可重放）。
func (k ErrorKind) Retryable() bool {
	return k == ErrTimeout || k == ErrConn || k == ErrOverload
}

// Request 是发往上游的业务请求。
type Request struct {
	ID         string
	Route      string
	APIVersion version.Version
	Idempotent bool
	Payload    []byte
}

// Response 是上游响应。
type Response struct {
	UpstreamID string
	Version    version.Version
	Body       []byte
	Degraded   bool
}

// Adapter 是一个上游业务能力适配器。
type Adapter interface {
	// ID 返回上游实例标识。
	ID() string
	// APIVersion 返回该上游实现的 API 版本。
	APIVersion() version.Version
	// Capabilities 返回该上游提供的能力及语义代际。
	Capabilities() []version.Capability
	// Invoke 执行一次业务调用。
	Invoke(ctx context.Context, req Request) (*Response, error)
}

// Descriptor 是注册时的静态描述信息。
type Descriptor struct {
	ID           string
	APIVersion   version.Version
	Capabilities []version.Capability
	Weight       int
}

// Registry 保存全部上游适配器并支持按版本/能力选择。
type Registry struct {
	adapters []Adapter
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{adapters: make([]Adapter, 0)}
}

// Register 注册一个适配器。
func (r *Registry) Register(a Adapter) {
	r.adapters = append(r.adapters, a)
}

// Candidate 描述一次匹配到的候选上游。
type Candidate struct {
	Adapter Adapter
	Version version.Version
}

// Select 按调用方版本与所需能力选择兼容候选。
//
// 兼容规则（绝不静默转发到不兼容上游）：
//  1. major 必须相等；
//  2. 上游 minor 不得高于调用方 minor（新 minor 可能引入调用方未知的
//     语义变化），即只在同代内向旧兼容；
//  3. 调用方要求的每项能力，上游都必须提供且语义代际兼容；
//  4. 若存在能力但所有提供者语义不兼容 -> semantic_conflict；
//     若没有任何上游具备该能力 -> capability_missing。
//     这两类原因通过 error 的 RejectReason 区分。
func (r *Registry) Select(caller version.Version, required []version.RequiredCapability) ([]Candidate, error) {
	var cands []Candidate
	capabilityNameExists := false // 任意上游是否提供过所需能力名
	semanticsExists := false      // 任意上游（不论版本）是否提供语义兼容的所需能力

	for _, a := range r.adapters {
		av := a.APIVersion()
		offered := capabilityMap(a.Capabilities())

		namesPresent := true
		semanticsOK := true
		for _, req := range required {
			off, present := offered[req.Name]
			if !present {
				namesPresent = false
				semanticsOK = false
				break
			}
			if !version.SemanticCompatible(req, off) {
				semanticsOK = false
			}
		}
		if namesPresent {
			capabilityNameExists = true
		}
		if semanticsOK {
			semanticsExists = true
		}

		// 版本门控：major 相等、上游 minor 不得高于调用方 minor，
		// 否则绝不静默转发到调用方不认识的新版本。
		if av.Major != caller.Major || av.Minor > caller.Minor {
			continue
		}
		if semanticsOK {
			cands = append(cands, Candidate{Adapter: a, Version: av})
		}
	}

	if len(cands) > 0 {
		return cands, nil
	}

	// 无可选候选时给出可区分原因：
	//  - 存在语义兼容的提供者，但都被版本门控排除（如只部署了 2.0 上游）；
	//  - 或能力存在但语义代际不兼容 -> semantic_conflict；
	//  - 没有任何上游提供该能力 -> capability_missing。
	if semanticsExists {
		return nil, &version.NegotiateError{
			Reason:  version.RejectSemanticConflict,
			Message: fmt.Sprintf("compatible capability exists only on API-incompatible upstreams for caller %s", caller),
		}
	}
	if capabilityNameExists {
		return nil, &version.NegotiateError{
			Reason:  version.RejectSemanticConflict,
			Message: fmt.Sprintf("capability semantics incompatible for caller %s", caller),
		}
	}
	for _, req := range required {
		return nil, &version.NegotiateError{
			Reason:  version.RejectCapabilityMissing,
			Message: fmt.Sprintf("no upstream provides capability %q for caller %s", req.Name, caller),
		}
	}
	return nil, &version.NegotiateError{Reason: version.RejectCapabilityMissing, Message: "no capability required"}
}

func capabilityMap(cs []version.Capability) map[string]version.Capability {
	m := make(map[string]version.Capability, len(cs))
	for _, c := range cs {
		m[c.Name] = c
	}
	return m
}

// All 返回全部已注册适配器。
func (r *Registry) All() []Adapter {
	out := make([]Adapter, len(r.adapters))
	copy(out, r.adapters)
	return out
}
