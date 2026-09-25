// Package breaker 实现每上游熔断器（关闭/打开/半开三态）。
//
// 状态语义：
//   - closed：正常放行；连续失败达到阈值后迁移到 open；
//   - open：一律快速失败（degrade_circuit_open），不打上游；
//     经过 OpenCoolDown 后进入 half-open；
//   - half-open：只允许“一个”探测请求通过（探测令牌在锁内抢占，
//     其余并发请求仍按 open/half-open 拒绝，杜绝恢复瞬间惊群）；
//     探测成功 -> closed（清空失败计数）；探测失败 -> 重新 open 并重置冷却。
//
// 所有状态迁移都通过 onTransition 回调同步对外可观测。
package breaker

import (
	"context"
	"sync"
	"time"
)

// State 熔断状态。
type State int32

// 熔断三态。
const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

// String 返回状态名。
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Config 熔断器配置。
type Config struct {
	FailureThreshold int           // 连续失败多少次后打开
	OpenCoolDown     time.Duration // 打开后多久允许探测
}

// Decision 熔断放行结论。
type Decision struct {
	Allowed bool
	State   State
	Reason  string
}

// Breaker 单上游熔断器。
type Breaker struct {
	cfg Config

	mu            sync.Mutex
	state         State
	failures      int
	openedAt      time.Time
	probeInFlight bool

	onTransition func(from, to State, reason string)
}

// New 构造熔断器。
func New(cfg Config) *Breaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.OpenCoolDown <= 0 {
		cfg.OpenCoolDown = 30 * time.Second
	}
	return &Breaker{cfg: cfg, state: StateClosed}
}

// SetTransitionListener 注册状态迁移回调（由 Registry 在创建时注入）。
func (b *Breaker) SetTransitionListener(f func(from, to State, reason string)) {
	b.mu.Lock()
	b.onTransition = f
	b.mu.Unlock()
}

func (b *Breaker) transitionLocked(to State, reason string) {
	if b.state == to {
		return
	}
	from := b.state
	b.state = to
	if b.onTransition != nil {
		// 在锁内同步回调，保证迁移顺序与日志顺序一致；
		// 回调实现不得反向调用 Breaker，避免重入。
		b.onTransition(from, to, reason)
	}
}

// CurrentState 返回当前状态。
func (b *Breaker) CurrentState() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Allow 判断当前是否允许请求通过。now 注入便于确定性测试。
func (b *Breaker) Allow(ctx context.Context, now time.Time) Decision {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		return Decision{Allowed: true, State: StateClosed, Reason: "circuit closed"}
	case StateHalfOpen:
		if b.probeInFlight {
			return Decision{Allowed: false, State: StateHalfOpen,
				Reason: "half-open probe already in flight (thundering herd prevented)"}
		}
		b.probeInFlight = true
		return Decision{Allowed: true, State: StateHalfOpen,
			Reason: "half-open single probe admitted"}
	case StateOpen:
		if now.Sub(b.openedAt) >= b.cfg.OpenCoolDown {
			b.transitionLocked(StateHalfOpen, "cool-down elapsed, entering half-open")
			b.probeInFlight = true
			return Decision{Allowed: true, State: StateHalfOpen,
				Reason: "cool-down elapsed, admitting one probe"}
		}
		return Decision{Allowed: false, State: StateOpen,
			Reason: "circuit open, fast fail"}
	default:
		return Decision{Allowed: false, State: b.state, Reason: "unknown state"}
	}
}

// OnResult 根据一次调用结果更新熔断状态。err==nil 视为成功。
func (b *Breaker) OnResult(ctx context.Context, now time.Time, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateHalfOpen:
		b.probeInFlight = false
		if err == nil {
			b.failures = 0
			b.transitionLocked(StateClosed, "half-open probe succeeded, circuit closed")
		} else {
			b.openedAt = now
			b.transitionLocked(StateOpen, "half-open probe failed, re-open and reset cool-down")
		}
	case StateClosed:
		if err != nil {
			b.failures++
			if b.failures >= b.cfg.FailureThreshold {
				b.openedAt = now
				b.transitionLocked(StateOpen, "failure threshold reached")
			}
		} else {
			b.failures = 0
		}
	}
}

// StateListener 观测熔断状态迁移（带上游 ID）。
type StateListener func(upstreamID string, from, to State, reason string)

// Registry 按上游 ID 管理一组熔断器。
type Registry struct {
	cfg      Config
	listener StateListener
	mu       sync.Mutex
	breakers map[string]*Breaker
}

// NewRegistry 构造熔断器注册表。
func NewRegistry(cfg Config, listener StateListener) *Registry {
	return &Registry{cfg: cfg, listener: listener, breakers: make(map[string]*Breaker)}
}

// Get 取上游熔断器，不存在则创建并接入状态监听器。
func (r *Registry) Get(upstreamID string) *Breaker {
	r.mu.Lock()
	defer r.mu.Unlock()
	if b, ok := r.breakers[upstreamID]; ok {
		return b
	}
	b := New(r.cfg)
	if r.listener != nil {
		l := r.listener
		b.SetTransitionListener(func(from, to State, reason string) {
			l(upstreamID, from, to, reason)
		})
	}
	r.breakers[upstreamID] = b
	return b
}
