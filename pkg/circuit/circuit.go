// Package circuit 实现每上游熔断器：关闭/打开/半开三态，
// 半开期只放行单个探测请求，避免恢复瞬间惊群。
package circuit

import (
	"sync"
	"time"
)

// State 为熔断状态。
type State string

const (
	Closed   State = "closed"
	Open     State = "open"
	HalfOpen State = "half_open"
)

// Decision 是一次熔断判定结果。
type Decision struct {
	State   State
	Allowed bool
	Probe   bool // 本次是否为半开探测
	Reason  string
}

// Event 为可观测的状态迁移事件。
type Event struct {
	UpstreamID string
	From       State
	To         State
	At         time.Time
	Reason     string
}

// Config 为熔断参数。
type Config struct {
	FailureThreshold int           // 连续失败多少次后打开
	OpenCoolDown     time.Duration // 打开后多久进入半开
	SuccessThreshold int           // 半开期连续成功多少次后关闭
}

// Breaker 是单上游熔断器。
type Breaker struct {
	id  string
	cfg Config
	mu  sync.Mutex

	state         State
	failures      int
	halfSuccesses int
	openedAt      time.Time
	probeInFlight bool
	listeners     []func(Event)
	now           func() time.Time
}

// New 创建熔断器（初始 Closed）。
func New(id string, cfg Config) *Breaker {
	return &Breaker{
		id:    id,
		cfg:   cfg,
		state: Closed,
		now:   time.Now,
	}
}

// Allow 判定请求是否可通过；半开时至多一个探测请求放行。
func (b *Breaker) Allow() Decision {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case Closed:
		return Decision{State: Closed, Allowed: true, Reason: "closed"}
	case Open:
		if b.now().Sub(b.openedAt) >= b.cfg.OpenCoolDown {
			// 冷却到期：进入半开，并由本次请求独占探测权，
			// 其余并发请求继续被拒绝，避免恢复瞬间惊群。
			b.transitionLocked(HalfOpen, "cooldown_elapsed")
			b.probeInFlight = true
			return Decision{State: HalfOpen, Allowed: true, Probe: true, Reason: "half_open_probe"}
		}
		return Decision{State: Open, Allowed: false, Reason: "open_cooldown"}
	case HalfOpen:
		if b.probeInFlight {
			return Decision{State: HalfOpen, Allowed: false, Reason: "half_open_probe_in_flight"}
		}
		b.probeInFlight = true
		return Decision{State: HalfOpen, Allowed: true, Probe: true, Reason: "half_open_probe"}
	default:
		return Decision{State: b.state, Allowed: false, Reason: "unknown_state"}
	}
}

// OnSuccess 上报成功。
func (b *Breaker) OnSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		b.failures = 0
	case HalfOpen:
		b.probeInFlight = false
		b.halfSuccesses++
		if b.halfSuccesses >= b.cfg.SuccessThreshold {
			b.transitionLocked(Closed, "half_open_success_threshold")
			b.failures = 0
			b.halfSuccesses = 0
		}
	}
}

// OnFailure 上报失败。
func (b *Breaker) OnFailure(reason string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		b.probeInFlight = false
		b.failures++
		if b.failures >= b.cfg.FailureThreshold {
			b.openedAt = b.now()
			b.transitionLocked(Open, reason)
		}
	case HalfOpen:
		// 探测失败：立即重新打开，冷却时间从现在重新计起。
		b.probeInFlight = false
		b.halfSuccesses = 0
		b.openedAt = b.now()
		b.transitionLocked(Open, "half_open_probe_failed:"+reason)
	case Open:
		b.probeInFlight = false
	}
}

// State 返回当前状态。
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Listen 注册状态迁移监听器。
func (b *Breaker) Listen(fn func(Event)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.listeners = append(b.listeners, fn)
}

// transitionLocked 在持锁前提下迁移状态并通知监听器。
func (b *Breaker) transitionLocked(to State, reason string) {
	if b.state == to {
		return
	}
	ev := Event{UpstreamID: b.id, From: b.state, To: to, At: b.now(), Reason: reason}
	b.state = to
	for _, fn := range b.listeners {
		fn(ev)
	}
}

// Manager 按上游 ID 管理一组熔断器。
type Manager struct {
	cfg      Config
	mu       sync.Mutex
	breakers map[string]*Breaker
	now      func() time.Time
}

// NewManager 创建熔断器管理器。
func NewManager(cfg Config) *Manager {
	return &Manager{
		cfg:      cfg,
		breakers: make(map[string]*Breaker),
		now:      time.Now,
	}
}

// Get 取（或创建）指定上游的熔断器。
func (m *Manager) Get(upstreamID string) *Breaker {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.breakers[upstreamID]
	if b == nil {
		b = New(upstreamID, m.cfg)
		m.breakers[upstreamID] = b
	}
	return b
}

// Snapshot 返回各熔断器当前状态快照。
func (m *Manager) Snapshot() map[string]State {
	m.mu.Lock()
	ids := make([]string, 0, len(m.breakers))
	for id := range m.breakers {
		ids = append(ids, id)
	}
	m.mu.Unlock()

	out := make(map[string]State, len(ids))
	for _, id := range ids {
		out[id] = m.Get(id).State()
	}
	return out
}
