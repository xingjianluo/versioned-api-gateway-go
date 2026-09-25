// Package ratelimit 实现调用方层与路由层共享的分层令牌桶限流。
//
// 语义：
//   - 每个调用方一个桶（限额该调用方在所有路由上的总 QPS）；
//   - 每条路由一个桶（限额该路由被所有调用方共享的总 QPS）；
//   - 一次请求必须同时拿到两层各 1 个令牌才放行；
//   - “补充令牌→检查两层→扣减”在同一把互斥锁内原子完成，
//     并发下不会超额放行；任一层不足时两层都不扣减，不泄漏配额。
package ratelimit

import (
	"context"
	"sync"
	"time"
)

// Decision 是一次配额申请的结论。
type Decision struct {
	Allowed         bool
	Layer           string // "caller" | "route"
	Reason          string
	CallerRemaining float64
	RouteRemaining  float64
}

// LayerConfig 单层限流配置：每秒补充 rate 个令牌，桶容量 burst。
type LayerConfig struct {
	Rate  float64 // 令牌/秒
	Burst int     // 桶容量（突发配额）
}

type bucket struct {
	cfg    LayerConfig
	tokens float64
	last   time.Time
}

func newBucket(cfg LayerConfig, now time.Time) *bucket {
	return &bucket{cfg: cfg, tokens: float64(cfg.Burst), last: now}
}

// refill 按经过时间补充令牌，上限为 burst。
func (b *bucket) refill(now time.Time) {
	if b.last.IsZero() {
		b.last = now
		return
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.cfg.Rate
		if b.tokens > float64(b.cfg.Burst) {
			b.tokens = float64(b.cfg.Burst)
		}
		b.last = now
	}
}

// Limiter 分层限流器。
type Limiter struct {
	mu        sync.Mutex
	callerCfg map[string]LayerConfig
	routeCfg  map[string]LayerConfig
	callers   map[string]*bucket
	routes    map[string]*bucket
}

// New 构造分层限流器。未在配置中声明的调用方/路由使用零配额（拒绝），
// 这迫使本地验证显式声明配额，避免“漏配即无限放行”。
func New(callerCfg, routeCfg map[string]LayerConfig) *Limiter {
	return &Limiter{
		callerCfg: cloneCfg(callerCfg),
		routeCfg:  cloneCfg(routeCfg),
		callers:   make(map[string]*bucket),
		routes:    make(map[string]*bucket),
	}
}

func cloneCfg(in map[string]LayerConfig) map[string]LayerConfig {
	out := make(map[string]LayerConfig, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// Allow 申请 1 个令牌（原子地跨两层）。
func (l *Limiter) Allow(ctx context.Context, caller, route string) Decision {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()

	cb, ok1 := l.bucketFor(l.callers, l.callerCfg, caller, now)
	rb, ok2 := l.bucketFor(l.routes, l.routeCfg, route, now)
	if !ok1 {
		return Decision{Allowed: false, Layer: "caller",
			Reason: "caller has no rate-limit config (zero quota)"}
	}
	if !ok2 {
		return Decision{Allowed: false, Layer: "route",
			Reason: "route has no rate-limit config (zero quota)"}
	}
	cb.refill(now)
	rb.refill(now)

	if cb.tokens < 1 {
		return Decision{Allowed: false, Layer: "caller",
			Reason:          "caller quota exhausted",
			CallerRemaining: cb.tokens,
			RouteRemaining:  rb.tokens}
	}
	if rb.tokens < 1 {
		return Decision{Allowed: false, Layer: "route",
			Reason:          "route shared quota exhausted",
			CallerRemaining: cb.tokens,
			RouteRemaining:  rb.tokens}
	}
	// 两层都充足：同时扣减。
	cb.tokens--
	rb.tokens--
	return Decision{Allowed: true,
		CallerRemaining: cb.tokens, RouteRemaining: rb.tokens}
}

// bucketFor 返回（并惰性创建）桶；未配置则返回 ok=false。
func (l *Limiter) bucketFor(m map[string]*bucket, cfg map[string]LayerConfig,
	key string, now time.Time) (*bucket, bool) {
	if b, ok := m[key]; ok {
		return b, true
	}
	c, ok := cfg[key]
	if !ok {
		return nil, false
	}
	b := newBucket(c, now)
	m[key] = b
	return b, true
}

// Snapshot 返回某调用方/路由当前剩余令牌（会先按当前时间补充）。
func (l *Limiter) Snapshot(caller, route string) (callerTokens float64, routeTokens float64) {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if cb, ok := l.callers[caller]; ok {
		cb.refill(now)
		callerTokens = cb.tokens
	}
	if rb, ok := l.routes[route]; ok {
		rb.refill(now)
		routeTokens = rb.tokens
	}
	return
}
