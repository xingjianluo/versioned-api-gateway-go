// Package ratelimit 实现调用方层与路由层共享的分层令牌桶限流。
package ratelimit

import (
	"context"
	"sync"
	"time"
)

// BucketConfig 是单个令牌桶配置。
type BucketConfig struct {
	Rate  float64 // 每秒补充令牌数
	Burst float64 // 桶容量（突发配额）
}

// Decision 说明一次配额判定的依据，供日志解释。
type Decision struct {
	Allowed         bool
	CallerID        string
	Route           string
	CallerAllowed   bool
	RouteAllowed    bool
	CallerRemaining float64
	RouteRemaining  float64
	Reason          string
}

// bucket 是惰性补充的令牌桶。
type bucket struct {
	tokens float64
	last   time.Time
}

// Limiter 是调用方桶 + 路由桶的两层限流器。
//
// 并发正确性：所有判定在同一把锁内原子完成（先按调用方桶补充/预扣，
// 再按路由桶补充/预扣）。两层都放行才消耗；若调用方有令牌而路由桶
// 耗尽，则回补调用方桶预扣的 1 个令牌——因此并发下既不会超额放行，
// 也不会因回滚路径泄漏配额。
type Limiter struct {
	mu        sync.Mutex
	callerCfg BucketConfig
	routeCfg  BucketConfig
	callers   map[string]*bucket
	routes    map[string]*bucket
	now       func() time.Time
}

// New 创建分层限流器。
func New(callerCfg, routeCfg BucketConfig) *Limiter {
	return &Limiter{
		callerCfg: callerCfg,
		routeCfg:  routeCfg,
		callers:   make(map[string]*bucket),
		routes:    make(map[string]*bucket),
		now:       time.Now,
	}
}

func (l *Limiter) callerBucket(id string) *bucket {
	b := l.callers[id]
	if b == nil {
		b = &bucket{tokens: l.callerCfg.Burst, last: l.now()}
		l.callers[id] = b
	}
	return b
}

func (l *Limiter) routeBucket(id string) *bucket {
	b := l.routes[id]
	if b == nil {
		b = &bucket{tokens: l.routeCfg.Burst, last: l.now()}
		l.routes[id] = b
	}
	return b
}

// refill 按经过时间补充令牌（上限为桶容量）。
func (l *Limiter) refill(b *bucket, cfg BucketConfig, now time.Time) {
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * cfg.Rate
		if b.tokens > cfg.Burst {
			b.tokens = cfg.Burst
		}
		b.last = now
	}
}

// Allow 判定一次请求是否可放行（消耗 1 个令牌）。
func (l *Limiter) Allow(callerID, route string) Decision {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()

	cb := l.callerBucket(callerID)
	rb := l.routeBucket(route)
	l.refill(cb, l.callerCfg, now)
	l.refill(rb, l.routeCfg, now)

	d := Decision{
		CallerID:        callerID,
		Route:           route,
		CallerRemaining: cb.tokens,
		RouteRemaining:  rb.tokens,
	}

	if cb.tokens < 1 {
		d.Reason = "caller_quota_exhausted"
		return d
	}
	// 先在调用方桶预扣。
	cb.tokens--

	if rb.tokens < 1 {
		// 路由层拒绝：回补调用方桶，避免配额泄漏。
		cb.tokens++
		d.CallerAllowed = true
		d.CallerRemaining = cb.tokens
		d.Reason = "route_quota_exhausted"
		return d
	}
	rb.tokens--

	d.Allowed = true
	d.CallerAllowed = true
	d.RouteAllowed = true
	d.CallerRemaining = cb.tokens
	d.RouteRemaining = rb.tokens
	d.Reason = "allowed"
	return d
}

// waitLocked 计算两个桶中至少有一个令牌所需的最短等待。
func waitLocked(cb, rb *bucket, cc, rc BucketConfig) time.Duration {
	need := func(tokens float64, rate float64) time.Duration {
		if tokens >= 1 || rate <= 0 {
			return 0
		}
		return time.Duration((1 - tokens) / rate * float64(time.Second))
	}
	d1 := need(cb.tokens, cc.Rate)
	d2 := need(rb.tokens, rc.Rate)
	if d1 > d2 {
		return d1
	}
	return d2
}

// Wait 阻塞直到两层都获得令牌或 ctx 取消。
// 多个等待者同时醒来时竞争令牌，失败者继续等待下一轮，不会超额放行。
func (l *Limiter) Wait(ctx context.Context, callerID, route string) error {
	for {
		l.mu.Lock()
		now := l.now()
		cb := l.callerBucket(callerID)
		rb := l.routeBucket(route)
		l.refill(cb, l.callerCfg, now)
		l.refill(rb, l.routeCfg, now)

		if cb.tokens >= 1 && rb.tokens >= 1 {
			// 持锁直接完成与 Allow 等价的原子扣减。
			cb.tokens--
			rb.tokens--
			l.mu.Unlock()
			return nil
		}
		d := waitLocked(cb, rb, l.callerCfg, l.routeCfg)
		l.mu.Unlock()

		// 补充速率为 0 且桶已空：永远无法满足，避免无意义忙等。
		if d <= 0 {
			if cb.tokens < 1 && l.callerCfg.Rate <= 0 {
				return &QuotaExhaustedError{Layer: "caller"}
			}
			if rb.tokens < 1 && l.routeCfg.Rate <= 0 {
				return &QuotaExhaustedError{Layer: "route"}
			}
			d = time.Millisecond
		}
		timer := time.NewTimer(d)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// QuotaExhaustedError 表示补充速率为 0 且令牌已永久耗尽，等待无意义。
type QuotaExhaustedError struct{ Layer string }

func (e *QuotaExhaustedError) Error() string {
	return "ratelimit: " + e.Layer + " quota permanently exhausted (rate=0)"
}

// Snapshot 返回观测到的剩余令牌（可观测性）。
func (l *Limiter) Snapshot(callerID, route string) (caller, routeTok float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	cb := l.callerBucket(callerID)
	rb := l.routeBucket(route)
	l.refill(cb, l.callerCfg, now)
	l.refill(rb, l.routeCfg, now)
	return cb.tokens, rb.tokens
}
