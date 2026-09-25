// Package retry 提供有界退避重试策略。
// 重试仅允许：操作幂等、错误可安全重试，且由上层保证只在同一
// 协商版本的兼容候选集合内重试（不跨不兼容版本）。
package retry

import (
	"context"
	"math/rand"
	"time"
)

// Policy 为有界退避重试策略。
type Policy struct {
	MaxAttempts    int // 总尝试次数上限（含首次）
	BaseDelay      time.Duration
	MaxDelay       time.Duration
	JitterFraction float64
}

// DefaultPolicy 返回默认策略：最多 3 次、指数退避带抖动。
func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts:    3,
		BaseDelay:      10 * time.Millisecond,
		MaxDelay:       time.Second,
		JitterFraction: 0.2,
	}
}

// Backoff 计算第 attempt 次失败后的等待时长（attempt 从 1 起）。
func (p Policy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := p.BaseDelay << (attempt - 1) // 指数增长
	if d > p.MaxDelay || d <= 0 {
		d = p.MaxDelay
	}
	if p.JitterFraction > 0 {
		jitter := (rand.Float64()*2 - 1) * p.JitterFraction // [-j, +j)
		d += time.Duration(float64(d) * jitter)
		if d < 0 {
			d = 0
		}
	}
	return d
}

// Sleep 在 ctx 上可取消地等待。
func (p Policy) Sleep(ctx context.Context, attempt int) error {
	d := p.Backoff(attempt)
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
