package breaker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func testCfg() Config {
	return Config{FailureThreshold: 3, OpenCoolDown: 100 * time.Millisecond}
}

func TestOpensAfterThreshold(t *testing.T) {
	b := New(testCfg())
	now := time.Unix(1000, 0)
	ctx := context.Background()
	up := errors.New("boom")

	for i := 0; i < 3; i++ {
		if d := b.Allow(ctx, now); !d.Allowed {
			t.Fatal("should be allowed while closed")
		}
		b.OnResult(ctx, now, up)
	}
	if b.CurrentState() != StateOpen {
		t.Fatalf("want open, got %s", b.CurrentState())
	}
	// open 期快速失败
	d := b.Allow(ctx, now.Add(50*time.Millisecond))
	if d.Allowed || d.State != StateOpen {
		t.Fatalf("want fast-fail, got %+v", d)
	}
	t.Logf("request_id=test-open breaker decision: state=%s reason=%q", d.State, d.Reason)
}

func TestHalfOpenSingleProbeThunderingHerd(t *testing.T) {
	var events []string
	var mu sync.Mutex
	b := New(testCfg())
	b.SetTransitionListener(func(from, to State, reason string) {
		mu.Lock()
		events = append(events, fmt.Sprintf("%s->%s(%s)", from, to, reason))
		mu.Unlock()
	})
	ctx := context.Background()
	t0 := time.Unix(2000, 0)
	up := errors.New("down")

	for i := 0; i < 3; i++ {
		b.Allow(ctx, t0)
		b.OnResult(ctx, t0, up)
	}
	if b.CurrentState() != StateOpen {
		t.Fatal("want open")
	}

	// 冷却到期：100 个并发请求，只有 1 个能成为探测。
	probeTime := t0.Add(101 * time.Millisecond)
	start := make(chan struct{})
	var wg sync.WaitGroup
	admitted, rejected := 0, 0
	var amu sync.Mutex
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			d := b.Allow(ctx, probeTime)
			amu.Lock()
			if d.Allowed {
				admitted++
			} else {
				rejected++
			}
			amu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	if admitted != 1 || rejected != 99 {
		t.Fatalf("thundering herd: admitted=%d rejected=%d want 1/99", admitted, rejected)
	}
	if b.CurrentState() != StateHalfOpen {
		t.Fatalf("want half-open, got %s", b.CurrentState())
	}

	// 探测成功 -> closed
	b.OnResult(ctx, probeTime, nil)
	if b.CurrentState() != StateClosed {
		t.Fatalf("want closed after probe success, got %s", b.CurrentState())
	}
	// 关闸后失败计数应清零：再来 threshold-1 次失败不应打开。
	for i := 0; i < 2; i++ {
		b.Allow(ctx, probeTime)
		b.OnResult(ctx, probeTime, up)
	}
	if b.CurrentState() != StateClosed {
		t.Fatal("failures must reset after recovery")
	}
	mu.Lock()
	t.Logf("request_id=test-probe breaker transitions: %v; admitted=%d rejected=%d",
		events, admitted, rejected)
	mu.Unlock()
}

func TestFailedProbeReopensAndResetsCooldown(t *testing.T) {
	b := New(testCfg())
	ctx := context.Background()
	t0 := time.Unix(3000, 0)
	up := errors.New("down")
	for i := 0; i < 3; i++ {
		b.Allow(ctx, t0)
		b.OnResult(ctx, t0, up)
	}
	t1 := t0.Add(101 * time.Millisecond)
	d := b.Allow(ctx, t1) // 唯一探测
	if !d.Allowed {
		t.Fatal("probe should be admitted")
	}
	b.OnResult(ctx, t1, up) // 探测失败
	if b.CurrentState() != StateOpen {
		t.Fatal("want reopen")
	}
	// 冷却被重置：t1+50ms 仍应快速失败。
	if d := b.Allow(ctx, t1.Add(50*time.Millisecond)); d.Allowed {
		t.Fatal("cooldown must reset after failed probe")
	}
	// t1+101ms 才允许下一次探测。
	d = b.Allow(ctx, t1.Add(101*time.Millisecond))
	if !d.Allowed || d.State != StateHalfOpen {
		t.Fatalf("want next probe, got %+v", d)
	}
	t.Logf("request_id=test-reopen breaker reason=%q", d.Reason)
}

func TestRegistryPerUpstream(t *testing.T) {
	var got []string
	var mu sync.Mutex
	r := NewRegistry(testCfg(), func(id string, from, to State, reason string) {
		mu.Lock()
		got = append(got, id+":"+from.String()+"->"+to.String())
		mu.Unlock()
	})
	a1 := r.Get("a")
	a2 := r.Get("a")
	if a1 != a2 {
		t.Fatal("same upstream must share one breaker")
	}
	b := r.Get("b")
	if b == a1 {
		t.Fatal("different upstreams need different breakers")
	}
	ctx := context.Background()
	now := time.Unix(0, 0)
	up := errors.New("x")
	for i := 0; i < 3; i++ {
		a1.Allow(ctx, now)
		a1.OnResult(ctx, now, up)
	}
	mu.Lock()
	if len(got) == 0 {
		t.Fatal("transition listener not invoked")
	}
	t.Logf("transitions: %v", got)
	mu.Unlock()
}
