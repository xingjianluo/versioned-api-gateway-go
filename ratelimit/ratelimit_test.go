package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"
)

func cfg(burst int) map[string]LayerConfig {
	// rate=0：测试期间不补充令牌，便于精确断言“突发配额”
	return map[string]LayerConfig{
		"c1": {Rate: 0, Burst: burst},
		"r1": {Rate: 0, Burst: burst},
		"r2": {Rate: 0, Burst: burst},
	}
}

func TestConcurrentQuotaExactBurst(t *testing.T) {
	const burst = 50
	const clients = 500
	l := New(cfg(burst), cfg(burst))

	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	denied := 0
	start := make(chan struct{})
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			d := l.Allow(context.Background(), "c1", "r1")
			mu.Lock()
			if d.Allowed {
				allowed++
			} else {
				denied++
			}
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	if allowed != burst {
		t.Fatalf("exact burst violated: allowed=%d want %d (denied=%d)", allowed, burst, denied)
	}
	if denied != clients-burst {
		t.Fatalf("denied=%d want %d", denied, clients-burst)
	}
	ct, rt := l.Snapshot("c1", "r1")
	if ct != 0 || rt != 0 {
		t.Fatalf("tokens must be exhausted, got caller=%v route=%v", ct, rt)
	}
	t.Logf("request_id=test-concurrent quota decision: allowed=%d denied=%d caller_remaining=%.1f route_remaining=%.1f",
		allowed, denied, ct, rt)
}

func TestRouteDenyDoesNotLeakCallerTokens(t *testing.T) {
	// 调用方 burst=10，路由 r-full burst=2：路由耗尽后，
	// 调用方令牌不得被白白扣掉（否则配额泄漏）。
	caller := map[string]LayerConfig{"c1": {Rate: 0, Burst: 10}}
	route := map[string]LayerConfig{
		"r-full": {Rate: 0, Burst: 2},
		"r-ok":   {Rate: 0, Burst: 10},
	}
	l := New(caller, route)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if d := l.Allow(ctx, "c1", "r-full"); !d.Allowed {
			t.Fatal("first two should pass")
		}
	}
	d := l.Allow(ctx, "c1", "r-full")
	if d.Allowed || d.Layer != "route" {
		t.Fatalf("want route-layer denial, got %+v", d)
	}
	// 调用方桶应恰好消耗 2（未因路由拒绝而泄漏）。
	ct, _ := l.Snapshot("c1", "r-full")
	if ct != 8 {
		t.Fatalf("caller tokens leaked: got %v want 8", ct)
	}
	// 换一条仍有配额的路由：还能再放 8 个。
	pass := 0
	for i := 0; i < 10; i++ {
		if l.Allow(ctx, "c1", "r-ok").Allowed {
			pass++
		}
	}
	if pass != 8 {
		t.Fatalf("caller shared quota wrong: pass=%d want 8", pass)
	}
	t.Logf("request_id=test-leak layer=route reason=%q caller_remaining=%.1f extra_pass=%d",
		d.Reason, ct, pass)
}

func TestUnknownCallerZeroQuota(t *testing.T) {
	l := New(map[string]LayerConfig{"c1": {Rate: 1, Burst: 1}},
		map[string]LayerConfig{"r1": {Rate: 1, Burst: 1}})
	d := l.Allow(context.Background(), "ghost", "r1")
	if d.Allowed || d.Layer != "caller" {
		t.Fatalf("unknown caller must be denied at caller layer, got %+v", d)
	}
}

func TestRefillOverTime(t *testing.T) {
	// 直接对桶做时间推进：rate=10/s，burst=5。
	t0 := time.Unix(1000, 0)
	b := newBucket(LayerConfig{Rate: 10, Burst: 5}, t0)
	b.tokens = 0
	b.refill(t0.Add(200 * time.Millisecond)) // +2 个令牌
	if b.tokens < 1.99 || b.tokens > 2.01 {
		t.Fatalf("refill wrong: %v", b.tokens)
	}
	// 长时间后补充不得超过 burst
	b.refill(t0.Add(time.Hour))
	if b.tokens != 5 {
		t.Fatalf("cap wrong: %v", b.tokens)
	}
	// 无时间流逝不补充
	b.tokens = 3
	b.refill(t0.Add(time.Hour))
	if b.tokens != 3 {
		t.Fatalf("zero elapsed must not refill: %v", b.tokens)
	}
}
