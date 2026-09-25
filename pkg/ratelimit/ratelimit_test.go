package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestLayeredAllowAndRefund(t *testing.T) {
	// 调用方桶 100/100，路由桶只有 2：路由耗尽后调用方令牌必须被回补。
	l := New(BucketConfig{Rate: 0, Burst: 100}, BucketConfig{Rate: 0, Burst: 2})

	allowed := 0
	for i := 0; i < 5; i++ {
		d := l.Allow("c1", "r1")
		if d.Allowed {
			allowed++
		} else {
			if d.Reason != "route_quota_exhausted" {
				t.Fatalf("want route_quota_exhausted got %s", d.Reason)
			}
		}
	}
	if allowed != 2 {
		t.Fatalf("route burst must allow exactly 2, got %d", allowed)
	}
	// 3 次被路由桶拒绝期间预扣的调用方令牌必须全部回补，
	// 因此调用方桶余额应稳定在 98（无配额泄漏）。
	callerLeft, _ := l.Snapshot("c1", "r1")
	if callerLeft != 98 {
		t.Fatalf("caller quota leaked: want 98 tokens left, got %.0f", callerLeft)
	}

	// 调用方自身配额耗尽：3 个请求里第 3 个必须被调用方桶拒绝。
	l2 := New(BucketConfig{Rate: 0, Burst: 2}, BucketConfig{Rate: 0, Burst: 100})
	callerDenied := 0
	for i := 0; i < 3; i++ {
		d := l2.Allow("c2", "r3")
		if !d.Allowed && d.Reason == "caller_quota_exhausted" {
			callerDenied++
		}
	}
	if callerDenied != 1 {
		t.Fatalf("want exactly 1 caller denial, got %d", callerDenied)
	}
}

func TestConcurrentQuotaNoOverAdmit(t *testing.T) {
	const burst = 50
	// 两层桶容量都为 50，补充速率为 0：成功总数必须恰好 50。
	l := New(BucketConfig{Rate: 0, Burst: burst}, BucketConfig{Rate: 0, Burst: burst})

	var wg sync.WaitGroup
	var mu sync.Mutex
	success, fail := 0, 0
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d := l.Allow("hot-caller", "hot-route")
			mu.Lock()
			defer mu.Unlock()
			if d.Allowed {
				success++
			} else {
				fail++
			}
		}()
	}
	wg.Wait()
	if success != burst {
		t.Fatalf("over-admitted: %d successes with burst %d", success, burst)
	}
	if success+fail != 500 {
		t.Fatalf("decisions lost: %d", success+fail)
	}
	callerLeft, routeLeft := l.Snapshot("hot-caller", "hot-route")
	if callerLeft != 0 || routeLeft != 0 {
		t.Fatalf("quota leak: caller_left=%v route_left=%v", callerLeft, routeLeft)
	}
	t.Logf("concurrent decision basis: success=%d fail=%d caller_left=%.1f route_left=%.1f",
		success, fail, callerLeft, routeLeft)
}

func TestRouteSharedAcrossCallers(t *testing.T) {
	// 路由桶被多个调用方共享：总计放行不得超过路由突发。
	l := New(BucketConfig{Rate: 0, Burst: 1000}, BucketConfig{Rate: 0, Burst: 10})
	var wg sync.WaitGroup
	var mu sync.Mutex
	success := 0
	for c := 0; c < 10; c++ {
		caller := c
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if l.Allow(string(rune('a'+caller)), "shared").Allowed {
					mu.Lock()
					success++
					mu.Unlock()
				}
			}()
		}
	}
	wg.Wait()
	if success != 10 {
		t.Fatalf("shared route must admit exactly 10, got %d", success)
	}
}

func TestWaitRefillAndPermanentExhaustion(t *testing.T) {
	// 速率 50/s、突发 1：耗尽后 Wait 应在约一个补充周期后成功。
	l := New(BucketConfig{Rate: 50, Burst: 1}, BucketConfig{Rate: 50, Burst: 1})
	if !l.Allow("cw", "rw").Allowed {
		t.Fatal("first must pass")
	}
	start := time.Now()
	if err := l.Wait(context.Background(), "cw", "rw"); err != nil {
		t.Fatalf("Wait should succeed after refill: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 15*time.Millisecond {
		t.Fatalf("Wait returned too fast: %s", elapsed)
	}

	// 速率 0 且耗尽：必须明确报错而非忙等/假成功。
	l2 := New(BucketConfig{Rate: 0, Burst: 1}, BucketConfig{Rate: 0, Burst: 1})
	l2.Allow("cz", "rz")
	err := l2.Wait(context.Background(), "cz", "rz")
	if _, ok := err.(*QuotaExhaustedError); !ok {
		t.Fatalf("want *QuotaExhaustedError, got %T %v", err, err)
	}
}
