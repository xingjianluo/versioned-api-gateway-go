package circuit

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testCfg() Config {
	return Config{FailureThreshold: 3, OpenCoolDown: 20 * time.Millisecond, SuccessThreshold: 2}
}

type clock struct{ t time.Time }

func newTestBreaker() (*Breaker, *clock) {
	c := &clock{t: time.Unix(0, 0)}
	b := New("u1", testCfg())
	b.now = func() time.Time { return c.t }
	return b, c
}

func TestClosedToOpen(t *testing.T) {
	b, _ := newTestBreaker()
	var events []Event
	b.Listen(func(e Event) { events = append(events, e) })

	for i := 0; i < 2; i++ {
		b.OnFailure("x")
		if b.State() != Closed {
			t.Fatal("must stay closed before threshold")
		}
	}
	b.OnFailure("overloaded")
	if b.State() != Open {
		t.Fatalf("want open, got %s", b.State())
	}
	if len(events) != 1 || events[0].To != Open || events[0].Reason != "overloaded" {
		t.Fatalf("unexpected event: %+v", events)
	}

	d := b.Allow()
	if d.Allowed || d.Reason != "open_cooldown" {
		t.Fatalf("open breaker must deny, got %+v", d)
	}
}

func TestHalfOpenSingleProbeAndRecovery(t *testing.T) {
	b, c := newTestBreaker()
	for i := 0; i < 3; i++ {
		b.OnFailure("x")
	}
	if b.State() != Open {
		t.Fatal("want open")
	}

	// 冷却到期：第一个请求独占探测权，其余并发请求全部被挡在半开门外。
	c.t = c.t.Add(21 * time.Millisecond)
	first := b.Allow()
	if !first.Allowed || !first.Probe || first.State != HalfOpen {
		t.Fatalf("first post-cooldown call must be the unique probe, got %+v", first)
	}
	for i := 0; i < 10; i++ {
		d := b.Allow()
		if d.Allowed || d.Reason != "half_open_probe_in_flight" {
			t.Fatalf("concurrent calls during probe must be rejected, got %+v", d)
		}
	}

	// 一次成功尚不足以关闭（阈值 2），第二个探测继续独占。
	b.OnSuccess()
	if b.State() != HalfOpen {
		t.Fatal("need 2 probe successes")
	}
	second := b.Allow()
	if !second.Allowed || !second.Probe {
		t.Fatalf("second probe must be admitted, got %+v", second)
	}
	b.OnSuccess()
	if b.State() != Closed {
		t.Fatalf("want closed after 2 probe successes, got %s", b.State())
	}
}

func TestHalfOpenProbeFailureReopens(t *testing.T) {
	b, c := newTestBreaker()
	for i := 0; i < 3; i++ {
		b.OnFailure("x")
	}
	c.t = c.t.Add(21 * time.Millisecond)
	if d := b.Allow(); !d.Allowed {
		t.Fatal("probe expected")
	}
	b.OnFailure("timeout")
	if b.State() != Open {
		t.Fatal("failed probe must reopen immediately")
	}
	// 冷却重新计时：立刻请求仍被拒。
	if d := b.Allow(); d.Allowed {
		t.Fatal("reopened breaker must deny within cooldown")
	}
}

func TestProbeThunderingHerdUnderConcurrency(t *testing.T) {
	b, c := newTestBreaker()
	for i := 0; i < 3; i++ {
		b.OnFailure("x")
	}
	c.t = c.t.Add(21 * time.Millisecond)

	var probes int64
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if d := b.Allow(); d.Allowed && d.Probe {
				atomic.AddInt64(&probes, 1)
			}
		}()
	}
	wg.Wait()
	if got := atomic.LoadInt64(&probes); got != 1 {
		t.Fatalf("exactly one half-open probe must pass herd, got %d", got)
	}
	t.Logf("half-open decision under 200 concurrent requests: probes=%d", probes)
}
