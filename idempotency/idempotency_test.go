package idempotency

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentSameKeyExecutesOnce(t *testing.T) {
	e := NewExecutor()
	var execs atomic.Int32
	gate := make(chan struct{})

	var wg sync.WaitGroup
	vals := make([]any, 50)
	errs := make([]error, 50)
	start := make(chan struct{})
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			vals[i], errs[i] = e.Do(context.Background(), "k1", func() (any, error) {
				execs.Add(1)
				<-gate // 模拟业务耗时，把所有并发请求叠在一起
				return "ok", nil
			})
		}(i)
	}
	close(start)
	time.Sleep(20 * time.Millisecond) // 等所有 goroutine 挂到在途调用上
	close(gate)
	wg.Wait()

	if execs.Load() != 1 {
		t.Fatalf("business executed %d times, want 1", execs.Load())
	}
	for i := range vals {
		if vals[i] != "ok" || errs[i] != nil {
			t.Fatalf("caller %d got %v %v", i, vals[i], errs[i])
		}
	}
	st := e.StatsOf("k1")
	if st.Executed != 1 || st.Shared != 49 {
		t.Fatalf("stats=%+v want executed=1 shared=49", st)
	}
	t.Logf("request_id=test-idem-concurrent idem_key=k1 business_executions=%d shared=%d",
		st.Executed, st.Shared)
}

func TestSuccessReplayFromCache(t *testing.T) {
	e := NewExecutor()
	var execs int
	for i := 0; i < 5; i++ {
		v, err := e.Do(context.Background(), "k2", func() (any, error) {
			execs++
			return "v", nil
		})
		if err != nil || v != "v" {
			t.Fatalf("iter %d got %v %v", i, v, err)
		}
	}
	if execs != 1 {
		t.Fatalf("executed=%d want 1 (others must replay cache)", execs)
	}
	st := e.StatsOf("k2")
	if st.Replayed != 4 {
		t.Fatalf("replayed=%d want 4", st.Replayed)
	}
}

func TestFailureNotCachedAllowsRetry(t *testing.T) {
	e := NewExecutor()
	var execs int
	boom := errors.New("boom")

	_, err := e.Do(context.Background(), "k3", func() (any, error) {
		execs++
		return nil, boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	// 失败不缓存：重试会再次执行，第二次成功后才缓存。
	v, err := e.Do(context.Background(), "k3", func() (any, error) {
		execs++
		return "recovered", nil
	})
	if err != nil || v != "recovered" {
		t.Fatalf("retry got %v %v", v, err)
	}
	_, _ = e.Do(context.Background(), "k3", func() (any, error) {
		execs++
		return "should-not-run", nil
	})
	if execs != 2 {
		t.Fatalf("executed=%d want 2 (fail, success; third must hit cache)", execs)
	}
	t.Logf("request_id=test-idem-fail idem_key=k3 retries_executed=2 then cached")
}

func TestDistinctKeysIndependent(t *testing.T) {
	e := NewExecutor()
	var execs atomic.Int32
	fn := func() (any, error) { execs.Add(1); return "x", nil }
	var wg sync.WaitGroup
	for _, k := range []string{"a", "b", "c"} {
		wg.Add(1)
		go func(k string) { defer wg.Done(); _, _ = e.Do(context.Background(), k, fn) }(k)
	}
	wg.Wait()
	if execs.Load() != 3 {
		t.Fatalf("distinct keys executed %d want 3", execs.Load())
	}
}
