package idempotency

import (
	"sync"
	"testing"
)

func TestConcurrentSameKeyExecutedOnce(t *testing.T) {
	s := NewStore()
	const n = 50
	var started int64
	var wg sync.WaitGroup
	resps := make([][]byte, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r, fresh := s.StartOrGet("op-1")
			if fresh {
				started++
				s.Complete("op-1", []byte("result"))
				return
			}
			<-r.Done()
			resps[idx] = r.Response
		}(i)
	}
	wg.Wait()

	if started != 1 {
		t.Fatalf("business operation executed %d times, want exactly 1", started)
	}
	replied := 0
	for _, b := range resps {
		if string(b) == "result" {
			replied++
		}
	}
	if replied != n-1 {
		t.Fatalf("want %d concurrent duplicates share one result, got %d", n-1, replied)
	}
	t.Logf("idempotency: executions=%d duplicate_replays=%d", started, replied)
}

func TestFailedRecordAllowsReplay(t *testing.T) {
	s := NewStore()
	r, fresh := s.StartOrGet("op-2")
	if !fresh || r.Status != InFlight {
		t.Fatal("first start must be fresh in-flight")
	}
	s.Fail("op-2", "timeout")
	if got := s.Get("op-2"); got != nil {
		t.Fatal("failed record must be removed so a later safe replay can re-execute")
	}
	if _, fresh := s.StartOrGet("op-2"); !fresh {
		t.Fatal("replay after explicit failure must register again")
	}
}
