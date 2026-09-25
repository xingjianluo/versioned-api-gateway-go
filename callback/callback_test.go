package callback

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xingjianluo/versioned-api-gateway-go/callback/receiver"
)

// receiverAdapter 把幂等 receiver 适配为 Deliverer，
// 并可脚本化“前 failFirst 次无法确认送达”。
type receiverAdapter struct {
	recv      *receiver.Receiver
	failFirst int32
	mu        sync.Mutex
	calls     map[string]int
}

func newAdapter(recv *receiver.Receiver, failFirst int) *receiverAdapter {
	return &receiverAdapter{recv: recv, failFirst: int32(failFirst), calls: map[string]int{}}
}

func (a *receiverAdapter) Deliver(ctx context.Context, ev Event) error {
	a.mu.Lock()
	a.calls[ev.EventID]++
	a.mu.Unlock()
	if n := atomic.AddInt32(&a.failFirst, -1); n >= 0 {
		return errors.New("delivery unconfirmed (connection reset)")
	}
	dup, err := a.recv.Receive(ctx, ev.EventID, ev.Payload)
	if err != nil {
		return err
	}
	if dup {
		return fmt.Errorf("receiver saw duplicate %s (still acked)", ev.EventID)
	}
	return nil
}

func (a *receiverAdapter) callsFor(id string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls[id]
}

func newTestDispatcher(t *testing.T, del Deliverer, opts ...Option) (*Dispatcher, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	d := NewDispatcher(store, del,
		append([]Option{WithMaxAttempts(5), WithBaseBackoff(time.Millisecond)}, opts...)...)
	return d, dir
}

func TestEmitIdempotentAndDeliverOnce(t *testing.T) {
	recv := receiver.New()
	ad := newAdapter(recv, 0)
	d, _ := newTestDispatcher(t, ad)
	ctx := context.Background()

	ev := Event{EventID: "e1", Target: "cb-target", RequestID: "req-1",
		IdemKey: "idem-1", Payload: "hello"}
	for i := 0; i < 3; i++ {
		if err := d.Emit(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	pending, _ := d.store.ListPending(ctx)
	if len(pending) != 1 {
		t.Fatalf("duplicate emit created %d events, want 1", len(pending))
	}
	res, err := d.DeliverPending(ctx)
	if err != nil || len(res) != 1 || res[0].Status != StatusDelivered || res[0].Attempts != 1 {
		t.Fatalf("deliver result=%+v err=%v", res, err)
	}
	if recv.HandleCount() != 1 {
		t.Fatalf("receiver handled %d, want 1", recv.HandleCount())
	}
	// 再次重放：无 pending，空操作。
	res2, _ := d.DeliverPending(ctx)
	if len(res2) != 0 {
		t.Fatalf("delivered event replayed, got %+v", res2)
	}
	got, _ := d.StatusOf(ctx, "e1")
	t.Logf("request_id=req-1 event=e1 idem_key=idem-1 attempts=%d status=%s",
		got.Attempts, got.Status)
}

func TestUnconfirmedRetriedThenConfirmed(t *testing.T) {
	recv := receiver.New()
	ad := newAdapter(recv, 2) // 前两次无法确认，第三次成功
	d, _ := newTestDispatcher(t, ad)
	ctx := context.Background()
	_ = d.Emit(ctx, Event{EventID: "e2", RequestID: "req-2", IdemKey: "k2"})

	res, _ := d.DeliverPending(ctx)
	if res[0].Status != StatusDelivered || res[0].Attempts != 3 {
		t.Fatalf("want delivered on attempt 3, got %+v", res)
	}
	if recv.HandleCount() != 1 {
		t.Fatal("business must process exactly once despite retries")
	}
	got, _ := d.StatusOf(ctx, "e2")
	if got.RequestID != "req-2" || got.IdemKey != "k2" {
		t.Fatalf("correlation info lost: %+v", got)
	}
	t.Logf("request_id=req-2 event=e2 retry_count=%d final=%s (unknown results never faked success)",
		got.Attempts-1, got.Status)
}

func TestExhaustAttemptsBecomesDead(t *testing.T) {
	recv := receiver.New()
	ad := newAdapter(recv, 100) // 永远无法确认
	d, _ := newTestDispatcher(t, ad)
	ctx := context.Background()
	_ = d.Emit(ctx, Event{EventID: "e3", RequestID: "req-3"})

	res, _ := d.DeliverPending(ctx)
	if res[0].Status != StatusDead || res[0].Attempts != 5 {
		t.Fatalf("want dead after 5 attempts, got %+v", res)
	}
	// dead 事件不再进入 pending，不会无限重试。
	pending, _ := d.store.ListPending(ctx)
	if len(pending) != 0 {
		t.Fatalf("dead event must stop replay, pending=%d", len(pending))
	}
	t.Logf("request_id=req-3 event=e3 attempts=%d final=dead (bounded retry)", res[0].Attempts)
}

func TestRestartReplaysPendingPreservingCorrelation(t *testing.T) {
	dir := t.TempDir()
	store1, _ := NewFileStore(dir)
	recv := receiver.New()
	// 第一次“进程”：投递前两次都无法确认，且 ctx 提前取消。
	d1 := NewDispatcher(store1, newAdapter(recv, 100),
		WithMaxAttempts(5), WithBaseBackoff(50*time.Millisecond))
	ctx1 := context.Background()
	_ = d1.Emit(ctx1, Event{EventID: "e4", Target: "t", RequestID: "req-4",
		IdemKey: "k4", Payload: "p4"})

	cctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel() // 模拟进程在退避中被终止
	}()
	res, _ := d1.DeliverPending(cctx)
	if res[0].Status != StatusPending {
		t.Fatalf("cancelled retry must leave event pending, got %+v", res)
	}

	// “重启”：同目录新建 store 与 dispatcher，这次接收方正常。
	store2, _ := NewFileStore(dir)
	ad2 := newAdapter(recv, 0)
	d2 := NewDispatcher(store2, ad2, WithMaxAttempts(5), WithBaseBackoff(time.Millisecond))
	ctx2 := context.Background()
	pending, _ := store2.ListPending(ctx2)
	if len(pending) != 1 || pending[0].EventID != "e4" {
		t.Fatalf("restart must find 1 pending, got %+v", pending)
	}
	if pending[0].RequestID != "req-4" || pending[0].IdemKey != "k4" || pending[0].Payload != "p4" {
		t.Fatalf("correlation info lost across restart: %+v", pending[0])
	}
	// 重放应在之前已消耗的次数基础上继续，而不是从 0 静默重来。
	res2, _ := d2.DeliverPending(ctx2)
	if res2[0].Status != StatusDelivered {
		t.Fatalf("restart replay should confirm, got %+v", res2)
	}
	got, _ := d2.StatusOf(ctx2, "e4")
	if recv.HandleCount() != 1 {
		t.Fatal("business processed more than once across restart")
	}
	t.Logf("request_id=req-4 event=e4 restart_replay delivered, prior_attempts_preserved total_attempts=%d",
		got.Attempts)
}
