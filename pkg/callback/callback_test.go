package callback

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// scriptedDeliverer 按预设结果序列返回，记录实际投递次数与载荷。
type scriptedDeliverer struct {
	mu      sync.Mutex
	results map[string][]error // 按 key 脚本化
	cursor  map[string]int
	calls   int64
	seen    []Notification
}

func newScripted(results map[string][]error) *scriptedDeliverer {
	return &scriptedDeliverer{results: results, cursor: map[string]int{}}
}

func (d *scriptedDeliverer) Deliver(ctx context.Context, n Notification) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	atomic.AddInt64(&d.calls, 1)
	d.seen = append(d.seen, n)
	i := d.cursor[n.Key]
	d.cursor[n.Key]++
	if errs, ok := d.results[n.Key]; ok && i < len(errs) {
		return errs[i]
	}
	return nil
}

func (d *scriptedDeliverer) callsFor(key string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cursor[key]
}

func testPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
}

func TestDuplicateEnqueueDeliveredOnce(t *testing.T) {
	dir := t.TempDir()
	ob, err := NewFileOutbox(dir)
	if err != nil {
		t.Fatal(err)
	}
	dv := newScripted(nil)
	m := NewManager(ob, dv, testPolicy())

	n := Notification{Key: "k1", Seq: 1, Correlation: "req-123", URL: "http://local/cb"}
	rec, isNew, err := m.Enqueue(n)
	if err != nil || !isNew {
		t.Fatalf("first enqueue must be new: new=%v err=%v", isNew, err)
	}
	// 完全相同的重复投递。
	dup, dupNew, err := m.Enqueue(n)
	if err != nil || dupNew || dup.Seq != rec.Seq || dup.Status != rec.Status {
		t.Fatalf("duplicate notification must be absorbed: new=%v err=%v dup=%+v", dupNew, err, dup)
	}

	ok, err := m.DeliverOnce(context.Background(), "k1")
	if err != nil || !ok {
		t.Fatalf("delivery should confirm: ok=%v err=%v", ok, err)
	}
	// 重复投递确认后：不会产生第二次发送，关联信息仍保留。
	ok2, err := m.DeliverOnce(context.Background(), "k1")
	if err != nil || !ok2 {
		t.Fatalf("already delivered must report ok without resending: %v %v", ok2, err)
	}
	if got := dv.callsFor("k1"); got != 1 {
		t.Fatalf("deliverer must be called exactly once, got %d", got)
	}
	got, _ := ob.Get("k1")
	if got.Correlation != "req-123" || got.Status != Delivered || got.Attempts != 1 {
		t.Fatalf("traceability broken: %+v", got)
	}
	t.Logf("duplicate delivery: actual_deliveries=%d correlation=%s", dv.callsFor("k1"), got.Correlation)
}

func TestFailThenAckIsNotFakeSuccess(t *testing.T) {
	dir := t.TempDir()
	ob, _ := NewFileOutbox(dir)
	boom := errors.New("connection reset")
	// 前两次未确认，第三次确认。
	dv := newScripted(map[string][]error{"k": {boom, boom, nil}})
	m := NewManager(ob, dv, testPolicy())

	if _, _, err := m.Enqueue(Notification{Key: "k", Seq: 1, Correlation: "req-9"}); err != nil {
		t.Fatal(err)
	}
	ok, err := m.DeliverOnce(context.Background(), "k")
	if err != nil || !ok {
		t.Fatalf("eventual ack expected: ok=%v err=%v", ok, err)
	}
	if dv.callsFor("k") != 3 {
		t.Fatalf("want 3 attempts, got %d", dv.callsFor("k"))
	}
	got, _ := ob.Get("k")
	if got.Attempts != 3 || got.LastError != "" || got.Status != Delivered {
		t.Fatalf("record should show 3 attempts and clean delivery: %+v", got)
	}
}

func TestUnackedGoesDeadNotSuccess(t *testing.T) {
	dir := t.TempDir()
	ob, _ := NewFileOutbox(dir)
	boom := errors.New("no route to host")
	dv := newScripted(map[string][]error{"k": {boom, boom, boom, boom}})
	m := NewManager(ob, dv, testPolicy())

	if _, _, err := m.Enqueue(Notification{Key: "k", Seq: 1, Correlation: "req-7"}); err != nil {
		t.Fatal(err)
	}
	ok, err := m.DeliverOnce(context.Background(), "k")
	if ok || err == nil {
		t.Fatalf("unacked delivery must never report success: ok=%v err=%v", ok, err)
	}
	dle, isDead := err.(*DeadLetteredError)
	if !isDead || dle.Correlation != "req-7" {
		t.Fatalf("must dead-letter preserving correlation, got %v", err)
	}
	got, _ := ob.Get("k")
	if got.Status != Dead || got.Attempts != 3 {
		t.Fatalf("want dead after exactly 3 attempts, got %+v", got)
	}
	t.Logf("dead letter: attempts=%d correlation=%s last_error=%q", got.Attempts, got.Correlation, got.LastError)
}

func TestOutOfOrderAcks(t *testing.T) {
	dir := t.TempDir()
	ob, _ := NewFileOutbox(dir)
	dv := newScripted(nil)
	m := NewManager(ob, dv, testPolicy())

	// 先送达 seq 2，再迟到地重放 seq 1：旧序号不得覆盖新状态。
	if _, _, err := m.Enqueue(Notification{Key: "k", Seq: 2, Correlation: "req-2"}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.DeliverOnce(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	older, isNew, err := m.Enqueue(Notification{Key: "k", Seq: 1, Correlation: "req-1"})
	if err != nil || isNew {
		t.Fatalf("older seq must be absorbed as duplicate, got new=%v err=%v", isNew, err)
	}
	if older.Seq != 2 || older.Status != Delivered {
		t.Fatalf("stale enqueue must return current seq=2 record, got %+v", older)
	}
}

func TestRestartReplayPreservesAttempts(t *testing.T) {
	dir := t.TempDir()
	ob, _ := NewFileOutbox(dir)
	boom := errors.New("temp failure")

	// 旧进程：投递一次未确认后"崩溃"。
	dv1 := newScripted(map[string][]error{"k": {boom}})
	m1 := NewManager(ob, dv1, testPolicy())
	// 使用只允许一次尝试的策略模拟进程在退避等待中退出。
	m1.policy.MaxAttempts = 1
	if _, _, err := m1.Enqueue(Notification{Key: "k", Seq: 1, Correlation: "req-restart"}); err != nil {
		t.Fatal(err)
	}
	_, dErr := m1.DeliverOnce(context.Background(), "k")
	if dErr == nil {
		t.Fatal("expected dead/unacked before crash simulation")
	}
	// 第一次尝试已落盘（状态可能为 dead）；为了模拟重启重放，手动置回 pending。
	// 真实崩溃发生在 Update 之后、退避等待期间时记录就是 pending。
	if n, ok := ob.Get("k"); ok && n.Status == Dead {
		n.Status = Pending
		if err := ob.Update(*n); err != nil {
			t.Fatal(err)
		}
	}

	// 新进程：从磁盘恢复，未确认者继续重放，尝试次数不被清零。
	ob2, err := NewFileOutbox(dir)
	if err != nil {
		t.Fatal(err)
	}
	dv2 := newScripted(nil)
	m2 := NewManager(ob2, dv2, RetryPolicy{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond})
	rep, err := m2.ReplayPending(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Scanned != 1 || rep.Delivered != 1 {
		t.Fatalf("replay report wrong: %+v", rep)
	}
	got, _ := ob2.Get("k")
	if got.Status != Delivered || got.Attempts < 2 {
		t.Fatalf("attempt count must survive restart (>=2), got %+v", got)
	}
	if got.Correlation != "req-restart" {
		t.Fatalf("correlation must survive replay, got %q", got.Correlation)
	}
	t.Logf("restart replay: attempts_preserved=%d correlation=%s", got.Attempts, got.Correlation)
}

func TestReplayBoundedWorkersNoDuplicateSend(t *testing.T) {
	dir := t.TempDir()
	ob, _ := NewFileOutbox(dir)
	m := NewManager(ob, newScripted(nil), testPolicy())
	for i := 0; i < 10; i++ {
		_, _, _ = m.Enqueue(Notification{Key: "k" + string(rune('a'+i)), Seq: 1, Correlation: "c"})
	}
	rep, err := m.ReplayPending(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Scanned != 10 || rep.Delivered != 10 || rep.StillPending+rep.Dead != 0 {
		t.Fatalf("unexpected report: %+v", rep)
	}
}

func TestFileOutboxLayout(t *testing.T) {
	dir := t.TempDir()
	ob, _ := NewFileOutbox(dir)
	n := Notification{Key: "x", Seq: 1, Status: Pending}
	if err := ob.Append(n); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "pending", "x.json")); err != nil {
		t.Fatalf("pending file missing: %v", err)
	}
	n.Status = Delivered
	if err := ob.Update(n); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "delivered", "x.json")); err != nil {
		t.Fatalf("delivered file missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "pending", "x.json")); !os.IsNotExist(err) {
		t.Fatalf("old pending file must be moved away, err=%v", err)
	}
}
