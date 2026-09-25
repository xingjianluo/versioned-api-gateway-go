package receiver

import (
	"context"
	"testing"
)

func TestDuplicateAndOutOfOrder(t *testing.T) {
	r := New()
	ctx := context.Background()

	// 乱序到达：e2 先于 e1；且夹杂重复投递。
	deliveries := []struct{ id, p string }{
		{"e2", "p2"},
		{"e1", "p1"},
		{"e2", "p2-again"}, // 重复：负载不同也不得覆盖首达
		{"e1", "p1"},
		{"e3", "p3"},
	}
	dups := 0
	for _, d := range deliveries {
		dup, err := r.Receive(ctx, d.id, d.p)
		if err != nil {
			t.Fatal(err)
		}
		if dup {
			dups++
		}
	}
	if r.HandleCount() != 3 {
		t.Fatalf("handled=%d want 3", r.HandleCount())
	}
	if dups != 2 {
		t.Fatalf("duplicates=%d want 2", dups)
	}
	if p, _ := r.FirstPayload("e2"); p != "p2" {
		t.Fatalf("first payload must be preserved, got %s", p)
	}
	order := r.Order()
	if len(order) != 3 || order[0] != "e2" || order[1] != "e1" || order[2] != "e3" {
		t.Fatalf("out-of-order first-arrival order wrong: %v", order)
	}
	t.Logf("callback dedup: unique_handled=%d duplicates_ignored=%d first_arrival_order=%v",
		r.HandleCount(), dups, order)
}
