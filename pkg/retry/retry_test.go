package retry

import (
	"context"
	"testing"
	"time"
)

func TestBackoffBounded(t *testing.T) {
	p := Policy{MaxAttempts: 5, BaseDelay: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond, JitterFraction: 0}
	got := []time.Duration{p.Backoff(1), p.Backoff(2), p.Backoff(3), p.Backoff(4), p.Backoff(10)}
	want := []time.Duration{10, 20, 40, 80, 100}
	for i := range got {
		if got[i] != want[i]*time.Millisecond {
			t.Fatalf("attempt %d: want %s got %s", i+1, want[i], got[i])
		}
	}
}

func TestSleepCancel(t *testing.T) {
	p := Policy{BaseDelay: time.Hour, MaxDelay: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Sleep(ctx, 1); err == nil {
		t.Fatal("canceled context must abort sleep")
	}
}
