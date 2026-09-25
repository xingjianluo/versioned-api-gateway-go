package upstream

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestScriptedFailureSequence(t *testing.T) {
	a := &ScriptedAdapter{
		StaticDesc: StaticDesc{ID: "u1", Version: "1.2.0", Caps: []string{"orders"}},
		Behavior: &ScriptedBehavior{FailNextKinds: []ErrorKind{
			KindTimeout, KindConnection, KindOverload, KindSemantic,
		}},
	}
	req := Request{RequestID: "r1", Payload: "p"}
	want := []ErrorKind{KindTimeout, KindConnection, KindOverload, KindSemantic}
	for i, w := range want {
		_, err := a.Invoke(context.Background(), req)
		var ue *UpstreamError
		if !errors.As(err, &ue) || ue.Kind != w {
			t.Fatalf("call %d want kind %s, got %v", i, w, err)
		}
		if i < 3 && !ue.Kind.Retryable() {
			t.Fatalf("%s should be retryable", ue.Kind)
		}
		if i == 3 && ue.Kind.Retryable() {
			t.Fatal("semantic must not be retryable")
		}
	}
	resp, err := a.Invoke(context.Background(), req)
	if err != nil || resp.Result != "processed:p" {
		t.Fatalf("after script exhausted want success, got %+v %v", resp, err)
	}
	if a.Behavior.Count() != 5 {
		t.Fatalf("invoke count=%d want 5", a.Behavior.Count())
	}
	t.Logf("request_id=r1 upstream=%s retryable kinds=[timeout,connection,overload] semantic=non-retryable",
		a.Desc().ID)
}

func TestContextDeadlineBecomesTimeout(t *testing.T) {
	a := &ScriptedAdapter{
		StaticDesc: StaticDesc{ID: "u1"},
		Behavior:   &ScriptedBehavior{Latency: 100 * time.Millisecond},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := a.Invoke(ctx, Request{})
	var ue *UpstreamError
	if !errors.As(err, &ue) || ue.Kind != KindTimeout {
		t.Fatalf("want timeout on deadline, got %v", err)
	}
}
