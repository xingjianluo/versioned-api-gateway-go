package upstream

import (
	"context"
	"testing"

	"github.com/xingjianluo/versioned-api-gateway-go/pkg/version"
)

type fakeAdapter struct {
	id     string
	v      version.Version
	caps   []version.Capability
	invoke func(ctx context.Context, req Request) (*Response, error)
}

func (f *fakeAdapter) ID() string                         { return f.id }
func (f *fakeAdapter) APIVersion() version.Version        { return f.v }
func (f *fakeAdapter) Capabilities() []version.Capability { return f.caps }
func (f *fakeAdapter) Invoke(ctx context.Context, req Request) (*Response, error) {
	return f.invoke(ctx, req)
}

func newFake(id, v string, caps ...version.Capability) *fakeAdapter {
	return &fakeAdapter{
		id:   id,
		v:    version.MustParse(v),
		caps: caps,
		invoke: func(context.Context, Request) (*Response, error) {
			return &Response{UpstreamID: id}, nil
		},
	}
}

func TestSelectVersionAndCapability(t *testing.T) {
	r := NewRegistry()
	r.Register(newFake("old-110", "1.1.0", version.Capability{Name: "pay", Semantic: 1}))
	r.Register(newFake("new-120", "1.2.0", version.Capability{Name: "pay", Semantic: 2}))
	r.Register(newFake("v2-200", "2.0.0", version.Capability{Name: "pay", Semantic: 2}))

	// 1.2 调用方：只能选 1.1/1.2，不选 2.0（major 不同），也不选 1.3+（minor 更高）。
	cands, err := r.Select(version.MustParse("1.2.0"),
		[]version.RequiredCapability{{Name: "pay", MinSemantic: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if len(cands) != 2 {
		t.Fatalf("want 2 candidates got %d", len(cands))
	}

	// 语义代际要求 3：有 pay 能力但无代际3 -> semantic_conflict。
	_, err = r.Select(version.MustParse("1.2.0"),
		[]version.RequiredCapability{{Name: "pay", MinSemantic: 3}})
	if err == nil || err.(*version.NegotiateError).Reason != version.RejectSemanticConflict {
		t.Fatalf("want semantic_conflict, got %v", err)
	}

	// 能力不存在 -> capability_missing。
	_, err = r.Select(version.MustParse("1.2.0"),
		[]version.RequiredCapability{{Name: "refund", MinSemantic: 1}})
	if err == nil || err.(*version.NegotiateError).Reason != version.RejectCapabilityMissing {
		t.Fatalf("want capability_missing, got %v", err)
	}

	// 2.0 调用方：只匹配 major=2。
	cands, err = r.Select(version.MustParse("2.0.0"),
		[]version.RequiredCapability{{Name: "pay", MinSemantic: 1}})
	if err != nil || len(cands) != 1 || cands[0].Adapter.ID() != "v2-200" {
		t.Fatalf("2.0 caller must only see v2-200, got %v err=%v", cands, err)
	}
}
