package versionx

import (
	"context"
	"testing"
)

func mustVer(t *testing.T, s string) Version {
	t.Helper()
	v, err := Parse(s)
	if err != nil {
		t.Fatalf("parse %s: %v", s, err)
	}
	return v
}

func TestParseAndCompare(t *testing.T) {
	v := mustVer(t, "v1.2.3")
	if v != (Version{1, 2, 3}) {
		t.Fatalf("got %v", v)
	}
	if v.String() != "v1.2.3" {
		t.Fatalf("string %s", v.String())
	}
	if Compare(mustVer(t, "1.0.0"), mustVer(t, "1.2.0")) >= 0 {
		t.Fatal("compare wrong")
	}
	if _, err := Parse("bad"); err == nil {
		t.Fatal("expected parse error")
	}
}

func sampleUpstreams(t *testing.T) []UpstreamDesc {
	return []UpstreamDesc{
		{
			ID:      "u-old",
			Version: mustVer(t, "1.1.0"),
			Caps: map[string]Capability{
				"orders": {Name: "orders", Since: mustVer(t, "1.0.0")},
				// 不提供 refunds
			},
		},
		{
			ID:      "u-new",
			Version: mustVer(t, "1.4.0"),
			Caps: map[string]Capability{
				"orders":  {Name: "orders", Since: mustVer(t, "1.0.0")},
				"refunds": {Name: "refunds", Since: mustVer(t, "1.2.0")},
			},
		},
		{
			ID:      "u-v2",
			Version: mustVer(t, "2.0.0"),
			Caps: map[string]Capability{
				"orders":  {Name: "orders", Since: mustVer(t, "2.0.0")},
				"refunds": {Name: "refunds", Since: mustVer(t, "2.0.0")},
			},
		},
	}
}

func rule(t *testing.T) RouteRule {
	return RouteRule{
		Route:        "/pay",
		MinSupported: mustVer(t, "1.0.0"),
		MaxSupported: mustVer(t, "1.99.0"),
		RequiredCaps: []string{"orders", "refunds"},
	}
}

func TestNegotiate_HappyPathPicksHighest(t *testing.T) {
	n := NewNegotiator()
	d := n.Negotiate(context.Background(), mustVer(t, "1.3.0"), rule(t), sampleUpstreams(t))
	if !d.Allowed || d.Selected.ID != "u-new" {
		t.Fatalf("want u-new, got allowed=%v selected=%+v reason=%s", d.Allowed, d.Selected, d.Reason)
	}
	t.Logf("decision: reason=%s detail=%s", d.Reason, d.Detail)
}

func TestNegotiate_RejectReasons(t *testing.T) {
	n := NewNegotiator()
	ups := sampleUpstreams(t)
	ctx := context.Background()

	cases := []struct {
		name   string
		client string
		rule   RouteRule
		want   DecisionReason
	}{
		{"client too old", "0.9.0", rule(t), ReasonClientTooOld},
		{"client too new", "1.99.1", rule(t), ReasonClientTooNew},
		{"major mismatch", "3.0.0", RouteRule{Route: "/pay",
			RequiredCaps: []string{"orders", "refunds"}}, ReasonNoUpstreamForMajor},
		{"capability missing", "1.3.0", RouteRule{
			Route:        "/x",
			RequiredCaps: []string{"ghost"},
		}, ReasonCapabilityMissing},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := n.Negotiate(ctx, mustVer(t, tc.client), tc.rule, ups)
			if d.Allowed || d.Reason != tc.want {
				t.Fatalf("want reason=%s, got allowed=%v reason=%s detail=%s",
					tc.want, d.Allowed, d.Reason, d.Detail)
			}
			t.Logf("reject: reason=%s detail=%s", d.Reason, d.Detail)
		})
	}
}

func TestNegotiate_CapabilityRemovedIsSemanticIncompatible(t *testing.T) {
	n := NewNegotiator()
	ups := []UpstreamDesc{
		{
			ID:      "u-removed",
			Version: mustVer(t, "1.5.0"),
			Caps: map[string]Capability{
				"orders": {Name: "orders", Since: mustVer(t, "1.0.0")},
				"refunds": {Name: "refunds", Since: mustVer(t, "1.0.0"),
					Removed: true}, // 已移除：语义不兼容
			},
		},
	}
	d := n.Negotiate(context.Background(), mustVer(t, "1.3.0"), rule(t), ups)
	if d.Allowed || d.Reason != ReasonCapabilityRemoved {
		t.Fatalf("want removed rejection, got allowed=%v reason=%s", d.Allowed, d.Reason)
	}
	t.Logf("reject: reason=%s detail=%s", d.Reason, d.Detail)
}

func TestNegotiate_V1ClientNeverRoutedToV2(t *testing.T) {
	n := NewNegotiator()
	// 只有 v2 上游
	ups := []UpstreamDesc{sampleUpstreams(t)[2]}
	d := n.Negotiate(context.Background(), mustVer(t, "1.0.0"),
		RouteRule{Route: "/pay", RequiredCaps: []string{"orders", "refunds"}}, ups)
	if d.Allowed || d.Reason != ReasonNoUpstreamForMajor {
		t.Fatalf("v1 must not hit v2, got %+v", d)
	}
}

func TestNegotiate_CandidatesSortedDescAndSameMajorOnly(t *testing.T) {
	ups := append(sampleUpstreams(t), UpstreamDesc{
		ID:      "u-mid",
		Version: mustVer(t, "1.3.0"),
		Caps: map[string]Capability{
			"orders":  {Name: "orders", Since: mustVer(t, "1.0.0")},
			"refunds": {Name: "refunds", Since: mustVer(t, "1.2.0")},
		},
	})
	n := NewNegotiator()
	d := n.Negotiate(context.Background(), mustVer(t, "1.2.0"), rule(t), ups)
	if !d.Allowed {
		t.Fatalf("got %+v", d)
	}
	// u-old 缺 refunds、u-v2 跨 major：候选只有 u-new 与 u-mid，按版本降序。
	if len(d.Candidates) != 2 {
		t.Fatalf("candidates=%+v", d.Candidates)
	}
	if d.Candidates[0].ID != "u-new" || d.Candidates[1].ID != "u-mid" {
		t.Fatalf("candidates not sorted desc: %+v", d.Candidates)
	}
	if d.Selected.ID != d.Candidates[0].ID {
		t.Fatalf("selected must equal candidates[0]: %+v", d)
	}
}
