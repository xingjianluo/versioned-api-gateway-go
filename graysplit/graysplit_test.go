package graysplit

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestSetPlanRejectsInvalid(t *testing.T) {
	r := NewRouter()
	cases := []struct {
		name    string
		entries []Entry
	}{
		{"empty", nil},
		{"negative", []Entry{{UpstreamID: "a", Weight: -1}, {UpstreamID: "b", Weight: 2}}},
		{"all zero", []Entry{{UpstreamID: "a", Weight: 0}, {UpstreamID: "b", Weight: 0}}},
		{"overflow", []Entry{{UpstreamID: "a", Weight: MaxTotalWeight + 1}}},
		{"dup id", []Entry{{UpstreamID: "a", Weight: 1}, {UpstreamID: "a", Weight: 2}}},
		{"empty id", []Entry{{UpstreamID: "", Weight: 1}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := r.SetPlan("/r", tc.entries); err == nil {
				t.Fatalf("invalid plan %q must be rejected", tc.name)
			}
		})
	}
	// 全部拒绝后路由仍无计划。
	if p := r.CurrentPlan("/r"); p != nil {
		t.Fatalf("rejected plans must not be published, got %+v", p)
	}

	// 合法计划发布后，非法更新不得覆盖旧计划。
	if _, err := r.SetPlan("/r", []Entry{{UpstreamID: "a", Weight: 5}, {UpstreamID: "b", Weight: 5}}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.SetPlan("/r", []Entry{{UpstreamID: "a", Weight: -3}}); err == nil {
		t.Fatal("negative update must be rejected")
	}
	p := r.CurrentPlan("/r")
	if p == nil || p.Revision != 1 || p.String() != "a=5,b=5" {
		t.Fatalf("old plan must stay intact after rejected update, got %+v", p)
	}
}

func TestPickDeterministicAndStickyAcrossPlanChange(t *testing.T) {
	r := NewRouter()
	cands := []string{"a", "b"}
	if _, err := r.SetPlan("/r", []Entry{{UpstreamID: "a", Weight: 9}, {UpstreamID: "b", Weight: 1}}); err != nil {
		t.Fatal(err)
	}
	d1 := r.Pick("/r", "biz-1", cands)
	if d1.Rule != RuleWeighted || d1.Revision != 1 {
		t.Fatalf("first pick: %+v", d1)
	}
	// 调整比例：已固定的键不得漂移。
	if _, err := r.SetPlan("/r", []Entry{{UpstreamID: "a", Weight: 1}, {UpstreamID: "b", Weight: 9}}); err != nil {
		t.Fatal(err)
	}
	d2 := r.Pick("/r", "biz-1", cands)
	if d2.Rule != RuleSticky || d2.Ordered[0] != d1.Ordered[0] {
		t.Fatalf("sticky must survive ratio change: d1=%+v d2=%+v", d1, d2)
	}
	if d2.Revision != 2 {
		t.Fatalf("decision must carry new revision, got %+v", d2)
	}
	// 新键按计划权重分配（确定性：同键多次结果一致）。
	d3 := r.Pick("/r", "biz-2", cands)
	d4 := r.Pick("/r", "biz-2", cands)
	if d3.Ordered[0] != d4.Ordered[0] {
		t.Fatalf("same key must be deterministic: %s vs %s", d3.Ordered[0], d4.Ordered[0])
	}
}

func TestPickNeverLeavesCandidateSet(t *testing.T) {
	r := NewRouter()
	// 计划包含候选之外的上游 c：Pick 绝不能把它放进 Ordered。
	if _, err := r.SetPlan("/r", []Entry{{UpstreamID: "a", Weight: 1}, {UpstreamID: "c", Weight: 99}}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		d := r.Pick("/r", fmt.Sprintf("k%d", i), []string{"a", "b"})
		for _, id := range d.Ordered {
			if id != "a" && id != "b" {
				t.Fatalf("pick escaped candidate set: %+v", d)
			}
		}
	}
}

func TestPinWithdrawnWhenUpstreamRemovedFromPlan(t *testing.T) {
	r := NewRouter()
	cands := []string{"a", "b"}
	if _, err := r.SetPlan("/r", []Entry{{UpstreamID: "a", Weight: 5}, {UpstreamID: "b", Weight: 5}}); err != nil {
		t.Fatal(err)
	}
	// 找一个被固定到 a 的键。
	key := ""
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("k%d", i)
		if d := r.Pick("/r", k, cands); d.Ordered[0] == "a" {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("no key pinned to a in 200 tries")
	}
	// 回收 a 的比例（移出计划）：该键按规则重选到 b 并重新固定。
	if _, err := r.SetPlan("/r", []Entry{{UpstreamID: "b", Weight: 1}}); err != nil {
		t.Fatal(err)
	}
	d := r.Pick("/r", key, cands)
	if d.Rule != RulePinWithdrawn || d.Ordered[0] != "b" {
		t.Fatalf("withdrawn pin must reselect under current plan, got %+v", d)
	}
	if d2 := r.Pick("/r", key, cands); d2.Ordered[0] != "b" || d2.Rule != RuleSticky {
		t.Fatalf("reselection must stick, got %+v", d2)
	}
}

func TestRollbackRestoresPreviousPlanAtomically(t *testing.T) {
	r := NewRouter()
	if _, err := r.Rollback("/r"); err == nil {
		t.Fatal("rollback without history must fail")
	}
	if _, err := r.SetPlan("/r", []Entry{{UpstreamID: "a", Weight: 10}}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.SetPlan("/r", []Entry{{UpstreamID: "a", Weight: 2}, {UpstreamID: "b", Weight: 8}}); err != nil {
		t.Fatal(err)
	}
	p, err := r.Rollback("/r")
	if err != nil {
		t.Fatal(err)
	}
	if p.String() != "a=10" || p.Revision != 3 {
		t.Fatalf("rollback must restore previous entries with fresh revision, got %+v", p)
	}
	if cur := r.CurrentPlan("/r"); cur.Revision != 3 {
		t.Fatalf("current plan revision=%d want 3", cur.Revision)
	}
	if _, err := r.Rollback("/r"); err == nil {
		t.Fatal("second rollback must fail (history exhausted)")
	}
}

func TestConcurrentPickAndSetPlanNoHalfUpdate(t *testing.T) {
	r := NewRouter()
	cands := []string{"a", "b"}
	if _, err := r.SetPlan("/r", []Entry{{UpstreamID: "a", Weight: 10}}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				d := r.Pick("/r", fmt.Sprintf("k%d", i), cands)
				// 任何时刻看到的计划都必须是某一份完整计划：
				// 权重串要么是旧计划要么是新计划，绝不出现混合。
				if d.Revision > 0 && d.Weights != "a=10" && d.Weights != "a=1,b=9" {
					t.Errorf("half-updated plan observed: %+v", d)
					return
				}
				if len(d.Ordered) == 0 {
					t.Errorf("empty order")
					return
				}
			}
		}(i)
	}
	for j := 0; j < 200; j++ {
		if j%2 == 0 {
			_, _ = r.SetPlan("/r", []Entry{{UpstreamID: "a", Weight: 1}, {UpstreamID: "b", Weight: 9}})
		} else {
			_, _ = r.SetPlan("/r", []Entry{{UpstreamID: "a", Weight: 10}})
		}
	}
	close(stop)
	wg.Wait()
}

func TestWeightDistributionRoughlyMatches(t *testing.T) {
	r := NewRouter()
	cands := []string{"a", "b"}
	if _, err := r.SetPlan("/r", []Entry{{UpstreamID: "a", Weight: 3}, {UpstreamID: "b", Weight: 7}}); err != nil {
		t.Fatal(err)
	}
	count := map[string]int{}
	for i := 0; i < 2000; i++ {
		d := r.Pick("/r", fmt.Sprintf("dist-%d", i), cands)
		count[d.Ordered[0]]++
	}
	// 宽松区间：b 应显著多于 a（7:3）。
	if count["b"] < count["a"] {
		t.Fatalf("distribution inverted: %v", count)
	}
	if count["b"] < 1000 || count["a"] < 300 {
		t.Fatalf("distribution far from 7:3: %v", count)
	}
	t.Logf("distribution a=%d b=%d (plan 3:7)", count["a"], count["b"])
}

func TestNoPlanAndNoPlanEntryRules(t *testing.T) {
	r := NewRouter()
	d := r.Pick("/norule", "k", []string{"x", "y"})
	if d.Rule != RuleNoPlan || d.Ordered[0] != "x" {
		t.Fatalf("no plan must keep negotiation order, got %+v", d)
	}
	if _, err := r.SetPlan("/r2", []Entry{{UpstreamID: "z", Weight: 1}}); err != nil {
		t.Fatal(err)
	}
	d = r.Pick("/r2", "k", []string{"x", "y"})
	if d.Rule != RuleNoPlanEntry || d.Ordered[0] != "x" {
		t.Fatalf("plan without candidate overlap must degrade to negotiation order, got %+v", d)
	}
	if !strings.Contains(d.Weights, "z=1") {
		t.Fatalf("decision must carry effective weights snapshot, got %+v", d)
	}
}
