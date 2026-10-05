package split

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/xingjianluo/versioned-api-gateway-go/versionx"
)

func mustV(s string) versionx.Version {
	v, _ := versionx.Parse(s)
	return v
}

func candsOf(ids ...string) []Candidate {
	var out []Candidate
	for i, id := range ids {
		out = append(out, Candidate{ID: id, Version: versionx.Version{Major: 1, Minor: i}})
	}
	return out
}

func allHealthy(string) bool { return true }

func healthyExcept(down ...string) func(string) bool {
	set := map[string]bool{}
	for _, d := range down {
		set[d] = true
	}
	return func(id string) bool { return !set[id] }
}

func known(ids ...string) func(string) bool {
	set := map[string]bool{}
	for _, id := range ids {
		set[id] = true
	}
	return func(id string) bool { return set[id] }
}

func TestSetConfigsRejectsInvalid(t *testing.T) {
	good := []RouteConfig{{Route: "/pay", Weights: []Weight{{"a", 50}, {"b", 50}}}}
	cases := []struct {
		name string
		cfgs []RouteConfig
		want string
	}{
		{"empty route", []RouteConfig{{Route: "", Weights: []Weight{{"a", 1}}}}, "empty route"},
		{"empty weights", []RouteConfig{{Route: "/pay"}}, "empty weights"},
		{"negative weight", []RouteConfig{{Route: "/pay", Weights: []Weight{{"a", -1}, {"b", 1}}}}, "must be positive"},
		{"zero weight", []RouteConfig{{Route: "/pay", Weights: []Weight{{"a", 0}, {"b", 1}}}}, "must be positive"},
		{"all zero", []RouteConfig{{Route: "/pay", Weights: []Weight{{"a", 0}, {"b", 0}}}}, "must be positive"},
		{"sum overflow", []RouteConfig{{Route: "/pay", Weights: []Weight{{"a", maxWeightSum}, {"b", 1}}}}, "exceeds"},
		{"unknown upstream", []RouteConfig{{Route: "/pay", Weights: []Weight{{"ghost", 1}}}}, "unknown upstream"},
		{"duplicate upstream", []RouteConfig{{Route: "/pay", Weights: []Weight{{"a", 1}, {"a", 2}}}}, "duplicate"},
		{"inverted range", []RouteConfig{{Route: "/pay", MinVersion: mustV("2.0.0"), MaxVersion: mustV("1.0.0"),
			Weights: []Weight{{"a", 1}}}}, "min version"},
		{"overlapping ranges", []RouteConfig{
			{Route: "/pay", MaxVersion: mustV("1.5.0"), Weights: []Weight{{"a", 1}}},
			{Route: "/pay", MinVersion: mustV("1.3.0"), Weights: []Weight{{"b", 1}}},
		}, "overlaps"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := New()
			if err := r.SetConfigs(good, known("a", "b")); err != nil {
				t.Fatalf("good config rejected: %v", err)
			}
			genBefore := r.Generation()
			err := r.SetConfigs(tc.cfgs, known("a", "b"))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
			// 非法配置整体拒绝：旧配置完整生效，代次不变。
			if r.Generation() != genBefore {
				t.Fatal("generation must not change on rejected config")
			}
			d := r.Select("/pay", mustV("1.0.0"), "k1", candsOf("a", "b"), allHealthy)
			if d.Reason != ReasonWeightedNew {
				t.Fatalf("old config must still be in effect, got %+v", d)
			}
		})
	}
}

func TestWeightedDistributionAndDeterminism(t *testing.T) {
	r := New()
	err := r.SetConfigs([]RouteConfig{
		{Route: "/pay", Weights: []Weight{{"a", 25}, {"b", 75}}},
	}, known("a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	count := map[string]int{}
	const n = 4000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		d := r.Select("/pay", mustV("1.0.0"), key, candsOf("a", "b"), allHealthy)
		if d.Reason != ReasonWeightedNew || d.Weights != "a=25,b=75" {
			t.Fatalf("unexpected decision %+v", d)
		}
		count[d.UpstreamID]++
		// 同一键重复选择必须确定性一致（重试/重放/并发语义）。
		again := r.Select("/pay", mustV("1.0.0"), key, candsOf("a", "b"), allHealthy)
		if again.UpstreamID != d.UpstreamID || again.Reason != ReasonStickyPin {
			t.Fatalf("same key must stick to %s, got %+v", d.UpstreamID, again)
		}
	}
	ratio := float64(count["b"]) / float64(n)
	if ratio < 0.70 || ratio > 0.80 {
		t.Fatalf("want b ~75%%, got a=%d b=%d", count["a"], count["b"])
	}
	t.Logf("distribution: a=%d b=%d (b ratio %.2f)", count["a"], count["b"], ratio)
}

func TestStickySurvivesConfigChangeAndRollback(t *testing.T) {
	r := New()
	v1 := []RouteConfig{{Route: "/pay", Weights: []Weight{{"a", 100}, {"b", 0 + 1}}}}
	// 让 a 占绝对多数，找出被钉到 a 的键。
	if err := r.SetConfigs(v1, known("a", "b")); err != nil {
		t.Fatal(err)
	}
	var pinnedA, pinnedB string
	for i := 0; ; i++ {
		key := fmt.Sprintf("k%d", i)
		d := r.Select("/pay", mustV("1.0.0"), key, candsOf("a", "b"), allHealthy)
		if d.UpstreamID == "a" && pinnedA == "" {
			pinnedA = key
		}
		if d.UpstreamID == "b" && pinnedB == "" {
			pinnedB = key
		}
		if pinnedA != "" && pinnedB != "" {
			break
		}
	}
	// 比例调整为完全反转：钉住关系不得漂移。
	v2 := []RouteConfig{{Route: "/pay", Weights: []Weight{{"a", 1}, {"b", 100}}}}
	if err := r.SetConfigs(v2, known("a", "b")); err != nil {
		t.Fatal(err)
	}
	if d := r.Select("/pay", mustV("1.0.0"), pinnedA, candsOf("a", "b"), allHealthy); d.UpstreamID != "a" || d.Reason != ReasonStickyPin {
		t.Fatalf("pinnedA must stay on a after ratio flip, got %+v", d)
	}
	if d := r.Select("/pay", mustV("1.0.0"), pinnedB, candsOf("a", "b"), allHealthy); d.UpstreamID != "b" {
		t.Fatalf("pinnedB must stay on b after ratio flip, got %+v", d)
	}
	// 整体回滚到 v1：钉住关系仍然不变。
	if err := r.SetConfigs(v1, known("a", "b")); err != nil {
		t.Fatal(err)
	}
	if d := r.Select("/pay", mustV("1.0.0"), pinnedA, candsOf("a", "b"), allHealthy); d.UpstreamID != "a" {
		t.Fatalf("pinnedA must stay on a after rollback, got %+v", d)
	}
	if d := r.Select("/pay", mustV("1.0.0"), pinnedB, candsOf("a", "b"), allHealthy); d.UpstreamID != "b" {
		t.Fatalf("pinnedB must stay on b after rollback, got %+v", d)
	}
}

func TestFallbackWhenPinnedUnhealthy(t *testing.T) {
	r := New()
	if err := r.SetConfigs([]RouteConfig{
		{Route: "/pay", Weights: []Weight{{"a", 50}, {"b", 50}}},
	}, known("a", "b")); err != nil {
		t.Fatal(err)
	}
	// 找一个被钉到 a 的键。
	var key string
	for i := 0; ; i++ {
		k := fmt.Sprintf("k%d", i)
		if d := r.Select("/pay", mustV("1.0.0"), k, candsOf("a", "b"), allHealthy); d.UpstreamID == "a" {
			key = k
			break
		}
	}
	// a 熔断打开：回退到健康的 b，并改钉到 b。
	d := r.Select("/pay", mustV("1.0.0"), key, candsOf("a", "b"), healthyExcept("a"))
	if d.UpstreamID != "b" || !d.Fallback || d.PinnedFrom != "a" || d.Reason != ReasonFallback {
		t.Fatalf("want fallback a->b, got %+v", d)
	}
	// a 恢复健康后不回跳（防抖动、防恢复瞬间冲击）。
	d = r.Select("/pay", mustV("1.0.0"), key, candsOf("a", "b"), allHealthy)
	if d.UpstreamID != "b" || d.Reason != ReasonStickyPin {
		t.Fatalf("must stay on b after a recovers, got %+v", d)
	}
	// 全部不健康：保留钉住，交由熔断快速失败，不在选择层腾挪。
	d = r.Select("/pay", mustV("1.0.0"), key, candsOf("a", "b"), healthyExcept("a", "b"))
	if d.UpstreamID != "b" || d.Reason != ReasonAllUnhealthy {
		t.Fatalf("all unhealthy must keep pin, got %+v", d)
	}
}

func TestDefaultModeHighestVersionAndHealthFallback(t *testing.T) {
	r := New() // 无任何分流配置
	cands := []Candidate{
		{ID: "old", Version: mustV("1.1.0")},
		{ID: "new", Version: mustV("1.5.0")},
	}
	d := r.Select("/pay", mustV("1.2.0"), "k", cands, allHealthy)
	if d.UpstreamID != "new" || d.Reason != ReasonDefaultHighest || d.Rule != "default" {
		t.Fatalf("want highest version, got %+v", d)
	}
	// 最高版本不健康：回退到健康的旧版本（仍是兼容候选）。
	d = r.Select("/pay", mustV("1.2.0"), "k2", cands, healthyExcept("new"))
	if d.UpstreamID != "old" || !d.Fallback || d.PinnedFrom != "new" {
		t.Fatalf("want fallback new->old, got %+v", d)
	}
	// 默认模式不钉住：恢复健康后回到最高版本。
	d = r.Select("/pay", mustV("1.2.0"), "k2", cands, allHealthy)
	if d.UpstreamID != "new" || d.Reason != ReasonDefaultHighest {
		t.Fatalf("default mode must not pin, got %+v", d)
	}
}

func TestVersionRangeScopedConfigs(t *testing.T) {
	r := New()
	err := r.SetConfigs([]RouteConfig{
		{Route: "/pay", MaxVersion: mustV("1.9.9"), Weights: []Weight{{"a", 100}, {"b", 1}}},
		{Route: "/pay", MinVersion: mustV("2.0.0"), Weights: []Weight{{"b", 100}, {"a", 1}}},
	}, known("a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	cands := candsOf("a", "b")
	hits := map[string]int{}
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("k%d", i)
		d1 := r.Select("/pay", mustV("1.5.0"), key, cands, allHealthy)
		d2 := r.Select("/pay", mustV("2.1.0"), "v2-"+key, cands, allHealthy)
		hits["v1:"+d1.UpstreamID]++
		hits["v2:"+d2.UpstreamID]++
		if !strings.Contains(d1.Rule, "/pay[*..v1.9.9]") || !strings.Contains(d2.Rule, "/pay[v2.0.0..*]") {
			t.Fatalf("wrong rule hit: %q / %q", d1.Rule, d2.Rule)
		}
	}
	if hits["v1:a"] < 150 || hits["v2:b"] < 150 {
		t.Fatalf("version-scoped weights not applied: %v", hits)
	}
	t.Logf("version scoped hits: %v", hits)
}

func TestConcurrentSelectSameKeyConsistent(t *testing.T) {
	r := New()
	if err := r.SetConfigs([]RouteConfig{
		{Route: "/pay", Weights: []Weight{{"a", 50}, {"b", 50}}},
	}, known("a", "b")); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	// 后台持续调整比例（含回滚），模拟发布期并发变更。
	go func() {
		v1 := []RouteConfig{{Route: "/pay", Weights: []Weight{{"a", 90}, {"b", 10}}}}
		v2 := []RouteConfig{{Route: "/pay", Weights: []Weight{{"a", 10}, {"b", 90}}}}
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if i%2 == 0 {
				_ = r.SetConfigs(v1, known("a", "b"))
			} else {
				_ = r.SetConfigs(v2, known("a", "b"))
			}
		}
	}()
	defer close(stop)

	results := make([]string, 64)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d := r.Select("/pay", mustV("1.0.0"), "hot-key", candsOf("a", "b"), allHealthy)
			results[i] = d.UpstreamID
		}(i)
	}
	wg.Wait()
	for _, got := range results {
		if got != results[0] {
			t.Fatalf("concurrent same-key selects diverged: %v", results)
		}
	}
	t.Logf("concurrent same-key selects all landed on %s", results[0])
}
