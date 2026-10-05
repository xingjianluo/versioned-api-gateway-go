package gateway

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/xingjianluo/versioned-api-gateway-go/graysplit"
	"github.com/xingjianluo/versioned-api-gateway-go/internal/logging"
	"github.com/xingjianluo/versioned-api-gateway-go/upstream"
)

// splitConfig 两个同 major 兼容上游（u-a 版本更高）+ 初始分流计划。
func splitConfig(weights ...graysplit.Entry) (Config, *upstream.ScriptedAdapter, *upstream.ScriptedAdapter) {
	ua := u("u-a", "1.4.0", []string{"orders", "refunds"})
	ub := u("u-b", "1.3.0", []string{"orders", "refunds"})
	cfg := testConfig(ua, ub)
	if len(weights) > 0 {
		cfg.SplitPlans = map[string][]graysplit.Entry{"/pay": weights}
	}
	return cfg, ua, ub
}

func TestNewRejectsInvalidSplitPlan(t *testing.T) {
	cases := []struct {
		name  string
		plans map[string][]graysplit.Entry
	}{
		{"negative weight", map[string][]graysplit.Entry{"/pay": {{UpstreamID: "u-a", Weight: -1}, {UpstreamID: "u-b", Weight: 2}}}},
		{"all zero", map[string][]graysplit.Entry{"/pay": {{UpstreamID: "u-a", Weight: 0}, {UpstreamID: "u-b", Weight: 0}}}},
		{"overflow", map[string][]graysplit.Entry{"/pay": {{UpstreamID: "u-a", Weight: graysplit.MaxTotalWeight + 1}}}},
		{"unknown upstream", map[string][]graysplit.Entry{"/pay": {{UpstreamID: "ghost", Weight: 1}}}},
		{"unknown route", map[string][]graysplit.Entry{"/nope": {{UpstreamID: "u-a", Weight: 1}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, _, _ := splitConfig()
			cfg.SplitPlans = tc.plans
			if _, err := New(cfg, logging.NopLogger{}); err == nil {
				t.Fatalf("invalid split plan %q must fail New", tc.name)
			}
		})
	}
}

func TestSetSplitPlanInvalidRejectedAndOldPlanKept(t *testing.T) {
	cfg, ua, ub := splitConfig(graysplit.Entry{UpstreamID: "u-a", Weight: 10})
	g, log := newTestGW(t, cfg)
	ctx := context.Background()

	// 非法调整：负数 / 全零 / 超上限 / 未知上游，全部整体拒绝。
	bad := [][]graysplit.Entry{
		{{UpstreamID: "u-a", Weight: -5}},
		{{UpstreamID: "u-a", Weight: 0}, {UpstreamID: "u-b", Weight: 0}},
		{{UpstreamID: "u-a", Weight: graysplit.MaxTotalWeight + 1}},
		{{UpstreamID: "ghost", Weight: 1}},
	}
	for i, entries := range bad {
		if err := g.SetSplitPlan("/pay", entries); err == nil {
			t.Fatalf("bad plan %d must be rejected", i)
		}
	}
	if err := g.SetSplitPlan("/nope", []graysplit.Entry{{UpstreamID: "u-a", Weight: 1}}); err == nil {
		t.Fatal("unknown route must be rejected")
	}
	// 旧计划完整保持：流量仍全部走 u-a。
	o := g.Serve(ctx, "c1", "req-after-reject", "1.2.0", "/pay", "k-reject", "p")
	if !o.Allowed || o.SelectedUpstream != "u-a" || o.SplitRevision != 1 {
		t.Fatalf("old plan must stay in force, got %+v", o)
	}
	if ua.Behavior.Count() != 1 || ub.Behavior.Count() != 0 {
		t.Fatalf("traffic must follow old plan: ua=%d ub=%d",
			ua.Behavior.Count(), ub.Behavior.Count())
	}
	t.Logf("log after rejected updates:\n%s", tailLines(log.String(), 6))
}

func TestSplitPlanAdjustConcurrentSameKeyExecutesOnce(t *testing.T) {
	cfg, ua, ub := splitConfig(graysplit.Entry{UpstreamID: "u-a", Weight: 9},
		graysplit.Entry{UpstreamID: "u-b", Weight: 1})
	g, log := newTestGW(t, cfg)
	ctx := context.Background()

	const keys = 16
	const perKey = 20

	var wg sync.WaitGroup
	stop := make(chan struct{})
	// 持续并发调整比例（两份完整计划之间切换 + 周期性整体回滚）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			switch i % 3 {
			case 0:
				_ = g.SetSplitPlan("/pay", []graysplit.Entry{
					{UpstreamID: "u-a", Weight: 1}, {UpstreamID: "u-b", Weight: 9}})
			case 1:
				_ = g.SetSplitPlan("/pay", []graysplit.Entry{
					{UpstreamID: "u-a", Weight: 9}, {UpstreamID: "u-b", Weight: 1}})
			default:
				_ = g.RollbackSplitPlan("/pay")
			}
		}
	}()

	// 并发打同一批业务键：每个键的业务只能被执行一次、
	// 且该键的所有请求结论里的上游必须唯一（粘性不漂移）。
	type rec struct {
		ups map[string]int
	}
	recs := make([]*rec, keys)
	for i := range recs {
		recs[i] = &rec{ups: map[string]int{}}
	}
	var mu sync.Mutex
	start := make(chan struct{})
	var reqWg sync.WaitGroup
	for k := 0; k < keys; k++ {
		for n := 0; n < perKey; n++ {
			reqWg.Add(1)
			go func(k, n int) {
				defer reqWg.Done()
				<-start
				key := fmt.Sprintf("biz-%d", k)
				o := g.Serve(ctx, "c1", fmt.Sprintf("req-%d-%d", k, n),
					"1.2.0", "/pay", key, "p")
				if !o.Allowed {
					t.Errorf("request lost during ratio change: %+v", o)
					return
				}
				mu.Lock()
				recs[k].ups[o.SelectedUpstream]++
				mu.Unlock()
			}(k, n)
		}
	}
	close(start)
	reqWg.Wait() // 等全部请求结束后再停止调比例
	close(stop)
	wg.Wait()

	totalInvocations := ua.Behavior.Count() + ub.Behavior.Count()
	if totalInvocations != keys {
		t.Fatalf("business executions=%d want %d (one per key)", totalInvocations, keys)
	}
	for k := 0; k < keys; k++ {
		st := g.idem.StatsOf(fmt.Sprintf("biz-%d", k))
		if st.Executed != 1 {
			t.Fatalf("key biz-%d executed %d times, want 1", k, st.Executed)
		}
		if len(recs[k].ups) != 1 {
			t.Fatalf("key biz-%d drifted across upstreams: %v", k, recs[k].ups)
		}
	}
	// 日志可解释：每个请求都能看到当时命中的规则/比例/版本。
	s := log.String()
	if !strings.Contains(s, "split_rule=") || !strings.Contains(s, "split_revision=") ||
		!strings.Contains(s, "split_weights=") {
		t.Fatalf("split decision must be observable:\n%s", tailLines(s, 20))
	}
	t.Logf("executions=%d (ua=%d ub=%d), keys=%d, sample log:\n%s",
		totalInvocations, ua.Behavior.Count(), ub.Behavior.Count(), keys,
		tailLines(s, 8))
}

func TestFallbackWhenPreferredUpstreamUnhealthy(t *testing.T) {
	// u-a 独占 10 份权重但持续超时；u-b 健康。
	ua := u("u-a", "1.4.0", []string{"orders", "refunds"})
	for i := 0; i < 50; i++ {
		ua.Behavior.FailNextKinds = append(ua.Behavior.FailNextKinds, upstream.KindTimeout)
	}
	ub := u("u-b", "1.3.0", []string{"orders", "refunds"})
	cfg := testConfig(ua, ub)
	cfg.SplitPlans = map[string][]graysplit.Entry{"/pay": {
		{UpstreamID: "u-a", Weight: 10},
	}}
	g, log := newTestGW(t, cfg)
	ctx := context.Background()

	// 首选 u-a 重试耗尽 -> 回退 u-b 成功，业务只执行一次（在 u-b 上）。
	o := g.Serve(ctx, "c1", "req-fb-1", "1.2.0", "/pay", "k-fb-1", "p")
	if !o.Allowed || !o.Fallback || o.SelectedUpstream != "u-b" {
		t.Fatalf("want fallback to u-b, got %+v", o)
	}
	if ub.Behavior.Count() != 1 {
		t.Fatalf("u-b invocations=%d want 1", ub.Behavior.Count())
	}

	// 持续打到 u-a 熔断打开：之后的请求不再触达 u-a（快速回退），
	// 恢复流量全部落在健康的 u-b 上，不丢请求。
	for i := 0; i < 6; i++ {
		oo := g.Serve(ctx, "c1", fmt.Sprintf("req-fb-%d", i+2), "1.2.0", "/pay",
			fmt.Sprintf("k-fb-%d", i+2), "p")
		if !oo.Allowed || oo.SelectedUpstream != "u-b" {
			t.Fatalf("request %d must fall back to u-b, got %+v", i, oo)
		}
	}
	br := g.breakers.Get("u-a")
	if br.CurrentState().String() != "open" {
		t.Fatalf("u-a breaker should be open, got %s", br.CurrentState())
	}
	before := ua.Behavior.Count()
	o = g.Serve(ctx, "c1", "req-fb-open", "1.2.0", "/pay", "k-fb-open", "p")
	if !o.Allowed || o.SelectedUpstream != "u-b" {
		t.Fatalf("circuit-open fallback must still succeed on u-b, got %+v", o)
	}
	if ua.Behavior.Count() != before {
		t.Fatalf("open circuit must not touch u-a (before=%d after=%d)",
			before, ua.Behavior.Count())
	}

	s := log.String()
	if !strings.Contains(s, "split_fallback") ||
		!strings.Contains(s, "from=u-a") || !strings.Contains(s, "to=u-b") {
		t.Fatalf("fallback must be explainable in log:\n%s", tailLines(s, 30))
	}
	if !strings.Contains(s, "split_weights=u-a=10") {
		t.Fatalf("log must carry effective weights:\n%s", tailLines(s, 30))
	}
	t.Logf("fallback log:\n%s", tailLines(s, 14))
}

func TestStickyAcrossRatioChangeRetryAndReplay(t *testing.T) {
	cfg, ua, ub := splitConfig(graysplit.Entry{UpstreamID: "u-b", Weight: 1})
	g, log := newTestGW(t, cfg)
	ctx := context.Background()

	// 首请求把业务键 pin 到 u-b。
	o1 := g.Serve(ctx, "c1", "req-st-1", "1.2.0", "/pay", "k-sticky", "p")
	if !o1.Allowed || o1.SelectedUpstream != "u-b" || o1.SplitRule != graysplit.RuleWeighted {
		t.Fatalf("first pick: %+v", o1)
	}
	// 比例翻转：u-a 拿 99%。该业务键仍必须稳定在 u-b。
	if err := g.SetSplitPlan("/pay", []graysplit.Entry{
		{UpstreamID: "u-a", Weight: 99}, {UpstreamID: "u-b", Weight: 1}}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		o := g.Serve(ctx, "c1", fmt.Sprintf("req-st-%d", i+2), "1.2.0", "/pay", "k-sticky", "p")
		if !o.Allowed || o.SelectedUpstream != "u-b" || o.SplitRule != graysplit.RuleSticky {
			t.Fatalf("sticky violated after ratio change: %+v", o)
		}
	}
	if ua.Behavior.Count() != 0 || ub.Behavior.Count() != 1 {
		t.Fatalf("sticky key must stay on u-b: ua=%d ub=%d",
			ua.Behavior.Count(), ub.Behavior.Count())
	}
	// 整体回滚到上一份计划（u-b 独占），结论不变、业务不重复执行。
	if err := g.RollbackSplitPlan("/pay"); err != nil {
		t.Fatal(err)
	}
	o := g.Serve(ctx, "c1", "req-st-rb", "1.2.0", "/pay", "k-sticky", "p")
	if !o.Allowed || o.SelectedUpstream != "u-b" {
		t.Fatalf("after rollback: %+v", o)
	}
	if ub.Behavior.Count() != 1 {
		t.Fatalf("rollback must not re-execute business, ub=%d", ub.Behavior.Count())
	}
	if !strings.Contains(log.String(), "split_rule=sticky") {
		t.Fatalf("sticky rule must be observable:\n%s", tailLines(log.String(), 20))
	}
	t.Logf("sticky log:\n%s", tailLines(log.String(), 10))
}

func TestSplitNeverCrossesIncompatibleMajor(t *testing.T) {
	ua := u("u-a", "1.4.0", []string{"orders", "refunds"})
	v2 := u("u-v2", "2.0.0", []string{"orders", "refunds"})
	cfg := testConfig(ua, v2)
	// 计划把 9 成比例投给 v2 上游：v1 客户端的候选集里没有 v2，
	// 分流绝不能把请求送出兼容集。
	cfg.SplitPlans = map[string][]graysplit.Entry{"/pay": {
		{UpstreamID: "u-v2", Weight: 9}, {UpstreamID: "u-a", Weight: 1},
	}}
	g, _ := newTestGW(t, cfg)
	ctx := context.Background()
	for i := 0; i < 30; i++ {
		o := g.Serve(ctx, "c1", fmt.Sprintf("req-x-%d", i), "1.2.0", "/pay",
			fmt.Sprintf("k-x-%d", i), "p")
		if !o.Allowed || o.SelectedUpstream != "u-a" {
			t.Fatalf("v1 request escaped compatible set: %+v", o)
		}
	}
	if v2.Behavior.Count() != 0 {
		t.Fatalf("incompatible v2 upstream invoked %d times", v2.Behavior.Count())
	}
}

func TestRollbackWithoutHistoryFails(t *testing.T) {
	cfg, _, _ := splitConfig()
	g, _ := newTestGW(t, cfg)
	if err := g.RollbackSplitPlan("/pay"); err == nil {
		t.Fatal("rollback without any plan must fail")
	}
	if err := g.SetSplitPlan("/pay", []graysplit.Entry{{UpstreamID: "u-a", Weight: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := g.RollbackSplitPlan("/pay"); err == nil {
		t.Fatal("first plan has no predecessor, rollback must fail")
	}
}
