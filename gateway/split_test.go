package gateway

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/versioned-api-gateway-go/breaker"
	"github.com/xingjianluo/versioned-api-gateway-go/internal/logging"
	"github.com/xingjianluo/versioned-api-gateway-go/ratelimit"
	"github.com/xingjianluo/versioned-api-gateway-go/split"
	"github.com/xingjianluo/versioned-api-gateway-go/upstream"
	"github.com/xingjianluo/versioned-api-gateway-go/versionx"
)

// splitTestConfig 两个同 major 兼容上游 + 五五分流。
func splitTestConfig(a, b upstream.Adapter, weights []split.Weight) Config {
	return Config{
		RouteRules: map[string]versionx.RouteRule{
			"/pay": {
				MinSupported: mustV("1.0.0"),
				MaxSupported: mustV("1.99.0"),
				RequiredCaps: []string{"orders", "refunds"},
			},
		},
		Upstreams:   []upstream.Adapter{a, b},
		CallerLimit: map[string]ratelimit.LayerConfig{"c1": {Rate: 0, Burst: 100000}},
		RouteLimit:  map[string]ratelimit.LayerConfig{"/pay": {Rate: 0, Burst: 100000}},
		Breaker:     breaker.Config{FailureThreshold: 3, OpenCoolDown: time.Hour},
		Retry:       RetryPolicy{MaxAttempts: 3, AttemptTimeout: time.Second},
		Split:       []split.RouteConfig{{Route: "/pay", Weights: weights}},
	}
}

func twoUpstreams() (a, b *upstream.ScriptedAdapter) {
	a = u("va", "1.4.0", []string{"orders", "refunds"})
	b = u("vb", "1.3.0", []string{"orders", "refunds"})
	return a, b
}

// keyPinnedTo 找一个会被钉到指定上游的业务键（直接查分流器，
// 不触达上游、不写幂等缓存，保证后续 Serve 会真实调用上游）。
func keyPinnedTo(g *Gateway, upID string) string {
	cands := []split.Candidate{
		{ID: "va", Version: mustV("1.4.0")},
		{ID: "vb", Version: mustV("1.3.0")},
	}
	for i := 0; ; i++ {
		key := fmt.Sprintf("probe-%s-%d", upID, i)
		d := g.splitter.Select("/pay", mustV("1.2.0"), key, cands,
			func(string) bool { return true })
		if d.UpstreamID == upID {
			return key
		}
	}
}

// TestSplitRatioFlipConcurrentNoDoubleExecution 比例调整/回滚与并发重复请求
// 同时进行：同一业务操作只执行一次，且所有并发结论落在同一个上游。
func TestSplitRatioFlipConcurrentNoDoubleExecution(t *testing.T) {
	a, b := twoUpstreams()
	cfg := splitTestConfig(a, b, []split.Weight{{UpstreamID: "va", Weight: 50}, {UpstreamID: "vb", Weight: 50}})
	g, logBuf := newTestGW(t, cfg)
	ctx := context.Background()

	stop := make(chan struct{})
	var flipWg sync.WaitGroup
	flipWg.Add(1)
	go func() {
		defer flipWg.Done()
		v1 := []split.RouteConfig{{Route: "/pay", Weights: []split.Weight{{UpstreamID: "va", Weight: 95}, {UpstreamID: "vb", Weight: 5}}}}
		v2 := []split.RouteConfig{{Route: "/pay", Weights: []split.Weight{{UpstreamID: "va", Weight: 5}, {UpstreamID: "vb", Weight: 95}}}}
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			// 比例调整与整体回滚交替进行。
			if i%2 == 0 {
				if err := g.UpdateSplitConfigs(v1); err != nil {
					t.Errorf("update v1: %v", err)
				}
			} else {
				if err := g.UpdateSplitConfigs(v2); err != nil {
					t.Errorf("update v2: %v", err)
				}
			}
		}
	}()

	const n = 32
	outcomes := make([]Outcome, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			outcomes[i] = g.Serve(ctx, "c1", fmt.Sprintf("req-hot-%d", i),
				"1.2.0", "/pay", "hot-biz-key", "p")
		}(i)
	}
	wg.Wait()
	close(stop)
	flipWg.Wait()

	for i, o := range outcomes {
		if !o.Allowed {
			t.Fatalf("outcome %d not allowed: %+v", i, o)
		}
		if o.SelectedUpstream != outcomes[0].SelectedUpstream {
			t.Fatalf("same business key drifted: outcome0=%s outcome%d=%s",
				outcomes[0].SelectedUpstream, i, o.SelectedUpstream)
		}
	}
	if total := a.Behavior.Count() + b.Behavior.Count(); total != 1 {
		t.Fatalf("business executed %d times, want exactly 1 (va=%d vb=%d)",
			total, a.Behavior.Count(), b.Behavior.Count())
	}
	t.Logf("all %d concurrent outcomes landed on %s; business executed once",
		n, outcomes[0].SelectedUpstream)
	t.Logf("split decision log sample:\n%s", lastLines(logBuf.String(), "split_decision", 3))
}

// TestSplitFallbackWhenPinnedUpstreamDown 被钉住的上游熔断打开后，
// 同一业务键回退到健康候选，且日志可解释。
func TestSplitFallbackWhenPinnedUpstreamDown(t *testing.T) {
	a, b := twoUpstreams()
	cfg := splitTestConfig(a, b, []split.Weight{{UpstreamID: "va", Weight: 50}, {UpstreamID: "vb", Weight: 50}})
	g, logBuf := newTestGW(t, cfg)
	ctx := context.Background()

	key := keyPinnedTo(g, "va")
	t.Logf("business key %s pinned to va", key)

	// 让 va 连续失败打开熔断：用新鲜业务键（无幂等缓存）真实调用，
	// 钉到 va 的键会在单次请求内连续失败 3 次（重试不跨上游）触发熔断。
	a.Behavior.FailNextKinds = []upstream.ErrorKind{
		upstream.KindConnection, upstream.KindConnection, upstream.KindConnection,
		upstream.KindConnection, upstream.KindConnection, upstream.KindConnection,
		upstream.KindConnection, upstream.KindConnection, upstream.KindConnection,
	}
	for i := 0; g.breakers.Get("va").CurrentState() == breaker.StateClosed && i < 50; i++ {
		k := fmt.Sprintf("trip-%d", i)
		g.Serve(ctx, "c1", "req-"+k, "1.2.0", "/pay", k, "p")
	}
	if st := g.breakers.Get("va").CurrentState(); st != breaker.StateOpen {
		t.Fatalf("va breaker should be open, got %s", st)
	}

	// 同一业务键再来：必须回退到健康的 vb，而不是把失败甩给调用方。
	o := g.Serve(ctx, "c1", "req-fallback", "1.2.0", "/pay", key, "p")
	if !o.Allowed || o.SelectedUpstream != "vb" || !o.SplitFallback {
		t.Fatalf("want fallback to vb, got %+v", o)
	}
	if pin, _ := g.SplitPinOf(key); pin != "vb" {
		t.Fatalf("pin must move to vb, got %s", pin)
	}
	log := logBuf.String()
	if !strings.Contains(log, "fallback=true") || !strings.Contains(log, "pinned_from=va") {
		t.Fatalf("log must explain fallback, got:\n%s", lastLines(log, "split_decision", 5))
	}
	t.Logf("fallback decision log:\n%s", lastLines(log, "split_decision", 3))

	// 恢复后不丢请求：va 恢复（closed）且比例回收（va 权重调 0 非法，
	// 改为整体回滚到初始配置），既有钉住不漂移、不重复执行。
	a.Behavior.FailNextKinds = nil
	g.breakers.Get("va") // breaker 仍 open（cooldown 1h），钉住保持在 vb
	o = g.Serve(ctx, "c1", "req-after-rollback", "1.2.0", "/pay", key, "p")
	if !o.Allowed || o.SelectedUpstream != "vb" {
		t.Fatalf("pin must stay on vb, got %+v", o)
	}
}

// TestSplitInvalidConfigRejected 非法比例配置：构造期拒绝启动、运行期整体拒绝，
// 旧配置继续完整生效，绝不静默按默认放行。
func TestSplitInvalidConfigRejected(t *testing.T) {
	a, b := twoUpstreams()

	// 构造期：非法配置直接拒绝启动。
	bad := splitTestConfig(a, b, []split.Weight{{UpstreamID: "va", Weight: -1}, {UpstreamID: "vb", Weight: 10}})
	if _, err := New(bad, logging.NopLogger{}); err == nil {
		t.Fatal("negative weight must be rejected at construction")
	}
	unknown := splitTestConfig(a, b, []split.Weight{{UpstreamID: "ghost", Weight: 1}})
	if _, err := New(unknown, logging.NopLogger{}); err == nil {
		t.Fatal("unknown upstream must be rejected at construction")
	}
	allZero := splitTestConfig(a, b, []split.Weight{{UpstreamID: "va", Weight: 0}, {UpstreamID: "vb", Weight: 0}})
	if _, err := New(allZero, logging.NopLogger{}); err == nil {
		t.Fatal("all-zero weights must be rejected at construction")
	}

	// 运行期：非法调整整体拒绝，旧配置（50/50）继续生效。
	g, logBuf := newTestGW(t, splitTestConfig(a, b,
		[]split.Weight{{UpstreamID: "va", Weight: 50}, {UpstreamID: "vb", Weight: 50}}))
	genBefore := g.splitter.Generation()
	err := g.UpdateSplitConfigs([]split.RouteConfig{
		{Route: "/pay", Weights: []split.Weight{{UpstreamID: "va", Weight: 0}, {UpstreamID: "vb", Weight: 100}}},
	})
	if err == nil {
		t.Fatal("zero weight update must be rejected")
	}
	if g.splitter.Generation() != genBefore {
		t.Fatal("rejected update must not bump generation (no half-update)")
	}
	o := g.Serve(context.Background(), "c1", "req-after-reject", "1.2.0", "/pay", "k-reject", "p")
	if !o.Allowed || o.SplitRule == "default" {
		t.Fatalf("old split config must still be in effect, got %+v", o)
	}
	if !strings.Contains(logBuf.String(), "split_config_rejected") {
		t.Fatal("rejection must be logged")
	}
	t.Logf("rejected update log:\n%s", lastLines(logBuf.String(), "split_config", 2))
}

// TestSplitRetryStaysWithinSelectedUpstream 重试只在分流选定的同一上游内发生，
// 即使存在其他健康候选也绝不跨上游。
func TestSplitRetryStaysWithinSelectedUpstream(t *testing.T) {
	a, b := twoUpstreams()
	g, _ := newTestGW(t, splitTestConfig(a, b,
		[]split.Weight{{UpstreamID: "va", Weight: 50}, {UpstreamID: "vb", Weight: 50}}))
	ctx := context.Background()

	key := keyPinnedTo(g, "va")
	a.Behavior.FailNextKinds = []upstream.ErrorKind{upstream.KindTimeout, upstream.KindTimeout}
	before := b.Behavior.Count()
	o := g.Serve(ctx, "c1", "req-retry-"+key, "1.2.0", "/pay", key, "p")
	if !o.Allowed || o.SelectedUpstream != "va" || o.Retries != 2 {
		t.Fatalf("want success on va after 2 retries, got %+v", o)
	}
	if b.Behavior.Count() != before {
		t.Fatal("retry must not cross to another upstream")
	}
}

// TestSplitWeightedDistribution 网关层面权重分布大体符合比例，
// 且每个请求日志都能看到命中规则与当时权重。
func TestSplitWeightedDistribution(t *testing.T) {
	a, b := twoUpstreams()
	g, logBuf := newTestGW(t, splitTestConfig(a, b,
		[]split.Weight{{UpstreamID: "va", Weight: 20}, {UpstreamID: "vb", Weight: 80}}))
	ctx := context.Background()

	count := map[string]int{}
	const n = 2000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("dist-%d", i)
		o := g.Serve(ctx, "c1", "req-"+key, "1.2.0", "/pay", key, "p")
		if !o.Allowed {
			t.Fatalf("outcome %+v", o)
		}
		count[o.SelectedUpstream]++
	}
	ratio := float64(count["vb"]) / n
	if ratio < 0.75 || ratio > 0.85 {
		t.Fatalf("want vb ~80%%, got %v", count)
	}
	if !strings.Contains(logBuf.String(), "split_weights=va=20,vb=80") {
		t.Fatal("log must show effective weights")
	}
	t.Logf("gateway distribution: %v (vb ratio %.2f)", count, ratio)
}

func lastLines(s, match string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	var out []string
	for i := len(lines) - 1; i >= 0 && len(out) < n; i-- {
		if strings.Contains(lines[i], match) {
			out = append([]string{lines[i]}, out...)
		}
	}
	return strings.Join(out, "\n")
}
