package gateway

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/versioned-api-gateway-go/breaker"
	"github.com/xingjianluo/versioned-api-gateway-go/internal/logging"
	"github.com/xingjianluo/versioned-api-gateway-go/ratelimit"
	"github.com/xingjianluo/versioned-api-gateway-go/upstream"
	"github.com/xingjianluo/versioned-api-gateway-go/versionx"
)

func testConfig(ups ...upstream.Adapter) Config {
	return Config{
		RouteRules: map[string]versionx.RouteRule{
			"/pay": {
				MinSupported: mustV("1.0.0"),
				MaxSupported: mustV("1.99.0"),
				RequiredCaps: []string{"orders", "refunds"},
			},
		},
		Upstreams:   ups,
		CallerLimit: map[string]ratelimit.LayerConfig{"c1": {Rate: 0, Burst: 1000}},
		RouteLimit:  map[string]ratelimit.LayerConfig{"/pay": {Rate: 0, Burst: 1000}},
		Breaker:     breaker.Config{FailureThreshold: 3, OpenCoolDown: 50 * time.Millisecond},
		Retry:       RetryPolicy{MaxAttempts: 3, AttemptTimeout: time.Second},
	}
}

func mustV(s string) versionx.Version {
	v, _ := versionx.Parse(s)
	return v
}

func newTestGW(t *testing.T, cfg Config) (*Gateway, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	g, err := New(cfg, logging.NewStdLogger(buf))
	if err != nil {
		t.Fatal(err)
	}
	return g, buf
}

func u(id, ver string, caps []string, kinds ...upstream.ErrorKind) *upstream.ScriptedAdapter {
	return &upstream.ScriptedAdapter{
		StaticDesc: upstream.StaticDesc{ID: id, Version: ver, Caps: caps},
		Behavior:   &upstream.ScriptedBehavior{FailNextKinds: kinds},
	}
}

func TestVersionRejectionReasons(t *testing.T) {
	cfg := testConfig(u("u1", "1.4.0", []string{"orders", "refunds"}))
	g, log := newTestGW(t, cfg)
	ctx := context.Background()

	cases := []struct {
		name    string
		version string
		route   string
		want    RejectReason
	}{
		{"bad version", "vX", "/pay", ReasonUnknownClientVersion},
		{"unknown route", "1.0.0", "/nope", ReasonRouteNotFound},
		{"too old", "0.5.0", "/pay", RejectReason("version_client_too_old")},
		{"too new", "1.99.1", "/pay", RejectReason("version_client_too_new")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := g.Serve(ctx, "c1", "req-"+tc.name, tc.version, tc.route, "k", "p")
			if o.Allowed || o.Status != "rejected" || o.Reason != tc.want {
				t.Fatalf("want %s, got %+v", tc.want, o)
			}
		})
	}
	t.Logf("version decision log:\n%s", log.String())
}

func TestNoCompatibleUpstreamAndSemanticIncompatible(t *testing.T) {
	ctx := context.Background()

	t.Run("missing capability", func(t *testing.T) {
		// 仅 v1.1 上游，缺 refunds 能力。
		cfg := testConfig(u("old", "1.1.0", []string{"orders"}))
		g, log := newTestGW(t, cfg)
		o := g.Serve(ctx, "c1", "r-miss", "1.2.0", "/pay", "k", "p")
		if o.Allowed || o.Reason != RejectReason("version_capability_missing") {
			t.Fatalf("got %+v", o)
		}
		if !strings.Contains(log.String(), "decision=reject") {
			t.Fatal("log must explain reject")
		}
	})

	t.Run("capability removed -> semantic", func(t *testing.T) {
		// 通过自定义描述构造 refunds 已移除的上游。
		removed := &removedCapAdapter{
			ScriptedAdapter: *u("rm", "1.5.0", []string{"orders"}),
		}
		cfg := testConfig(removed)
		// 手动把上游描述补成 orders+refunds，但 refunds 标记移除。
		gw, err := New(cfg, logging.NopLogger{})
		if err != nil {
			t.Fatal(err)
		}
		gw.upDesc = []versionx.UpstreamDesc{{
			ID:      "rm",
			Version: mustV("1.5.0"),
			Caps: map[string]versionx.Capability{
				"orders":  {Name: "orders", Since: mustV("1.0.0")},
				"refunds": {Name: "refunds", Since: mustV("1.0.0"), Removed: true},
			},
		}}
		o := gw.Serve(ctx, "c1", "r-rm", "1.3.0", "/pay", "k", "p")
		if o.Allowed || o.Reason != RejectReason("version_semantic_incompatible_capability_removed") {
			t.Fatalf("got %+v", o)
		}
	})
}

// removedCapAdapter 包装 ScriptedAdapter，仅用于测试语义移除场景。
type removedCapAdapter struct {
	upstream.ScriptedAdapter
}

func TestRetryOnJitterThenSuccessExecutesBusinessOnce(t *testing.T) {
	// 前两次 timeout，第三次成功；MaxAttempts=3。
	up := u("u1", "1.4.0", []string{"orders", "refunds"},
		upstream.KindTimeout, upstream.KindConnection)
	cfg := testConfig(up)
	g, log := newTestGW(t, cfg)

	o := g.Serve(context.Background(), "c1", "req-jitter", "1.2.0",
		"/pay", "pay-key-1", "charge")
	if !o.Allowed || o.Status != "ok" {
		t.Fatalf("want eventual ok, got %+v", o)
	}
	if o.Retries != 2 {
		t.Fatalf("retries=%d want 2", o.Retries)
	}
	if up.Behavior.Count() != 3 {
		t.Fatalf("upstream invocations=%d want 3 (2 failed + 1 success)", up.Behavior.Count())
	}
	if !strings.Contains(log.String(), "retry=2") {
		t.Fatalf("log must show retry count:\n%s", log.String())
	}
	t.Logf("request_id=req-jitter retries=%d invocations=%d result=ok\n%s",
		o.Retries, up.Behavior.Count(), log.String())
}

func TestSemanticErrorNotRetried(t *testing.T) {
	up := u("u1", "1.4.0", []string{"orders", "refunds"},
		upstream.KindSemantic, upstream.KindSemantic, upstream.KindSemantic)
	cfg := testConfig(up)
	g, _ := newTestGW(t, cfg)
	o := g.Serve(context.Background(), "c1", "req-sem", "1.2.0",
		"/pay", "pay-key-sem", "x")
	if o.Allowed || o.Reason != ReasonUpstreamSemantic || o.Retries != 0 {
		t.Fatalf("semantic must fail fast with 0 retries, got %+v", o)
	}
	if up.Behavior.Count() != 1 {
		t.Fatalf("invocations=%d want 1", up.Behavior.Count())
	}
}

func TestDegradeReasonsDistinguishable(t *testing.T) {
	cases := []struct {
		kind upstream.ErrorKind
		want RejectReason
	}{
		{upstream.KindTimeout, ReasonUpstreamTimeout},
		{upstream.KindConnection, ReasonUpstreamConnection},
		{upstream.KindOverload, ReasonUpstreamOverload},
	}
	for _, tc := range cases {
		t.Run(tc.kind.String(), func(t *testing.T) {
			up := u("u1", "1.4.0", []string{"orders", "refunds"},
				tc.kind, tc.kind, tc.kind)
			cfg := testConfig(up)
			g, log := newTestGW(t, cfg)
			o := g.Serve(context.Background(), "c1", "req-"+tc.kind.String(),
				"1.2.0", "/pay", "k-"+tc.kind.String(), "p")
			if o.Allowed || o.Status != "degraded" || o.Reason != tc.want {
				t.Fatalf("want %s, got %+v", tc.want, o)
			}
			if o.Retries != 2 {
				t.Fatalf("retryable kind should exhaust retries, got %d", o.Retries)
			}
			if !strings.Contains(log.String(), "error_kind="+tc.kind.String()) {
				t.Fatalf("log missing error kind basis:\n%s", log.String())
			}
		})
	}
}

func TestCircuitOpensThenHalfOpenProbe(t *testing.T) {
	up := u("u1", "1.4.0", []string{"orders", "refunds"})
	cfg := testConfig(up)
	// 上游永远 connection 失败。
	up.Behavior.FailNextKinds = nil
	for i := 0; i < 20; i++ {
		up.Behavior.FailNextKinds = append(up.Behavior.FailNextKinds, upstream.KindConnection)
	}
	g, log := newTestGW(t, cfg)
	ctx := context.Background()

	// 触发熔断：3 个不同请求（不同幂等键），每次内部 1 次尝试后熔断逻辑累计。
	for i := 0; i < 3; i++ {
		o := g.Serve(ctx, "c1", idFor(i), "1.2.0", "/pay", keyFor(i), "p")
		if i < 2 {
			// 熔断未开前：正常打满 3 次内部尝试会更快达到阈值；
			// 断言最终请求是降级即可。
			_ = o
		}
		_ = o
	}

	// 等待断路器确实打开：直接读注册表状态。
	br := g.breakers.Get("u1")
	if br.CurrentState() != breaker.StateOpen {
		t.Fatalf("breaker want open, got %s", br.CurrentState())
	}

	// 熔断打开期间：快速失败降级，且不触达上游。
	before := up.Behavior.Count()
	o := g.Serve(ctx, "c1", "req-open", "1.2.0", "/pay", "k-open", "p")
	if o.Reason != ReasonCircuitOpen {
		t.Fatalf("want circuit-open degrade, got %+v", o)
	}
	if up.Behavior.Count() != before {
		t.Fatal("open breaker must not call upstream")
	}

	// 冷却后半开：100 个并发请求只有 1 个能成为探测（触达上游），
	// 其余全部被熔断快速失败；探测仍失败 -> 熔断重新打开。
	time.Sleep(60 * time.Millisecond)
	probeStart := up.Behavior.Count()
	var wg sync.WaitGroup
	openRejected, other := 0, 0
	var mu sync.Mutex
	start := make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			oo := g.Serve(ctx, "c1", idFor(100+i), "1.2.0", "/pay", keyFor(100+i), "p")
			mu.Lock()
			if oo.Reason == ReasonCircuitOpen {
				openRejected++
			} else {
				other++
			}
			mu.Unlock()
		}(i)
	}
	close(start)
	wg.Wait()

	probeInvocations := up.Behavior.Count() - probeStart
	if probeInvocations != 1 {
		t.Fatalf("half-open must invoke upstream exactly once, got %d invocations",
			probeInvocations)
	}
	// 探测那次失败后熔断立即重开，其内部后续重试也会被熔断拒绝，
	// 因此 100 个请求的最终原因都应是 circuit-open；关键是上游只被触达 1 次。
	if openRejected != 100 {
		t.Fatalf("all requests must end at breaker path, got openRejected=%d other=%d",
			openRejected, other)
	}
	if br.CurrentState() != breaker.StateOpen {
		t.Fatalf("failed probe must re-open breaker, got %s", br.CurrentState())
	}
	if !strings.Contains(log.String(), "to=half-open") ||
		!strings.Contains(log.String(), "to=open") {
		t.Fatalf("breaker transitions not observable:\n%s", log.String())
	}
	t.Logf("request_id=breaker-test probe_invocations=%d open_rejected=%d\n%s",
		probeInvocations, openRejected, tailLines(log.String(), 12))
}

func TestConcurrentQuotaExhaustionAtGateway(t *testing.T) {
	up := u("u1", "1.4.0", []string{"orders", "refunds"})
	cfg := testConfig(up)
	cfg.CallerLimit = map[string]ratelimit.LayerConfig{"c1": {Rate: 0, Burst: 10}}
	cfg.RouteLimit = map[string]ratelimit.LayerConfig{"/pay": {Rate: 0, Burst: 10}}
	g, log := newTestGW(t, cfg)

	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed, rejected := 0, 0
	start := make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			o := g.Serve(context.Background(), "c1", idFor(i), "1.2.0",
				"/pay", keyFor(i), "p")
			mu.Lock()
			if o.Allowed {
				allowed++
			} else {
				rejected++
			}
			mu.Unlock()
		}(i)
	}
	close(start)
	wg.Wait()
	if allowed != 10 || rejected != 90 {
		t.Fatalf("quota violated allowed=%d rejected=%d want 10/90", allowed, rejected)
	}
	if !strings.Contains(log.String(), "quota_layer=") {
		t.Fatal("log must show quota layer basis")
	}
	t.Logf("request_id=quota-test allowed=%d rejected=%d (exact burst)", allowed, rejected)
}

func TestIdempotentReplayDoesNotReinvoke(t *testing.T) {
	up := u("u1", "1.4.0", []string{"orders", "refunds"})
	cfg := testConfig(up)
	g, _ := newTestGW(t, cfg)
	ctx := context.Background()

	// 同一业务幂等键、不同 requestID（模拟客户端重试/重放）。
	for i := 0; i < 5; i++ {
		o := g.Serve(ctx, "c1", idFor(900+i), "1.2.0", "/pay", "same-business-key", "p")
		if !o.Allowed {
			t.Fatalf("request %d should succeed via replay, got %+v", i, o)
		}
	}
	if up.Behavior.Count() != 1 {
		t.Fatalf("business executed %d times, want 1 (idempotent replay)",
			up.Behavior.Count())
	}

	// 同键并发：仍只执行一次。
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			o := g.Serve(ctx, "c1", idFor(2000+i), "1.2.0", "/pay",
				"concurrent-business-key", "p")
			if !o.Allowed {
				t.Errorf("concurrent replay %d failed: %+v", i, o)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if up.Behavior.Count() != 2 {
		t.Fatalf("concurrent same-key must execute once more, total invocations=%d want 2",
			up.Behavior.Count())
	}
}

func TestRetryNeverCrossesIncompatibleVersion(t *testing.T) {
	// 只有一个兼容上游；重试固定使用协商选中的上游 ID。
	up := u("only-compatible", "1.4.0", []string{"orders", "refunds"},
		upstream.KindTimeout, upstream.KindTimeout, upstream.KindTimeout)
	cfg := testConfig(up)
	// 再放一个 v2 上游（不兼容，绝不能被重试选中）。
	cfg.Upstreams = append(cfg.Upstreams,
		u("v2-upstream", "2.0.0", []string{"orders", "refunds"}))
	g, _ := newTestGW(t, cfg) // rebuild to register v2

	o := g.Serve(context.Background(), "c1", "req-cross", "1.2.0",
		"/pay", "k-cross", "p")
	if o.Allowed {
		t.Fatal("must degrade")
	}
	if o.SelectedUpstream != "only-compatible" {
		t.Fatalf("retry crossed to %s", o.SelectedUpstream)
	}
	v2 := g.adapters["v2-upstream"].(*upstream.ScriptedAdapter)
	if v2.Behavior.Count() != 0 {
		t.Fatal("incompatible v2 upstream must never be invoked, even during retry")
	}
}

func idFor(i int) string  { return "req-" + itoa(i) }
func keyFor(i int) string { return "key-" + itoa(i) }
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}

func tailLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) <= n {
		return s
	}
	return strings.Join(lines[len(lines)-n:], "\n")
}
