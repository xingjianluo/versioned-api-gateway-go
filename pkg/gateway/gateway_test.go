package gateway

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xingjianluo/versioned-api-gateway-go/pkg/circuit"
	gerrors "github.com/xingjianluo/versioned-api-gateway-go/pkg/errors"
	"github.com/xingjianluo/versioned-api-gateway-go/pkg/ratelimit"
	"github.com/xingjianluo/versioned-api-gateway-go/pkg/retry"
	"github.com/xingjianluo/versioned-api-gateway-go/pkg/upstream"
	"github.com/xingjianluo/versioned-api-gateway-go/pkg/version"
)

// tLogger 把网关决策日志打印到单测日志，包含：
// 请求标识、版本判定、配额/熔断决策依据、每次尝试与重试原因。
type tLogger struct{ t *testing.T }

func (l tLogger) Printf(format string, args ...any) { l.t.Logf(format, args...) }

// scriptedAdapter 按调用次序返回预设结果，统计被调用次数。
type scriptedAdapter struct {
	id      string
	v       version.Version
	caps    []version.Capability
	mu      sync.Mutex
	script  []*upstream.Error
	cursor  int
	invokes int64
}

func (a *scriptedAdapter) ID() string                         { return a.id }
func (a *scriptedAdapter) APIVersion() version.Version        { return a.v }
func (a *scriptedAdapter) Capabilities() []version.Capability { return a.caps }
func (a *scriptedAdapter) Invokes() int64                     { return atomic.LoadInt64(&a.invokes) }

func (a *scriptedAdapter) Invoke(ctx context.Context, req upstream.Request) (*upstream.Response, error) {
	atomic.AddInt64(&a.invokes, 1)
	a.mu.Lock()
	var err *upstream.Error
	if a.cursor < len(a.script) {
		err = a.script[a.cursor]
		a.cursor++
	}
	a.mu.Unlock()
	if err != nil {
		return nil, err
	}
	// 脚本用尽后恢复成功，用于模拟"抖动后恢复"。
	return &upstream.Response{UpstreamID: a.id, Version: a.v, Body: []byte("resp-" + a.id)}, nil
}

// alwaysFailAdapter 始终返回同一类故障，用于持续过载/熔断场景。
type alwaysFailAdapter struct {
	id   string
	v    version.Version
	caps []version.Capability
	kind upstream.ErrorKind
	n    int64
}

func (a *alwaysFailAdapter) ID() string                         { return a.id }
func (a *alwaysFailAdapter) APIVersion() version.Version        { return a.v }
func (a *alwaysFailAdapter) Capabilities() []version.Capability { return a.caps }
func (a *alwaysFailAdapter) Invoke(context.Context, upstream.Request) (*upstream.Response, error) {
	atomic.AddInt64(&a.n, 1)
	return nil, &upstream.Error{Kind: a.kind, Reason: "persistent:" + string(a.kind)}
}

func payCap(sem int) []version.Capability {
	return []version.Capability{{Name: "pay", Semantic: sem}}
}

func testCfg(callerBurst, routeBurst float64) Config {
	return Config{
		Policy: version.Policy{
			MinSupported: version.MustParse("1.1.0"),
			Known: []version.Version{
				version.MustParse("1.0.0"), version.MustParse("1.1.0"),
				version.MustParse("1.2.0"), version.MustParse("2.0.0"),
			},
		},
		CallerBucket: ratelimit.BucketConfig{Rate: 0, Burst: callerBurst},
		RouteBucket:  ratelimit.BucketConfig{Rate: 0, Burst: routeBurst},
		Circuit:      circuit.Config{FailureThreshold: 2, OpenCoolDown: 50 * time.Millisecond, SuccessThreshold: 1},
		Retry:        retry.Policy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond},
		Degrade:      map[string][]byte{"pay": []byte("fallback-pay")},
	}
}

func newRegistry(adapters ...upstream.Adapter) *upstream.Registry {
	r := upstream.NewRegistry()
	for _, a := range adapters {
		r.Register(a)
	}
	return r
}

func inReq(id string) In {
	return In{
		RequestID:  id,
		CallerID:   "caller-1",
		Route:      "pay",
		APIVersion: "1.2.0",
		Required:   []version.RequiredCapability{{Name: "pay", MinSemantic: 1}},
		Idempotent: true,
		IdemKey:    "biz-" + id,
	}
}

// 场景1：未知版本 / 旧版本 / 能力缺失 / 语义冲突，原因必须可区分且不转发。
func TestVersionNegotiationRejection(t *testing.T) {
	a := &scriptedAdapter{id: "u1", v: version.MustParse("1.2.0"), caps: payCap(2)}
	gw := New(testCfg(1000, 1000), newRegistry(a), tLogger{t})

	cases := []struct {
		name   string
		in     In
		reason version.RejectReason
	}{
		{"unknown_version", func() In { x := inReq("r1"); x.APIVersion = "9.9.9"; return x }(), version.RejectUnknownVersion},
		{"too_old", func() In { x := inReq("r2"); x.APIVersion = "1.0.0"; x.IdemKey = "b2"; return x }(), version.RejectTooOld},
		{"capability_missing", func() In {
			x := inReq("r3")
			x.Required = []version.RequiredCapability{{Name: "refund", MinSemantic: 1}}
			x.IdemKey = "b3"
			return x
		}(), version.RejectCapabilityMissing},
		{"semantic_conflict", func() In {
			x := inReq("r4")
			x.Required = []version.RequiredCapability{{Name: "pay", MinSemantic: 3}}
			x.IdemKey = "b4"
			return x
		}(), version.RejectSemanticConflict},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := gw.Handle(context.Background(), c.in)
			if out.Status != "rejected" || out.Err == nil || out.Err.Kind != gerrors.KindVersionRejected {
				t.Fatalf("want version rejected, got status=%s err=%v", out.Status, out.Err)
			}
			if out.Record.RejectReason != string(c.reason) {
				t.Fatalf("want reason %s got %s", c.reason, out.Record.RejectReason)
			}
			if a.Invokes() != 0 {
				t.Fatalf("rejected request must never be forwarded, invokes=%d", a.Invokes())
			}
			t.Logf("request=%s version=%q verdict=%s reject=%s -> rejected (not forwarded)",
				c.in.RequestID, c.in.APIVersion, out.Record.VersionVerdict, out.Record.RejectReason)
		})
	}
}

// 场景2：上游抖动（超时后恢复），幂等操作在兼容候选集合内成功重试。
func TestRetryWithinCompatibleSet(t *testing.T) {
	flaky := &scriptedAdapter{id: "u1", v: version.MustParse("1.1.0"), caps: payCap(1),
		script: []*upstream.Error{{Kind: upstream.ErrTimeout, Reason: "deadline"}}}
	good := &scriptedAdapter{id: "u2", v: version.MustParse("1.2.0"), caps: payCap(2)}
	gw := New(testCfg(1000, 1000), newRegistry(flaky, good), tLogger{t})

	out := gw.Handle(context.Background(), inReq("retry-1"))
	if out.Status != "ok" || out.UpstreamID != "u2" {
		t.Fatalf("want success via second candidate u2, got %s/%s err=%v", out.Status, out.UpstreamID, out.Err)
	}
	if out.Attempts != 2 {
		t.Fatalf("want 2 attempts, got %d", out.Attempts)
	}
	if len(out.Record.Attempts) != 2 || !out.Record.Attempts[0].Retry {
		t.Fatalf("first failure must be marked for retry: %+v", out.Record.Attempts)
	}
	t.Logf("request=retry-1 retry stayed within candidates %v; attempts=%d",
		out.Record.Candidates, out.Attempts)
}

// 场景3：重试不得跨越不兼容版本：只有 2.0 上游时，1.2 调用方必须被拒绝，
// 即使重试也绝不转发给 2.0。
func TestRetryNeverCrossesIncompatibleVersion(t *testing.T) {
	v2 := &scriptedAdapter{id: "u-v2", v: version.MustParse("2.0.0"), caps: payCap(1),
		script: []*upstream.Error{{Kind: upstream.ErrTimeout, Reason: "x"}}}
	gw := New(testCfg(1000, 1000), newRegistry(v2), tLogger{t})

	out := gw.Handle(context.Background(), inReq("cross-1"))
	if out.Status != "rejected" || out.Record.RejectReason != string(version.RejectSemanticConflict) {
		t.Fatalf("must refuse instead of forwarding to 2.0: %+v", out)
	}
	if v2.Invokes() != 0 {
		t.Fatalf("incompatible upstream must not be invoked, got %d", v2.Invokes())
	}
}

// 场景4：非幂等操作绝不重试，业务操作不被重复执行。
func TestNonIdempotentNoRetry(t *testing.T) {
	a := &scriptedAdapter{id: "u1", v: version.MustParse("1.2.0"), caps: payCap(2),
		script: []*upstream.Error{{Kind: upstream.ErrTimeout, Reason: "slow"}}}
	gw := New(testCfg(1000, 1000), newRegistry(a), tLogger{t})
	in := inReq("nonidem-1")
	in.Idempotent = false
	in.IdemKey = ""

	out := gw.Handle(context.Background(), in)
	if out.Status != "degraded" {
		t.Fatalf("single failure then fallback expected, got %s err=%v", out.Status, out.Err)
	}
	if a.Invokes() != 1 {
		t.Fatalf("non-idempotent op must execute exactly once, got %d", a.Invokes())
	}
	if out.Record.Attempts[0].Retry || out.Record.Attempts[0].RetryReason != "non_idempotent_operation_not_retried" {
		t.Fatalf("retry must be denied with explainable reason: %+v", out.Record.Attempts[0])
	}
	t.Logf("request=nonidem-1 attempts=%d retry_decision=%s degraded=%q",
		len(out.Record.Attempts), out.Record.Attempts[0].RetryReason, string(out.Body))
}

// 场景5：持续过载 + 熔断打开 -> 降级兜底；熔断状态可观测且错误类别可区分。
func TestCircuitOpenAndDegradation(t *testing.T) {
	a := &alwaysFailAdapter{id: "u1", v: version.MustParse("1.2.0"), caps: payCap(2), kind: upstream.ErrOverload}
	gw := New(testCfg(1000, 1000), newRegistry(a), tLogger{t})

	var last *Out
	for i := 0; i < 4; i++ {
		in := inReq(fmt.Sprintf("load-%d", i))
		in.IdemKey = fmt.Sprintf("biz-load-%d", i)
		last = gw.Handle(context.Background(), in)
	}
	if last.Status != "degraded" || string(last.Body) != "fallback-pay" {
		t.Fatalf("want fallback response, got %s body=%q err=%v", last.Status, last.Body, last.Err)
	}
	if last.Err == nil || last.Err.Kind != gerrors.KindDegraded {
		t.Fatalf("degradation must carry distinguishable kind, got %v", last.Err)
	}
	states := gw.CircuitSnapshot()
	if states["u1"] != circuit.Open {
		t.Fatalf("breaker must be observable as open, got %v", states)
	}
	t.Logf("circuit states=%v final outcome=%s reason=%q", states, last.Record.Outcome, last.Err.Reason)
}

// 场景6：并发同一业务键：业务只执行一次，其余请求共享同一结果。
func TestConcurrentIdempotentBusinessExecutedOnce(t *testing.T) {
	a := &scriptedAdapter{id: "u1", v: version.MustParse("1.2.0"), caps: payCap(2)}
	gw := New(testCfg(10000, 10000), newRegistry(a), tLogger{t})

	const n = 50
	var wg sync.WaitGroup
	oks := int64(0)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			in := inReq("idem-concurrent") // 相同 RequestID + IdemKey
			out := gw.Handle(context.Background(), in)
			if out.Status == "ok" {
				atomic.AddInt64(&oks, 1)
			}
		}()
	}
	wg.Wait()
	if oks != n {
		t.Fatalf("all %d duplicates must return ok, got %d", n, oks)
	}
	if a.Invokes() != 1 {
		t.Fatalf("business op must execute exactly once under concurrency, got %d", a.Invokes())
	}
	t.Logf("request=idem-concurrent concurrent=%d upstream_invocations=%d", n, a.Invokes())
}

// 场景7：并发配额耗尽：路由突发 20 时，200 并发恰好放行 20。
func TestConcurrentQuotaExhaustion(t *testing.T) {
	a := &scriptedAdapter{id: "u1", v: version.MustParse("1.2.0"), caps: payCap(2)}
	gw := New(testCfg(1000, 20), newRegistry(a), tLogger{t})

	const n = 200
	var wg sync.WaitGroup
	var allowed, limited int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			in := inReq(fmt.Sprintf("q-%d", i))
			in.IdemKey = fmt.Sprintf("biz-q-%d", i)
			out := gw.Handle(context.Background(), in)
			if out.Status == "ok" {
				atomic.AddInt64(&allowed, 1)
			} else if out.Err != nil && out.Err.Kind == gerrors.KindRateLimited {
				atomic.AddInt64(&limited, 1)
			}
		}(i)
	}
	wg.Wait()
	if allowed != 20 {
		t.Fatalf("exactly 20 must be admitted, got %d", allowed)
	}
	if limited != n-20 {
		t.Fatalf("remaining must be rate-limited, got %d", limited)
	}
	t.Logf("quota decision: allowed=%d rate_limited=%d", allowed, limited)
}

// 场景8：连接失败与超时同样可区分、可重试，并可进入降级。
func TestErrorKindsDistinguishable(t *testing.T) {
	cases := []struct {
		name string
		kind upstream.ErrorKind
		want gerrors.Kind
	}{
		{"timeout", upstream.ErrTimeout, gerrors.KindUpstreamTimeout},
		{"connection", upstream.ErrConn, gerrors.KindUpstreamConn},
		{"overload", upstream.ErrOverload, gerrors.KindUpstreamOverload},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a := &scriptedAdapter{id: "u-" + c.name, v: version.MustParse("1.2.0"), caps: payCap(2),
				script: []*upstream.Error{{Kind: c.kind, Reason: c.name}}}
			gw := New(testCfg(1000, 1000), newRegistry(a), tLogger{t})
			in := inReq("err-" + c.name)
			in.Idempotent = false
			in.IdemKey = ""
			out := gw.Handle(context.Background(), in)
			if out.Status != "degraded" || out.Err == nil {
				t.Fatalf("want degraded, got %s", out.Status)
			}
			// 降级包装的原因中必须保留原始故障类别。
			if got := out.Record.Attempts[0].ErrKind; got != string(c.kind) {
				t.Fatalf("want upstream err kind %s got %s", c.kind, got)
			}
			t.Logf("request=err-%s upstream_kind=%s degraded=true", c.name, c.kind)
		})
	}
}

// 场景9：熔断半开恢复：冷却后探测成功，后续请求恢复正常（单探测、无惊群）。
func TestCircuitRecovery(t *testing.T) {
	a := &scriptedAdapter{id: "u1", v: version.MustParse("1.2.0"), caps: payCap(2),
		script: []*upstream.Error{
			{Kind: upstream.ErrOverload, Reason: "b1"},
			{Kind: upstream.ErrOverload, Reason: "b2"},
		}}
	gw := New(testCfg(1000, 1000), newRegistry(a), tLogger{t})

	failIn := func(id string) {
		in := inReq(id)
		in.IdemKey = "biz-" + id
		gw.Handle(context.Background(), in)
	}
	failIn("f1")
	failIn("f2")
	if s := gw.CircuitSnapshot()["u1"]; s != circuit.Open {
		t.Fatalf("want open, got %s", s)
	}
	time.Sleep(60 * time.Millisecond)

	// 冷却后第一次请求是半开探测：脚本已用尽 -> 成功 -> 熔断关闭。
	probe := inReq("probe")
	out := gw.Handle(context.Background(), probe)
	if out.Status != "ok" {
		t.Fatalf("probe should succeed and close breaker, got %s err=%v", out.Status, out.Err)
	}
	if s := gw.CircuitSnapshot()["u1"]; s != circuit.Closed {
		t.Fatalf("want closed after successful probe, got %s", s)
	}
	t.Logf("request=probe half_open_probe succeeded; circuit=closed")
}

// 场景10：版本兼容但唯一上游熔断打开：不是版本拒绝，而是可区分的降级。
func TestCompatibleButCircuitOpenDegrades(t *testing.T) {
	a := &alwaysFailAdapter{id: "u1", v: version.MustParse("1.2.0"), caps: payCap(2), kind: upstream.ErrOverload}
	gw := New(testCfg(1000, 1000), newRegistry(a), tLogger{t})

	// 前两个幂等请求把失败计数推到阈值（FailureThreshold=2）。
	for i := 0; i < 2; i++ {
		in := inReq(fmt.Sprintf("warm-%d", i))
		in.IdemKey = fmt.Sprintf("biz-warm-%d", i)
		gw.Handle(context.Background(), in)
	}
	if s := gw.CircuitSnapshot()["u1"]; s != circuit.Open {
		t.Fatalf("precondition: breaker open, got %s", s)
	}
	out := gw.Handle(context.Background(), func() In {
		x := inReq("open-1")
		x.IdemKey = "biz-open-1"
		return x
	}())
	if out.Status != "degraded" || out.Err == nil || out.Err.Kind != gerrors.KindDegraded {
		t.Fatalf("want degraded (not version rejection), got %s err=%v", out.Status, out.Err)
	}
	if out.Record.RejectReason != "" || out.Record.VersionVerdict == "reject" {
		t.Fatalf("version negotiation must have passed: verdict=%s reason=%s",
			out.Record.VersionVerdict, out.Record.RejectReason)
	}
	t.Logf("request=open-1 version=%s degraded via circuit-open; attempts=%d",
		out.Record.VersionVerdict, len(out.Record.Attempts))
}
