// Command demo 在单进程内演示版本化 API 网关的版本协商、分层限流、
// 熔断降级、重试不重复执行业务、幂等重放与回调投递，全部本地完成，
// 不依赖任何外部服务。
//
// 运行：go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/xingjianluo/versioned-api-gateway-go/breaker"
	"github.com/xingjianluo/versioned-api-gateway-go/callback"
	"github.com/xingjianluo/versioned-api-gateway-go/callback/receiver"
	"github.com/xingjianluo/versioned-api-gateway-go/gateway"
	"github.com/xingjianluo/versioned-api-gateway-go/graysplit"
	"github.com/xingjianluo/versioned-api-gateway-go/internal/logging"
	"github.com/xingjianluo/versioned-api-gateway-go/ratelimit"
	"github.com/xingjianluo/versioned-api-gateway-go/upstream"
	"github.com/xingjianluo/versioned-api-gateway-go/versionx"
)

func main() {
	log := logging.NewStdLogger(os.Stdout)
	ctx := context.Background()

	// 两个上游：v1.4（orders+refunds）与 v2.0（orders+refunds）。
	ordersV1 := &upstream.ScriptedAdapter{
		StaticDesc: upstream.StaticDesc{ID: "orders-v1", Version: "1.4.0",
			Caps: []string{"orders", "refunds"}},
		Behavior: &upstream.ScriptedBehavior{},
	}
	ordersV2 := &upstream.ScriptedAdapter{
		StaticDesc: upstream.StaticDesc{ID: "orders-v2", Version: "2.0.0",
			Caps: []string{"orders", "refunds"}},
		Behavior: &upstream.ScriptedBehavior{},
	}

	cfg := gateway.Config{
		RouteRules: map[string]versionx.RouteRule{
			"/pay": {MinSupported: v("1.0.0"), MaxSupported: v("1.99.0"),
				RequiredCaps: []string{"orders", "refunds"}},
		},
		Upstreams: []upstream.Adapter{ordersV1, ordersV2},
		CallerLimit: map[string]ratelimit.LayerConfig{
			"demo-caller": {Rate: 20, Burst: 20}, // 常规演示调用方
			"quota-burst": {Rate: 0, Burst: 3},   // 限流段专用：突发 3，不补充
		},
		RouteLimit: map[string]ratelimit.LayerConfig{
			"/pay": {Rate: 100, Burst: 1000},
		},
		Breaker: breaker.Config{FailureThreshold: 3, OpenCoolDown: 200 * time.Millisecond},
		Retry:   gateway.RetryPolicy{MaxAttempts: 3, AttemptTimeout: 500 * time.Millisecond},
	}

	// 回调链路：临时目录持久化 + 幂等接收方（前 1 次无法确认，模拟重放）。
	tmp, _ := os.MkdirTemp("", "gw-callback-demo-")
	defer os.RemoveAll(tmp)
	store, err := callback.NewFileStore(filepath.Join(tmp, "events"))
	must(err)
	recv := receiver.New()
	deliverer := &flakyDeliverer{recv: recv, failTimes: 1}
	dispatcher := callback.NewDispatcher(store, deliverer,
		callback.WithMaxAttempts(4), callback.WithBaseBackoff(5*time.Millisecond))

	gw, err := gateway.New(cfg, log, gateway.WithCallbackDispatcher(dispatcher))
	must(err)

	section("1. 正常调用：版本协商选中 orders-v1")
	printOutcome(gw.Serve(ctx, "demo-caller", "req-001", "1.2.0",
		"/pay", "biz-001", "charge-10"))

	section("2. 旧版 / 越界 / 未知版本 / v2 不兼容：必须拒绝且原因可区分")
	printOutcome(gw.Serve(ctx, "demo-caller", "req-002", "0.9.0", "/pay", "biz-002", "x"))
	printOutcome(gw.Serve(ctx, "demo-caller", "req-003", "1.99.9", "/pay", "biz-003", "x"))
	printOutcome(gw.Serve(ctx, "demo-caller", "req-004", "banana", "/pay", "biz-004", "x"))
	// v2 客户端不存在 v1 兼容窗口内的路径 -> 窗口拒绝；不会被静默转到 v2。
	printOutcome(gw.Serve(ctx, "demo-caller", "req-005", "2.0.0", "/pay", "biz-005", "x"))

	section("3. 并发分层限流：突发配额=3，10 并发只放行 3")
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed, rejected := 0, 0
	start := make(chan struct{})
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			o := gw.Serve(ctx, "quota-burst",
				fmt.Sprintf("req-q-%02d", i), "1.1.0", "/pay",
				fmt.Sprintf("biz-q-%02d", i), "x")
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
	fmt.Printf(">>> quota result: allowed=%d rejected=%d (want 3/7)\n", allowed, rejected)

	section("4. 上游抖动：前 2 次 timeout/connection，第 3 次成功；重试不跨版本")
	jittery := &upstream.ScriptedAdapter{
		StaticDesc: upstream.StaticDesc{ID: "jittery-v1", Version: "1.4.0",
			Caps: []string{"orders", "refunds"}},
		Behavior: &upstream.ScriptedBehavior{FailNextKinds: []upstream.ErrorKind{
			upstream.KindTimeout, upstream.KindConnection}},
	}
	cfg2 := cfg
	cfg2.Upstreams = []upstream.Adapter{jittery}
	gw2, err := gateway.New(cfg2, log)
	must(err)
	o := gw2.Serve(ctx, "demo-caller", "req-jitter", "1.2.0", "/pay", "biz-jitter", "charge")
	fmt.Printf(">>> retries=%d invocations=%d status=%s\n",
		o.Retries, jittery.Behavior.Count(), o.Status)

	section("5. 幂等重放：同 biz 键 3 次请求，业务只执行 1 次")
	beforeReplay := ordersV1.Behavior.Count()
	for i := 0; i < 3; i++ {
		gw.Serve(ctx, "demo-caller", fmt.Sprintf("req-rep-%d", i),
			"1.2.0", "/pay", "biz-idempotent-1", "charge")
	}
	fmt.Printf(">>> orders-v1 invocations during replay=%d (3 requests, business executes once then cache replay)\n",
		ordersV1.Behavior.Count()-beforeReplay)

	section("6. 回调投递：首次无法确认 -> 退避重放 -> 确认；接收方只处理一次")
	must(dispatcher.Emit(ctx, callback.Event{
		EventID: "evt-1", Target: "billing-hook",
		RequestID: "req-001", IdemKey: "biz-001", Payload: `{"paid":10}`,
	}))
	res, err := dispatcher.DeliverPending(ctx)
	must(err)
	for _, r := range res {
		fmt.Printf(">>> callback %s final=%s attempts=%d receiver_handled=%d\n",
			r.EventID, r.Status, r.Attempts, recv.HandleCount())
	}
	ev, _ := dispatcher.StatusOf(ctx, "evt-1")
	fmt.Printf(">>> correlation preserved: request_id=%s idem_key=%s\n",
		ev.RequestID, ev.IdemKey)

	section("7. 熔断：连续过载后打开 -> 快速失败 -> 冷却后单探测恢复")
	demoBreaker(ctx, log)

	section("8. 灰度分流：按比例投放 -> 粘性不漂移 -> 故障回退 -> 整体回滚")
	demoGraySplit(ctx, log)

	fmt.Println("\nDEMO OK")
}

// demoGraySplit 演示灰度分流：两个同 major 兼容上游按比例投放，
// 业务键粘性固定，首选故障时回退，比例可整体回滚。
func demoGraySplit(ctx context.Context, log logging.Logger) {
	grayA := &upstream.ScriptedAdapter{
		StaticDesc: upstream.StaticDesc{ID: "gray-a", Version: "1.4.0",
			Caps: []string{"orders", "refunds"}},
		Behavior: &upstream.ScriptedBehavior{},
	}
	grayB := &upstream.ScriptedAdapter{
		StaticDesc: upstream.StaticDesc{ID: "gray-b", Version: "1.3.0",
			Caps: []string{"orders", "refunds"}},
		Behavior: &upstream.ScriptedBehavior{},
	}
	cfg := gateway.Config{
		RouteRules: map[string]versionx.RouteRule{
			"/pay": {MinSupported: v("1.0.0"), MaxSupported: v("1.99.0"),
				RequiredCaps: []string{"orders", "refunds"}},
		},
		Upstreams:   []upstream.Adapter{grayA, grayB},
		CallerLimit: map[string]ratelimit.LayerConfig{"gray": {Rate: 0, Burst: 1000}},
		RouteLimit:  map[string]ratelimit.LayerConfig{"/pay": {Rate: 0, Burst: 1000}},
		Breaker:     breaker.Config{FailureThreshold: 2, OpenCoolDown: time.Second},
		Retry:       gateway.RetryPolicy{MaxAttempts: 2, AttemptTimeout: 200 * time.Millisecond},
		// 初始计划：gray-a 拿 8 成，gray-b 拿 2 成。
		SplitPlans: map[string][]graysplit.Entry{"/pay": {
			{UpstreamID: "gray-a", Weight: 8}, {UpstreamID: "gray-b", Weight: 2},
		}},
	}
	gw, err := gateway.New(cfg, log)
	must(err)

	// 非法比例被明确拒绝，旧计划不变。
	if err := gw.SetSplitPlan("/pay", []graysplit.Entry{
		{UpstreamID: "gray-a", Weight: -1}}); err != nil {
		fmt.Printf(">>> invalid plan rejected as expected: %v\n", err)
	}

	// 按比例投放 20 个不同业务键。
	hit := map[string]int{}
	for i := 0; i < 20; i++ {
		o := gw.Serve(ctx, "gray", fmt.Sprintf("gray-%d", i), "1.2.0", "/pay",
			fmt.Sprintf("gray-biz-%d", i), "x")
		hit[o.SelectedUpstream]++
	}
	fmt.Printf(">>> weighted split over 20 keys (plan 8:2): %v\n", hit)

	// 比例翻转为 2:8：已固定的业务键不漂移。
	must(gw.SetSplitPlan("/pay", []graysplit.Entry{
		{UpstreamID: "gray-a", Weight: 2}, {UpstreamID: "gray-b", Weight: 8}}))
	o := gw.Serve(ctx, "gray", "gray-sticky", "1.2.0", "/pay", "gray-biz-0", "x")
	fmt.Printf(">>> sticky after ratio flip: upstream=%s rule=%s (want gray-a/sticky)\n",
		o.SelectedUpstream, o.SplitRule)

	// 把比例全量压到 gray-a（新键必选 gray-a），随后 gray-a 持续超时：
	// 首选不可用时回退到健康的 gray-b，不丢请求。
	must(gw.SetSplitPlan("/pay", []graysplit.Entry{{UpstreamID: "gray-a", Weight: 10}}))
	grayA.Behavior = &upstream.ScriptedBehavior{FailNextKinds: []upstream.ErrorKind{
		upstream.KindTimeout, upstream.KindTimeout, upstream.KindTimeout,
		upstream.KindTimeout, upstream.KindTimeout, upstream.KindTimeout,
	}}
	o = gw.Serve(ctx, "gray", "gray-fb", "1.2.0", "/pay", "gray-biz-fb", "x")
	fmt.Printf(">>> fallback when preferred unhealthy: upstream=%s fallback=%v rule=%s\n",
		o.SelectedUpstream, o.Fallback, o.SplitRule)

	// 整体回滚到上一份计划（2:8）。
	must(gw.RollbackSplitPlan("/pay"))
	fmt.Printf(">>> rolled back to previous plan (revision bump, weights 2:8)\n")
}

func demoBreaker(ctx context.Context, log logging.Logger) {
	flaky := &upstream.ScriptedAdapter{
		StaticDesc: upstream.StaticDesc{ID: "overloaded-v1", Version: "1.4.0",
			Caps: []string{"orders", "refunds"}},
		Behavior: &upstream.ScriptedBehavior{},
	}
	// 前 4 次调用过载（3 次预热触发熔断 + 半开探测恢复前的一次失败演示），
	// 之后全部成功。本节探测使用第二次冷却窗口，确保探测成功并关闸。
	for i := 0; i < 4; i++ {
		flaky.Behavior.FailNextKinds = append(flaky.Behavior.FailNextKinds, upstream.KindOverload)
	}
	cfg := gateway.Config{
		RouteRules: map[string]versionx.RouteRule{
			"/pay": {MinSupported: v("1.0.0"), MaxSupported: v("1.99.0"),
				RequiredCaps: []string{"orders", "refunds"}},
		},
		Upstreams:   []upstream.Adapter{flaky},
		CallerLimit: map[string]ratelimit.LayerConfig{"c": {Rate: 1000, Burst: 1000}},
		RouteLimit:  map[string]ratelimit.LayerConfig{"/pay": {Rate: 1000, Burst: 1000}},
		Breaker:     breaker.Config{FailureThreshold: 3, OpenCoolDown: 150 * time.Millisecond},
		Retry:       gateway.RetryPolicy{MaxAttempts: 1, AttemptTimeout: time.Second},
	}
	gw, err := gateway.New(cfg, log)
	must(err)

	// 3 个不同请求即可触发熔断（每次 1 次失败尝试）。
	for i := 0; i < 3; i++ {
		o := gw.Serve(ctx, "c", fmt.Sprintf("req-b-%d", i), "1.2.0",
			"/pay", fmt.Sprintf("biz-b-%d", i), "x")
		fmt.Printf(">>> warmup %d: status=%s reason=%s invocations=%d\n",
			i, o.Status, o.Reason, flaky.Behavior.Count())
	}
	// 熔断打开：不触达上游。
	before := flaky.Behavior.Count()
	o := gw.Serve(ctx, "c", "req-open", "1.2.0", "/pay", "biz-open", "x")
	fmt.Printf(">>> open: reason=%s upstream_delta=%d (expect 0)\n",
		o.Reason, flaky.Behavior.Count()-before)

	// 第一次冷却窗口：探测仍过载（脚本第 4 次失败），熔断重开。
	time.Sleep(200 * time.Millisecond)
	o = gw.Serve(ctx, "c", "req-probe-fail", "1.2.0", "/pay", "biz-probe-fail", "x")
	fmt.Printf(">>> first probe still overloaded: status=%s reason=%s invocations=%d\n",
		o.Status, o.Reason, flaky.Behavior.Count())

	// 第二次冷却窗口：上游已恢复，探测成功 -> 关闸。
	time.Sleep(200 * time.Millisecond)
	o = gw.Serve(ctx, "c", "req-probe", "1.2.0", "/pay", "biz-probe", "x")
	fmt.Printf(">>> probe after recovery: status=%s reason=%s invocations=%d\n",
		o.Status, o.Reason, flaky.Behavior.Count())
	if o.Status != "ok" {
		panic("expected successful probe closing the circuit")
	}
}

func v(s string) versionx.Version {
	ver, err := versionx.Parse(s)
	if err != nil {
		panic(err)
	}
	return ver
}

func section(title string) {
	fmt.Printf("\n===== %s =====\n", title)
}

func printOutcome(o gateway.Outcome) {
	fmt.Printf(">>> request=%s allowed=%v status=%s reason=%s upstream=%s retries=%d note=%s\n",
		o.RequestID, o.Allowed, o.Status, o.Reason, o.SelectedUpstream, o.Retries, o.VersionNote)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// flakyDeliverer 前 failTimes 次返回“无法确认”，之后确认。
type flakyDeliverer struct {
	recv      *receiver.Receiver
	failTimes int
	mu        sync.Mutex
}

func (f *flakyDeliverer) Deliver(ctx context.Context, ev callback.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failTimes > 0 {
		f.failTimes--
		return fmt.Errorf("unconfirmed: simulated connection reset")
	}
	_, err := f.recv.Receive(ctx, ev.EventID, ev.Payload)
	return err
}
