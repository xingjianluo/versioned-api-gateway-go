// Command demo 在本地（不依赖外部服务）演示版本协商、分层限流、
// 熔断降级、幂等重试与回调投递的完整判定过程。
//
// 运行：go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xingjianluo/versioned-api-gateway-go/pkg/callback"
	"github.com/xingjianluo/versioned-api-gateway-go/pkg/circuit"
	"github.com/xingjianluo/versioned-api-gateway-go/pkg/gateway"
	"github.com/xingjianluo/versioned-api-gateway-go/pkg/ratelimit"
	"github.com/xingjianluo/versioned-api-gateway-go/pkg/retry"
	"github.com/xingjianluo/versioned-api-gateway-go/pkg/upstream"
	"github.com/xingjianluo/versioned-api-gateway-go/pkg/version"
)

type demoAdapter struct {
	id   string
	v    version.Version
	caps []version.Capability
	fail func(n int64) *upstream.Error
	mu   sync.Mutex
	n    int64
}

func (a *demoAdapter) ID() string                         { return a.id }
func (a *demoAdapter) APIVersion() version.Version        { return a.v }
func (a *demoAdapter) Capabilities() []version.Capability { return a.caps }
func (a *demoAdapter) Invoke(ctx context.Context, req upstream.Request) (*upstream.Response, error) {
	n := atomic.AddInt64(&a.n, 1)
	if a.fail != nil {
		if e := a.fail(n); e != nil {
			return nil, e
		}
	}
	fmt.Printf("    -> upstream %s invoked (call #%d, idempotent=%v)\n", a.id, n, req.Idempotent)
	return &upstream.Response{UpstreamID: a.id, Version: a.v, Body: []byte("result-from-" + a.id)}, nil
}

type httpStub struct{ failFirst int64 }

func (s *httpStub) Deliver(ctx context.Context, n callback.Notification) error {
	c := atomic.AddInt64(&s.failFirst, 1)
	if c <= 2 {
		fmt.Printf("    -> callback %s attempt %d NOT acknowledged (simulated)\n", n.Key, n.Attempts)
		return fmt.Errorf("simulated connection reset")
	}
	fmt.Printf("    -> callback %s attempt %d ACKed (correlation=%s)\n", n.Key, n.Attempts, n.Correlation)
	return nil
}

func main() {
	logger := log.New(os.Stdout, "", 0)
	ctx := context.Background()

	reg := upstream.NewRegistry()
	reg.Register(&demoAdapter{id: "pay-110", v: version.MustParse("1.1.0"),
		caps: []version.Capability{{Name: "pay", Semantic: 1}}})
	reg.Register(&demoAdapter{id: "pay-120", v: version.MustParse("1.2.0"),
		caps: []version.Capability{{Name: "pay", Semantic: 2}}})

	cfg := gateway.Config{
		Policy: version.Policy{
			MinSupported: version.MustParse("1.1.0"),
			Known: []version.Version{
				version.MustParse("1.1.0"), version.MustParse("1.2.0"), version.MustParse("2.0.0"),
			},
		},
		CallerBucket: ratelimit.BucketConfig{Rate: 20, Burst: 4},
		RouteBucket:  ratelimit.BucketConfig{Rate: 20, Burst: 3},
		Circuit:      circuit.Config{FailureThreshold: 3, OpenCoolDown: 300 * time.Millisecond, SuccessThreshold: 1},
		Retry:        retry.Policy{MaxAttempts: 3, BaseDelay: 5 * time.Millisecond, MaxDelay: 50 * time.Millisecond},
		Degrade:      map[string][]byte{"pay": []byte("stale-cached-payment-options")},
	}
	gw := gateway.New(cfg, reg, logger)

	pay := func(reqID, apiVer string, idem bool) *gateway.Out {
		in := gateway.In{
			RequestID: reqID, CallerID: "demo-caller", Route: "pay",
			APIVersion: apiVer,
			Required:   []version.RequiredCapability{{Name: "pay", MinSemantic: 1}},
			Idempotent: idem, IdemKey: "biz-" + reqID,
		}
		return gw.Handle(ctx, in)
	}

	section := func(title string) { fmt.Printf("\n==== %s ====\n", title) }

	section("1. 正常调用（版本协商通过，路由到兼容上游）")
	printOut(pay("req-001", "1.2.0", true))

	section("2. 未知版本 -> 拒绝，绝不转发")
	printOut(pay("req-002", "7.0.0", true))

	section("3. 旧版本调用 -> 拒绝")
	printOut(pay("req-003", "1.0.0", true))

	section("4. 分层限流：调用方突发4 / 路由突发3，连发6个不同业务")
	for i := 0; i < 6; i++ {
		printOut(pay(fmt.Sprintf("req-q%02d", i), "1.2.0", true))
	}

	section("5. 上游抖动后在兼容集合内重试成功")
	flaky := &demoAdapter{id: "pay-120-flaky", v: version.MustParse("1.2.0"),
		caps: []version.Capability{{Name: "pay", Semantic: 2}},
		fail: func(n int64) *upstream.Error {
			if n == 1 {
				return &upstream.Error{Kind: upstream.ErrTimeout, Reason: "simulated timeout"}
			}
			return nil
		}}
	reg2 := upstream.NewRegistry()
	reg2.Register(flaky)
	gw2 := gateway.New(cfg, reg2, logger)
	out := gw2.Handle(ctx, gateway.In{
		RequestID: "req-010", CallerID: "demo-caller", Route: "pay",
		APIVersion: "1.2.0", Required: []version.RequiredCapability{{Name: "pay", MinSemantic: 1}},
		Idempotent: true, IdemKey: "biz-010",
	})
	printOut(out)

	section("6. 持续过载 -> 熔断打开 -> 降级兜底；随后冷却半开探测恢复")
	bad := &demoAdapter{id: "pay-bad", v: version.MustParse("1.2.0"),
		caps: []version.Capability{{Name: "pay", Semantic: 2}},
		fail: func(n int64) *upstream.Error {
			if n <= 3 {
				return &upstream.Error{Kind: upstream.ErrOverload, Reason: "simulated overload"}
			}
			return nil // 探测时恢复
		}}
	reg3 := upstream.NewRegistry()
	reg3.Register(bad)
	gw3 := gateway.New(cfg, reg3, logger)
	for i := 0; i < 5; i++ {
		printOut(gw3.Handle(ctx, gateway.In{
			RequestID: fmt.Sprintf("req-cb%d", i), CallerID: "demo-caller", Route: "pay",
			APIVersion: "1.2.0", Required: []version.RequiredCapability{{Name: "pay", MinSemantic: 1}},
			Idempotent: true, IdemKey: fmt.Sprintf("biz-cb%d", i),
		}))
	}
	fmt.Printf("    circuit snapshot: %v\n", gw3.CircuitSnapshot())
	fmt.Println("    ... waiting cooldown 350ms ...")
	time.Sleep(350 * time.Millisecond)
	printOut(gw3.Handle(ctx, gateway.In{
		RequestID: "req-probe", CallerID: "demo-caller", Route: "pay",
		APIVersion: "1.2.0", Required: []version.RequiredCapability{{Name: "pay", MinSemantic: 1}},
		Idempotent: true, IdemKey: "biz-probe",
	}))
	fmt.Printf("    circuit snapshot: %v\n", gw3.CircuitSnapshot())

	section("7. 回调投递：前两次未确认（不伪装成功），有界重放后确认；重复投递只发一次")
	dir := filepath.Join(os.TempDir(), "vgw-demo-outbox")
	_ = os.RemoveAll(dir)
	ob, err := callback.NewFileOutbox(dir)
	if err != nil {
		log.Fatal(err)
	}
	mgr := callback.NewManager(ob, &httpStub{}, callback.RetryPolicy{
		MaxAttempts: 5, BaseDelay: 5 * time.Millisecond, MaxDelay: 50 * time.Millisecond,
	})
	n := callback.Notification{Key: "order-42", Seq: 1, Correlation: "req-010/order-42",
		URL: "http://localhost/local-callback", Payload: []byte(`{"event":"paid"}`)}
	if _, isNew, err := mgr.Enqueue(n); err != nil {
		log.Fatal(err)
	} else {
		fmt.Printf("    enqueue isNew=%v\n", isNew)
	}
	ok, derr := mgr.DeliverOnce(ctx, "order-42")
	fmt.Printf("    DeliverOnce -> acked=%v err=%v\n", ok, derr)
	ok2, _ := mgr.DeliverOnce(ctx, "order-42")
	fmt.Printf("    duplicate DeliverOnce -> acked=%v (no second real send)\n", ok2)
	if rec, exists := ob.Get("order-42"); exists {
		fmt.Printf("    outbox record: status=%s attempts=%d correlation=%s\n",
			rec.Status, rec.Attempts, rec.Correlation)
	}

	section("8. 重启重放：新建 Manager 从磁盘恢复 pending（本例已全部送达）")
	ob2, _ := callback.NewFileOutbox(dir)
	mgr2 := callback.NewManager(ob2, &httpStub{}, callback.RetryPolicy{
		MaxAttempts: 5, BaseDelay: 5 * time.Millisecond, MaxDelay: 50 * time.Millisecond})
	report, _ := mgr2.ReplayPending(ctx, 2)
	fmt.Printf("    replay report: %+v (outbox dir: %s)\n", report, dir)

	fmt.Println("\ndemo complete")
}

func printOut(o *gateway.Out) {
	if o.Err != nil {
		fmt.Printf("    [%s] request=%s kind=%s retryable=%t attempts=%d: %s\n",
			o.Status, o.RequestID, o.Err.Kind, o.Err.Retryable, o.Attempts, o.Err.Reason)
		return
	}
	fmt.Printf("    [%s] request=%s upstream=%s version=%s attempts=%d body=%q\n",
		o.Status, o.RequestID, o.UpstreamID, o.Version, o.Attempts, string(o.Body))
}
