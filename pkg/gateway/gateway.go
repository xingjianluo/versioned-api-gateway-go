// Package gateway 组装版本协商、分层限流、熔断、重试/幂等与降级，
// 每个请求产生一条包含全部决策依据的决策记录。
package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/xingjianluo/versioned-api-gateway-go/pkg/circuit"
	gerrors "github.com/xingjianluo/versioned-api-gateway-go/pkg/errors"
	"github.com/xingjianluo/versioned-api-gateway-go/pkg/idempotency"
	"github.com/xingjianluo/versioned-api-gateway-go/pkg/ratelimit"
	"github.com/xingjianluo/versioned-api-gateway-go/pkg/retry"
	"github.com/xingjianluo/versioned-api-gateway-go/pkg/upstream"
	"github.com/xingjianluo/versioned-api-gateway-go/pkg/version"
)

// Logger 为决策日志接口。
type Logger interface {
	Printf(format string, args ...any)
}

// Config 为网关配置。
type Config struct {
	Policy       version.Policy
	CallerBucket ratelimit.BucketConfig
	RouteBucket  ratelimit.BucketConfig
	Circuit      circuit.Config
	Retry        retry.Policy
	// Degrade 为过载/熔断全开时按 route 返回的兜底响应；缺失则直接失败。
	Degrade map[string][]byte
}

// In 是入站请求。
type In struct {
	RequestID  string
	CallerID   string
	Route      string
	APIVersion string
	Required   []version.RequiredCapability
	Idempotent bool
	IdemKey    string
	Payload    []byte
}

// Out 是出站结果。
type Out struct {
	RequestID  string
	Status     string // ok | rejected | degraded | failed
	Body       []byte
	UpstreamID string
	Version    string
	Attempts   int
	Err        *gerrors.GatewayError
	Record     DecisionRecord
}

// DecisionRecord 记录该请求的全部判定依据（可解释性）。
type DecisionRecord struct {
	RequestID       string
	At              time.Time
	CallerID        string
	Route           string
	CallerVersion   string
	VersionVerdict  string
	RejectReason    string
	QuotaCallerLeft float64
	QuotaRouteLeft  float64
	QuotaDecision   string
	Candidates      []string
	CircuitStates   map[string]string
	Attempts        []AttemptRecord
	Outcome         string
}

// AttemptRecord 记录单次尝试的依据。
type AttemptRecord struct {
	N           int
	UpstreamID  string
	Version     string
	Circuit     string
	Result      string
	ErrKind     string
	Retry       bool
	RetryReason string
}

// Gateway 为网关核心。
type Gateway struct {
	cfg      Config
	registry *upstream.Registry
	limiter  *ratelimit.Limiter
	breaks   *circuit.Manager
	idem     *idempotency.Store
	logger   Logger
	now      func() time.Time
}

// New 组装网关。
func New(cfg Config, registry *upstream.Registry, logger Logger) *Gateway {
	return &Gateway{
		cfg:      cfg,
		registry: registry,
		limiter:  ratelimit.New(cfg.CallerBucket, cfg.RouteBucket),
		breaks:   circuit.NewManager(cfg.Circuit),
		idem:     idempotency.NewStore(),
		logger:   logger,
		now:      time.Now,
	}
}

func (g *Gateway) reject(in In, rec DecisionRecord, kind gerrors.Kind, reason string, retryable bool) *Out {
	rec.Outcome = "rejected:" + string(kind)
	g.log(rec)
	return &Out{
		RequestID: in.RequestID,
		Status:    "rejected",
		Err:       gerrors.New(kind, reason, in.RequestID, retryable),
		Record:    rec,
	}
}

// Handle 处理一次请求：协商 -> 限流 -> 选路 -> 熔断 -> 调用 -> 重试/降级。
func (g *Gateway) Handle(ctx context.Context, in In) *Out {
	rec := DecisionRecord{
		RequestID:     in.RequestID,
		At:            g.now(),
		CallerID:      in.CallerID,
		Route:         in.Route,
		CallerVersion: in.APIVersion,
		CircuitStates: map[string]string{},
	}

	// 1) 版本协商：未知版本/过旧版本直接拒绝，不进入后续流程。
	ver, err := version.Parse(in.APIVersion)
	if err != nil {
		rec.VersionVerdict = "reject"
		rec.RejectReason = string(version.RejectUnknownVersion)
		return g.reject(in, rec, gerrors.KindVersionRejected, err.Error(), false)
	}
	if err := g.cfg.Policy.Accept(ver); err != nil {
		ne := err.(*version.NegotiateError)
		rec.VersionVerdict = "reject"
		rec.RejectReason = string(ne.Reason)
		return g.reject(in, rec, gerrors.KindVersionRejected, ne.Message, false)
	}
	rec.VersionVerdict = "accepted:" + ver.String()

	// 2) 分层限流：调用方桶与路由桶都放行才放行。
	d := g.limiter.Allow(in.CallerID, in.Route)
	rec.QuotaCallerLeft = d.CallerRemaining
	rec.QuotaRouteLeft = d.RouteRemaining
	rec.QuotaDecision = d.Reason
	if !d.Allowed {
		return g.reject(in, rec, gerrors.KindRateLimited,
			fmt.Sprintf("rate limited: %s (caller_left=%.2f route_left=%.2f)",
				d.Reason, d.CallerRemaining, d.RouteRemaining), false)
	}

	// 3) 上游能力匹配：能力缺失/语义不兼容一律拒绝，绝不静默转发。
	cands, err := g.registry.Select(ver, in.Required)
	if err != nil {
		ne := err.(*version.NegotiateError)
		rec.RejectReason = string(ne.Reason)
		return g.reject(in, rec, gerrors.KindVersionRejected, ne.Message, false)
	}
	for _, c := range cands {
		rec.Candidates = append(rec.Candidates, c.Adapter.ID()+"@"+c.Version.String())
	}

	// 4) 幂等去重：同一业务键的并发/重复请求不重复执行业务操作。
	idemKey := in.IdemKey
	if in.Idempotent && idemKey != "" {
		for {
			r, fresh := g.idem.StartOrGet(idemKey)
			if fresh {
				break
			}
			<-r.Done()
			if r.Status == idempotency.Complete {
				rec.Outcome = "idempotent_replay"
				g.log(rec)
				return &Out{
					RequestID:  in.RequestID,
					Status:     "ok",
					Body:       r.Response,
					UpstreamID: "idempotent:" + idemKey,
					Attempts:   0,
					Record:     rec,
				}
			}
			// 前序已明确失败（记录已移除）：循环重新登记，
			// 若被其他并发等待者抢先登记则继续等待，避免重复执行。
		}
	}

	// 5) 在"同一协商版本的兼容候选集合"内有界尝试；
	//    非幂等操作绝不重试，保证业务操作不被重复执行。
	maxAttempts := 1
	if in.Idempotent {
		maxAttempts = g.cfg.Retry.MaxAttempts
	}

	var lastKind upstream.ErrorKind
	var lastErr error
	degradeCandidate := false // 是否出现过载/熔断全开等可降级信号

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		c := cands[(attempt-1)%len(cands)]
		br := g.breaks.Get(c.Adapter.ID())
		bd := br.Allow()
		rec.CircuitStates[c.Adapter.ID()] = string(bd.State)

		ar := AttemptRecord{N: attempt, UpstreamID: c.Adapter.ID(), Version: c.Version.String(), Circuit: string(bd.State)}

		if !bd.Allowed {
			ar.Result = "rejected_by_circuit"
			ar.ErrKind = "circuit_open"
			degradeCandidate = true
			lastKind = upstream.ErrOverload
			lastErr = fmt.Errorf("circuit %s: %s", bd.State, bd.Reason)
			ar.Retry, ar.RetryReason = g.shouldRetry(in, attempt, maxAttempts, upstream.ErrorKind("circuit_open"))
			rec.Attempts = append(rec.Attempts, ar)
			if ar.Retry {
				if werr := g.waitBackoff(ctx, attempt); werr == nil {
					continue
				}
			}
			break
		}

		resp, ierr := c.Adapter.Invoke(ctx, upstream.Request{
			ID:         in.RequestID,
			Route:      in.Route,
			APIVersion: ver,
			Idempotent: in.Idempotent,
			Payload:    in.Payload,
		})
		if ierr != nil {
			ue, ok := ierr.(*upstream.Error)
			if !ok {
				ue = &upstream.Error{Kind: upstream.ErrConn, Reason: ierr.Error()}
			}
			br.OnFailure(string(ue.Kind))
			ar.Result = "upstream_error"
			ar.ErrKind = string(ue.Kind)
			// 超时、连接失败、过载均属于可降级故障（错误种类仍分别保留）。
			if ue.Kind.Retryable() {
				degradeCandidate = true
			}
			lastKind = ue.Kind
			lastErr = ue
			ar.Retry, ar.RetryReason = g.shouldRetry(in, attempt, maxAttempts, ue.Kind)
			rec.Attempts = append(rec.Attempts, ar)
			if ar.Retry {
				if werr := g.waitBackoff(ctx, attempt); werr == nil {
					continue
				}
			}
			break
		}

		br.OnSuccess()
		ar.Result = "ok"
		rec.Attempts = append(rec.Attempts, ar)
		if in.Idempotent && idemKey != "" {
			g.idem.Complete(idemKey, resp.Body)
		}
		rec.Outcome = "ok"
		g.log(rec)
		status := "ok"
		if resp.Degraded {
			status = "degraded"
			rec.Outcome = "ok_degraded_by_upstream"
		}
		return &Out{
			RequestID:  in.RequestID,
			Status:     status,
			Body:       resp.Body,
			UpstreamID: c.Adapter.ID(),
			Version:    c.Version.String(),
			Attempts:   attempt,
			Record:     rec,
		}
	}

	// 6) 全部尝试失败：记录幂等失败，然后进入可区分的降级或直接失败。
	if in.Idempotent && idemKey != "" {
		g.idem.Fail(idemKey, string(lastKind))
	}
	if body, ok := g.cfg.Degrade[in.Route]; ok && degradeCandidate {
		rec.Outcome = "degraded"
		g.log(rec)
		return &Out{
			RequestID: in.RequestID,
			Status:    "degraded",
			Body:      body,
			Attempts:  len(rec.Attempts),
			Err:       gerrors.New(gerrors.KindDegraded, "served from fallback: "+safeErr(lastErr), in.RequestID, false),
			Record:    rec,
		}
	}

	kind, retryable := mapFailure(lastKind)
	rec.Outcome = "failed:" + string(kind)
	g.log(rec)
	return &Out{
		RequestID: in.RequestID,
		Status:    "failed",
		Attempts:  len(rec.Attempts),
		Err:       gerrors.New(kind, safeErr(lastErr), in.RequestID, retryable),
		Record:    rec,
	}
}

// shouldRetry 决定一次失败后是否重试，并给出可解释原因。
// 重试仅在：操作幂等、还有剩余次数、故障属于可安全重放类别时发生；
// 重试始终停留在同一协商版本的兼容候选集合内（由调用点的候选轮换保证），
// 因此不会跨越不兼容版本。
func (g *Gateway) shouldRetry(in In, attempt, maxAttempts int, kind upstream.ErrorKind) (bool, string) {
	if !in.Idempotent {
		return false, "non_idempotent_operation_not_retried"
	}
	if attempt >= maxAttempts {
		return false, "max_attempts_reached"
	}
	if kind == "circuit_open" || kind.Retryable() {
		return true, "retryable_failure_within_compatible_candidate_set:" + string(kind)
	}
	return false, "non_retryable_error:" + string(kind)
}

func (g *Gateway) waitBackoff(ctx context.Context, attempt int) error {
	return g.cfg.Retry.Sleep(ctx, attempt)
}

func mapFailure(kind upstream.ErrorKind) (gerrors.Kind, bool) {
	switch kind {
	case upstream.ErrTimeout:
		return gerrors.KindUpstreamTimeout, true
	case upstream.ErrConn:
		return gerrors.KindUpstreamConn, true
	case upstream.ErrOverload:
		return gerrors.KindUpstreamOverload, true
	default:
		return gerrors.KindUpstreamConn, false
	}
}

func safeErr(err error) string {
	if err == nil {
		return "unknown error"
	}
	return err.Error()
}

// CircuitSnapshot 暴露熔断状态（可观测）。
func (g *Gateway) CircuitSnapshot() map[string]circuit.State {
	return g.breaks.Snapshot()
}

func (g *Gateway) log(rec DecisionRecord) {
	if g.logger == nil {
		return
	}
	g.logger.Printf(
		"gateway decision request=%s caller=%s route=%s version_verdict=%q reject=%s "+
			"quota={caller_left=%.2f route_left=%.2f decision=%s} candidates=%v circuits=%v "+
			"attempts=%d outcome=%s",
		rec.RequestID, rec.CallerID, rec.Route, rec.VersionVerdict, rec.RejectReason,
		rec.QuotaCallerLeft, rec.QuotaRouteLeft, rec.QuotaDecision,
		rec.Candidates, rec.CircuitStates, len(rec.Attempts), rec.Outcome,
	)
	for _, a := range rec.Attempts {
		g.logger.Printf(
			"  attempt request=%s n=%d upstream=%s version=%s circuit=%s result=%s err=%s retry=%t reason=%s",
			rec.RequestID, a.N, a.UpstreamID, a.Version, a.Circuit, a.Result, a.ErrKind, a.Retry, a.RetryReason,
		)
	}
}
