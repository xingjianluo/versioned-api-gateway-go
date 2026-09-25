// Package gateway 组合版本协商、分层限流、熔断、重试/降级、
// 业务幂等去重，构成可在本地验证的版本化 API 网关核心。
//
// 单次请求处理顺序（任一环节拒绝都会给出可区分原因，绝不静默转发）：
//
//  1. 版本协商：解析客户端版本 -> 路由兼容窗口 -> 同 major 上游能力匹配；
//  2. 分层限流：调用方层 + 路由层原子双扣；
//  3. 熔断：该上游断路器是否放行（half-open 只放一个探测）；
//  4. 幂等去重执行：以业务幂等键去重，内部做受限重试；
//     - 重试只在“协商选定的同一个上游”内发生，绝不重新协商/跨版本；
//     - 仅 timeout/connection/overload 可重试，semantic 立即失败；
//     - 失败的调用没有产生业务结果，业务操作只会在首个成功调用执行一次；
//     成功结果被幂等层缓存，重复请求直接重放，不再触达上游；
//  5. 全部尝试失败则进入与故障类别一一对应的降级路径。
package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xingjianluo/versioned-api-gateway-go/breaker"
	"github.com/xingjianluo/versioned-api-gateway-go/callback"
	"github.com/xingjianluo/versioned-api-gateway-go/idempotency"
	"github.com/xingjianluo/versioned-api-gateway-go/internal/logging"
	"github.com/xingjianluo/versioned-api-gateway-go/ratelimit"
	"github.com/xingjianluo/versioned-api-gateway-go/upstream"
	"github.com/xingjianluo/versioned-api-gateway-go/versionx"
)

// RejectReason 网关级拒绝/降级结论的可区分原因。
type RejectReason string

// 网关结论原因。
const (
	ReasonOK                   RejectReason = "ok"
	ReasonUnknownClientVersion RejectReason = "unknown_client_version"
	ReasonVersionRejected      RejectReason = "version_negotiation_rejected"
	ReasonRouteNotFound        RejectReason = "route_not_found"
	ReasonCallerQuotaExhausted RejectReason = "caller_quota_exhausted"
	ReasonRouteQuotaExhausted  RejectReason = "route_quota_exhausted"
	ReasonCircuitOpen          RejectReason = "degrade_circuit_open"
	ReasonUpstreamTimeout      RejectReason = "degrade_upstream_timeout"
	ReasonUpstreamConnection   RejectReason = "degrade_upstream_connection"
	ReasonUpstreamOverload     RejectReason = "degrade_upstream_overload"
	ReasonUpstreamSemantic     RejectReason = "upstream_semantic_error"
	ReasonNoUpstream           RejectReason = "no_upstream_binding"
)

// Outcome 是一次网关调用的完整、可解释结论。
type Outcome struct {
	RequestID        string
	Allowed          bool
	Status           string // "ok" | "rejected" | "degraded"
	Reason           RejectReason
	SelectedUpstream string
	UpstreamVersion  string
	VersionNote      string
	Retries          int
	Response         *upstream.Response
	Detail           string
}

// RetryPolicy 重试策略：仅对可重试错误且在同一兼容上游内重试。
type RetryPolicy struct {
	MaxAttempts    int           // 总尝试次数（含首次）
	AttemptTimeout time.Duration // 单次上游调用超时
}

// Config 网关配置。
type Config struct {
	RouteRules  map[string]versionx.RouteRule
	Upstreams   []upstream.Adapter
	CallerLimit map[string]ratelimit.LayerConfig
	RouteLimit  map[string]ratelimit.LayerConfig
	Breaker     breaker.Config
	Retry       RetryPolicy
}

// Gateway 版本化 API 网关核心。
type Gateway struct {
	cfg        Config
	limiter    *ratelimit.Limiter
	breakers   *breaker.Registry
	negotiator *versionx.Negotiator
	idem       *idempotency.Executor
	dispatcher *callback.Dispatcher
	log        logging.Logger

	adapters map[string]upstream.Adapter
	upDesc   []versionx.UpstreamDesc
	now      func() time.Time
}

// Option 网关可选项。
type Option func(*Gateway)

// WithClock 注入时钟（确定性测试用）。
func WithClock(f func() time.Time) Option {
	return func(g *Gateway) { g.now = f }
}

// WithCallbackDispatcher 挂载回调投递调度器。
func WithCallbackDispatcher(d *callback.Dispatcher) Option {
	return func(g *Gateway) { g.dispatcher = d }
}

// New 构造网关；上游静态版本无法解析时返回错误（启动期暴露配置问题）。
func New(cfg Config, log logging.Logger, opts ...Option) (*Gateway, error) {
	if log == nil {
		log = logging.NopLogger{}
	}
	if cfg.Retry.MaxAttempts <= 0 {
		cfg.Retry.MaxAttempts = 1
	}
	if cfg.Retry.AttemptTimeout <= 0 {
		cfg.Retry.AttemptTimeout = 2 * time.Second
	}

	adapters := make(map[string]upstream.Adapter, len(cfg.Upstreams))
	descs := make([]versionx.UpstreamDesc, 0, len(cfg.Upstreams))
	for _, a := range cfg.Upstreams {
		d := a.Desc()
		v, err := versionx.Parse(d.Version)
		if err != nil {
			return nil, fmt.Errorf("upstream %s invalid version %q: %w", d.ID, d.Version, err)
		}
		if _, dup := adapters[d.ID]; dup {
			return nil, fmt.Errorf("duplicate upstream id %q", d.ID)
		}
		adapters[d.ID] = a
		caps := make(map[string]versionx.Capability, len(d.Caps))
		for _, c := range d.Caps {
			caps[c] = versionx.Capability{Name: c, Since: v}
		}
		descs = append(descs, versionx.UpstreamDesc{ID: d.ID, Version: v, Caps: caps})
	}

	g := &Gateway{
		cfg:        cfg,
		limiter:    ratelimit.New(cfg.CallerLimit, cfg.RouteLimit),
		negotiator: versionx.NewNegotiator(),
		idem:       idempotency.NewExecutor(),
		log:        log,
		adapters:   adapters,
		upDesc:     descs,
		now:        time.Now,
	}
	g.breakers = breaker.NewRegistry(cfg.Breaker, func(id string, from, to breaker.State, reason string) {
		g.log.Log("WARN", "breaker_transition", map[string]any{
			"upstream": id, "from": from.String(), "to": to.String(),
			"reason": reason,
		})
	})
	for _, o := range opts {
		o(g)
	}
	return g, nil
}

// Dispatcher 返回已挂载的回调调度器（可能为 nil）。
func (g *Gateway) Dispatcher() *callback.Dispatcher { return g.dispatcher }

func (g *Gateway) logf(level, msg string, kv ...any) {
	fields := map[string]any{}
	for i := 0; i+1 < len(kv); i += 2 {
		fields[fmt.Sprint(kv[i])] = kv[i+1]
	}
	g.log.Log(level, msg, fields)
}

// Serve 处理一次调用。
func (g *Gateway) Serve(ctx context.Context, caller, requestID, clientVersion,
	route, idemKey, payload string) Outcome {

	if idemKey == "" {
		idemKey = "anon:" + requestID // 无业务幂等键时退化为请求级，不做跨请求去重
	}
	baseFields := []any{"request_id", requestID, "caller", caller, "route", route,
		"client_version", clientVersion, "idem_key", idemKey}

	// 1. 版本解析与协商
	clientV, err := versionx.Parse(clientVersion)
	if err != nil {
		g.logf("WARN", "gateway_reject", append(baseFields,
			"stage", "version", "decision", "reject", "reason", ReasonUnknownClientVersion)...)
		return Outcome{RequestID: requestID, Allowed: false, Status: "rejected",
			Reason: ReasonUnknownClientVersion, Detail: err.Error()}
	}
	rule, ok := g.cfg.RouteRules[route]
	if !ok {
		g.logf("WARN", "gateway_reject", append(baseFields,
			"stage", "route", "decision", "reject", "reason", ReasonRouteNotFound)...)
		return Outcome{RequestID: requestID, Allowed: false, Status: "rejected",
			Reason: ReasonRouteNotFound, Detail: "no rule for route"}
	}
	rule.Route = route
	dec := g.negotiator.Negotiate(ctx, clientV, rule, g.upDesc)
	if !dec.Allowed {
		reason := mapNegotiationReason(dec.Reason)
		g.logf("WARN", "gateway_reject", append(baseFields,
			"stage", "version", "decision", "reject",
			"version_reason", dec.Reason, "reason", reason, "detail", dec.Detail)...)
		return Outcome{RequestID: requestID, Allowed: false, Status: "rejected",
			Reason: reason, VersionNote: dec.Detail}
	}
	g.logf("INFO", "version_decided", append(baseFields,
		"stage", "version", "decision", "allow",
		"upstream", dec.Selected.ID, "upstream_version", dec.Selected.Version.String(),
		"version_basis", dec.Detail)...)

	// 2. 分层限流
	ld := g.limiter.Allow(ctx, caller, route)
	if !ld.Allowed {
		reason := ReasonCallerQuotaExhausted
		if ld.Layer == "route" {
			reason = ReasonRouteQuotaExhausted
		}
		g.logf("WARN", "gateway_reject", append(baseFields,
			"stage", "ratelimit", "decision", "reject", "quota_layer", ld.Layer,
			"reason", reason, "quota_basis", ld.Reason,
			"caller_remaining", fmt.Sprintf("%.2f", ld.CallerRemaining),
			"route_remaining", fmt.Sprintf("%.2f", ld.RouteRemaining))...)
		return Outcome{RequestID: requestID, Allowed: false, Status: "rejected",
			Reason: reason, SelectedUpstream: dec.Selected.ID, Detail: ld.Reason}
	}
	g.logf("INFO", "quota_granted", append(baseFields,
		"stage", "ratelimit", "decision", "allow",
		"caller_remaining", fmt.Sprintf("%.2f", ld.CallerRemaining),
		"route_remaining", fmt.Sprintf("%.2f", ld.RouteRemaining))...)

	adapter := g.adapters[dec.Selected.ID]
	if adapter == nil {
		return Outcome{RequestID: requestID, Allowed: false, Status: "rejected",
			Reason: ReasonNoUpstream}
	}
	br := g.breakers.Get(dec.Selected.ID)

	// 3+4. 幂等去重执行（内含同上游受限重试）
	req := upstream.Request{
		RequestID: requestID, Route: route, Version: dec.Selected.Version.String(),
		IdemKey: idemKey, Payload: payload,
	}
	res, err := g.idem.Do(ctx, idemKey, func() (any, error) {
		return g.invokeWithRetry(ctx, br, dec.Selected.ID, adapter, req, baseFields)
	})
	if err != nil {
		o := g.mapUpstreamFailure(requestID, dec.Selected.ID, dec.Selected.Version.String(),
			err, baseFields)
		o.VersionNote = dec.Detail
		return o
	}
	ir := res.(invokeResult)
	stats := g.idem.StatsOf(idemKey)
	g.logf("INFO", "gateway_ok", append(baseFields,
		"stage", "complete", "decision", "ok", "upstream", dec.Selected.ID,
		"business_executions", stats.Executed, "replayed", stats.Replayed,
		"shared", stats.Shared, "retry_count", ir.attempts)...)
	return Outcome{
		RequestID: requestID, Allowed: true, Status: "ok", Reason: ReasonOK,
		SelectedUpstream: dec.Selected.ID, UpstreamVersion: dec.Selected.Version.String(),
		VersionNote: dec.Detail, Retries: ir.attempts, Response: &ir.resp,
	}
}

// invokeWithRetry 在“已选定的同一个上游”内做受限重试。
// 不重新协商、不切换上游（重试不跨版本）。
type invokeResult struct {
	resp     upstream.Response
	attempts int
}

func (g *Gateway) invokeWithRetry(ctx context.Context, br *breaker.Breaker,
	upstreamID string, adapter upstream.Adapter, req upstream.Request,
	baseFields []any) (any, error) {

	var lastErr error
	attempts := 0
	for attempt := 1; attempt <= g.cfg.Retry.MaxAttempts; attempt++ {
		now := g.now()
		bd := br.Allow(ctx, now)
		if !bd.Allowed {
			g.logf("WARN", "breaker_decision", append(baseFields,
				"stage", "breaker", "upstream", upstreamID,
				"breaker_state", bd.State.String(), "reason", bd.Reason,
				"retry", attempts)...)
			// 熔断打开（或半开探测已被占用）：停止重试，走降级。
			return nil, &gatewayError{reason: ReasonCircuitOpen,
				kind: upstream.KindOverload, detail: bd.Reason, attempts: attempts}
		}
		g.logf("INFO", "breaker_decision", append(baseFields,
			"stage", "breaker", "upstream", upstreamID,
			"breaker_state", bd.State.String(), "reason", bd.Reason)...)

		attemptCtx, cancel := context.WithTimeout(ctx, g.cfg.Retry.AttemptTimeout)
		resp, err := adapter.Invoke(attemptCtx, req)
		cancel()
		now = g.now()

		if err == nil {
			br.OnResult(ctx, now, nil)
			g.logf("INFO", "upstream_attempt", append(baseFields,
				"stage", "invoke", "upstream", upstreamID,
				"attempt", attempt, "retry", attempts, "result", "success")...)
			return invokeResult{resp: resp, attempts: attempts}, nil
		}

		var ue *upstream.UpstreamError
		if !errors.As(err, &ue) {
			ue = &upstream.UpstreamError{Kind: upstream.KindConnection, Message: err.Error()}
		}
		br.OnResult(ctx, now, ue) // 非 nil 错误计入连续失败
		lastErr = &gatewayError{kind: ue.Kind, detail: ue.Message, attempts: attempts}
		g.logf("WARN", "upstream_attempt", append(baseFields,
			"stage", "invoke", "upstream", upstreamID,
			"attempt", attempt, "retry", attempts,
			"result", "failure", "error_kind", ue.Kind.String(),
			"retryable", ue.Kind.Retryable())...)

		// 语义错误不可重试；不可重试或已达上限则结束。
		if !ue.Kind.Retryable() {
			lastErr.(*gatewayError).reason = ReasonUpstreamSemantic
			return nil, lastErr
		}
		if attempt == g.cfg.Retry.MaxAttempts {
			break
		}
		attempts++
	}
	ge := lastErr.(*gatewayError)
	ge.reason = mapKindToDegrade(ge.kind)
	ge.attempts = attempts
	return nil, ge
}

func (g *Gateway) mapUpstreamFailure(requestID, upID, upVer string, err error,
	baseFields []any) Outcome {
	var ge *gatewayError
	if !errors.As(err, &ge) {
		ge = &gatewayError{reason: ReasonUpstreamConnection, kind: upstream.KindConnection}
	}
	o := Outcome{
		RequestID: requestID, Allowed: false, Status: "degraded",
		Reason: ge.reason, SelectedUpstream: upID, UpstreamVersion: upVer,
		Retries: ge.attempts, Detail: ge.detail,
	}
	g.logf("WARN", "gateway_degrade", append(baseFields,
		"stage", "complete", "decision", o.Status, "upstream", upID,
		"reason", ge.reason, "error_kind", ge.kind.String(),
		"retry_count", ge.attempts)...)
	return o
}

// gatewayError 是网关内部携带决策依据的错误。
type gatewayError struct {
	reason   RejectReason
	kind     upstream.ErrorKind
	detail   string
	attempts int
}

func (e *gatewayError) Error() string {
	return fmt.Sprintf("%s(%s): %s", e.reason, e.kind, e.detail)
}

func mapKindToDegrade(k upstream.ErrorKind) RejectReason {
	switch k {
	case upstream.KindTimeout:
		return ReasonUpstreamTimeout
	case upstream.KindConnection:
		return ReasonUpstreamConnection
	case upstream.KindOverload:
		return ReasonUpstreamOverload
	default:
		return ReasonUpstreamConnection
	}
}

func mapNegotiationReason(r versionx.DecisionReason) RejectReason {
	switch r {
	case versionx.ReasonCapabilityRemoved:
		return RejectReason("version_semantic_incompatible_capability_removed")
	case versionx.ReasonCapabilityMissing:
		return RejectReason("version_capability_missing")
	case versionx.ReasonNoUpstreamForMajor:
		return RejectReason("version_no_upstream_for_major")
	case versionx.ReasonClientTooOld:
		return RejectReason("version_client_too_old")
	case versionx.ReasonClientTooNew:
		return RejectReason("version_client_too_new")
	default:
		return ReasonVersionRejected
	}
}
