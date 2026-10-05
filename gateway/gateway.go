// Package gateway 组合版本协商、分层限流、熔断、重试/降级、
// 业务幂等去重，构成可在本地验证的版本化 API 网关核心。
//
// 单次请求处理顺序（任一环节拒绝都会给出可区分原因，绝不静默转发）：
//
//  1. 版本协商：解析客户端版本 -> 路由兼容窗口 -> 同 major 上游能力匹配，
//     产出“兼容候选集”（按版本降序）；
//  2. 分层限流：调用方层 + 路由层原子双扣；
//     2.5 灰度分流：只在兼容候选集内按路由分流计划投放；同一业务幂等键
//     粘性固定，比例调整/回滚不漂移；计划整体校验、原子发布、可整体回滚；
//  3. 熔断：该上游断路器是否放行（half-open 只放一个探测）；
//  4. 幂等去重执行：以业务幂等键去重，内部做受限重试；
//     - 重试只在“分流选定的同一个上游”内发生，绝不重新协商/跨版本；
//     - 首选上游熔断打开或重试耗尽时，按确定性顺序回退到仍健康的
//     兼容候选（可解释的回退日志），semantic 错误除外（立即失败）；
//     - 失败的调用没有产生业务结果，业务操作只会在首个成功调用执行一次；
//     成功结果被幂等层缓存，重复请求直接重放，不再触达上游；
//  5. 全部候选失败则进入与故障类别一一对应的降级路径。
package gateway

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xingjianluo/versioned-api-gateway-go/breaker"
	"github.com/xingjianluo/versioned-api-gateway-go/callback"
	"github.com/xingjianluo/versioned-api-gateway-go/graysplit"
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

	// 灰度分流决策依据（未配置分流计划时 Rule 为 graysplit.RuleNoPlan）。
	SplitRule     string // 命中规则：sticky / weighted / no_plan / ...
	SplitRevision uint64 // 做出选择时的分流计划版本
	SplitWeights  string // 当时生效的权重快照 "u1=7,u2=3"
	Fallback      bool   // 是否因首选不可用而回退到了其他候选上游
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
	// SplitPlans 按路由的灰度分流计划（可空：空则沿用“版本最高者优先”的
	// 默认顺序，故障时按确定性顺序回退到其他兼容候选）。
	// 启动期整体校验：非法计划（负权重/全零/超上限/未知上游/未知路由）
	// 直接使 New 返回错误，绝不静默按默认值放行。
	SplitPlans map[string][]graysplit.Entry
}

// Gateway 版本化 API 网关核心。
type Gateway struct {
	cfg        Config
	limiter    *ratelimit.Limiter
	breakers   *breaker.Registry
	negotiator *versionx.Negotiator
	idem       *idempotency.Executor
	split      *graysplit.Router
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
		split:      graysplit.NewRouter(),
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
	// 启动期发布初始分流计划：整体校验，非法配置直接失败。
	for route, entries := range cfg.SplitPlans {
		if err := g.validateSplitEntries(route, entries); err != nil {
			return nil, err
		}
		p, err := g.split.SetPlan(route, entries)
		if err != nil {
			return nil, fmt.Errorf("route %q: %w", route, err)
		}
		g.logf("INFO", "split_plan_published",
			"route", route, "split_revision", p.Revision, "split_weights", p.String())
	}
	for _, o := range opts {
		o(g)
	}
	return g, nil
}

// validateSplitEntries 校验分流条目引用的路由与上游真实存在。
func (g *Gateway) validateSplitEntries(route string, entries []graysplit.Entry) error {
	if _, ok := g.cfg.RouteRules[route]; !ok {
		return fmt.Errorf("split plan for unknown route %q", route)
	}
	for _, e := range entries {
		if _, ok := g.adapters[e.UpstreamID]; !ok {
			return fmt.Errorf("split plan route %q references unknown upstream %q",
				route, e.UpstreamID)
		}
	}
	return nil
}

// SetSplitPlan 运行时调整某条路由的分流比例。
// 校验失败（非法比例/未知上游/未知路由）整体拒绝，旧计划保持不变；
// 校验通过则原子发布，进行中的请求要么看到完整旧计划、要么看到完整新计划。
// 已粘性固定的业务键不随比例调整漂移。
func (g *Gateway) SetSplitPlan(route string, entries []graysplit.Entry) error {
	if err := g.validateSplitEntries(route, entries); err != nil {
		return err
	}
	p, err := g.split.SetPlan(route, entries)
	if err != nil {
		return err
	}
	g.logf("INFO", "split_plan_published",
		"route", route, "split_revision", p.Revision, "split_weights", p.String())
	return nil
}

// RollbackSplitPlan 整体回滚到上一份分流计划（以新 Revision 发布）。
// 没有历史计划时返回错误，当前计划不变。
func (g *Gateway) RollbackSplitPlan(route string) error {
	p, err := g.split.Rollback(route)
	if err != nil {
		return err
	}
	g.logf("INFO", "split_plan_rollback",
		"route", route, "split_revision", p.Revision, "split_weights", p.String())
	return nil
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

	// 2.5 灰度分流：只在协商得出的兼容候选集内选择目标上游。
	// 同一业务幂等键粘性固定；比例调整不漂移；首选不可用时按确定性
	// 顺序回退到其他兼容候选（绝不跨出版本兼容集）。
	candIDs := make([]string, len(dec.Candidates))
	versions := make(map[string]string, len(dec.Candidates))
	for i, c := range dec.Candidates {
		candIDs[i] = c.ID
		versions[c.ID] = c.Version.String()
	}
	sd := g.split.Pick(route, idemKey, candIDs)
	g.logf("INFO", "split_decided", append(baseFields,
		"stage", "split", "decision", sd.Ordered[0],
		"split_rule", sd.Rule, "split_revision", sd.Revision,
		"split_weights", sd.Weights, "split_pinned", sd.Pinned,
		"candidates", strings.Join(candIDs, ","))...)

	// 3+4. 幂等去重执行（内含同上游受限重试 + 兼容候选间回退）
	req := upstream.Request{
		RequestID: requestID, Route: route, Version: dec.Selected.Version.String(),
		IdemKey: idemKey, Payload: payload,
	}
	res, err := g.idem.Do(ctx, idemKey, func() (any, error) {
		return g.invokeWithFallback(ctx, sd, versions, req, baseFields)
	})
	if err != nil {
		o := g.mapUpstreamFailure(requestID, sd, err, baseFields)
		o.VersionNote = dec.Detail
		return o
	}
	ir := res.(invokeResult)
	stats := g.idem.StatsOf(idemKey)
	g.logf("INFO", "gateway_ok", append(baseFields,
		"stage", "complete", "decision", "ok", "upstream", ir.upstreamID,
		"split_rule", sd.Rule, "split_revision", sd.Revision,
		"split_fallback", ir.fallback,
		"business_executions", stats.Executed, "replayed", stats.Replayed,
		"shared", stats.Shared, "retry_count", ir.attempts)...)
	return Outcome{
		RequestID: requestID, Allowed: true, Status: "ok", Reason: ReasonOK,
		SelectedUpstream: ir.upstreamID, UpstreamVersion: ir.version,
		VersionNote: dec.Detail, Retries: ir.attempts, Response: &ir.resp,
		SplitRule: sd.Rule, SplitRevision: sd.Revision, SplitWeights: sd.Weights,
		Fallback: ir.fallback,
	}
}

// invokeWithFallback 按分流给出的确定性偏好顺序尝试候选上游：
//   - 每个上游内部仍只做“同上游受限重试”，重试绝不跨上游；
//   - 首选上游熔断打开/重试耗尽（timeout/connection/overload）时，
//     回退到下一个仍然健康的兼容候选，并记录可解释的回退日志；
//   - semantic 错误说明请求本身不合法，立即失败，绝不回退放大。
func (g *Gateway) invokeWithFallback(ctx context.Context, sd graysplit.Decision,
	versions map[string]string, req upstream.Request, baseFields []any) (any, error) {

	var lastErr error
	for i, upID := range sd.Ordered {
		adapter := g.adapters[upID]
		if adapter == nil {
			continue
		}
		if i > 0 {
			g.logf("WARN", "split_fallback", append(baseFields,
				"stage", "split", "from", sd.Ordered[0], "to", upID,
				"split_rule", sd.Rule, "split_revision", sd.Revision,
				"split_weights", sd.Weights,
				"cause", lastErr.Error())...)
		}
		br := g.breakers.Get(upID)
		upReq := req
		upReq.Version = versions[upID]
		res, err := g.invokeWithRetry(ctx, br, upID, adapter, upReq, baseFields)
		if err == nil {
			ir := res.(invokeResult)
			ir.upstreamID = upID
			ir.version = versions[upID]
			ir.fallback = i > 0
			return ir, nil
		}
		var ge *gatewayError
		if !errors.As(err, &ge) {
			ge = &gatewayError{reason: ReasonUpstreamConnection,
				kind: upstream.KindConnection, detail: err.Error()}
		}
		ge.upstream = upID
		ge.version = versions[upID]
		if ge.reason == ReasonUpstreamSemantic {
			return nil, ge // 语义错误：不回退
		}
		lastErr = ge
	}
	return nil, lastErr
}

// invokeWithRetry 在“已选定的同一个上游”内做受限重试。
// 不重新协商、不切换上游（重试不跨版本）。
type invokeResult struct {
	resp       upstream.Response
	attempts   int
	upstreamID string
	version    string
	fallback   bool // 是否因首选不可用而回退到了该上游
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

func (g *Gateway) mapUpstreamFailure(requestID string, sd graysplit.Decision, err error,
	baseFields []any) Outcome {
	var ge *gatewayError
	if !errors.As(err, &ge) {
		ge = &gatewayError{reason: ReasonUpstreamConnection, kind: upstream.KindConnection}
	}
	fallback := ge.upstream != "" && len(sd.Ordered) > 0 && ge.upstream != sd.Ordered[0]
	o := Outcome{
		RequestID: requestID, Allowed: false, Status: "degraded",
		Reason: ge.reason, SelectedUpstream: ge.upstream, UpstreamVersion: ge.version,
		Retries: ge.attempts, Detail: ge.detail,
		SplitRule: sd.Rule, SplitRevision: sd.Revision, SplitWeights: sd.Weights,
		Fallback: fallback,
	}
	g.logf("WARN", "gateway_degrade", append(baseFields,
		"stage", "complete", "decision", o.Status, "upstream", ge.upstream,
		"reason", ge.reason, "error_kind", ge.kind.String(),
		"split_rule", sd.Rule, "split_revision", sd.Revision,
		"split_fallback", fallback,
		"retry_count", ge.attempts)...)
	return o
}

// gatewayError 是网关内部携带决策依据的错误。
type gatewayError struct {
	reason   RejectReason
	kind     upstream.ErrorKind
	detail   string
	attempts int
	upstream string
	version  string
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
