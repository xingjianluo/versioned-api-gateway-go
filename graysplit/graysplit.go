// Package graysplit 实现按路由的灰度分流（比例配置、原子生效、整体回滚、
// 业务级粘性选择与确定性回退顺序）。
//
// 语义概述（详见 docs/GRAY_SPLIT.md）：
//   - 每条路由可配置一份分流计划 Plan：若干 {上游ID, 权重} 条目；
//   - 计划整体校验、整体原子替换：任何请求要么看到完整的旧计划，
//     要么看到完整的新计划，绝不出现半更新；
//   - 非法配置（负权重、权重和为 0、超过上限、重复/空上游ID）被明确拒绝，
//     旧计划保持不变，绝不静默按默认值放行；
//   - 同一业务键（幂等键）的选择被粘性固定：比例调整不导致漂移；
//   - 选择只在“与调用方版本兼容的候选集”内进行，绝不跨版本。
package graysplit

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// MaxTotalWeight 单条路由全部条目权重之和的上限（含）。
const MaxTotalWeight = 10000

// 分流命中规则（Decision.Rule 的可区分取值）。
const (
	RuleSticky       = "sticky"                 // 命中既有粘性固定
	RuleWeighted     = "weighted"               // 按计划权重哈希分桶
	RulePinWithdrawn = "pin_withdrawn_weighted" // 粘性上游已被计划移出，按计划重选并重新固定
	RuleNoPlan       = "no_plan"                // 路由无分流计划，沿用协商默认顺序
	RuleNoPlanEntry  = "no_plan_entry"          // 计划与候选集无交集，沿用协商默认顺序
)

// Entry 一条分流条目：把 Weight 份流量投给 UpstreamID。
type Entry struct {
	UpstreamID string
	Weight     int
}

// Plan 一份完整校验通过的分流计划（不可变，原子替换）。
type Plan struct {
	Revision uint64 // 单调递增，回滚也会产生新 Revision，避免 ABA
	Entries  []Entry
}

// totalWeight 全部条目权重之和。
func (p *Plan) totalWeight() int {
	s := 0
	for _, e := range p.Entries {
		s += e.Weight
	}
	return s
}

// String 以 "id=weight,..." 形式展示（按 ID 排序，便于日志对比）。
func (p *Plan) String() string {
	es := append([]Entry(nil), p.Entries...)
	sort.Slice(es, func(i, j int) bool { return es[i].UpstreamID < es[j].UpstreamID })
	parts := make([]string, 0, len(es))
	for _, e := range es {
		parts = append(parts, fmt.Sprintf("%s=%d", e.UpstreamID, e.Weight))
	}
	return strings.Join(parts, ",")
}

// Decision 一次分流选择的结论（可解释）。
type Decision struct {
	Ordered  []string // 候选上游的偏好顺序，Ordered[0] 为首选
	Rule     string   // 命中规则：见 Rule* 常量
	Revision uint64   // 做出选择时使用的计划版本（无计划为 0）
	Weights  string   // 当时对候选集生效的权重快照，如 "u1=7,u2=3"
	Pinned   string   // 该业务键当前粘性固定的上游（无则空）
}

// Router 按路由管理分流计划与粘性选择，并发安全。
type Router struct {
	mu     sync.Mutex
	routes map[string]*routeState
	sticky map[string]map[string]string // route -> 业务键 -> 上游ID
}

type routeState struct {
	current atomic.Pointer[Plan]
	history []*Plan // 供整体回滚；仅在 mu 下访问
}

// NewRouter 构造分流路由器。
func NewRouter() *Router {
	return &Router{
		routes: make(map[string]*routeState),
		sticky: make(map[string]map[string]string),
	}
}

func (r *Router) stateOf(route string) *routeState {
	st := r.routes[route]
	if st == nil {
		st = &routeState{}
		r.routes[route] = st
	}
	return st
}

// validate 整体校验一份计划；任何一项不合法都整体拒绝。
func validate(entries []Entry) error {
	if len(entries) == 0 {
		return errors.New("graysplit: plan must contain at least one entry")
	}
	seen := make(map[string]bool, len(entries))
	sum := 0
	for _, e := range entries {
		if e.UpstreamID == "" {
			return errors.New("graysplit: entry with empty upstream id")
		}
		if seen[e.UpstreamID] {
			return fmt.Errorf("graysplit: duplicate upstream id %q", e.UpstreamID)
		}
		seen[e.UpstreamID] = true
		if e.Weight < 0 {
			return fmt.Errorf("graysplit: negative weight %d for %q", e.Weight, e.UpstreamID)
		}
		sum += e.Weight
	}
	if sum <= 0 {
		return errors.New("graysplit: total weight must be > 0 (all-zero plan rejected)")
	}
	if sum > MaxTotalWeight {
		return fmt.Errorf("graysplit: total weight %d exceeds max %d", sum, MaxTotalWeight)
	}
	return nil
}

// SetPlan 校验并原子发布新计划；非法配置整体拒绝并返回错误，旧计划不变。
// 发布是单次原子指针替换：进行中的请求要么看到完整旧计划，要么看到完整新计划。
func (r *Router) SetPlan(route string, entries []Entry) (Plan, error) {
	if err := validate(entries); err != nil {
		return Plan{}, err
	}
	cp := append([]Entry(nil), entries...)

	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.stateOf(route)
	rev := uint64(1)
	if cur := st.current.Load(); cur != nil {
		rev = cur.Revision + 1
		st.history = append(st.history, cur)
	}
	p := &Plan{Revision: rev, Entries: cp}
	st.current.Store(p)
	return *p, nil
}

// Rollback 整体回滚到上一份计划（以新 Revision 发布，避免与旧 Revision 混淆）。
// 没有历史计划时返回错误，当前计划不变。
func (r *Router) Rollback(route string) (Plan, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.routes[route]
	if st == nil || len(st.history) == 0 {
		return Plan{}, fmt.Errorf("graysplit: no previous plan for route %q", route)
	}
	prev := st.history[len(st.history)-1]
	st.history = st.history[:len(st.history)-1]
	cur := st.current.Load()
	rev := uint64(1)
	if cur != nil {
		rev = cur.Revision + 1
	}
	p := &Plan{Revision: rev, Entries: append([]Entry(nil), prev.Entries...)}
	st.current.Store(p)
	return *p, nil
}

// CurrentPlan 返回路由当前计划（无计划返回 nil）。
func (r *Router) CurrentPlan(route string) *Plan {
	r.mu.Lock()
	st := r.routes[route]
	r.mu.Unlock()
	if st == nil {
		return nil
	}
	return st.current.Load()
}

// hashKey 业务键的确定性散列（分桶用，进程内稳定即可）。
func hashKey(route, key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(route))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

// Pick 在兼容候选集 candidates 内为该业务键给出确定性的偏好顺序。
// candidates 由调用方按“协商默认偏好”排序（版本最高在前）；
// 返回的 Ordered[0] 是首选，其余为确定性的回退次序。
// 同一 (route, key) 在计划不变时结果稳定；计划变更后已固定的业务键
// 仍粘在原上游（RuleSticky），除非该上游已被计划移出（RulePinWithdrawn）。
func (r *Router) Pick(route, key string, candidates []string) Decision {
	plan := r.CurrentPlan(route)
	if plan == nil {
		return Decision{Ordered: append([]string(nil), candidates...), Rule: RuleNoPlan}
	}

	// 计划条目与候选集求交（权重 0 的条目不参与投放，仅作最后回退）。
	type eff struct {
		id string
		w  int
	}
	inCand := make(map[string]bool, len(candidates))
	for _, c := range candidates {
		inCand[c] = true
	}
	var effs []eff
	for _, e := range plan.Entries {
		if inCand[e.UpstreamID] && e.Weight > 0 {
			effs = append(effs, eff{id: e.UpstreamID, w: e.Weight})
		}
	}
	if len(effs) == 0 {
		// 计划与当前候选集无交集：不静默按权重投放，退化为协商默认顺序，
		// 由日志中的规则名给出明确解释。
		return Decision{
			Ordered:  append([]string(nil), candidates...),
			Rule:     RuleNoPlanEntry,
			Revision: plan.Revision,
			Weights:  plan.String(),
		}
	}
	// 确定性次序：权重降序，权重相同按 ID 升序。
	sort.Slice(effs, func(i, j int) bool {
		if effs[i].w != effs[j].w {
			return effs[i].w > effs[j].w
		}
		return effs[i].id < effs[j].id
	})

	r.mu.Lock()
	defer r.mu.Unlock()
	keys := r.sticky[route]
	if keys == nil {
		keys = make(map[string]string)
		r.sticky[route] = keys
	}

	effSet := make(map[string]bool, len(effs))
	for _, e := range effs {
		effSet[e.id] = true
	}

	rule := RuleWeighted
	preferred := ""
	pinned := keys[key]
	if pinned != "" && effSet[pinned] {
		// 粘性优先：比例调整不漂移。
		preferred = pinned
		rule = RuleSticky
	} else {
		if pinned != "" {
			// 原固定上游已被计划移出（或权重回收为 0）：按计划重选并重新固定。
			rule = RulePinWithdrawn
		}
		// 权重分桶：hash 落在 [0,sum) 的哪个桶就选谁，确定性。
		sum := 0
		for _, e := range effs {
			sum += e.w
		}
		bucket := int(hashKey(route, key) % uint32(sum))
		acc := 0
		preferred = effs[len(effs)-1].id
		for _, e := range effs {
			acc += e.w
			if bucket < acc {
				preferred = e.id
				break
			}
		}
		keys[key] = preferred
		pinned = preferred
	}

	// 组装偏好顺序：首选 + 其余生效条目（确定性次序）+ 计划中权重为 0
	// 或不在计划内的候选（仅作最后回退，不参与正常投放）。
	ordered := []string{preferred}
	for _, e := range effs {
		if e.id != preferred {
			ordered = append(ordered, e.id)
		}
	}
	rest := append([]string(nil), candidates...)
	sort.Strings(rest)
	for _, c := range rest {
		found := false
		for _, o := range ordered {
			if o == c {
				found = true
				break
			}
		}
		if !found {
			ordered = append(ordered, c)
		}
	}

	return Decision{
		Ordered:  ordered,
		Rule:     rule,
		Revision: plan.Revision,
		Weights:  plan.String(),
		Pinned:   pinned,
	}
}
