// Package split 实现按路由（可带客户端版本范围）的灰度分流。
//
// 语义：
//   - 分流只在“版本协商后的兼容候选集合”内进行，绝不把请求送出兼容范围；
//   - 配置按路由+版本范围匹配，SetConfigs 全量校验后原子换入：任一配置非法
//     （权重为负/为零、权重和为零或超上限、引用未知上游、版本范围重叠）则整体
//     拒绝，旧配置继续完整生效，不存在半更新；整体回滚 = 用旧配置再调一次；
//   - 业务级粘性：同一业务幂等键首次选型后被钉住（pin），比例调整、回滚、
//     重试、回调重放、并发重复请求都稳定落到同一上游；钉住关系保存在配置
//     快照之外，配置换入换出不影响既有钉住；
//   - 健康回退：仅当被钉住/被选中的上游当前不健康（熔断打开等，即请求根本
//     不会被发往该上游）时，才在“仍然健康的兼容候选”中按同一权重规则重选并
//     改钉到新上游；已实际发往上游的请求绝不因分流而换上游（重试仍在原上游内），
//     因此不会出现同一业务操作在两个后端各执行一次；
//   - 所有候选都不健康时不做选择层面的腾挪：保留原选择交由熔断器快速失败，
//     避免恢复瞬间集中冲击（半开单探测由熔断器保证）。
package split

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/xingjianluo/versioned-api-gateway-go/versionx"
)

// maxWeightSum 单条配置权重和上限，防止溢出与异常配置。
const maxWeightSum = 1 << 20

// Weight 一个上游候选的分流权重。
type Weight struct {
	UpstreamID string
	Weight     int
}

// RouteConfig 一条路由（可限定客户端版本范围）的分流配置。
type RouteConfig struct {
	Route      string
	MinVersion versionx.Version // 零值表示不设下限
	MaxVersion versionx.Version // 零值表示不设上限
	Weights    []Weight
}

// matches 报告该配置是否适用于给定客户端版本。
func (c RouteConfig) matches(client versionx.Version) bool {
	if c.MinVersion != (versionx.Version{}) && versionx.Compare(client, c.MinVersion) < 0 {
		return false
	}
	if c.MaxVersion != (versionx.Version{}) && versionx.Compare(client, c.MaxVersion) > 0 {
		return false
	}
	return true
}

// describe 配置的可读描述（日志用）。
func (c RouteConfig) describe() string {
	return fmt.Sprintf("%s[%s..%s]", c.Route, verOrAny(c.MinVersion), verOrAny(c.MaxVersion))
}

func verOrAny(v versionx.Version) string {
	if v == (versionx.Version{}) {
		return "*"
	}
	return v.String()
}

// weightsSnapshot 权重快照字符串（日志用），按上游 ID 排序保证稳定。
func weightsSnapshot(ws []Weight) string {
	parts := make([]string, 0, len(ws))
	for _, w := range ws {
		parts = append(parts, fmt.Sprintf("%s=%d", w.UpstreamID, w.Weight))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// SelectReason 分流结论的可区分原因。
type SelectReason string

// 分流结论原因。
const (
	ReasonDefaultHighest SelectReason = "default_highest_version" // 无分流配置：取版本最高候选
	ReasonWeightedNew    SelectReason = "weighted_new"            // 按权重为新业务键选型
	ReasonStickyPin      SelectReason = "sticky_pin"              // 命中既有粘性钉住
	ReasonFallback       SelectReason = "fallback_healthy"        // 钉住/选中上游不健康，回退到健康候选
	ReasonAllUnhealthy   SelectReason = "all_unhealthy"           // 无健康候选，保留原选择交由熔断裁决
)

// Decision 一次分流结论。
type Decision struct {
	UpstreamID string
	Reason     SelectReason
	Rule       string // 命中的配置描述（路由+版本范围）；无配置时为 "default"
	Weights    string // 生效时的权重快照（可观测）
	Generation uint64 // 配置代次
	Fallback   bool   // 是否发生了健康回退
	PinnedFrom string // 回退前被钉住/应选的上游（仅 Fallback 时非空）
}

// Candidate 参与分流的上游候选。
type Candidate struct {
	ID      string
	Version versionx.Version
}

// snapshot 是不可变的配置快照；整体原子替换，绝不半更新。
type snapshot struct {
	routes map[string][]RouteConfig // route -> 该路由的配置（按版本范围区分，互不重叠）
	gen    uint64
}

// Router 灰度分流路由器：配置原子换入换出，业务键粘性钉住。
type Router struct {
	mu   sync.Mutex // 保护 pins 与选型决策的原子性
	pins map[string]string
	cur  atomic.Pointer[snapshot]
	gen  atomic.Uint64
}

// New 构造空路由器（无任何分流配置时全部走默认最高版本）。
func New() *Router {
	r := &Router{pins: make(map[string]string)}
	r.cur.Store(&snapshot{routes: map[string][]RouteConfig{}})
	return r
}

// SetConfigs 校验并原子替换全量分流配置；任一配置非法则整体拒绝，
// 旧配置继续完整生效（不存在半更新）。整体回滚即用旧配置再次调用本方法。
// knownUpstream 报告上游 ID 是否存在（由网关注入，防止引用不存在的上游）。
func (r *Router) SetConfigs(cfgs []RouteConfig, knownUpstream func(id string) bool) error {
	routes := make(map[string][]RouteConfig, len(cfgs))
	for i, c := range cfgs {
		if c.Route == "" {
			return fmt.Errorf("split config #%d: empty route", i)
		}
		if c.MinVersion != (versionx.Version{}) && c.MaxVersion != (versionx.Version{}) &&
			versionx.Compare(c.MinVersion, c.MaxVersion) > 0 {
			return fmt.Errorf("split config %s: min version %s > max version %s",
				c.Route, c.MinVersion, c.MaxVersion)
		}
		if len(c.Weights) == 0 {
			return fmt.Errorf("split config %s: empty weights", c.describe())
		}
		seen := make(map[string]bool, len(c.Weights))
		sum := 0
		for _, w := range c.Weights {
			if w.UpstreamID == "" {
				return fmt.Errorf("split config %s: empty upstream id", c.describe())
			}
			if knownUpstream != nil && !knownUpstream(w.UpstreamID) {
				return fmt.Errorf("split config %s: unknown upstream %q", c.describe(), w.UpstreamID)
			}
			if seen[w.UpstreamID] {
				return fmt.Errorf("split config %s: duplicate upstream %q", c.describe(), w.UpstreamID)
			}
			seen[w.UpstreamID] = true
			if w.Weight <= 0 {
				return fmt.Errorf("split config %s: upstream %q weight %d must be positive",
					c.describe(), w.UpstreamID, w.Weight)
			}
			sum += w.Weight
			if sum > maxWeightSum {
				return fmt.Errorf("split config %s: weight sum exceeds %d", c.describe(), maxWeightSum)
			}
		}
		// 同一路由的版本范围不得重叠，否则同一次请求可能命中两条规则（半更新语义）。
		for _, prev := range routes[c.Route] {
			if rangesOverlap(prev, c) {
				return fmt.Errorf("split config %s: version range overlaps %s",
					c.describe(), prev.describe())
			}
		}
		routes[c.Route] = append(routes[c.Route], c)
	}
	// 全部校验通过后一次性原子换入；校验失败时旧快照完全不受影响。
	r.cur.Store(&snapshot{routes: routes, gen: r.gen.Add(1)})
	return nil
}

func rangesOverlap(a, b RouteConfig) bool {
	// a 完全在 b 之下，或 a 完全在 b 之上，才是不重叠。
	if a.MaxVersion != (versionx.Version{}) && b.MinVersion != (versionx.Version{}) &&
		versionx.Compare(a.MaxVersion, b.MinVersion) < 0 {
		return false
	}
	if b.MaxVersion != (versionx.Version{}) && a.MinVersion != (versionx.Version{}) &&
		versionx.Compare(b.MaxVersion, a.MinVersion) < 0 {
		return false
	}
	return true
}

// Generation 返回当前配置代次（可观测用）。
func (r *Router) Generation() uint64 { return r.cur.Load().gen }

// Select 在兼容候选中按配置与粘性选择上游。
// healthy 报告某上游当前是否健康（由网关按熔断状态判定）。
// 整个决策在锁内完成：同一业务键的并发请求必然得到同一个上游。
func (r *Router) Select(route string, client versionx.Version, idemKey string,
	cands []Candidate, healthy func(id string) bool) Decision {

	if len(cands) == 0 {
		return Decision{Reason: ReasonAllUnhealthy, Rule: "default"}
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	snap := r.cur.Load()
	cfg, hasCfg := findConfig(snap, route, client)
	rule := "default"
	weights := ""
	if hasCfg {
		rule = cfg.describe()
		weights = weightsSnapshot(cfg.Weights)
	}
	base := Decision{Rule: rule, Weights: weights, Generation: snap.gen}

	inCands := func(id string) bool {
		for _, c := range cands {
			if c.ID == id {
				return true
			}
		}
		return false
	}
	anyHealthy := false
	for _, c := range cands {
		if healthy(c.ID) {
			anyHealthy = true
			break
		}
	}

	// 1. 粘性钉住：同一业务键稳定落同一上游（比例调整/回滚/重试/重放/并发均不变）。
	if pinned, ok := r.pins[idemKey]; ok && inCands(pinned) {
		d := base
		d.UpstreamID = pinned
		switch {
		case !anyHealthy || healthy(pinned):
			if !anyHealthy {
				d.Reason = ReasonAllUnhealthy // 无健康候选：不腾挪，交由熔断快速失败
			} else {
				d.Reason = ReasonStickyPin
			}
			return d
		default:
			// 被钉住的上游不健康：在健康候选内按当前权重重选并改钉。
			d = base
			d.UpstreamID = r.pickLocked(cfg, hasCfg, cands, idemKey, healthy)
			d.Reason = ReasonFallback
			d.Fallback = true
			d.PinnedFrom = pinned
			r.pins[idemKey] = d.UpstreamID
			return d
		}
	}

	// 2. 首次选型（或钉住的上游已不在兼容候选中：按新键处理）。
	d := base
	d.UpstreamID = r.pickLocked(cfg, hasCfg, cands, idemKey, healthy)
	if !anyHealthy {
		d.Reason = ReasonAllUnhealthy
	} else if hasCfg {
		d.Reason = ReasonWeightedNew
	} else {
		// 默认模式：若版本最高候选不健康则视为回退到健康候选。
		highest := highestVersion(cands)
		if d.UpstreamID != highest {
			d.Reason = ReasonFallback
			d.Fallback = true
			d.PinnedFrom = highest
		} else {
			d.Reason = ReasonDefaultHighest
		}
	}
	// 只有命中显式分流配置才钉住：默认模式保持既有“最高版本”语义，
	// 配置被移除后不再影响后续选型。
	if hasCfg {
		r.pins[idemKey] = d.UpstreamID
	}
	return d
}

// findConfig 在快照中找路由+版本范围匹配的配置（校验已保证不重叠，至多一条）。
func findConfig(s *snapshot, route string, client versionx.Version) (RouteConfig, bool) {
	for _, c := range s.routes[route] {
		if c.matches(client) {
			return c, true
		}
	}
	return RouteConfig{}, false
}

// pickLocked 按当前配置在候选中做确定性加权选择。
// 有健康候选时只在健康候选中选；选择由 idemKey 哈希决定，
// 同一键在相同配置与相同健康集合下结果稳定（重试/重放/并发一致）。
func (r *Router) pickLocked(cfg RouteConfig, hasCfg bool, cands []Candidate,
	idemKey string, healthy func(id string) bool) string {

	if !hasCfg {
		return highestHealthy(cands, healthy)
	}
	// 配置权重 ∩ 兼容候选。
	type entry struct {
		id string
		w  int
	}
	var all, healthyPool []entry
	wByID := make(map[string]int, len(cfg.Weights))
	for _, w := range cfg.Weights {
		wByID[w.UpstreamID] = w.Weight
	}
	for _, c := range cands {
		if w, ok := wByID[c.ID]; ok {
			all = append(all, entry{id: c.ID, w: w})
			if healthy(c.ID) {
				healthyPool = append(healthyPool, entry{id: c.ID, w: w})
			}
		}
	}
	pool := all
	if len(healthyPool) > 0 {
		pool = healthyPool // 有健康候选：只在健康候选内按权重归一化选择
	}
	if len(pool) == 0 {
		// 配置引用的上游都不在兼容候选中：退化为默认最高版本。
		return highestHealthy(cands, healthy)
	}
	sum := 0
	for _, e := range pool {
		sum += e.w
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(idemKey))
	bucket := int(h.Sum32() % uint32(sum))
	for _, e := range pool {
		if bucket < e.w {
			return e.id
		}
		bucket -= e.w
	}
	return pool[len(pool)-1].id
}

// highestVersion 返回候选中版本最高的上游 ID。
func highestVersion(cands []Candidate) string {
	best := cands[0]
	for _, c := range cands[1:] {
		if versionx.Compare(c.Version, best.Version) > 0 {
			best = c
		}
	}
	return best.ID
}

// highestHealthy 优先在健康候选中取版本最高者；都不健康时取版本最高者。
func highestHealthy(cands []Candidate, healthy func(id string) bool) string {
	var best *Candidate
	for i := range cands {
		c := &cands[i]
		if !healthy(c.ID) {
			continue
		}
		if best == nil || versionx.Compare(c.Version, best.Version) > 0 {
			best = c
		}
	}
	if best == nil {
		return highestVersion(cands)
	}
	return best.ID
}

// PinOf 返回某业务键当前钉住的上游（测试与可观测用）。
func (r *Router) PinOf(idemKey string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id, ok := r.pins[idemKey]
	return id, ok
}

// ErrNoCandidates 预留：候选为空时的可区分错误。
var ErrNoCandidates = errors.New("split: no compatible candidates")
