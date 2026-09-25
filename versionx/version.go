// Package versionx 实现语义化版本协商与上游能力匹配。
//
// 兼容规则（详见仓库 docs/COMPATIBILITY.md）：
//   - 客户端版本必须可解析，且落在路由声明的 [MinSupported, MaxSupported] 窗口内；
//   - 上游与客户端必须同属一个 major（major 之间存在破坏性变更，绝不跨 major 转发）；
//   - 同一 major 内选择“满足全部所需能力且版本最高”的上游；
//   - 能力尚未提供（Since 晚于上游版本/未声明）判为能力缺失；
//     能力已被移除（Removed 或晚于 Until）判为语义不兼容；二者原因可区分。
package versionx

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Version 表示一个语义化版本（major.minor.patch）。
type Version struct {
	Major int
	Minor int
	Patch int
}

// Parse 把 "v1.2.3" / "1.2.3"（允许缺省 minor/patch）解析为 Version。
func Parse(s string) (Version, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Version{}, errors.New("empty version")
	}
	s = strings.TrimPrefix(s, "v")
	s = strings.TrimPrefix(s, "V")
	parts := strings.Split(s, ".")
	if len(parts) > 3 {
		return Version{}, fmt.Errorf("invalid version %q", s)
	}
	var nums [3]int
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return Version{}, fmt.Errorf("invalid version %q", s)
		}
		nums[i] = n
	}
	return Version{Major: nums[0], Minor: nums[1], Patch: nums[2]}, nil
}

// String 返回 vMAJOR.MINOR.PATCH 形式。
func (v Version) String() string {
	return fmt.Sprintf("v%d.%d.%d", v.Major, v.Minor, v.Patch)
}

// Compare 语义化比较：-1 表示 a<b，0 相等，1 表示 a>b。
func Compare(a, b Version) int {
	switch {
	case a.Major != b.Major:
		return boolCmp(a.Major > b.Major)
	case a.Minor != b.Minor:
		return boolCmp(a.Minor > b.Minor)
	case a.Patch != b.Patch:
		return boolCmp(a.Patch > b.Patch)
	default:
		return 0
	}
}

func boolCmp(greater bool) int {
	if greater {
		return 1
	}
	return -1
}

// Capability 描述上游声明的一项业务能力。
type Capability struct {
	Name    string
	Since   Version // 该能力从哪个版本开始提供
	Until   Version // 该能力在哪个版本（含）之后不再提供；零值表示仍提供
	Removed bool    // 是否被显式标记移除（语义不兼容）
}

// AvailableAt 判断该能力在给定上游版本上是否可用（未移除）。
// 返回 (provided, removed)：
//   - provided=true：能力在该版本正常提供；
//   - removed=true：能力曾存在但已被移除（语义不兼容）；
//   - 两者皆 false：能力尚未提供或从未声明（能力缺失）。
func (c Capability) AvailableAt(at Version) (provided bool, removed bool) {
	if Compare(at, c.Since) < 0 {
		return false, false
	}
	if c.Removed {
		return false, true
	}
	if c.Until != (Version{}) && Compare(at, c.Until) > 0 {
		return false, true
	}
	return true, false
}

// UpstreamDesc 描述一个上游适配器的版本与能力集合。
type UpstreamDesc struct {
	ID      string
	Version Version
	Caps    map[string]Capability
}

// DecisionReason 版本协商结论的可区分原因。
type DecisionReason string

// 协商结论原因。
const (
	ReasonOK                 DecisionReason = "ok"
	ReasonUnknownVersion     DecisionReason = "unknown_version"
	ReasonClientTooOld       DecisionReason = "client_too_old"
	ReasonClientTooNew       DecisionReason = "client_too_new"
	ReasonNoUpstreamForMajor DecisionReason = "no_upstream_for_major"
	ReasonCapabilityMissing  DecisionReason = "capability_missing"
	ReasonCapabilityRemoved  DecisionReason = "capability_removed_semantic_incompatible"
)

// RouteRule 描述一条路由的版本兼容窗口与所需能力。
type RouteRule struct {
	Route        string
	MinSupported Version // 零值表示不设下限
	MaxSupported Version // 零值表示不设上限
	RequiredCaps []string
}

// Accepts 判断客户端版本是否落在路由兼容窗口内，返回不满足时的原因。
func (r RouteRule) Accepts(client Version) (bool, DecisionReason) {
	if r.MinSupported != (Version{}) && Compare(client, r.MinSupported) < 0 {
		return false, ReasonClientTooOld
	}
	if r.MaxSupported != (Version{}) && Compare(client, r.MaxSupported) > 0 {
		return false, ReasonClientTooNew
	}
	return true, ""
}

// Decision 是版本协商的结论。
type Decision struct {
	Allowed  bool
	Reason   DecisionReason
	Selected *UpstreamDesc
	Detail   string
}

// Negotiator 根据路由规则在上游集合中做版本协商与能力匹配。
type Negotiator struct{}

// NewNegotiator 构造协商器。
func NewNegotiator() *Negotiator { return &Negotiator{} }

// Negotiate 执行协商。任何拒绝都带可区分原因，绝不静默转发。
func (n *Negotiator) Negotiate(ctx context.Context, client Version, rule RouteRule, upstreams []UpstreamDesc) Decision {
	if ok, reason := rule.Accepts(client); !ok {
		return Decision{
			Allowed: false,
			Reason:  reason,
			Detail: fmt.Sprintf("client %s outside route window [%s, %s]",
				client, verOrAny(rule.MinSupported), verOrAny(rule.MaxSupported)),
		}
	}

	// 仅同 major 候选可参与匹配。
	var sameMajor []UpstreamDesc
	for _, u := range upstreams {
		if u.Version.Major == client.Major {
			sameMajor = append(sameMajor, u)
		}
	}
	if len(sameMajor) == 0 {
		return Decision{
			Allowed: false,
			Reason:  ReasonNoUpstreamForMajor,
			Detail:  fmt.Sprintf("no upstream for major v%d", client.Major),
		}
	}

	// 在同 major 候选中找出满足全部能力的集合，取版本最高者。
	var best *UpstreamDesc
	sawRemoved := false
	sawCandidate := false
	for i := range sameMajor {
		u := &sameMajor[i]
		capsOK := true
		for _, capName := range rule.RequiredCaps {
			cap, ok := u.Caps[capName]
			if !ok {
				capsOK = false // 从未声明：缺失
				continue
			}
			provided, removed := cap.AvailableAt(u.Version)
			if removed {
				sawRemoved = true
				capsOK = false
				break
			}
			if !provided {
				capsOK = false
			}
		}
		if capsOK {
			sawCandidate = true
			if best == nil || Compare(u.Version, best.Version) > 0 {
				best = u
			}
		}
	}
	if best != nil {
		return Decision{
			Allowed:  true,
			Reason:   ReasonOK,
			Selected: best,
			Detail: fmt.Sprintf("client %s -> upstream %s %s (same major, caps %v satisfied)",
				client, best.ID, best.Version, rule.RequiredCaps),
		}
	}

	// 无兼容候选：区分语义不兼容与能力缺失。
	if sawRemoved {
		return Decision{
			Allowed: false,
			Reason:  ReasonCapabilityRemoved,
			Detail: fmt.Sprintf("required capability removed on a v%d upstream; refusing forward",
				client.Major),
		}
	}
	_ = sawCandidate
	return Decision{
		Allowed: false,
		Reason:  ReasonCapabilityMissing,
		Detail: fmt.Sprintf("no v%d upstream provides all required caps %v",
			client.Major, rule.RequiredCaps),
	}
}

func verOrAny(v Version) string {
	if v == (Version{}) {
		return "*"
	}
	return v.String()
}
