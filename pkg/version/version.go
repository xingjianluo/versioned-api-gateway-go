// Package version 负责语义版本解析与调用方版本协商。
package version

import (
	"fmt"
	"strconv"
	"strings"
)

// Version 为语义化版本号 major.minor.patch。
type Version struct {
	Major int
	Minor int
	Patch int
}

// Parse 解析语义版本字符串（x.y.z，允许省略 minor/patch）。
func Parse(s string) (Version, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Version{}, fmt.Errorf("empty version")
	}
	parts := strings.Split(s, ".")
	if len(parts) > 3 {
		return Version{}, fmt.Errorf("invalid version %q", s)
	}
	nums := [3]int{}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return Version{}, fmt.Errorf("invalid version %q", s)
		}
		nums[i] = n
	}
	return Version{Major: nums[0], Minor: nums[1], Patch: nums[2]}, nil
}

// MustParse 解析版本，失败时 panic（供测试与静态配置使用）。
func MustParse(s string) Version {
	v, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return v
}

// String 返回 x.y.z 形式。
func (v Version) String() string {
	return strconv.Itoa(v.Major) + "." + strconv.Itoa(v.Minor) + "." + strconv.Itoa(v.Patch)
}

// Compare 返回 -1/0/1。
func (v Version) Compare(o Version) int {
	switch {
	case v.Major != o.Major:
		return cmpInt(v.Major, o.Major)
	case v.Minor != o.Minor:
		return cmpInt(v.Minor, o.Minor)
	default:
		return cmpInt(v.Patch, o.Patch)
	}
}

func cmpInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// RejectReason 描述版本协商被拒绝的可区分原因。
type RejectReason string

const (
	RejectUnknownVersion    RejectReason = "unknown_version"     // 未知/非法版本
	RejectTooOld            RejectReason = "version_too_old"     // 调用方版本低于网关支持下限
	RejectCapabilityMissing RejectReason = "capability_missing"  // 无上游具备所需能力
	RejectSemanticConflict  RejectReason = "semantic_conflict"   // 存在能力但语义契约不兼容
	RejectNoHealthyUpstream RejectReason = "no_healthy_upstream" // 版本匹配但无健康上游
)

// NegotiateError 携带可区分的拒绝原因。
type NegotiateError struct {
	Reason  RejectReason
	Message string
}

func (e *NegotiateError) Error() string {
	return string(e.Reason) + ": " + e.Message
}

// Capability 描述一项业务能力及其语义契约代际。
type Capability struct {
	Name     string
	Semantic int // 语义代际；同一能力不同代际视为不兼容
}

// RequiredCapability 是调用方在某版本上要求的能力及语义代际约束。
type RequiredCapability struct {
	Name          string
	MinSemantic   int
	ExactSemantic bool // true 时必须精确匹配；false 时要求 >= MinSemantic
}

// Policy 描述网关的版本兼容规则。
type Policy struct {
	MinSupported Version
	Known        []Version
}

// Accept 判断调用方版本是否在网关支持范围内。
// 未知版本一律拒绝；低于 MinSupported 的旧版本一律拒绝，绝不静默转发。
func (p Policy) Accept(caller Version) error {
	known := false
	for _, k := range p.Known {
		if k.Compare(caller) == 0 {
			known = true
			break
		}
	}
	if !known {
		return &NegotiateError{
			Reason:  RejectUnknownVersion,
			Message: fmt.Sprintf("caller version %s is not a known API version", caller),
		}
	}
	if caller.Compare(p.MinSupported) < 0 {
		return &NegotiateError{
			Reason:  RejectTooOld,
			Message: fmt.Sprintf("caller version %s below min supported %s", caller, p.MinSupported),
		}
	}
	return nil
}

// SemanticCompatible 判断能力语义是否兼容。
// ExactSemantic 要求同代际；否则要求上游语义代际不低于调用方要求
// （新代际对旧调用方保持向后兼容）。
func SemanticCompatible(required RequiredCapability, offered Capability) bool {
	if required.Name != offered.Name {
		return false
	}
	if required.ExactSemantic {
		return offered.Semantic == required.MinSemantic
	}
	return offered.Semantic >= required.MinSemantic
}
