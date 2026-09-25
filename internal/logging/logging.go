// Package logging 提供网关内部统一的结构化决策日志。
//
// 本地验证时使用 StdLogger，把每次请求的关键决策按行打印为
// key=value 形式，至少包含：
//   - request_id：请求标识；
//   - stage/decision：所处环节与结论；
//   - version：版本协商依据；
//   - quota：限流（调用方层/路由层）依据；
//   - breaker：熔断状态依据；
//   - retry：重试次数。
package logging

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Logger 是最小日志接口。
type Logger interface {
	Log(level, msg string, fields map[string]any)
}

// NopLogger 丢弃所有日志。
type NopLogger struct{}

// Log 实现 Logger，空操作。
func (NopLogger) Log(level, msg string, fields map[string]any) {}

// StdLogger 把日志写到指定 io.Writer（默认 stderr），并发安全。
type StdLogger struct {
	mu sync.Mutex
	w  io.Writer
}

// NewStdLogger 构造标准输出日志器。
func NewStdLogger(w io.Writer) *StdLogger {
	if w == nil {
		w = os.Stderr
	}
	return &StdLogger{w: w}
}

// Log 输出一行结构化日志。
func (l *StdLogger) Log(level, msg string, fields map[string]any) {
	var sb strings.Builder
	sb.WriteString(time.Now().Format("15:04:05.000"))
	sb.WriteString(" ")
	sb.WriteString(level)
	sb.WriteString(" ")
	sb.WriteString(msg)

	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		sb.WriteString(" ")
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(fmt.Sprintf("%v", fields[k]))
	}
	sb.WriteString("\n")

	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = io.WriteString(l.w, sb.String())
}
