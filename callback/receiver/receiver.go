// Package receiver 是回调接收方参考实现：按 EventID 去重，
// 用于本地验证重复投递/乱序返回场景。
//
// 语义：
//   - 首次见到某 EventID：真正处理一次（handleCount++），记录首达负载；
//   - 再次见到同一 EventID：标记 duplicate=true，直接返回首次结果，
//     不再执行业务处理；
//   - 记录每个事件首次到达的次序，可用于断言“乱序返回”下处理仍正确。
package receiver

import (
	"context"
	"sync"
)

// Receiver 记录每个 EventID 的首次投递。
type Receiver struct {
	mu    sync.Mutex
	first map[string]string // eventID -> 首次负载
	order []string          // 首次到达次序
}

// New 构造接收方。
func New() *Receiver {
	return &Receiver{first: make(map[string]string)}
}

// Receive 幂等接收。
// 返回 duplicate=true 表示这是重复投递（未重复处理业务）。
func (r *Receiver) Receive(ctx context.Context, eventID, payload string) (duplicate bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, seen := r.first[eventID]; seen {
		return true, nil
	}
	r.first[eventID] = payload
	r.order = append(r.order, eventID)
	return false, nil
}

// SeenCount 返回已去重处理过的不同事件数。
func (r *Receiver) SeenCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.first)
}

// HandleCount 与 SeenCount 等价：业务真正被处理的次数。
func (r *Receiver) HandleCount() int { return r.SeenCount() }

// FirstPayload 返回某事件首次投递的负载。
func (r *Receiver) FirstPayload(eventID string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.first[eventID]
	return p, ok
}

// Order 返回事件首次到达的次序（用于乱序断言）。
func (r *Receiver) Order() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.order))
	copy(out, r.order)
	return out
}
