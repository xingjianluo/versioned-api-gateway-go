// Package idempotency 提供进程内幂等记录，保证同一业务键不被重复执行，
// 并发重复请求共享同一结果（等待 in-flight 记录）。
package idempotency

import (
	"sync"
	"time"
)

// Status 为幂等记录状态。
type Status string

const (
	InFlight Status = "in_flight"
	Complete Status = "complete"
	Failed   Status = "failed"
)

// Record 是一次业务操作的幂等记录。
type Record struct {
	Key        string
	Status     Status
	Response   []byte
	ErrKind    string
	FinishedAt time.Time
	done       chan struct{}
}

// Done 在记录终结时关闭。
func (r *Record) Done() <-chan struct{} { return r.done }

// Store 为键 -> 记录的并发安全存储。
type Store struct {
	mu      sync.Mutex
	records map[string]*Record
}

// NewStore 创建幂等存储。
func NewStore() *Store {
	return &Store{records: make(map[string]*Record)}
}

// StartOrGet 尝试登记一个新操作；若已存在则返回已有记录与 false。
// 已存在且为 Complete 时调用方可直接复用结果，绝不重复执行业务；
// InFlight 时调用方等待其 Done。
func (s *Store) StartOrGet(key string) (*Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.records[key]; ok {
		return r, false
	}
	r := &Record{Key: key, Status: InFlight, done: make(chan struct{})}
	s.records[key] = r
	return r, true
}

// Complete 标记操作成功完成并保存结果。
func (s *Store) Complete(key string, resp []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[key]
	if !ok {
		r = &Record{Key: key, done: make(chan struct{})}
		s.records[key] = r
	}
	r.Status = Complete
	r.Response = resp
	r.FinishedAt = time.Now()
	close(r.done)
}

// Fail 标记操作失败。
// 失败记录从存储中移除：这样在"未确认上游是否执行"的失败（如超时）
// 之外的明确失败，后续携带同键的新请求可以重新执行；而超时是否重放
// 由调用方依据操作幂等性决定（网关仅对幂等操作重试）。
func (s *Store) Fail(key, errKind string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[key]
	if !ok {
		return
	}
	r.Status = Failed
	r.ErrKind = errKind
	r.FinishedAt = time.Now()
	close(r.done)
	delete(s.records, key)
}

// Get 查询记录（不存在返回 nil）。
func (s *Store) Get(key string) *Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.records[key]
}
