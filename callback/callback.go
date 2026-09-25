// Package callback 实现网关回调/通知投递链路。
//
// 投递保证（本地验证版）：
//   - 事件在任何投递尝试之前先持久化（JSON 文件），携带原始关联信息
//     （RequestID、IdemKey、Target、Payload），进程重启后 ListPending 可重放；
//   - 每个事件按 EventID 幂等登记，重复 Emit 不会产生第二条；
//   - 投递成功必须得到接收方确认才标记 delivered；超时/连接失败/5xx 等
//     “无法确认送达”的情况一律保持 pending 并退避重放，绝不伪装成功；
//   - 指数退避，次数受 MaxAttempts 限制；耗尽后置为 dead，停止无限重试，
//     保留现场供人工介入；
//   - 已 delivered 的事件重复重放为无害空操作（接收方亦按 EventID 去重）。
package callback

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Status 事件投递状态。
type Status string

// 事件状态。
const (
	StatusPending   Status = "pending"
	StatusDelivered Status = "delivered"
	StatusDead      Status = "dead"
)

// Event 是一个待投递的回调事件。
type Event struct {
	EventID     string `json:"event_id"`
	Target      string `json:"target"`
	RequestID   string `json:"request_id"` // 原始关联信息
	IdemKey     string `json:"idem_key"`   // 原始关联信息
	Payload     string `json:"payload"`
	Attempts    int    `json:"attempts"`
	MaxAttempts int    `json:"max_attempts"`
	Status      Status `json:"status"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

// 存储层错误。
var (
	ErrNotFound = errors.New("callback event not found")
)

// Store 是事件持久化接口。
type Store interface {
	Save(ctx context.Context, ev Event) error
	Get(ctx context.Context, eventID string) (Event, error)
	ListPending(ctx context.Context) ([]Event, error)
	Update(ctx context.Context, ev Event) error
}

// FileStore 基于 JSON 文件的本地持久化（每个事件一个文件，支持重启重放）。
type FileStore struct {
	dir string
	mu  sync.Mutex
}

// NewFileStore 构造文件存储，目录不存在则创建。
func NewFileStore(path string) (*FileStore, error) {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, fmt.Errorf("create callback store dir: %w", err)
	}
	return &FileStore{dir: path}, nil
}

func (s *FileStore) path(id string) string {
	return filepath.Join(s.dir, id+".json")
}

// Save 仅在事件不存在时写入（幂等登记的存储侧保障）。
func (s *FileStore) Save(ctx context.Context, ev Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.path(ev.EventID)
	if _, err := os.Stat(p); err == nil {
		return nil // 已存在：幂等忽略
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return s.writeLocked(ev)
}

// Update 覆盖写入事件。
func (s *FileStore) Update(ctx context.Context, ev Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeLocked(ev)
}

func (s *FileStore) writeLocked(ev Event) error {
	data, err := json.MarshalIndent(ev, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path(ev.EventID) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path(ev.EventID)) // 原子替换
}

// Get 读取事件。
func (s *FileStore) Get(ctx context.Context, id string) (Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path(id))
	if errors.Is(err, os.ErrNotExist) {
		return Event{}, ErrNotFound
	}
	if err != nil {
		return Event{}, err
	}
	var ev Event
	if err := json.Unmarshal(data, &ev); err != nil {
		return Event{}, err
	}
	return ev, nil
}

// ListPending 返回所有 pending 事件（按创建时间稳定排序，便于确定性重放）。
func (s *FileStore) ListPending(ctx context.Context) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var out []Event
	for _, en := range entries {
		if en.IsDir() || filepath.Ext(en.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.dir, en.Name()))
		if err != nil {
			return nil, err
		}
		var ev Event
		if err := json.Unmarshal(data, &ev); err != nil {
			return nil, err
		}
		if ev.Status == StatusPending {
			out = append(out, ev)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt != out[j].CreatedAt {
			return out[i].CreatedAt < out[j].CreatedAt
		}
		return out[i].EventID < out[j].EventID
	})
	return out, nil
}

// Deliverer 实际投递回调的目标接口。
type Deliverer interface {
	// Deliver 返回 nil 表示接收方已确认收到；返回错误表示“无法确认”，
	// 必须保持 pending 并稍后重放。
	Deliver(ctx context.Context, ev Event) error
}

// config 调度器配置。
type config struct {
	MaxAttempts int
	BaseBackoff time.Duration
	now         func() time.Time
}

// Option 调度器选项。
type Option func(*config)

// WithMaxAttempts 设置最大尝试次数（含首次）。
func WithMaxAttempts(n int) Option {
	return func(c *config) { c.MaxAttempts = n }
}

// WithBaseBackoff 设置退避基数（实际退避 = base * 2^(attempt-1)，封顶 8 倍）。
func WithBaseBackoff(d time.Duration) Option {
	return func(c *config) { c.BaseBackoff = d }
}

func withClock(f func() time.Time) Option {
	return func(c *config) { c.now = f }
}

// Dispatcher 调度回调投递与受限退避重放。
type Dispatcher struct {
	store Store
	del   Deliverer
	cfg   config
	mu    sync.Mutex
	// 正在投递的事件，避免同一事件被并发重复投递。
	inflight map[string]struct{}
}

// NewDispatcher 构造调度器。
func NewDispatcher(store Store, deliverer Deliverer, opts ...Option) *Dispatcher {
	cfg := config{MaxAttempts: 5, BaseBackoff: 100 * time.Millisecond, now: time.Now}
	for _, o := range opts {
		o(&cfg)
	}
	return &Dispatcher{store: store, del: deliverer, cfg: cfg,
		inflight: make(map[string]struct{})}
}

// Emit 登记一个回调事件（幂等：相同 EventID 不重复登记）。
func (d *Dispatcher) Emit(ctx context.Context, ev Event) error {
	if ev.EventID == "" {
		return errors.New("event id required")
	}
	now := d.cfg.now().UTC().Format(time.RFC3339Nano)
	ev.Attempts = 0
	if ev.MaxAttempts == 0 {
		ev.MaxAttempts = d.cfg.MaxAttempts
	}
	ev.Status = StatusPending
	ev.CreatedAt = now
	ev.UpdatedAt = now
	return d.store.Save(ctx, ev) // Save 对已存在事件幂等
}

// DeliverResult 描述单个事件的投递结果。
type DeliverResult struct {
	EventID  string
	Status   Status
	Attempts int
}

// DeliverPending 投递所有未确认事件（含重启后重放）。
// 它只尝试每事件“一轮”（带退避），返回每个事件的结论；
// 调用方可以周期性调用以继续重放，避免在单次调用内无限阻塞。
func (d *Dispatcher) DeliverPending(ctx context.Context) ([]DeliverResult, error) {
	pending, err := d.store.ListPending(ctx)
	if err != nil {
		return nil, err
	}
	var results []DeliverResult
	for _, ev := range pending {
		// 并发去重：同一事件同一时刻只允许一个投递协程。
		d.mu.Lock()
		if _, busy := d.inflight[ev.EventID]; busy {
			d.mu.Unlock()
			continue
		}
		d.inflight[ev.EventID] = struct{}{}
		d.mu.Unlock()

		res := d.deliverWithRetry(ctx, ev)

		d.mu.Lock()
		delete(d.inflight, ev.EventID)
		d.mu.Unlock()

		results = append(results, res)
	}
	return results, nil
}

func (d *Dispatcher) deliverWithRetry(ctx context.Context, ev Event) DeliverResult {
	for {
		if ev.Attempts >= ev.MaxAttempts {
			ev.Status = StatusDead
			ev.UpdatedAt = d.cfg.now().UTC().Format(time.RFC3339Nano)
			_ = d.store.Update(ctx, ev)
			return DeliverResult{EventID: ev.EventID, Status: StatusDead, Attempts: ev.Attempts}
		}

		err := d.del.Deliver(ctx, ev)
		ev.Attempts++
		ev.UpdatedAt = d.cfg.now().UTC().Format(time.RFC3339Nano)

		if err == nil {
			// 只有得到确认才标记成功。
			ev.Status = StatusDelivered
			_ = d.store.Update(ctx, ev)
			return DeliverResult{EventID: ev.EventID, Status: StatusDelivered, Attempts: ev.Attempts}
		}

		// 无法确认：持久化已增加的尝试次数，继续 pending。
		_ = d.store.Update(ctx, ev)

		if ev.Attempts >= ev.MaxAttempts {
			ev.Status = StatusDead
			ev.UpdatedAt = d.cfg.now().UTC().Format(time.RFC3339Nano)
			_ = d.store.Update(ctx, ev)
			return DeliverResult{EventID: ev.EventID, Status: StatusDead, Attempts: ev.Attempts}
		}
		// 退避；尊重 ctx 取消。退避期间不消耗投递确认。
		backoff := d.backoff(ev.Attempts)
		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			// 取消不等于确认：事件仍为 pending，重启后可重放。
			t.Stop()
			return DeliverResult{EventID: ev.EventID, Status: StatusPending, Attempts: ev.Attempts}
		case <-t.C:
		}
	}
}

func (d *Dispatcher) backoff(attempt int) time.Duration {
	mult := 1
	for i := 1; i < attempt && mult < 8; i++ {
		mult *= 2
	}
	return d.cfg.BaseBackoff * time.Duration(mult)
}

// StatusOf 查询事件当前持久化状态。
func (d *Dispatcher) StatusOf(ctx context.Context, id string) (Event, error) {
	return d.store.Get(ctx, id)
}
