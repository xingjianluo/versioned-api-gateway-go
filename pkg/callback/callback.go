// Package callback 实现网关回调/通知投递链路：
// 先落盘 outbox 再投递（at-least-once），按通知键与单调序号去重，
// 乱序返回以序号判定新旧，进程重启后可重放；
// 超过有界重试次数进入死信，绝不把"未确认"伪装为成功。
package callback

import (
	"context"
	"sync"
	"time"
)

// Status 为通知投递状态。
type Status string

const (
	Pending   Status = "pending"
	Delivered Status = "delivered"
	Dead      Status = "dead" // 超过重试上限，等待人工/外部介入
)

// Notification 是一条待投递回调。
type Notification struct {
	Key         string // 业务幂等键（同一键重复投递只执行一次）
	Seq         int64  // 同一键内的单调序号，用于乱序返回去重
	Correlation string // 原始关联信息（请求 ID/业务 ID），重放时保留
	URL         string
	Payload     []byte
	Attempts    int
	Status      Status
	LastError   string
	UpdatedAt   time.Time
}

// Deliverer 是实际投递通道（本地验证用内存/HTTP 假实现）。
// 返回 nil 表示对端已确认；error 表示未确认，必须保留为待投递。
type Deliverer interface {
	Deliver(ctx context.Context, n Notification) error
}

// Outbox 是通知的持久化存储（本地用文件 JSONL，进程重启后可恢复）。
type Outbox interface {
	Append(n Notification) error
	Get(key string) (*Notification, bool)
	Update(n Notification) error
	List(status Status) ([]Notification, error)
	ListAll() ([]Notification, error)
}

// RetryPolicy 为有界重放策略。
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// Manager 负责去重、投递确认、有界重放与死信。
type Manager struct {
	mu        sync.Mutex
	outbox    Outbox
	deliverer Deliverer
	policy    RetryPolicy
	inflight  map[string]struct{}
	now       func() time.Time
}

// NewManager 创建回调管理器。
func NewManager(outbox Outbox, d Deliverer, p RetryPolicy) *Manager {
	if p.MaxAttempts < 1 {
		p.MaxAttempts = 1
	}
	return &Manager{
		outbox:    outbox,
		deliverer: d,
		policy:    p,
		inflight:  make(map[string]struct{}),
		now:       time.Now,
	}
}

// Enqueue 登记一条通知。
// 幂等规则：
//   - 同键同序号（或序号不高于已送达序号）视为重复投递，直接返回既有记录，
//     不会产生第二次投递；
//   - 同键更大序号视为新通知，覆盖待投递内容（旧序号的迟到确认将被忽略）。
//
// 返回记录与 true 表示这是一条新登记的通知。
func (m *Manager) Enqueue(n Notification) (*Notification, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	n.Status = Pending
	n.UpdatedAt = m.now()

	if existing, ok := m.outbox.Get(n.Key); ok {
		if n.Seq <= existing.Seq {
			// 重复或更旧的通知：幂等忽略，保留原始关联信息。
			return existing, false, nil
		}
		// 新序号：继承已用尝试次数中的状态，但作为新内容重新投递。
		n.Status = Pending
		if err := m.outbox.Update(n); err != nil {
			return nil, false, err
		}
		return &n, true, nil
	}

	if err := m.outbox.Append(n); err != nil {
		return nil, false, err
	}
	return &n, true, nil
}

// backoff 计算第 attempt 次失败后的退避。
func (p RetryPolicy) backoff(attempt int) time.Duration {
	d := p.BaseDelay
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= p.MaxDelay {
			d = p.MaxDelay
			break
		}
	}
	return d
}

// DeliverOnce 在有界次数内尝试投递一条通知。
//
// 保证：
//   - 每次尝试前先持久化 Attempts，进程在投递途中崩溃后重放仍可追溯，
//     且累计尝试次数不会因重启清零（避免无限重试）；
//   - 只有 Deliverer 返回 nil（对端确认）才标记 delivered；
//   - 未确认时保持 pending 或在超过上限后转 dead，绝不返回成功；
//   - 同键并发调用只有一方真正投递（inflight 互斥）。
func (m *Manager) DeliverOnce(ctx context.Context, key string) (bool, error) {
	if !m.tryLock(key) {
		// 已有投递在途：本次不重复发送；查询当前状态供调用方判断。
		if n, ok := m.outbox.Get(key); ok && n.Status == Delivered {
			return true, nil
		}
		return false, nil
	}
	defer m.unlock(key)

	n, ok := m.outbox.Get(key)
	if !ok {
		return false, &UnknownKeyError{Key: key}
	}
	if n.Status == Delivered {
		return true, nil
	}
	if n.Status == Dead {
		return false, &DeadLetteredError{Key: key, Correlation: n.Correlation}
	}

	for {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		// 达到有界上限：转死信，避免无限重试。
		if n.Attempts >= m.policy.MaxAttempts {
			n.Status = Dead
			n.UpdatedAt = m.now()
			if n.LastError == "" {
				n.LastError = "max attempts reached without acknowledgement"
			}
			if err := m.outbox.Update(*n); err != nil {
				return false, err
			}
			return false, &DeadLetteredError{Key: key, Correlation: n.Correlation}
		}

		n.Attempts++
		n.UpdatedAt = m.now()
		// 先落盘尝试次数再发送：崩溃后重放不会"忘记"已用掉的次数。
		if err := m.outbox.Update(*n); err != nil {
			return false, err
		}

		err := m.deliverer.Deliver(ctx, *n)
		if err == nil {
			// 乱序保护：若已有更大序号被确认，本次确认是迟到确认，
			// 不回退状态。
			if cur, ok := m.outbox.Get(key); ok && cur.Status == Delivered && cur.Seq > n.Seq {
				return true, nil
			}
			n.Status = Delivered
			n.LastError = ""
			n.UpdatedAt = m.now()
			if uerr := m.outbox.Update(*n); uerr != nil {
				return false, uerr
			}
			return true, nil
		}

		n.LastError = err.Error()
		n.UpdatedAt = m.now()
		_ = m.outbox.Update(*n)

		if n.Attempts >= m.policy.MaxAttempts {
			n.Status = Dead
			n.UpdatedAt = m.now()
			if derr := m.outbox.Update(*n); derr != nil {
				return false, derr
			}
			return false, &DeadLetteredError{Key: key, Correlation: n.Correlation}
		}

		// 有界退避后重试；等待可被取消，不会无限自旋。
		timer := time.NewTimer(m.policy.backoff(n.Attempts))
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		case <-timer.C:
		}
	}
}

func (m *Manager) tryLock(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, busy := m.inflight[key]; busy {
		return false
	}
	m.inflight[key] = struct{}{}
	return true
}

func (m *Manager) unlock(key string) {
	m.mu.Lock()
	delete(m.inflight, key)
	m.mu.Unlock()
}

// ReplayPending 重放全部 pending（用于进程重启恢复），并发受控。
// 未确认者保持 pending，可再次重放；死信计入报告，保证可追溯。
func (m *Manager) ReplayPending(ctx context.Context, workers int) (ReplayReport, error) {
	if workers < 1 {
		workers = 1
	}
	pending, err := m.outbox.List(Pending)
	if err != nil {
		return ReplayReport{}, err
	}
	report := ReplayReport{Scanned: len(pending)}

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := range pending {
		key := pending[i].Key
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			ok, err := m.DeliverOnce(ctx, key)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case ok:
				report.Delivered++
			default:
				if isDeadLettered(err) {
					report.Dead++
				} else {
					report.StillPending++
				}
			}
		}()
	}
	wg.Wait()
	return report, nil
}

// ReplayReport 汇总重放结果，保证可追溯。
type ReplayReport struct {
	Scanned      int
	Delivered    int
	StillPending int
	Dead         int
}

// UnknownKeyError 表示重放了一个不存在的键。
type UnknownKeyError struct{ Key string }

func (e *UnknownKeyError) Error() string { return "callback: unknown notification key: " + e.Key }

// DeadLetteredError 表示通知已耗尽有界重试，进入死信。
type DeadLetteredError struct {
	Key         string
	Correlation string
}

func (e *DeadLetteredError) Error() string {
	return "callback: notification dead-lettered key=" + e.Key + " correlation=" + e.Correlation
}

func isDeadLettered(err error) bool {
	_, ok := err.(*DeadLetteredError)
	return ok
}
