package callback

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FileOutbox 以目录形式持久化通知：
// dir/pending/<key>.json、dir/delivered/<key>.json、dir/dead/<key>.json。
// 状态更新通过在子目录间移动文件完成；进程重启后从 pending 重放。
type FileOutbox struct {
	mu   sync.Mutex
	root string
}

// NewFileOutbox 打开（必要时创建）outbox 目录。
func NewFileOutbox(root string) (*FileOutbox, error) {
	for _, sub := range []string{string(Pending), string(Delivered), string(Dead)} {
		if err := os.MkdirAll(filepath.Join(root, sub), 0o755); err != nil {
			return nil, fmt.Errorf("create outbox dir: %w", err)
		}
	}
	return &FileOutbox{root: root}, nil
}

func statusDir(s Status) string {
	switch s {
	case Pending:
		return string(Pending)
	case Delivered:
		return string(Delivered)
	case Dead:
		return string(Dead)
	default:
		return string(Pending)
	}
}

func (o *FileOutbox) path(s Status, key string) string {
	return filepath.Join(o.root, statusDir(s), key+".json")
}

func (o *FileOutbox) writeFile(s Status, n Notification) error {
	data, err := json.MarshalIndent(n, "", "  ")
	if err != nil {
		return err
	}
	tmp := o.path(s, n.Key) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, o.path(s, n.Key))
}

// Append 新增通知；已存在同键记录则返回既有内容与 false 语义由 Manager 处理，
// 这里仅在不存在时落盘。
func (o *FileOutbox) Append(n Notification) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if existing, ok := o.getLocked(n.Key); ok {
		_ = existing
		return os.ErrExist
	}
	return o.writeFile(n.Status, n)
}

func (o *FileOutbox) getLocked(key string) (*Notification, bool) {
	for _, s := range []Status{Pending, Delivered, Dead} {
		data, err := os.ReadFile(o.path(s, key))
		if err == nil {
			var n Notification
			if err := json.Unmarshal(data, &n); err != nil {
				continue
			}
			return &n, true
		}
	}
	return nil, false
}

func (o *FileOutbox) Get(key string) (*Notification, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.getLocked(key)
}

// Update 覆盖通知并按其状态移动到对应子目录。
func (o *FileOutbox) Update(n Notification) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	// 删除旧位置文件（可能在任一状态目录）。
	for _, s := range []Status{Pending, Delivered, Dead} {
		p := o.path(s, n.Key)
		if _, err := os.Stat(p); err == nil {
			if err := os.Remove(p); err != nil {
				return err
			}
		}
	}
	return o.writeFile(n.Status, n)
}

func (o *FileOutbox) listLocked(s Status) ([]Notification, error) {
	entries, err := os.ReadDir(filepath.Join(o.root, statusDir(s)))
	if err != nil {
		return nil, err
	}
	var out []Notification
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(o.root, statusDir(s), e.Name()))
		if err != nil {
			continue
		}
		var n Notification
		if err := json.Unmarshal(data, &n); err != nil {
			continue
		}
		out = append(out, n)
	}
	return out, nil
}

func (o *FileOutbox) List(s Status) ([]Notification, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.listLocked(s)
}

func (o *FileOutbox) ListAll() ([]Notification, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var all []Notification
	for _, s := range []Status{Pending, Delivered, Dead} {
		ns, err := o.listLocked(s)
		if err != nil {
			return nil, err
		}
		all = append(all, ns...)
	}
	return all, nil
}
