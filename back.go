package xrain

import "sync"

const Mb = 1 << 30

type (
	Back interface {
		Access(off, len int64, f func(p []byte))
		Copy(roff, off, len int64) error
		Size() int64
		Truncate(size int64) error
		Sync() error
	}

	MemBack struct {
		mu sync.RWMutex
		d  []byte
	}
)

func NewMemBack(size int64) *MemBack {
	return &MemBack{
		d: make([]byte, size),
	}
}

func (b *MemBack) Access(off, l int64, f func(p []byte)) {
	defer b.mu.RUnlock()
	b.mu.RLock()

	f(b.d[off : off+l])
}

func (b *MemBack) Copy(roff, off, len int64) error {
	defer b.mu.RUnlock()
	b.mu.RLock()

	copy(b.d[roff:], b.d[off:off+len])

	return nil
}

func (b *MemBack) Truncate(s int64) error {
	defer b.mu.Unlock()
	b.mu.Lock()

	if cap(b.d) >= int(s) {
		b.d = b.d[:s]
		return nil
	}
	c := make([]byte, s)
	copy(c, b.d)
	b.d = c
	return nil
}

func (b *MemBack) Size() int64 {
	defer b.mu.RUnlock()
	b.mu.RLock()

	return int64(len(b.d))
}

func (b *MemBack) Sync() error {
	return nil
}
