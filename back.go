package xrain

import (
	"sync"
)

type (
	Back interface {
		Access(off, len int64) []byte
		Access2(off, len, off2, len2 int64) (p, p2 []byte)
		Unlock(p []byte)
		Unlock2(p, p2 []byte)

		//	Copy(roff, loff, len int64)

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

func (b *MemBack) Access(off, l int64) []byte {
	b.mu.RLock()

	if off == NilPage || l == 0 {
		return nil
	}

	return b.d[off : off+l : off+l]
}

func (b *MemBack) Access2(off, l, off2, l2 int64) (p, p2 []byte) {
	b.mu.RLock()

	if off != NilPage && l != 0 {
		p = b.d[off : off+l : off+l]
	}

	if off2 != NilPage && l2 != 0 {
		p2 = b.d[off2 : off2+l2 : off2+l2]
	}

	return
}

func (b *MemBack) Unlock(p []byte) {
	b.mu.RUnlock()
}

func (b *MemBack) Unlock2(p, p2 []byte) {
	b.mu.RUnlock()
}

func (b *MemBack) Copy(roff, off, len int64) {
	defer b.mu.RUnlock()
	b.mu.RLock()

	copy(b.d[roff:], b.d[off:off+len])
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
