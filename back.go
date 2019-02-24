package xrain

import "log"

const Mb = 1 << 30

type (
	Back interface {
		Load(off, len int64) []byte
		Size() int64
		Grow(size int64) error
		Truncate(size int64) error
		Sync() error
	}

	seg struct {
		off int64
		d   []byte
	}
	MemBack struct {
		s []seg
	}
)

func NewMemBack(size int64) *MemBack {
	return &MemBack{
		s: []seg{{d: make([]byte, size)}},
	}
}

func (b *MemBack) Load(off, l int64) []byte {
	i := 0
	for i < len(b.s) {
		s := b.s[i]
		if s.off <= off && off+l <= s.off+int64(len(s.d)) {
			break
		}
		i++
	}
	if i == len(b.s) {
		l := b.s[len(b.s)-1]
		log.Printf("i %d  last %x + %x", i, l.off, len(l.d))
		panic("out of range")
	}

	s := b.s[i]
	off -= s.off

	return s.d[off : off+l]
}

func (b *MemBack) Truncate(s int64) error {
	for {
		l := b.s[len(b.s)-1]
		if l.off >= s {
			b.s = b.s[:len(b.s)-1]
			continue
		}
		if l.off+int64(len(l.d)) > s {
			l.d = l.d[:s-l.off]
		}
		break
	}
	l := b.s[len(b.s)-1]
	if l.off+int64(len(l.d)) == s {
		return nil
	}

	lend := l.off + int64(len(l.d))
	b.s = append(b.s, seg{off: lend, d: make([]byte, s-lend)})
	return nil
}

func (b *MemBack) Grow(s int64) error {
	return b.Truncate(s)
}

func (b *MemBack) Size() int64 {
	l := b.s[len(b.s)-1]
	return l.off + int64(len(l.d))
}

func (b *MemBack) Sync() error {
	if len(b.s) == 1 {
		return nil
	}
	d := make([]byte, b.Size())
	for i := 0; i < len(b.s); i++ {
		s := b.s[i]
		copy(d[s.off:], s.d)
	}
	b.s = []seg{{d: d}}
	return nil
}
