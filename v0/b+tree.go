package xrain

import (
	"bytes"
	"sort"
)

const (
	FlagIsLeaf = 1 << iota
)

type (
	Memory interface {
		Read(off, size int64) []byte
		Alloc(size int64) (int64, []byte)
		Write(off int64, p []byte)
	}
	BPTree struct {
		mem      Memory
		root     int64
		pagesize int64
	}

	Page []byte
)

func (p Page) IsFlag(f int) bool {
	h := int(p[0])
	return h&f == f
}

func (p Page) Size() int {
	return (int)(p[1])
}

func (p Page) Offset(i int) int {
	return p.int16(1 + 1 + 2*i)
}

func (p Page) Varlen(j int) (int, int) {
	x := 0
	s := uint(0)
	for i, b := range p[j:] {
		if b < 0x80 {
			if i > 9 || i == 9 && b > 1 {
				return 0, -(i + 1)
			}
			x |= (int)(b) << s
			return x, i + 1
		}
		x |= (int)(b&0x7f) << s
		s += 7
	}
	return 0, 0
}

func (p Page) Cmp(i int, x []byte) int {
	s := p.Offset(i)
	kl, ll := p.Varlen(s)
	ks := s + ll
	kn := ks + kl
	return bytes.Compare(p[ks:kn], x)
}

func (p Page) KeyValue(i int) ([]byte, []byte) {
	s := p.Offset(i)
	kl, ll := p.Varlen(s)
	ks := s + ll
	kn := ks + kl
	vn := p.Offset(i + 1)
	return p[ks:kn], p[kn:vn]
}

// int16 decodes LittleEndian int16 number starting from i-th byte
func (p Page) int16(i int) int {
	return (int)(p[i]) + (int)(p[i+1])<<8
}

func (t *BPTree) Get(k []byte) []byte {
	p := Page(t.mem.Read(t.root, t.pagesize))

	for !p.IsFlag(FlagIsLeaf) {
		n := p.Size()
		i := sort.Search(n, func(i int) bool {
			return p.Cmp(i, k) > 0
		})
		i--
		_, v := p.KeyValue(i)
		off := bytesToInt64(v)

		p = Page(t.mem.Read(off, t.pagesize))
	}

	n := p.Size()
	i := sort.Search(n, func(i int) bool {
		return p.Cmp(i, k) >= 0
	})
	if i == n || p.Cmp(i, k) != 0 {
		return nil
	}

	_, v := p.KeyValue(i)
	return v
}

func (t *BPTree) Set(k, v []byte) {
	t.setAt(t.root, k, v)
}

func (t *BPTree) setAt(off int64, k, v []byte) {
	p := Page(t.mem.Read(off, t.pagesize))

	if !p.IsFlag(FlagIsLeaf) {
	}

}

func bytesToInt64(v []byte) int64 {
	return (int64)(v[0]) + (int64)(v[1])<<8 + (int64)(v[2])<<16 + (int64)(v[3])<<24 +
		(int64)(v[4])<<32 + (int64)(v[5])<<40 + (int64)(v[6])<<48 + (int64)(v[7])<<56
}
