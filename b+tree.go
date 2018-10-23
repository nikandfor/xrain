package xrain

import (
	"sort"
)

type (
	bptree struct {
		b Back

		free int64

		root int64
		page int64

		err error
	}

	keylink int64 // 1 byte is an overflow indicator, 51 bytes is for page offset, 12 bytes is for index at page
)

func (t *bptree) Put(k, v []byte) {
	var s [32]keylink

	kl := keylink(t.root)

	var d, i int
	var leaf bool
	var p []byte
	for !leaf {
		s[d] = kl
		d++

		p, leaf = t.pageread(kl.Off())

		i, _ = t.search(p, k)

		kl = keylink(t.pagelink(p, i) | int64(i))
	}
	// we at leaf
	off := s[d-1].Off()

	loff, roff, l, r := t.pageput(off, p, i, k, v)
	_, _ = loff, roff
	_, _ = l, r
}

func (t *bptree) search(p []byte, k []byte) (int, bool) {
	ln := t.pagesize(p)
	var cmp int
	i := sort.Search(ln, func(i int) bool {
		cmp = t.pagekeycmp(p, i, k)
		return cmp <= 0
	})
	return i, cmp == 0
}

func (l keylink) Off() int64 {
	return int64(l) & 0xfff
}
