package xrain

import (
	"bytes"
	"encoding/binary"
)

/*
	BigEndian

	type page {
		h   int8
		n   int8
		off []int16
		_ []byte
		struct {
			keylen byte
			key []byte
			val []byte
		}
	}
*/

const (
	pHead = 2
	pLink = 2
)

const (
	fLeaf = 1 << iota
)

func (t *bptree) pageput(off int64, p []byte, i int, k, v []byte) (loff, roff int64, l, r []byte) {
	n, sp := t.pagesizespace(p)
	if sp < 3+len(k)+len(v) {
		m := n / 2

		loff, l = t.pagewrite(off)
		roff, r = t.pagealloc()
		if i <= m {
			t.pagemove(r, p, 0, m, n)
			t.pagesetsize(r, n-m)

			if loff == off {
				t.pagesetsize(l, m)
				t.pageinsert(l, i, k, v)
			} else {
				t.pagemove(l, p, 0, 0, i)
				t.pageinsert(l, i, k, v)
				t.pagemove(l, p, i+1, i, m)
				t.pagesetsize(l, m+1)
			}
		} else {
			t.pagemove(r, p, 0, m, i)
			t.pageinsert(r, i-m, k, v)
			t.pagemove(r, p, i+1, i, n)
			t.pagesetsize(r, n-m+1)

			if loff == off {
				t.pagesetsize(l, m)
			} else {
				t.pagemove(l, p, 0, i, m)
				t.pagesetsize(l, m)
			}
		}
	} else {
		loff, l = t.pagewrite(off)
		if loff == off {
			t.pageinsert(l, i, k, v)
		} else {
			t.pagemove(l, p, 0, 0, i)
			t.pageinsert(l, i, k, v)
			t.pagemove(l, p, i+1, i, n)
			t.pagesetsize(l, n+1)
		}
	}
	return
}

func (t *bptree) pagealloc() (int64, []byte) {
	off := t.free
	p, err := t.b.Read(off, t.page)
	t.err = err
	t.free += t.page
	return off, p
}

func (t *bptree) pagewrite(off int64) (int64, []byte) {
	p, err := t.b.Read(off, t.page)
	t.err = err
	return off, p
}

func (t *bptree) pageread(off int64) ([]byte, bool) {
	p, err := t.b.Read(off, t.page)
	t.err = err
	return p, t.pageflag(p, fLeaf)
}

func (t *bptree) pagemove(r, s []byte, ri, im, iM int) {
	if im == iM {
		return
	}

	n := iM - im

	off := t.offget(s, iM-1)
	end := t.offget(s, im-1)
	sz := end - off
	dst := t.offget(r, ri-1) - sz

	copy(r[dst:], s[off:end])

	diff := dst - off
	for i := 0; i < n; i++ {
		sh := t.offget(s, im+i)
		t.offset(r, ri+i, sh+diff)
	}
}

func (t *bptree) pageinsert(p []byte, i int, k, v []byte) {
	end := t.offget(p, i-1)
	size := 1 + len(k) + len(v)
	n := t.pagesize(p)

	st := t.offget(p, n-1)
	copy(p[st-size:], p[st:end])

	for j := n; j > i; j-- {
		sh := t.offget(p, j-1)
		t.offset(p, j, sh-size)
	}

	st = end - size
	t.offset(p, i, st)

	p[st] = byte(len(k))
	st++
	copy(p[st:], k)
	copy(p[st+len(k):], v)

	t.pagesetsize(p, n+1)
}

func (t *bptree) pagekey(p []byte, i int) []byte {
	st := t.offget(p, i)
	kl := int(p[st])
	st++
	return p[st : st+kl]
}

func (t *bptree) pagevalue(p []byte, i int) []byte {
	st := t.offget(p, i)
	end := t.offget(p, i-1)
	kl := int(p[st])
	return p[st+1+kl : end]
}

func (t *bptree) pagekeyvalue(p []byte, i int) (k, v []byte) {
	st := t.offget(p, i)
	end := t.offget(p, i-1)
	kl := int(p[st])
	st++
	return p[st : st+kl], p[st+kl : end]
}

func (t *bptree) pagelink(p []byte, i int) int64 {
	start := t.offget(p, i)
	ksize := int(p[start])
	v := binary.BigEndian.Uint64(p[start+ksize:])
	return int64(v)
}

func (t *bptree) pagesize(p []byte) int {
	return int(p[1])
}

func (t *bptree) pagekeycmp(p []byte, i int, k []byte) int {
	off := t.offget(p, i)
	l := int(p[off])
	off++
	return bytes.Compare(p[off:off+l], k)
}

func (t *bptree) pagesizespace(p []byte) (int, int) {
	if p == nil {
		return 0, int(t.page) - pHead
	}
	n := int(p[1])
	start := t.offidx(n)
	end := t.offget(p, n-1)
	return n, end - start
}

func (t *bptree) pagesetsize(p []byte, n int) {
	p[1] = byte(n)
}

func (t *bptree) pageflag(p []byte, f int) bool {
	return int(p[0])&f == f
}

func (t *bptree) offidx(i int) int {
	return pHead + i*pLink
}

func (t *bptree) offset(p []byte, i, off int) {
	s := pHead + i*pLink
	p[s] = byte(off >> 8)
	p[s+1] = byte(off)
}

func (t *bptree) offget(p []byte, i int) int {
	if i == -1 {
		return int(t.page)
	}
	s := pHead + i*pLink
	return int(p[s])<<8 | int(p[s+1])
}
