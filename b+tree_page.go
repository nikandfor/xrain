package xrain

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
	fBranch = 1 << iota
)

func (t *tree) pageput(off int64, p []byte, i int, k, v []byte) (loff, roff int64, l, r []byte, err error) {
	n, sp := t.pagesizespace(p)
	if sp < 3+len(k)+len(v) {
		m := n / 2

		loff, l, err = t.a.Write(off, nil)
		if err != nil {
			return
		}
		roff, r, err = t.a.Alloc()
		if err != nil {
			return
		}
		if f := t.pageflags(p); true {
			t.pagesetflag(l, f)
			t.pagesetflag(r, f)
		}
		if i <= m {
			t.pagemove(r, p, 0, m, n)
			t.pagesetsize(r, n-m)

			if loff == off {
				t.pagesetsize(l, m)
				t.pageinsert(l, i, k, v)
			} else {
				t.pagemove(l, p, 0, 0, i)
				t.pagesetsize(l, i)
				t.pageinsert(l, i, k, v)
				t.pagemove(l, p, i+1, i, m)
				t.pagesetsize(l, m+1)
			}
		} else {
			t.pagemove(r, p, 0, m, i)
			t.pagesetsize(r, i-m)
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
		loff, l, err = t.a.Write(off, nil)
		if err != nil {
			return
		}
		if loff == off {
			t.pageinsert(l, i, k, v)
		} else {
			t.pagemove(l, p, 0, 0, i)
			t.pagesetsize(l, i)
			t.pageinsert(l, i, k, v)
			t.pagemove(l, p, i+1, i, n)
			t.pagesetsize(l, n+1)
		}
	}
	return
}

func (t *tree) pagedel(off int64, p []byte, i int) (loff int64, l []byte, reb bool, err error) {
	loff, l, err = t.a.Write(off, p)
	if err != nil {
		return
	}
	n := t.pagesize(l)
	if loff == off {
		t.pageuninsert(l, i)
	} else {
		t.pagemove(l, p, 0, 0, i)
		t.pagemove(l, p, i, i+1, n)
		t.pagesetsize(l, n-1)
	}
	reb = t.pageneedrebalance(p)
	return
}

func (t *tree) pagerebalance(loff, roff int64, l, r []byte) (lwoff, rwoff int64, lw, rw []byte, err error) {
	if l == nil {
		l, err = t.a.Read(loff)
		if err != nil {
			return
		}
	}
	if r == nil {
		r, err = t.a.Read(roff)
		if err != nil {
			return
		}
	}

	ln, lsp := t.pagesizespace(l)
	rn, rsp := t.pagesizespace(r)

	lwoff, lw, err = t.a.Write(loff, l)
	if err != nil {
		return
	}
	if lwoff != loff {
		f := t.pageflags(l)
		t.pagesetflag(lw, f)
	}

	if lsp+rsp+pHead >= len(l) {
		if lwoff != loff {
			t.pagemove(lw, l, 0, 0, ln)
		}
		t.pagemove(lw, r, ln, 0, rn)
		t.pagesetsize(lw, ln+rn)
		return
	}

	rwoff, rw, err = t.a.Write(roff, r)
	if err != nil {
		return
	}
	if rwoff != roff {
		f := t.pageflags(r)
		t.pagesetflag(rw, f)
	}

	if lsp < rsp {
		// l -> r
		i := ln
		for lsp < rsp {
			kvsize := t.pagekeyvaluesize(l, i-1) + 2
			if lsp+kvsize >= rsp-kvsize {
				break
			}
			lsp += kvsize
			rsp -= kvsize
			i--
		}
		for j := i; j < ln; j++ {
			k, v := t.pagekeyvalue(l, j)
			t.pageinsert(rw, j-i, k, v)
		}

		if rwoff != roff {
			t.pagemove(rw, r, ln-i, 0, rn)
			t.pagesetsize(rw, ln-i+rn)
		}

		if lwoff != loff {
			t.pagemove(lw, l, 0, 0, i)
		}
		t.pagesetsize(lw, i)
	} else {
		// r -> l
		i := 0
		for lsp > rsp {
			kvsize := t.pagekeyvaluesize(r, i) + 2
			if rsp+kvsize >= lsp-kvsize {
				break
			}
			rsp += kvsize
			lsp -= kvsize
			i++
		}

		if lwoff != loff {
			t.pagemove(lw, l, 0, 0, ln)
		}
		t.pagemove(lw, r, ln, 0, i)
		t.pagesetsize(lw, ln+i)

		t.pagemove(rw, r, 0, i, rn)
		t.pagesetsize(rw, rn-i)
	}
	return
}

func (t *tree) pagesiblings(off int64, p []byte, i int) (li, ri int, loff, roff int64) {
	n := t.pagesize(p)
	if i < n-1 && i%2 == 0 {
		return i, i + 1, t.pagelink(p, i), t.pagelink(p, i+1)
		//	return t.pagelink(p, i), t.pagelink(p, i+1), nil, nil, nil
	} else {
		return i - 1, i, t.pagelink(p, i-1), t.pagelink(p, i)
		//	return t.pagelink(p, i-1), t.pagelink(p, i), nil, nil, nil
	}
}

func (t *tree) pagemove(r, s []byte, ri, im, iM int) {
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

func (t *tree) pageinsert(p []byte, i int, k, v []byte) {
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

func (t *tree) pageuninsert(p []byte, i int) {
	n := t.pagesize(p)
	st := t.offget(p, n-1)
	end := t.offget(p, i)
	dst := t.offget(p, i-1) - (end - st)
	copy(p[dst:], p[st:end])
	n--

	diff := dst - st
	for j := i; j < n; j++ {
		sh := t.offget(p, j+1)
		t.offset(p, j, sh+diff)
	}

	t.pagesetsize(p, n)
}

func (t *tree) pageneedrebalance(p []byte) bool {
	_, sp := t.pagesizespace(p)
	return sp > len(p)/2
}

func (t *tree) pagelastkey(p []byte) []byte {
	s := t.pagesize(p)
	if s == 0 {
		return nil
	}
	return t.pagekey(p, s-1)
}

func (t *tree) pagekey(p []byte, i int) []byte {
	st := t.offget(p, i)
	kl := int(p[st])
	st++
	return p[st : st+kl]
}

func (t *tree) pagevalue(p []byte, i int) []byte {
	st := t.offget(p, i)
	end := t.offget(p, i-1)
	kl := int(p[st])
	return p[st+1+kl : end]
}

func (t *tree) pagekeyvalue(p []byte, i int) (k, v []byte) {
	st := t.offget(p, i)
	end := t.offget(p, i-1)
	kl := int(p[st])
	st++
	return p[st : st+kl], p[st+kl : end]
}

func (t *tree) pagekeyvaluesize(p []byte, i int) int {
	st := t.offget(p, i)
	end := t.offget(p, i-1)
	return end - st
}

func (t *tree) pagelink(p []byte, i int) int64 {
	start := t.offget(p, i)
	ksize := int(p[start])
	v := binary.BigEndian.Uint64(p[start+1+ksize:])
	return int64(v)
}

func (t *tree) pageinsertlink(p []byte, i int, k []byte, l int64) {
	buf := t.buf[:8]
	binary.BigEndian.PutUint64(buf, uint64(l))
	t.pageinsert(p, i, k, buf)
}

func (t *tree) pageputlink(off int64, p []byte, i int, k []byte, link int64) (loff, roff int64, l, r []byte, err error) {
	buf := t.buf[:8]
	binary.BigEndian.PutUint64(buf, uint64(link))
	return t.pageput(off, p, i, k, buf)
}

func (t *tree) pagesize(p []byte) int {
	return int(p[1])
}

func (t *tree) pagekeycmp(p []byte, i int, k []byte) int {
	off := t.offget(p, i)
	l := int(p[off])
	off++
	return bytes.Compare(p[off:off+l], k)
}

func (t *tree) pagesizespace(p []byte) (int, int) {
	if p == nil {
		return 0, len(p) - pHead
	}
	n := int(p[1])
	start := t.offidx(n)
	end := t.offget(p, n-1)
	return n, end - start
}

func (t *tree) pagesetsize(p []byte, n int) {
	p[1] = byte(n)
}

func (t *tree) pagesetflag(p []byte, f int) {
	p[0] |= byte(f)
}

func (t *tree) pageflag(p []byte, f int) bool {
	return int(p[0])&f == f
}

func (t *tree) pageflags(p []byte) int {
	return int(p[0])
}

func (t *tree) offidx(i int) int {
	return pHead + i*pLink
}

func (t *tree) offset(p []byte, i, off int) {
	s := pHead + i*pLink
	p[s] = byte(off >> 8)
	p[s+1] = byte(off)
}

func (t *tree) offget(p []byte, i int) int {
	if i == -1 {
		return len(p)
	}
	s := pHead + i*pLink
	return int(p[s])<<8 | int(p[s+1])
}

func (t *tree) pagedump(p []byte) string {
	var buf bytes.Buffer
	n := t.pagesize(p)
	br := t.pageflag(p, fBranch)
	brc := 'l'
	if br {
		brc = 'b'
	}
	fmt.Fprintf(&buf, "size %d %#4x %c:", n, len(p), brc)
	for i := 0; i < n; i++ {
		if br {
			fmt.Fprintf(&buf, " [%s -> %#4x]", t.pagekey(p, i), t.pagelink(p, i))
		} else {
			fmt.Fprintf(&buf, " [%s -> %s]", t.pagekey(p, i), t.pagevalue(p, i))
		}
	}
	fmt.Fprintf(&buf, "\n")
	return buf.String()
}
