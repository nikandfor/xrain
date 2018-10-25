package xrain

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
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

type (
	PageLayout interface {
		Size(p []byte) int

		Flag(p []byte, f int) bool
		Flags(p []byte) int
		SetFlag(p []byte, f int)

		KeyCmp(p []byte, i int, k []byte) int
		LastKey(p []byte) []byte

		Int64(p []byte, i int) int64
		PutInt64(off int64, p []byte, i int, k []byte, v int64) (loff, roff int64, l, r []byte, err error)

		Put(off int64, p []byte, i int, k, v []byte) (loff, roff int64, l, r []byte, err error)
		Del(off int64, p []byte, i int) (loff int64, l []byte, reb bool, err error)
		Key(p []byte, i int) []byte
		Value(p []byte, i int) []byte
		KeyValue(p []byte, i int) (k, v []byte)
		KeyValueSize(p []byte, i int) int

		Siblings(off int64, p []byte, i int) (li, ri int, loff, roff int64)
		Rebalance(loff, roff int64, l, r []byte) (lwoff, rwoff int64, lw, rw []byte, err error)
		NeedRebalance(p []byte) bool
	}

	BytesPage struct {
		a Allocator
	}
)

func (t BytesPage) Put(off int64, p []byte, i int, k, v []byte) (loff, roff int64, l, r []byte, err error) {
	n, sp := t.sizespace(p)
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
		if f := t.Flags(p); true {
			t.SetFlag(l, f)
			t.SetFlag(r, f)
		}
		if i <= m {
			t.move(r, p, 0, m, n)
			t.setsize(r, n-m)

			if loff == off {
				t.setsize(l, m)
				t.insert(l, i, k, v)
			} else {
				t.move(l, p, 0, 0, i)
				t.setsize(l, i)
				t.insert(l, i, k, v)
				t.move(l, p, i+1, i, m)
				t.setsize(l, m+1)
			}
		} else {
			t.move(r, p, 0, m, i)
			t.setsize(r, i-m)
			t.insert(r, i-m, k, v)
			t.move(r, p, i+1, i, n)
			t.setsize(r, n-m+1)

			if loff == off {
				t.setsize(l, m)
			} else {
				t.move(l, p, 0, i, m)
				t.setsize(l, m)
			}
		}
	} else {
		loff, l, err = t.a.Write(off, nil)
		if err != nil {
			return
		}
		if loff == off {
			t.insert(l, i, k, v)
		} else {
			t.move(l, p, 0, 0, i)
			t.setsize(l, i)
			t.insert(l, i, k, v)
			t.move(l, p, i+1, i, n)
			t.setsize(l, n+1)
		}
	}
	return
}

func (t BytesPage) Del(off int64, p []byte, i int) (loff int64, l []byte, reb bool, err error) {
	loff, l, err = t.a.Write(off, p)
	if err != nil {
		return
	}
	n := t.Size(p)
	if loff == off {
		t.uninsert(l, i)
	} else {
		t.move(l, p, 0, 0, i)
		t.move(l, p, i, i+1, n)
		t.setsize(l, n-1)
	}
	reb = t.NeedRebalance(p)
	return
}

func (t BytesPage) Rebalance(loff, roff int64, l, r []byte) (lwoff, rwoff int64, lw, rw []byte, err error) {
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

	ln, lsp := t.sizespace(l)
	rn, rsp := t.sizespace(r)

	lwoff, lw, err = t.a.Write(loff, l)
	if err != nil {
		return
	}
	if lwoff != loff {
		f := t.Flags(l)
		t.SetFlag(lw, f)
	}

	if lsp+rsp+pHead >= len(l) {
		if lwoff != loff {
			t.move(lw, l, 0, 0, ln)
		}
		t.move(lw, r, ln, 0, rn)
		t.setsize(lw, ln+rn)
		return
	}

	rwoff, rw, err = t.a.Write(roff, r)
	if err != nil {
		return
	}
	if rwoff != roff {
		f := t.Flags(r)
		t.SetFlag(rw, f)
	}

	if lsp < rsp {
		// l -> r
		i := ln
		for lsp < rsp {
			kvsize := t.KeyValueSize(l, i-1) + 2
			if lsp+kvsize >= rsp-kvsize {
				break
			}
			lsp += kvsize
			rsp -= kvsize
			i--
		}
		for j := i; j < ln; j++ {
			k, v := t.KeyValue(l, j)
			t.insert(rw, j-i, k, v)
		}

		if rwoff != roff {
			t.move(rw, r, ln-i, 0, rn)
			t.setsize(rw, ln-i+rn)
		}

		if lwoff != loff {
			t.move(lw, l, 0, 0, i)
		}
		t.setsize(lw, i)
	} else {
		// r -> l
		i := 0
		for lsp > rsp {
			kvsize := t.KeyValueSize(r, i) + 2
			if rsp+kvsize >= lsp-kvsize {
				break
			}
			rsp += kvsize
			lsp -= kvsize
			i++
		}

		if lwoff != loff {
			t.move(lw, l, 0, 0, ln)
		}
		t.move(lw, r, ln, 0, i)
		t.setsize(lw, ln+i)

		t.move(rw, r, 0, i, rn)
		t.setsize(rw, rn-i)
	}
	return
}

func (t BytesPage) Siblings(off int64, p []byte, i int) (li, ri int, loff, roff int64) {
	n := t.Size(p)
	if i < n-1 && i%2 == 0 {
		return i, i + 1, t.Int64(p, i), t.Int64(p, i+1)
		//	return t.link(p, i), t.link(p, i+1), nil, nil, nil
	} else {
		return i - 1, i, t.Int64(p, i-1), t.Int64(p, i)
		//	return t.link(p, i-1), t.link(p, i), nil, nil, nil
	}
}

func (t BytesPage) move(r, s []byte, ri, im, iM int) {
	if im == iM {
		return
	}

	n := iM - im

	off := t.offget(s, iM-1)
	end := t.offget(s, im-1)
	sz := end - off
	dst := t.offget(r, ri-1) - sz

	log.Printf("dst %x off:end %x:%x  sz %x  ri %d <- %d %d", dst, off, end, sz, ri, im, iM)

	copy(r[dst:], s[off:end])

	diff := dst - off
	for i := 0; i < n; i++ {
		sh := t.offget(s, im+i)
		t.offset(r, ri+i, sh+diff)
	}
}

func (t BytesPage) insert(p []byte, i int, k, v []byte) {
	end := t.offget(p, i-1)
	size := 1 + len(k) + len(v)
	n := t.Size(p)

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

	t.setsize(p, n+1)
}

func (t BytesPage) uninsert(p []byte, i int) {
	n := t.Size(p)
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

	t.setsize(p, n)
}

func (t BytesPage) NeedRebalance(p []byte) bool {
	_, sp := t.sizespace(p)
	return sp > len(p)/2
}

func (t BytesPage) LastKey(p []byte) []byte {
	s := t.Size(p)
	if s == 0 {
		return nil
	}
	return t.Key(p, s-1)
}

func (t BytesPage) Key(p []byte, i int) []byte {
	st := t.offget(p, i)
	kl := int(p[st])
	st++
	return p[st : st+kl]
}

func (t BytesPage) Value(p []byte, i int) []byte {
	st := t.offget(p, i)
	end := t.offget(p, i-1)
	kl := int(p[st])
	return p[st+1+kl : end]
}

func (t BytesPage) KeyValue(p []byte, i int) (k, v []byte) {
	st := t.offget(p, i)
	end := t.offget(p, i-1)
	kl := int(p[st])
	st++
	return p[st : st+kl], p[st+kl : end]
}

func (t BytesPage) KeyValueSize(p []byte, i int) int {
	st := t.offget(p, i)
	end := t.offget(p, i-1)
	return end - st
}

func (t BytesPage) Int64(p []byte, i int) int64 {
	start := t.offget(p, i)
	ksize := int(p[start])
	v := binary.BigEndian.Uint64(p[start+1+ksize:])
	return int64(v)
}

func (t BytesPage) insertlink(p []byte, i int, k []byte, l int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(l))
	t.insert(p, i, k, buf[:])
}

func (t BytesPage) PutInt64(off int64, p []byte, i int, k []byte, link int64) (loff, roff int64, l, r []byte, err error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(link))
	return t.Put(off, p, i, k, buf[:])
}

func (t BytesPage) Size(p []byte) int {
	return int(p[1])
}

func (t BytesPage) KeyCmp(p []byte, i int, k []byte) int {
	off := t.offget(p, i)
	l := int(p[off])
	off++
	return bytes.Compare(p[off:off+l], k)
}

func (t BytesPage) sizespace(p []byte) (int, int) {
	if p == nil {
		return 0, len(p) - pHead
	}
	n := int(p[1])
	start := t.offidx(n)
	end := t.offget(p, n-1)
	return n, end - start
}

func (t BytesPage) setsize(p []byte, n int) {
	p[1] = byte(n)
}

func (t BytesPage) SetFlag(p []byte, f int) {
	p[0] |= byte(f)
}

func (t BytesPage) Flag(p []byte, f int) bool {
	return int(p[0])&f == f
}

func (t BytesPage) Flags(p []byte) int {
	return int(p[0])
}

func (t BytesPage) offidx(i int) int {
	return pHead + i*pLink
}

func (t BytesPage) offset(p []byte, i, off int) {
	s := pHead + i*pLink
	p[s] = byte(off >> 8)
	p[s+1] = byte(off)
}

func (t BytesPage) offget(p []byte, i int) int {
	if i == -1 {
		return len(p)
	}
	s := pHead + i*pLink
	return int(p[s])<<8 | int(p[s+1])
}

func (t BytesPage) Dump(p []byte) string {
	var buf bytes.Buffer
	n := t.Size(p)
	br := t.Flag(p, fBranch)
	brc := 'l'
	if br {
		brc = 'b'
	}
	fmt.Fprintf(&buf, "size %d %#4x %c:", n, len(p), brc)
	for i := 0; i < n; i++ {
		if br {
			fmt.Fprintf(&buf, " [%s -> %#4x]", t.Key(p, i), t.Int64(p, i))
		} else {
			fmt.Fprintf(&buf, " [%s -> %s]", t.Key(p, i), t.Value(p, i))
		}
	}
	fmt.Fprintf(&buf, "\n")
	return buf.String()
}
