package xrain

import (
	"bytes"
	"encoding/binary"
	"log"
)

const NilPage = -1

type (
	PageLayout interface {
		PageSize() int64
		AllocRoot() (int64, error)

		NKeys(p int64) int
		IsLeaf(p int64) bool

		Key(p int64, i int) []byte
		KeyCmp(p int64, i int, k []byte) int
		LastKey(p int64) []byte

		Value(p int64, i int) []byte
		Int64(p int64, i int) int64

		Put(p int64, i int, k, v []byte) (l, r int64, _ error)
		PutInt64(p int64, i int, k []byte, v int64) (l, r int64, _ error)
		Del(p int64, i int) (int64, error)

		NeedRebalance(p int64) bool
		Siblings(p int64, i int) (li int, l, r int64)
		Rebalance(l, r int64) (l_, r_ int64, _ error)
	}

	BaseLayout struct { // isbranch bit, size uint15, _ [6]byte, ver int64
		b    Back
		page int64
		ver  int64
		keep int64

		free *Tree
		last []byte
		exht bool
		endf int64
	}

	KVLayout struct { // base [2]byte, keys [size]int16, data []byte
		BaseLayout
	}

	IntLayout struct { // base [2]byte, _ [14]byte, keyval []{int64,int64}
		BaseLayout
	}
)

func NewPageLayout(b Back, psize, ver, keep int64, free *Tree) BaseLayout {
	return BaseLayout{
		b:    b,
		page: psize,
		ver:  ver,
		keep: keep,
		free: free,
		endf: b.Size(),
	}
}

func (l *BaseLayout) PageSize() int64 {
	return l.page
}

func (l *BaseLayout) NKeys(off int64) int {
	p := l.b.Load(off, l.page)
	return int(p[0])&0x7f<<8 | int(p[1])
}

func (l *BaseLayout) IsLeaf(off int64) bool {
	p := l.b.Load(off, l.page)
	return p[0]&0x80 == 0
}

func (l *BaseLayout) Write(off int64, p []byte) (_ int64, _ []byte, err error) {
	if off == NilPage {
		goto alloc
	}

	if p == nil {
		p = l.b.Load(off, l.page)
	}
	if l.getver(p) == l.ver {
		log.Printf("Write in %x", off)
		return off, p, nil
	}
	if l.exht {
		goto new
	}

alloc:
	// get from free list

new:
	off = l.endf
	err = l.b.Grow(off + l.page)
	if err != nil {
		return 0, nil, err
	}

	p1 := l.b.Load(off, l.page)
	copy(p1, p)

	l.setver(p1, l.ver)

	l.endf += l.page

	return off, p1, nil
}

func (l *BaseLayout) AllocRoot() (int64, error) {
	off, p, err := l.Write(NilPage, nil)
	p[0] = 0x80
	return off, err
}

func (l *BaseLayout) getver(p []byte) int64 {
	return int64(p[8])<<56 | int64(p[9])<<48 | int64(p[10])<<40 | int64(p[11])<<32 | int64(p[12])<<24 | int64(p[13])<<16 | int64(p[14])<<8 | int64(p[15])
}

func (l *BaseLayout) setver(p []byte, v int64) {
	p[8] = byte(v >> 56)
	p[9] = byte(v >> 48)
	p[10] = byte(v >> 40)
	p[11] = byte(v >> 32)
	p[12] = byte(v >> 24)
	p[13] = byte(v >> 16)
	p[14] = byte(v >> 8)
	p[15] = byte(v)
}

func (l *BaseLayout) nkeys(p []byte) int {
	return int(p[0])&0x7f<<8 | int(p[1])
}

func (l *BaseLayout) setSize(p []byte, n int) {
	p[0] = p[0]&0x80 | byte(n>>8&0x7f)
	p[1] = byte(n)
}

func (l *KVLayout) dataoff(p []byte, i int) int {
	if i == -1 {
		return len(p)
	}
	s := 16 + i*2
	st := int(p[s])<<8 | int(p[s+1])
	return st
}

func (l *KVLayout) setoff(p []byte, i int, off int) {
	s := 16 + i*2
	p[s] = byte(off >> 8)
	p[s+1] = byte(off)
}

func (l *KVLayout) KeyCmp(off int64, i int, k []byte) int {
	p := l.b.Load(off, l.page)
	st := l.dataoff(p, i)
	kl := int(p[st])
	st++
	return bytes.Compare(p[st:st+kl], k)
}

func (l *KVLayout) LastKey(off int64) []byte {
	p := l.b.Load(off, l.page)
	n := l.nkeys(p)
	st := l.dataoff(p, n-1)
	kl := int(p[st])
	st++
	return p[st : st+kl]
}

func (l *KVLayout) Value(off int64, i int) []byte {
	p := l.b.Load(off, l.page)
	st := l.dataoff(p, i)
	end := l.dataoff(p, i-1)
	kl := int(p[st])
	st++
	st += kl
	return p[st:end]
}

func (l *KVLayout) Int64(off int64, i int) int64 {
	v := l.Value(off, i)
	return int64(binary.BigEndian.Uint64(v))
}

func (l *KVLayout) Del(off int64, i int) (int64, error) {
	off, p, err := l.Write(off, nil)
	if err != nil {
		return 0, err
	}
	n := l.nkeys(p)
	st := l.dataoff(p, i)
	end := l.dataoff(p, i-1)
	size := end - st
	b := l.dataoff(p, n-1)
	copy(p[b+size:], p[b:st])
	for j := i; j < n-1; j++ {
		off := l.dataoff(p, j+1)
		l.setoff(p, j, off)
	}
	return off, nil
}

func (l *KVLayout) Put(off int64, i int, k, v []byte) (loff, roff int64, err error) {
	p := l.b.Load(off, l.page)
	n := l.nkeys(p)
	b := l.dataoff(p, n-1)
	sp := b - (16 + n*2)
	size := 2 + 1 + len(k) + len(v)
	if sp < size {
		loff, roff, lp, rp, err := l.split(off, p)
		if err != nil {
			return 0, 0, err
		}
		n = int(lp[0])&^0x80<<8 | int(lp[1])
		if i <= n {
			l.putPage(lp, i, k, v)
		} else {
			l.putPage(rp, i-n, k, v)
		}
		return loff, roff, nil
	} else {
		off, p, err = l.Write(off, p)
		if err != nil {
			return 0, 0, err
		}
		l.putPage(p, i, k, v)
		return off, NilPage, nil
	}
}

func (l *KVLayout) PutInt64(off int64, i int, k []byte, v int64) (loff, roff int64, err error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	return l.Put(off, i, k, buf[:])
}

func (l *KVLayout) putPage(p []byte, i int, k, v []byte) {
	n := l.nkeys(p)
	b := l.dataoff(p, n-1)
	size := 1 + len(k) + len(v)
	end := l.dataoff(p, i)
	copy(p[b-size:], p[b:end])
	for j := n; j > i; j-- {
		off := l.dataoff(p, j-1)
		l.setoff(p, j, off)
	}

	st := end - size
	l.setoff(p, i, st)

	p[st] = byte(len(k))
	st++
	st += copy(p[st:], k)
	copy(p[st:], v)
}

func (l *KVLayout) split(off int64, p []byte) (loff, roff int64, lp, rp []byte, err error) {
	n := l.nkeys(p)
	m := n / 2

	loff, lp, err = l.Write(off, p)
	if err != nil {
		return
	}
	roff, rp, err = l.Write(NilPage, nil)
	if err != nil {
		return
	}

	l.move(rp, p, 0, m, n)

	lp[0] = p[0]
	rp[0] = p[0]
	l.setSize(lp, n-m)
	l.setSize(rp, m)

	return
}

func (l *KVLayout) move(rp, p []byte, ri, i, I int) {
	st := l.dataoff(p, I)
	end := l.dataoff(p, i-1)
	rst := l.dataoff(rp, ri)
	copy(rp[rst:], p[st:end])

	diff := rst - st
	for j := 0; j < I-i; j++ {
		off := l.dataoff(p, i+j)
		l.setoff(rp, ri+j, off+diff)
	}
}

func (l *KVLayout) NeedRebalance(off int64) bool {
	p := l.b.Load(off, l.page)
	n := l.nkeys(p)
	b := l.dataoff(p, n-1)
	sp := b - (16 + n*2)
	return sp < len(p)/2
}

func (l *KVLayout) Siblings(off int64, i int) (li int, loff, roff int64) {
	p := l.b.Load(off, l.page)
	n := l.nkeys(p)

	var ri int
	if i+1 < n && i&1 == 0 {
		li, ri = i, i+1
	} else {
		li, ri = i-1, i
	}

	loff = l.Int64(off, li)
	roff = l.Int64(off, ri)

	return
}

func (l *KVLayout) Rebalance(lpoff, rpoff int64) (loff, roff int64, err error) {
	loff, lp, err := l.Write(lpoff, nil)
	if err != nil {
		return
	}
	roff, rp, err := l.Write(rpoff, nil)
	if err != nil {
		return
	}
	rp[0] = lp[0]

	ln := l.nkeys(lp)
	lb := l.dataoff(lp, ln-1)
	rn := l.nkeys(rp)
	rb := l.dataoff(rp, rn-1)

	if lb < rb { // left have less space
		i := 1
		for {
			diff := l.dataoff(lp, ln-i) - lb
			if lb-diff > rb+diff {
				break
			}
			i++
		}
		l.move(rp, lp, rn, ln-i, ln)
		l.setSize(lp, ln-i)
		l.setSize(rp, rn+i)
	} else {
		i := 1
		for {
			diff := l.dataoff(rp, rn-i) - rb
			if lb-diff < rb+diff {
				break
			}
			i++
		}
		l.move(lp, rp, ln, rn-i, rn)
		l.setSize(lp, ln+i)
		l.setSize(rp, rn-i)
	}

	return
}

func (l *IntLayout) KeyCmp(off int64, i int, k []byte) int {
	if k == nil {
		return 1
	}
	if len(k) != 8 {
		panic(len(k))
	}
	p := l.b.Load(off, l.page)
	s := 16 + i*16
	return bytes.Compare(p[s:s+8], k)
}

func (l *IntLayout) Key(off int64, i int) []byte {
	p := l.b.Load(off, l.page)
	s := 16 + i*16
	return p[s : s+8]
}

func (l *IntLayout) LastKey(off int64) []byte {
	p := l.b.Load(off, l.page)
	n := l.nkeys(p)
	st := 16 + (n-1)*16
	return p[st : st+8]
}

func (l *IntLayout) Value(off int64, i int) []byte {
	p := l.b.Load(off, l.page)
	s := 16 + i*16
	return p[s+8 : s+16]
}

func (l *IntLayout) Int64(off int64, i int) int64 {
	v := l.Value(off, i)
	return int64(binary.BigEndian.Uint64(v))
}

func (l *IntLayout) Del(off int64, i int) (int64, error) {
	off, p, err := l.Write(off, nil)
	if err != nil {
		return 0, err
	}
	n := l.nkeys(p)
	st := 16 + i*16
	end := 16 + n*16
	copy(p[st:], p[st+16:end])
	l.setSize(p, n-1)
	return off, nil
}

func (l *IntLayout) Put(off int64, i int, k, v []byte) (loff, roff int64, err error) {
	if len(k) != 8 {
		panic(k)
	}
	if len(v) != 8 {
		panic(v)
	}
	p := l.b.Load(off, l.page)
	n := l.nkeys(p)
	st := 16 + n*16
	if st < len(p) {
		loff, p, err = l.Write(off, p)
		if err != nil {
			return 0, 0, err
		}
		l.putPage(p, i, n, k, v)
		return loff, NilPage, nil
	}

	// split
	var lp, rp []byte

	loff, lp, err = l.Write(off, p)
	if err != nil {
		return 0, 0, err
	}

	roff, rp, err = l.Write(NilPage, nil)
	if err != nil {
		return 0, 0, err
	}

	m := n / 2

	copy(rp[16:], lp[16+m*16:16+n*16])

	lp[0] = p[0]
	rp[0] = p[0]
	l.setSize(lp, m)
	l.setSize(rp, n-m)

	// add
	if i < m {
		l.putPage(lp, i, m, k, v)
	} else {
		l.putPage(rp, i-m, n-m, k, v)
	}
	return
}

func (l *IntLayout) putPage(p []byte, i, n int, k, v []byte) {
	st := 16 + i*16
	end := 16 + n*16
	copy(p[st+16:], p[st:end])
	copy(p[st:], k)
	copy(p[st+8:], v)
	l.setSize(p, n+1)
}

func (l *IntLayout) PutInt64(off int64, i int, k []byte, v int64) (loff, roff int64, err error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	return l.Put(off, i, k, buf[:])
}

func (l *IntLayout) NeedRebalance(off int64) bool {
	p := l.b.Load(off, l.page)
	n := l.nkeys(p)
	end := 16 + n*16
	if end < len(p)/2 {
		return true
	}
	return false
}

func (l *IntLayout) Siblings(off int64, i int) (li int, loff, roff int64) {
	p := l.b.Load(off, l.page)
	n := l.nkeys(p)

	var ri int
	if i+1 < n && i&1 == 0 {
		li, ri = i, i+1
	} else {
		li, ri = i-1, i
	}

	loff = l.Int64(off, li)
	roff = l.Int64(off, ri)

	return
}

func (l *IntLayout) Rebalance(lpoff, rpoff int64) (loff, roff int64, err error) {
	loff, lp, err := l.Write(lpoff, nil)
	if err != nil {
		return
	}
	roff, rp, err := l.Write(rpoff, nil)
	if err != nil {
		return
	}
	rp[0] = lp[0]

	ln := l.nkeys(lp)
	rn := l.nkeys(rp)
	sum := ln + rn
	n := sum / 2
	end := 16 + n*16
	rend := 16 * rn * 16
	lend := 16 * ln * 16

	if ln > rn {
		copy(rp[rend:], lp[end:lend])
		l.setSize(lp, n)
		l.setSize(rp, sum-n)
	} else {
		copy(lp[lend:], rp[end:rend])
		l.setSize(rp, n)
		l.setSize(lp, sum-n)
	}

	return
}
