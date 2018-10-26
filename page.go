package xrain

import (
	"bytes"
	"encoding/binary"
)

const NilPage = -1

type (
	PageLayout interface {
		PageSize() int64

		Size(p int64) (int, error)
		IsLeaf(p int64) (bool, error)

		KeyCmp(p int64, i int, k []byte) (int, error)
		LastKey(p int64) ([]byte, error)

		Value(p int64, i int) ([]byte, error)
		Int64(p int64, i int) (int64, error)

		Put(p int64, i int, k, v []byte) (l, r int64, _ error)
		PutInt64(p int64, i int, k []byte, v int64) (l, r int64, _ error)
		Del(p int64, i int) (int64, error)

		NeedRebalance(p int64) (bool, error)
		Siblings(p int64, i int) (li int, l, r int64, _ error)
		Rebalance(l, r int64) (l_, r_ int64, _ error)
	}

	BaseLayout struct { // isbranch bit, size uint15, _ [6]byte, ver int64
		b    Back
		page int64
		ver  int64
		keep int64
	}

	KVLayout struct { // base [2]byte, keys [size]int16, data []byte
		BaseLayout
	}

	IntLayout struct { // base [2]byte, _ [14]byte, keyval []{int64,int64}
		BaseLayout
	}
)

func (l *BaseLayout) PageSize() int64 {
	return l.page
}

func (l *BaseLayout) Size(off int64) (int, error) {
	p, err := l.b.Read(off, l.page)
	if err != nil {
		return 0, err
	}
	return int(p[0])&0x7f<<8 | int(p[1]), nil
}

func (l *BaseLayout) IsLeaf(off int64) (bool, error) {
	p, err := l.b.Read(off, l.page)
	if err != nil {
		return false, err
	}
	return p[0]&0x80 != 0, nil
}

func (l *BaseLayout) Write(off int64) (int64, []byte, error) {
	return 0, nil, nil
}

func (l *BaseLayout) size(p []byte) int {
	return int(p[0])&^0x80<<8 | int(p[1])
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

func (l *KVLayout) setSize(p []byte, n int) {
	p[0] = p[0]&0x80 | byte(n>>8&0x7f)
	p[1] = byte(n)
}

func (l *KVLayout) KeyCmp(off int64, i int, k []byte) (int, error) {
	p, err := l.b.Read(off, l.page)
	if err != nil {
		return 0, err
	}
	st := l.dataoff(p, i)
	kl := int(p[st])
	st++
	return bytes.Compare(p[st:st+kl], k), nil
}

func (l *KVLayout) LastKey(off int64) ([]byte, error) {
	p, err := l.b.Read(off, l.page)
	if err != nil {
		return nil, err
	}
	n := l.size(p)
	st := l.dataoff(p, n-1)
	kl := int(p[st])
	st++
	return p[st : st+kl], nil
}

func (l *KVLayout) Value(off int64, i int) ([]byte, error) {
	p, err := l.b.Read(off, l.page)
	if err != nil {
		return nil, err
	}
	st := l.dataoff(p, i)
	end := l.dataoff(p, i-1)
	kl := int(p[st])
	st++
	st += kl
	return p[st:end], nil
}

func (l *KVLayout) Int64(off int64, i int) (int64, error) {
	v, err := l.Value(off, i)
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(v)), nil
}

func (l *KVLayout) Del(off int64, i int) (int64, error) {
	off, p, err := l.Write(off)
	if err != nil {
		return 0, err
	}
	n := l.size(p)
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
	p, err := l.b.Read(off, l.page)
	if err != nil {
		return 0, 0, err
	}
	n := l.size(p)
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
		off, p, err = l.Write(off)
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
	n := l.size(p)
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
	loff, lp, err = l.Write(off)
	if err != nil {
		return
	}
	roff, rp, err = l.Write(NilPage)
	if err != nil {
		return
	}
	n := l.size(p)
	m := n / 2

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

func (l *KVLayout) NeedRebalance(off int64) (bool, error) {
	p, err := l.b.Read(off, l.page)
	if err != nil {
		return false, err
	}
	n := l.size(p)
	b := l.dataoff(p, n-1)
	sp := b - (16 + n*2)
	return sp < len(p)/2, nil
}

func (l *KVLayout) Siblings(off int64, i int) (li int, loff, roff int64, err error) {
	p, err := l.b.Read(off, l.page)
	if err != nil {
		return
	}
	n := l.size(p)

	var ri int
	if i+1 < n && i&1 == 0 {
		li, ri = i, i+1
	} else {
		li, ri = i-1, i
	}

	loff, err = l.Int64(off, li)
	if err != nil {
		return
	}
	roff, err = l.Int64(off, ri)
	if err != nil {
		return
	}

	return
}

func (l *KVLayout) Rebalance(lpoff, rpoff int64) (loff, roff int64, err error) {
	loff, lp, err := l.Write(lpoff)
	if err != nil {
		return
	}
	roff, rp, err := l.Write(rpoff)
	if err != nil {
		return
	}

	ln := l.size(lp)
	lb := l.dataoff(lp, ln-1)
	rn := l.size(rp)
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

func (l *IntLayout) KeyCmp(off int64, i int, k []byte) (int, error) {
	if len(k) != 8 {
		panic(len(k))
	}
	p, err := l.b.Read(off, l.page)
	if err != nil {
		return 0, err
	}
	s := 16 + i*16
	return bytes.Compare(p[s:s+8], k), nil
}

func (l *IntLayout) LastKey(off int64) ([]byte, error) {
	p, err := l.b.Read(off, l.page)
	if err != nil {
		return nil, err
	}
	m := 8
	if p[0]&0x80 != 0 {
		m = 16
	}
	n := l.size(p)
	st := 16 + (n-1)*m
	return p[st : st+8], nil
}

func (l *IntLayout) Value(off int64, i int) ([]byte, error) {
	p, err := l.b.Read(off, l.page)
	if err != nil {
		return nil, err
	}
	m := 16
	if p[0]&0x80 == 0 {
		panic(off)
	}
	st := 16 + i*m
	return p[st+8 : st+16], nil
}

func (l *IntLayout) Int64(off int64, i int) (int64, error) {
	v, err := l.Value(off, i)
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(v)), nil
}
