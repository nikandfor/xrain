package xrain

import (
	"bytes"
	"encoding/binary"
	"sort"
)

/*
	Key Value Pair

	Total len of the struct must not exceed ((page_size - header_size) / 0x10) bytes.

	// index
	offset int16

	// data
	flags  byte
	keylen byte // maxKeyLen-0xff means prefix, 0xff means keylen at overflow page
	key    []byte
	value  []byte


*/

type (
	KVLayout struct { // base [16]byte, freelist int64, links [size]int16, data []{F, keylen byte, key, value []byte}
		BaseLayout
		maxkey int
	}
)

var (
	kvIndexStart = 0x10

//	maxRecordLen = (0x1000 - kvIndexStart) / 0x10
//	maxKeyLen    = maxRecordLen - (2 + 2 + 8)
)

// var _ PageLayout = &KVLayout{}

func NewKVLayout(b Back, page int64, fl Freelist) *KVLayout {
	l := &KVLayout{
		BaseLayout: BaseLayout{
			b:    b,
			free: fl,
		},
	}
	l.SetPageSize(page)
	return l
}

func (l *KVLayout) pagefree(p []byte, n int) int {
	if n == 0 {
		return len(p) - kvIndexStart
	}
	fst := kvIndexStart + 2*n
	dst := int(p[fst-2])<<8 | int(p[fst-1])
	return dst - fst
}

func (l *KVLayout) pagedatasize(p []byte, n int) int {
	if n == 0 {
		return 0
	}
	fst := kvIndexStart + 2*n
	dst := int(p[fst-2])<<8 | int(p[fst-1])
	return len(p) - dst + fst - kvIndexStart
}

func (l *KVLayout) dataoff(p []byte, i int) int {
	if i == -1 {
		panic(i)
	}
	st := kvIndexStart + 2*i
	return int(p[st])<<8 | int(p[st+1])
}

func (l *KVLayout) dataend(p []byte, i int) int {
	if i == 0 {
		return len(p)
	}
	i--
	st := kvIndexStart + 2*i
	return int(p[st])<<8 | int(p[st+1])
}

func (l *KVLayout) datasize(p []byte, i, n int) int {
	return l.dataend(p, i) - l.dataoff(p, n)
}

func (l *KVLayout) setdataoff(p []byte, i, off int) {
	st := kvIndexStart + 2*i
	p[st] = byte(off >> 8)
	p[st+1] = byte(off)
}

func (l *KVLayout) suballoc(n int) int64 {
	return 0
}

func (l *KVLayout) Serialize(p []byte) int {
	return 0
}

func (l *KVLayout) Deserialize(p []byte) (int, error) {
	return 0, nil
}

func (l *KVLayout) Alloc(leaf bool) (int64, error) {
	off, err := l.free.Alloc(1)
	if err != nil {
		return NilPage, err
	}
	p := l.b.Access(off, 0x10)
	l.setleaf(p, leaf)
	l.setnkeys(p, 0)
	l.setver(p, l.ver)
	l.setoverflow(p, 1)
	l.b.Unlock(p)
	return off, nil
}

func (l *KVLayout) Free(off int64, r bool) (err error) {
	if !r {
		return l.BaseLayout.Free(off, false)
	}

	var sub []int64
	p := l.b.Access(off, 0x10)
	func() {
		if l.isleaf(p) {
			return
		}
		n := l.nkeys(p)
		if n == 0 {
			return
		}

		sub = make([]int64, n)
		for i := 0; i < n; i++ {
			panic("implement")
			//	s := 16 + i*(l.k+8) + l.k
			//	sub[i] = int64(binary.BigEndian.Uint64(p[s : s+8]))
		}
	}()
	l.b.Unlock(p)

	for _, off := range sub {
		err = l.Free(off, true)
		if err != nil {
			return
		}
	}

	return l.BaseLayout.Free(off, false)
}

func (l *KVLayout) Search(off int64, k []byte) (i int, eq bool) {
	pp := l.b.Access(off, l.page)
	p := pp

	ln := l.nkeys(p)

	keycmp := func(i int) int {
		dst := l.dataoff(p, i)
		//	iF := int(p[dst])
		kl := int(p[dst+1])
		dst += 2

		ik := p[dst : dst+kl]

		return bytes.Compare(ik, k)
	}

	i = sort.Search(ln, func(i int) bool {
		return keycmp(i) >= 0
	})

	eq = i < ln && keycmp(i) == 0

	if pp != nil {
		l.b.Unlock(pp)
	}

	return
}

func (l *KVLayout) Key(off int64, i int, buf []byte) (r []byte, F int) {
	p := l.b.Access(off, l.page)
	dst := l.dataoff(p, i)
	F = int(p[dst])
	kl := int(p[dst+1])
	dst += 2

	r = append(buf[:0], p[dst:dst+kl]...)
	l.b.Unlock(p)

	return
}

func (l *KVLayout) Value(off int64, i int, buf []byte) (r []byte) {
	p := l.b.Access(off, l.page)

	dst := l.dataoff(p, i)
	dend := l.dataend(p, i)

	kl := int(p[dst+1])
	dst += 2 + kl

	r = append(buf[:0], p[dst:dend]...)

	l.b.Unlock(p)

	return
}

func (l *KVLayout) Int64(off int64, i int) int64 {
	p := l.b.Access(off, l.page)
	defer l.b.Unlock(p)

	dst := l.dataoff(p, i)
	dend := l.dataend(p, i)

	kl := int(p[dst+1])
	dst += 2 + kl

	return int64(binary.BigEndian.Uint64(p[dst:dend]))
}

func (l *KVLayout) Delete(off int64, i int) (_ int64, err error) {
	var ver int64
	var alloc bool
again:
	p := l.b.Access(off, l.page)
	func() {
		if alloc {
			l.setver(p, ver)
			l.setoverflow(p, 0)
			alloc = false
		} else {
			ver = l.getver(p)
			if ver != l.ver {
				alloc = true
				return
			}
		}

		n := l.nkeys(p)

		if i+1 < n {
			l.pageMove(p, p, i, i+1, n)
		}

		l.setnkeys(p, n-1)
	}()
	l.b.Unlock(p)

	if alloc {
		off, err = l.realloc(1, off, ver)
		if err != nil {
			return
		}
		goto again
	}

	return off, nil
}

func (l *KVLayout) UpdatePageLink(off int64, i int, cp int64) (loff, roff int64, err error) {
	var lk []byte

	p0, p1 := l.b.Access2(off, l.page, cp, l.page)
	// parent page old key
	//	dst0 := l.dataoff(p0, i)
	//	kl0 := int(p0[dst0+1])
	//	dst0 += 2

	// child page last key
	n := l.nkeys(p1)
	dst1 := l.dataoff(p1, n-1)
	kl1 := int(p1[dst1+1])
	dst1 += 2

	//	eq := bytes.Equal(p0[dst0:dst0+kl0], p1[dst1:dst1+kl1])

	//	if eq {
	lk = make([]byte, kl1+8)
	copy(lk, p1[dst1:dst1+kl1])
	//	}

	l.b.Unlock2(p0, p1)

	loff, err = l.Delete(off, i)
	if err != nil {
		return
	}

	binary.BigEndian.PutUint64(lk[kl1:], uint64(cp))

	return l.Insert(off, i, 0, lk[:kl1], lk[kl1:])
}

func (l *KVLayout) InsertPageLink(off int64, i int, cp int64) (loff, roff int64, err error) {
	var lk []byte

	p0, p1 := l.b.Access2(off, l.page, cp, l.page)
	// child page last key
	n := l.nkeys(p1)
	dst1 := l.dataoff(p1, n-1)
	kl1 := int(p1[dst1+1])
	dst1 += 2

	lk = make([]byte, kl1+8)
	copy(lk, p1[dst1:dst1+kl1])

	l.b.Unlock2(p0, p1)

	binary.BigEndian.PutUint64(lk[kl1:], uint64(cp))

	return l.Insert(off, i, 0, lk[:kl1], lk[kl1:])
}

func (l *KVLayout) Insert(off int64, i, F int, k, v []byte) (loff, roff int64, err error) {
	return l.insert(off, i, F, k, v)
}

func (l *KVLayout) insert(off int64, i, ff int, k, v []byte) (loff, roff int64, err error) {
	loff = off
	var alloc, split bool
	var ver int64
again:
	p := l.b.Access(loff, l.page)
	func() {
		if alloc {
			l.setver(p, l.ver)
			l.setoverflow(p, 0)
			ver = l.ver
			alloc = false
		} else {
			ver = l.getver(p)
			alloc = ver != l.ver
		}

		n := l.nkeys(p)
		free := l.pagefree(p, n)

		//	tlog.Printf("insert at %d / %d  dsize %x / %x", i, n, 2+len(k)+len(v), free)

		if free >= 1+1+len(k)+len(v) {
			if ver == l.ver {
				l.pageInsert(p, i, n, ff, k, v)
			}
		} else {
			split = true
		}
	}()
	l.b.Unlock(p)
	if !alloc && !split {
		return loff, NilPage, nil
	}
	if alloc {
		loff, err = l.realloc(1, loff, ver)
		if err != nil {
			return
		}
		goto again
	}
	if !split {
		goto again
	}

	roff, err = l.free.Alloc(1)
	if err != nil {
		return
	}

	lp, rp := l.b.Access2(loff, l.page, roff, l.page)
	func() {
		if alloc {
			l.setver(lp, l.ver)
			l.setoverflow(lp, 0)
		}
		rp[4] = lp[4] // isleaf
		l.setver(rp, l.ver)
		l.setoverflow(rp, 0)

		n := l.nkeys(lp)
		m := n / 2
		if i > m {
			m = (n + 1) / 2
		}

		l.setnkeys(lp, m)
		l.setnkeys(rp, n-m)

		l.pageMove(rp, lp, 0, m, n)

		if i <= m {
			l.pageInsert(lp, i, m, ff, k, v)
		} else {
			l.pageInsert(rp, i-m, n-m, ff, k, v)
		}
	}()
	l.b.Unlock2(lp, rp)

	return
}

func (l *KVLayout) pageInsert(p []byte, i, n, ff int, k, v []byte) {
	dlen := 1 + 1 + len(k) + len(v)
	if i < n {
		l.pageMoveFwd(p, p, 1, dlen, i, n)
	}

	dend := l.dataend(p, i)
	dst := dend - dlen

	//	tlog.Printf("insert %d/%d  to %x - %x (%x)", i, n, dst, dend, dlen)

	p[dst] = byte(ff) // flags
	p[dst+1] = byte(len(k))
	copy(p[dst+2:], k)
	copy(p[dst+2+len(k):], v)

	l.setdataoff(p, i, dst)

	l.setnkeys(p, n+1)
}

func (l *KVLayout) pageMove(r, p []byte, i, st, nxt int) {
	if &r[0] == &p[0] && i > st {
		panic("forward move")
	}
	if st == nxt {
		panic("qq")
	}

	dst := l.dataoff(p, nxt-1)
	dend := l.dataend(p, st)
	dlen := dend - dst

	drend := l.dataend(r, i)

	//	tlog.Printf("move %d <- %d - %d  | %x - %x (%x) <- %x - %x", i, st, nxt, drend-dlen, drend, dlen, dst, dend)

	copy(r[drend-dlen:], p[dst:dend])

	di := i - st
	diff := drend - dend

	for j := st; j < nxt; j++ {
		off := l.dataoff(p, j)
		l.setdataoff(r, j+di, off+diff)
		//	tlog.Printf("index %d %x -> %d %x", j, off, j+di, off+diff)
	}
}

func (l *KVLayout) pageMoveFwd(r, p []byte, di, dlen, i, n int) {
	dst := l.dataoff(p, n-1)
	dend := l.dataend(p, i)
	copy(p[dst-dlen:], p[dst:dend])

	for j := i; j < n; j++ {
		off := l.dataoff(p, j)
		l.setdataoff(p, j+di, off-dlen)
	}
}

func (l *KVLayout) NeedRebalance(off int64) (r bool) {
	p := l.b.Access(off, l.page)
	n := l.nkeys(p)
	s := l.pagedatasize(p, n)
	r = s < len(p)*2/5
	l.b.Unlock(p)
	return
}

func (l *KVLayout) Siblings(off int64, i int, ioff int64) (li int, loff, roff int64) {
	readoff := func(p []byte, i int) int64 {
		s := l.dataoff(p, i)
		kl := int(p[s+1])
		return int64(binary.BigEndian.Uint64(p[s+2+kl:]))
	}

	p := l.b.Access(off, l.page)
	n := l.nkeys(p)
	if i+1 < n && i&1 == 0 {
		li = i
		loff = ioff
		roff = readoff(p, i+1)
	} else {
		li = i - 1
		loff = readoff(p, i-1)
		roff = ioff
	}
	l.b.Unlock(p)
	return
}

func (l *KVLayout) Rebalance(lpoff, rpoff int64) (loff, roff int64, err error) {
	loff, roff = lpoff, rpoff
	var lalloc, ralloc bool
	var rfree bool
	var lver, rver int64
again:
	lp, rp := l.b.Access2(loff, l.page, roff, l.page)
	func() {
		if lalloc {
			l.setver(lp, l.ver)
			l.setoverflow(lp, 0)
			lalloc = false
		} else {
			lver = l.getver(lp)
			if lver != l.ver {
				lalloc = true
			}
		}

		if ralloc {
			l.setver(rp, l.ver)
			l.setoverflow(rp, 0)
			ralloc = false
		} else {
			rver = l.getver(rp)
			if rver != l.ver {
				ralloc = true
			}
		}

		ln := l.nkeys(lp)
		rn := l.nkeys(rp)
		sum := ln + rn
		m := (sum + 1) / 2

		lds := l.pagedatasize(lp, ln)
		rds := l.pagedatasize(rp, rn)

		if kvIndexStart+lds+rds <= len(lp) {
			ralloc = false
		} else {
			d := ln - m
			if d < 0 {
				d = -d
			}
			diff := lds - rds
			if diff < 0 {
				diff = -diff
			}
			if d <= 1 || diff <= len(lp)/8 {
				lalloc = false
				ralloc = false
				return // do nothing
			}
		}

		if lalloc || ralloc {
			return
		}

		if kvIndexStart+lds+rds <= len(lp) {
			l.pageMove(lp, rp, ln, 0, rn)
			l.setnkeys(lp, ln+rn)

			rfree = true
			return
		}

		if ln > rn {
			ds := l.datasize(lp, m, ln)
			l.pageMoveFwd(rp, rp, ln-m, ds, 0, rn)
			l.pageMove(rp, lp, 0, m, ln)
			l.setnkeys(lp, m)
			l.setnkeys(rp, sum-m)
		} else {
			l.pageMove(lp, rp, ln, 0, m-ln)
			l.pageMove(rp, rp, 0, m-ln, rn)
			l.setnkeys(lp, m)
			l.setnkeys(rp, sum-m)
		}
	}()
	l.b.Unlock2(lp, rp)
	if lalloc {
		loff, err = l.realloc(1, loff, lver)
		if err != nil {
			return
		}
	}
	if ralloc {
		roff, err = l.realloc(1, roff, rver)
		if err != nil {
			return
		}
	}
	if lalloc || ralloc {
		goto again
	}

	if rfree {
		err = l.free.Free(1, roff, rver)
		if err != nil {
			return
		}
		roff = NilPage
	}

	return
}
