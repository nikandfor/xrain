package xrain

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"sort"

	"github.com/nikandfor/tlog"
)

/*
	Key Value Pair

	Total len of the struct must not exceed ((page_size - header_size) / 0x10) bytes.

	// index
	offset int16

	// data
	flags  byte
	keylen byte
	kcplen byte // key common prefix; it's index (or prefix) encoding
	key    []byte
	value  []byte


	Index (prefix) encoding
	v - common prefix
	  v - rest of string

	0 ab0        // ab0
	2   cd11     // abcd11
	4     ef222  // abcdef222
	6       gh   // abcdefgh
	2   qw11     // abqw11
	4     er222  // abqwer222
	6       ty   // abqwerty
*/

type (
	KVLayout struct { // base [16]byte, links [size]int16, data []{F, keylen byte, key, value []byte}
		BaseLayout
	}
)

var (
	kvIndexStart = 0x10

//	maxRecordLen = (0x1000 - kvIndexStart) / 0x10
//	maxKeyLen    = maxRecordLen - (2 + 3 + 8)
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

//var max int

func (l *KVLayout) Search(off int64, k []byte) (i int, eq bool) {
	p := l.b.Access(off, l.page)
	defer l.b.Unlock(p)

	var cpsbuf [1000]byte
	ln := l.nkeys(p)
	fastpath := -1
	fastval := -1

	keycmp := func(i int) int {
		cps := cpsbuf[:0]
		pcp := byte(255)

		j := i
		for ; j != 0; j-- {
			if j < fastpath {
				if fastval == -1 {
					return -1
				} else {
					break
				}
			}

			dst := l.dataoff(p, j)
			//	kl := int(p[dst+1])
			cp := p[dst+2]
			if cp == 0 {
				pcp = cp
				break
			}
			if cp > pcp {
				cp = pcp
			}
			//	tlog.Printf("add common prefix %x of %q [%x:]", cp, p[dst+3:dst+3+kl], p[dst+2])
			cps = append(cps, cp)
			pcp = cp
		}
		//	if len(cps) > max {
		//		max = len(cps)
		//	}

		for ; j <= i; j++ {
			dst := l.dataoff(p, j)
			kl := int(p[dst+1])
			cp := int(p[dst+2])
			lim := cp + kl
			if j < i {
				cl := len(cps) - 1
				ncp := cps[cl]
				cps = cps[:cl]
				pcp = ncp

				if int(ncp) < lim {
					lim = int(ncp)
				}

				/*
					dst := l.dataoff(p, j)
					nkl := int(p[dst+1])
					ncp2 := int(p[dst+2])

					tlog.Printf("limit %d: %x from cp+kl %x and cps %x  key %q %x:%x", j, lim, cp+kl, ncp, p[dst+3:dst+3+nkl], ncp2, nkl)
				*/
			}

			if lim < cp {
				continue
			}

			ik := p[dst+3 : dst+3+lim-cp]

			klen := lim
			if klen > len(k) {
				klen = len(k)
			}

			//	tlog.Printf("cmp %q[%x:%x] with %d/%d ik %q[%x:%x]", k, cp, klen, j, i, p[dst+3:dst+3+kl], cp, lim)

			c := bytes.Compare(ik, k[cp:klen])
			if c != 0 {
				if c == -1 && j > fastpath {
					fastpath = j
					fastval = -1
				}
				return c
			}
		}

		if i > fastpath {
			fastpath = i
			fastval = 0
		}

		eq = true

		return 0
	}

	//	tlog.Printf("search %q in %d\n%v", k, ln, hex.Dump(p))

	i = sort.Search(ln, func(i int) bool {
		return keycmp(i) >= 0
	})

	//	eq = i < ln && keycmp(i) == 0

	return
}

func (l *KVLayout) Key(off int64, i int, buf []byte) (r []byte, F int) {
	p := l.b.Access(off, l.page)
	//	tlog.Printf("get key off %x i %d from page\n%v", off, i, hex.Dump(p))
	r, F = l.key(p, i, buf)
	l.b.Unlock(p)
	return
}

func (l *KVLayout) key(p []byte, i int, buf []byte) (r []byte, F int) {
	dst := l.dataoff(p, i)
	F = int(p[dst])
	kl := int(p[dst+1])
	cp := int(p[dst+2])
	//	tlog.Printf("key i %d dst %x F %x kl %x cp %x\n%v", i, dst, F, kl, cp, hex.Dump(p))
	dst += 3

	if cp != 0 {
		buf, _ = l.key(p, i-1, buf)
	}

	r = append(buf[:cp], p[dst:dst+kl]...)

	return
}

func (l *KVLayout) Value(off int64, i int, buf []byte) (r []byte) {
	p := l.b.Access(off, l.page)

	dst := l.dataoff(p, i)
	dend := l.dataend(p, i)

	kl := int(p[dst+1])
	dst += 3 + kl

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
	dst += 3 + kl

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
			l.pageSavePrefix(p, i)
			l.pageMoveCopy(p, p, i, i+1, n)
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
	//	dst0 += 3

	// child page last key
	n := l.nkeys(p1)
	dst1 := l.dataoff(p1, n-1)
	kl1 := int(p1[dst1+1])
	dst1 += 3

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

	return l.insert(off, i, 0, lk[:kl1], lk[kl1:])
}

func (l *KVLayout) InsertPageLink(off int64, i int, cp int64) (loff, roff int64, err error) {
	var lk []byte

	p0, p1 := l.b.Access2(off, l.page, cp, l.page)
	// child page last key
	n := l.nkeys(p1)
	dst1 := l.dataoff(p1, n-1)
	kl1 := int(p1[dst1+1])
	dst1 += 3

	lk = make([]byte, kl1+8)
	copy(lk, p1[dst1:dst1+kl1])

	l.b.Unlock2(p0, p1)

	binary.BigEndian.PutUint64(lk[kl1:], uint64(cp))

	return l.insert(off, i, 0, lk[:kl1], lk[kl1:])
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

		//	tlog.Printf("insert at %d / %d  dsize %x / %x", i, n, 3+len(k)+len(v), free)

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

		l.pageMoveSplit(rp, lp, m, n)

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
	var cp int
	if i != 0 {
		if i < n {
			dst := l.dataoff(p, i)
			cp = int(p[dst+2])
		}

		dst := l.dataoff(p, i-1)
		jkl := int(p[dst+1])

		for cp < jkl && p[dst+3+cp] == k[cp] {
			cp++
		}
		//	tlog.Printf("insert key %q into pos %d. common prefix %x %+x with %q", k, i, cp, p[dst+3:dst+3+jkl])
	}
	k = k[cp:]

	dlen := 3 + len(k) + len(v)
	if i < n {
		dst := l.dataoff(p, i)
		jkl := int(p[dst+1])
		diff := 0

		for diff < jkl && diff < len(k) && p[dst+3+diff] == k[diff] {
			diff++
		}

		//	tlog.Printf("next key: cp %x %+x  kl %x of %q  new %q", jcp, diff, jkl, p[dst+3:dst+3+jkl], k)

		if diff != 0 {
			dend := l.dataend(p, i)
			p[dst+1] -= byte(diff)
			p[dst+2] += byte(diff)
			dst += 3
			copy(p[dst:], p[dst+diff:dend])
			//	tlog.Printf("next key cp increased %x %+x : %q", jcp, diff, p[dst:dst+int(p[dst-2])])
		}

		l.pageMoveFwd(p, p, 1, dlen-diff, i, n)
	}

	dend := l.dataend(p, i)
	dst := dend - dlen

	//	tlog.Printf("insert %d/%d  to %x - %x (%x)", i, n, dst, dend, dlen)

	p[dst] = byte(ff) // flags
	p[dst+1] = byte(len(k))
	p[dst+2] = byte(cp)
	copy(p[dst+3:], k)
	copy(p[dst+3+len(k):], v)

	l.setdataoff(p, i, dst)

	l.setnkeys(p, n+1)
}

func (l *KVLayout) pageMoveSplit(r, p []byte, i, n int) {
	l.pageMoveOne(r, p, 0, i)
	if i < n {
		l.pageMoveCopy(r, p, 1, i, n)
	}
}

func (l *KVLayout) pageMoveJoin(p, r []byte, pn, rn int) {
	l.pageMoveOne(p, r, pn, 0)
	if rn > 1 {
		l.pageMoveCopy(p, r, pn+1, 1, rn)
	}
}

func (l *KVLayout) pageMoveOne(r, p []byte, ri, i int) {
	tlog.Printf("move one key %d <- %d", ri, i)
	// first key
	j := i
	for ; j != 0; j-- {
		st := l.dataoff(p, j)
		cp := int(p[st+2])
		if cp == 0 {
			break
		}
	}

	st := l.dataoff(p, i)
	end := l.dataend(p, i)
	kl := int(p[st+1])
	cp := int(p[st+2])

	dlen := end - st + cp
	dend := l.dataend(r, ri)
	dst := dend - dlen

	r[dst] = p[st]
	r[dst+1] = byte(cp + kl)
	r[dst+2] = 0

	l.setdataoff(r, ri, dst)

	st += 3
	dst += 3

	for ; j <= i; j++ {
		st := l.dataoff(p, j)
		kl := int(p[st+1])
		cp := int(p[st+2])
		st += 3
		tlog.Printf("j %d/%d  key %x %x : %q", j, i, cp, kl, p[st:st+kl])

		copy(r[dst+cp:], p[st:st+kl])
	}

	copy(r[dst+cp+kl:], p[st+kl:end])

	tlog.Printf("split:\n%v\n%v", hex.Dump(r), hex.Dump(p))
}

func (l *KVLayout) pageMoveCopy(r, p []byte, i, st, nxt int) {
	if &r[0] == &p[0] && i > st {
		panic("forward move")
	}
	if st == nxt {
		panic("qq")
	}

	dst := l.dataend(p, nxt)
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

func (l *KVLayout) pageMove(r, p []byte, i, st, nxt int) {
	l.pageMoveCopy(r, p, i, st, nxt)
}

func (l *KVLayout) pageSavePrefix(p []byte, i int) {
	tlog.Printf("save key prefix: %d\n%v", i, hex.Dump(p))

	st := l.dataoff(p, i+1)
	end := l.dataend(p, i+1)
	kl := int(p[st+1])
	cp := int(p[st+2])

	pst := end
	pend := l.dataend(p, i)
	pkl := int(p[pst+1])
	pcp := int(p[pst+2])
	pvlen := pend - (pst + 3 + pkl)

	diff := cp - pcp
	if diff <= 0 {
		return
	}

	tlog.Printf("key cp %x kl %x  prefix cp %x kl %x  : %q", cp, kl, pcp, pkl, p[pst+3:pst+3+cp-pcp])
	if diff <= 3+pvlen {
		copy(p[pend-diff:pend], p[pst+3:]) // save prefix
		copy(p[st+3+diff:], p[st+3:end])   // move key + value
		copy(p[st+3:], p[pend-diff:pend])  // move prefix
	} else {
		n := l.nkeys(p)
		bufend := l.dataoff(p, n-1)
		bufst := kvIndexStart + 2*n

		kvlen := end - (st + 3)

		if diff <= bufend-bufst {
			copy(p[bufend-diff:bufend], p[pst+3:]) // save prefix
			copy(p[st+3+diff:], p[st+3:end])       // move key + value
			copy(p[st+3:], p[bufend-diff:bufend])  // move prefix
		} else if kvlen < bufend-bufst {
			copy(p[bufend-kvlen:bufend], p[st+3:end])   // save key + value
			copy(p[st+3:], p[pst+3:pst+3+diff])         // move prefix
			copy(p[st+3+diff:], p[bufend-kvlen:bufend]) // move key + value
		} else {
			buf := make([]byte, diff)
			copy(buf, p[pst+3:])             // save prefix
			copy(p[st+3+diff:], p[st+3:end]) // move key + value
			copy(p[st+3:], buf)              // move prefix
		}
	}
	p[st+1] += byte(diff)
	p[st+2] -= byte(diff)

	l.setdataoff(p, i, pst+diff)

	tlog.Printf("prefix saved: %d\n%v", i, hex.Dump(p))
}

func (l *KVLayout) pageMoveFwd(r, p []byte, di, dlen, i, n int) {
	dst := l.dataoff(p, n-1)
	dend := l.dataend(p, i)
	copy(p[dst-dlen:], p[dst:dend])

	for j := n - 1; j >= i; j-- {
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
		return int64(binary.BigEndian.Uint64(p[s+3+kl:]))
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
			l.pageMoveJoin(lp, rp, ln, rn)
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
