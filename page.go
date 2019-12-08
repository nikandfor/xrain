package xrain

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"sort"
)

const NilPage = -1

const (
	Flags = 1 << iota
)

type (
	PageLayout interface {
		Serializer

		Supports(f int) bool

		Alloc(leaf bool) (int64, error)
		Free(p int64, recursive bool) error

		NKeys(p int64) int
		IsLeaf(p int64) bool
		SetLeaf(p int64, y bool)

		Search(p int64, k []byte) (i int, eq bool)
		Key(p int64, i int, buf []byte) ([]byte, int)

		Value(p int64, i int, buf []byte) []byte
		Int64(p int64, i int) int64

		Insert(p int64, i, F int, k, v []byte) (loff, roff int64, _ error)
		Delete(p int64, i int) (int64, error)

		UpdatePageLink(p int64, i int, c int64) (loff, roff int64, _ error)
		InsertPageLink(p int64, i int, c int64) (loff, roff int64, _ error)

		NeedRebalance(p int64) bool
		Siblings(p int64, i int, pi int64) (li int, l, r int64)
		Rebalance(l, r int64) (nl, nr int64, _ error)

		SetPageSize(page int64)
		SetVer(ver int64)
		SetFreelist(fl Freelist)
	}

	BaseLayout struct { // crc32 uint32, isbranch bit, size uint15, overflow uint16, ver int64
		b    Back
		page int64
		ver  int64
		free Freelist
	}

	FixedLayout struct { // base [16]byte, keyval []{fixed,fixed}
		BaseLayout
		k, v, kv, pm int
		p            int64
	}
)

func NewFixedLayout(b Back, page int64, fl Freelist) *FixedLayout {
	return &FixedLayout{
		BaseLayout: BaseLayout{
			b:    b,
			page: page,
			free: fl,
		},
		k:  8,
		v:  8,
		kv: 16,
		pm: 1,
		p:  page,
	}
}

func (l *BaseLayout) Supports(f int) bool { return false }

func (l *BaseLayout) SetVer(ver int64) {
	l.ver = ver
}

func (l *BaseLayout) SetPageSize(page int64) {
	l.page = page
}

func (l *BaseLayout) SetFreelist(fl Freelist) {
	l.free = fl
}

func (l *BaseLayout) NKeys(off int64) (r int) {
	p := l.b.Access(off, 0x10)
	r = int(p[4])&^0x80<<8 | int(p[5])
	l.b.Unlock(p)
	return r
}

func (l *BaseLayout) IsLeaf(off int64) (r bool) {
	p := l.b.Access(off, 0x10)
	r = l.isleaf(p)
	l.b.Unlock(p)
	return r
}

func (l *BaseLayout) SetLeaf(off int64, y bool) {
	p := l.b.Access(off, 0x10)
	l.setleaf(p, y)
	l.b.Unlock(p)
}

func (l *BaseLayout) Free(off int64, r bool) error {
	if l.free == nil {
		return nil
	}
	if r {
		panic("not supported")
	}

	p := l.b.Access(off, 0x10)
	ver := l.getver(p)
	n := 1 + l.overflow(p)
	l.b.Unlock(p)

	return l.free.Free(n, off, ver)
}

func (l *BaseLayout) realloc(n int, off, ver int64) (noff int64, err error) {
	noff, err = l.free.Alloc(n)
	if err != nil {
		return
	}

	l.b.Copy(noff, off, int64(n)*l.page)

	err = l.free.Free(n, off, ver)

	return
}

func (l *BaseLayout) isleaf(p []byte) bool {
	return p[4]&0x80 == 0
}

func (l *BaseLayout) setleaf(p []byte, y bool) {
	if y {
		p[4] &^= 0x80
	} else {
		p[4] |= 0x80
	}
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
	return int(p[4])&^0x80<<8 | int(p[5])
}

func (l *BaseLayout) setnkeys(p []byte, n int) {
	p[4] = p[4]&0x80 | byte(n>>8&^0x80)
	p[5] = byte(n)
}

func (l *BaseLayout) overflow(p []byte) int {
	return (int(p[6])<<8 | int(p[7]))
}

func (l *BaseLayout) setoverflow(p []byte, n int) {
	p[6] = byte(n >> 8)
	p[7] = byte(n)
}

func (l *BaseLayout) crccheck(p []byte) bool {
	sum := crc32.ChecksumIEEE(p[4:])
	exp := binary.BigEndian.Uint32(p)
	return sum == exp
}

func (l *BaseLayout) crccalc(p []byte) {
	sum := crc32.ChecksumIEEE(p[4:])
	binary.BigEndian.PutUint32(p, sum)
}

func (l *FixedLayout) Serialize(p []byte) int {
	if p == nil {
		return 3 * 8
	}
	s := 0
	s += binary.PutUvarint(p[s:], uint64(l.k))
	s += binary.PutUvarint(p[s:], uint64(l.v))
	s += binary.PutUvarint(p[s:], uint64(l.pm))
	return s
}

func (l *FixedLayout) Deserialize(p []byte) (int, error) {
	s := 0
	k, n := binary.Uvarint(p[s:])
	s += n
	v, n := binary.Uvarint(p[s:])
	s += n
	pm, n := binary.Uvarint(p[s:])
	s += n

	l.SetKVSize(int(k), int(v), int(pm))

	return s, nil
}

func (l *FixedLayout) SetPageSize(page int64) {
	l.page = page
	l.p = l.page * int64(l.pm)
}

func (l *FixedLayout) SetKVSize(k, v, pm int) {
	l.k = k
	l.v = v
	l.kv = k + v
	l.pm = pm
	l.p = l.page * int64(pm)
}

func (l *FixedLayout) Free(off int64, r bool) (err error) {
	if !r {
		return l.BaseLayout.Free(off, false)
	}

	var sub []int64
	p := l.b.Access(off, l.p)
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
			s := 16 + i*(l.k+8) + l.k
			sub[i] = int64(binary.BigEndian.Uint64(p[s : s+8]))
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

func (l *FixedLayout) setheader(p []byte) {
	l.setver(p, l.ver)
	l.setoverflow(p, l.pm-1)
}

func (l *FixedLayout) Alloc(leaf bool) (int64, error) {
	off, err := l.free.Alloc(l.pm)
	if err != nil {
		return NilPage, err
	}
	p := l.b.Access(off, 0x10)
	l.setleaf(p, leaf)
	l.setnkeys(p, 0)
	l.setheader(p)
	l.b.Unlock(p)
	return off, nil
}

func (l *FixedLayout) Search(off int64, k []byte) (i int, eq bool) {
	p := l.b.Access(off, l.p)

	ln := l.nkeys(p)
	kv := l.v
	if !l.isleaf(p) {
		kv = 8
	}
	kv += l.k

	keycmp := func(i int) int {
		s := 16 + i*kv

		return bytes.Compare(p[s:s+l.k], k)
	}

	i = sort.Search(ln, func(i int) bool {
		return keycmp(i) >= 0
	})

	eq = i < ln && keycmp(i) == 0

	l.b.Unlock(p)

	return
}

func (l *FixedLayout) Key(off int64, i int, buf []byte) (r []byte, F int) {
	if cap(buf) >= l.k {
		r = buf[:l.k]
	} else {
		r = make([]byte, l.k)
	}

	p := l.b.Access(off, l.p)
	v := l.v
	if !l.isleaf(p) {
		v = 8
	}
	s := 16 + i*(l.k+v)

	copy(r, p[s:s+l.k])
	l.b.Unlock(p)

	return
}

func (l *FixedLayout) Value(off int64, i int, buf []byte) []byte {
	v := l.v

	p := l.b.Access(off, l.p)
	if !l.isleaf(p) {
		v = 8
	}
	s := 16 + i*(l.k+v) + l.k

	buf = append(buf[:0], p[s:s+v]...)

	l.b.Unlock(p)

	return buf
}

func (l *FixedLayout) Int64(off int64, i int) int64 {
	v := l.v

	p := l.b.Access(off, l.p)
	defer l.b.Unlock(p)

	if !l.isleaf(p) {
		v = 8
	}
	if v != 8 {
		panic(v)
	}
	s := 16 + i*(l.k+v) + l.k

	return int64(binary.BigEndian.Uint64(p[s:]))
}

func (l *FixedLayout) Delete(off int64, i int) (_ int64, err error) {
	var ver int64
	var alloc bool
again:
	p := l.b.Access(off, l.p)
	func() {
		if alloc {
			l.setheader(p)
			alloc = false
		} else {
			ver = l.getver(p)
			if ver != l.ver {
				alloc = true
				return
			}
		}

		kv := l.kv
		if !l.isleaf(p) {
			kv = l.k + 8
		}

		n := l.nkeys(p)
		st := 16 + i*kv
		end := 16 + n*kv

		copy(p[st:], p[st+kv:end])
		l.setnkeys(p, n-1)
	}()
	l.b.Unlock(p)

	if alloc {
		off, err = l.realloc(l.pm, off, ver)
		if err != nil {
			return
		}
		goto again
	}

	return off, nil
}

func (l *FixedLayout) UpdatePageLink(off int64, i int, cp int64) (loff, roff int64, err error) {
	lk := make([]byte, l.k+8)

	p0, p1 := l.b.Access2(off, l.p, cp, l.p)
	// parent page old key
	//	s0 := 16 + i*(l.k+8)

	// child page last key
	v := l.v
	if !l.isleaf(p1) {
		v = 8
	}
	j := l.nkeys(p1) - 1
	s1 := 16 + j*(l.k+v)

	//	eq := bytes.Equal(p0[s0:s0+l.k], p1[s1:s1+l.k])

	//	if eq {
	copy(lk[:l.k], p1[s1:s1+l.k])
	//	}
	l.b.Unlock2(p0, p1)

	loff, err = l.Delete(off, i)
	if err != nil {
		return
	}

	binary.BigEndian.PutUint64(lk[l.k:], uint64(cp))

	return l.Insert(off, i, 0, lk[:l.k], lk[l.k:])
}

func (l *FixedLayout) InsertPageLink(off int64, i int, cp int64) (loff, roff int64, err error) {
	lk := make([]byte, l.k+8)

	p0, p1 := l.b.Access2(off, l.p, cp, l.p)
	// child page last key
	v := l.v
	if !l.isleaf(p1) {
		v = 8
	}
	j := l.nkeys(p1) - 1
	s1 := 16 + j*(l.k+v)

	copy(lk[:l.k], p1[s1:s1+l.k])
	l.b.Unlock2(p0, p1)

	binary.BigEndian.PutUint64(lk[l.k:], uint64(cp))

	return l.Insert(off, i, 0, lk[:l.k], lk[l.k:])
}

func (l *FixedLayout) Insert(off int64, i, F int, k, v []byte) (loff, roff int64, err error) {
	if len(k) != l.k {
		panic(len(k))
	}
	if len(v) != l.v {
		panic(len(v))
	}

	loff = off
	var ver int64
	var alloc, split bool
again:
	p := l.b.Access(loff, l.p)
	func() {
		if alloc {
			l.setheader(p)
			ver = l.ver
			alloc = false
		} else {
			ver = l.getver(p)
			alloc = ver != l.ver
		}

		kv := l.kv
		if !l.isleaf(p) {
			kv = l.k + 8
		}
		n := l.nkeys(p)
		st := 16 + n*kv

		if st+kv <= len(p) {
			if ver == l.ver {
				l.insertPage(p, i, n, k, v)
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
		loff, err = l.realloc(l.pm, loff, ver)
		if err != nil {
			return
		}
		goto again
	}
	if !split {
		goto again
	}

	roff, err = l.free.Alloc(l.pm)
	if err != nil {
		return
	}

	lp, rp := l.b.Access2(loff, l.p, roff, l.p)
	func() {
		rp[4] = lp[4]
		l.setheader(lp)
		l.setheader(rp)

		kv := l.kv
		if !l.isleaf(lp) {
			kv = l.k + 8
		}

		n := l.nkeys(lp)
		m := n / 2
		if i > m {
			m = (n + 1) / 2
		}

		l.setnkeys(lp, m)
		l.setnkeys(rp, n-m)

		copy(rp[16:], lp[16+m*kv:16+n*kv])

		if i <= m {
			l.insertPage(lp, i, m, k, v)
		} else {
			l.insertPage(rp, i-m, n-m, k, v)
		}
	}()
	l.b.Unlock2(lp, rp)

	return
}

func (l *FixedLayout) insertPage(p []byte, i, n int, k, v []byte) {
	st := 16 + i*l.kv
	end := 16 + n*l.kv
	copy(p[st+l.kv:], p[st:end])
	copy(p[st:st+l.k], k)
	copy(p[st+l.k:st+l.kv], v)
	l.setnkeys(p, n+1)
}

func (l *FixedLayout) NeedRebalance(off int64) (r bool) {
	p := l.b.Access(off, l.p)
	n := l.nkeys(p)
	end := 16 + n*16
	r = end < len(p)*2/5
	l.b.Unlock(p)
	return
}

func (l *FixedLayout) Siblings(off int64, i int, ioff int64) (li int, loff, roff int64) {
	readoff := func(p []byte, i int) int64 {
		s := 16 + i*(l.k+8) + l.k
		return int64(binary.BigEndian.Uint64(p[s:]))
	}

	p := l.b.Access(off, l.p)
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

func (l *FixedLayout) Rebalance(lpoff, rpoff int64) (loff, roff int64, err error) {
	loff, roff = lpoff, rpoff
	var lalloc, ralloc bool
	var rfree bool
	var lver, rver int64
again:
	lp, rp := l.b.Access2(loff, l.p, roff, l.p)
	func() {
		kv := l.kv
		if !l.isleaf(lp) {
			kv = l.k + 8
		}

		if lalloc {
			l.setheader(lp)
			lver = l.ver
			lalloc = false
		} else {
			lver = l.getver(lp)
			if lver != l.ver {
				lalloc = true
			}
		}

		if ralloc {
			l.setheader(rp)
			rver = l.ver
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
		rend := 16 + rn*kv
		lend := 16 + ln*kv

		if 16+sum*kv <= len(lp) {
			ralloc = false
		} else {
			d := ln - rn
			if d < 0 {
				d = -d
			}
			if d <= 1 {
				lalloc = false
				ralloc = false
				return // do not rebalance if no profit
			}
		}

		if lalloc || ralloc {
			return
		}

		if 16+sum*kv <= len(lp) {
			copy(lp[lend:], rp[16:rend])
			l.setnkeys(lp, sum)

			rfree = true
			return
		}

		m := (sum + 1) / 2
		end := 16 + m*16

		if ln > rn {
			diff := lend - end
			copy(rp[16+diff:], rp[16:rend])
			copy(rp[16:], lp[end:lend])
			l.setnkeys(lp, m)
			l.setnkeys(rp, sum-m)
		} else {
			diff := rend - end
			copy(lp[lend:], rp[16:16+diff])
			copy(rp[16:], rp[16+diff:rend])
			l.setnkeys(rp, m)
			l.setnkeys(lp, sum-m)
		}
	}()
	l.b.Unlock2(lp, rp)
	if lalloc {
		loff, err = l.realloc(l.pm, loff, lver)
		if err != nil {
			return
		}
	}
	if ralloc {
		roff, err = l.realloc(l.pm, roff, rver)
		if err != nil {
			return
		}
	}
	if lalloc || ralloc {
		goto again
	}

	if rfree {
		err = l.free.Free(l.pm, roff, rver)
		if err != nil {
			return
		}
		roff = NilPage
	}

	return
}
