package xrain

import (
	"bytes"
	"encoding/binary"
)

const NilPage = -1

type (
	PageLayout interface {
		AllocRoot() (int64, error)
		Reclaim(p int64) error

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
		Siblings(p int64, i int, pi int64) (li int, l, r int64)
		Rebalance(l, r int64) (l_, r_ int64, _ error)
	}

	BaseLayout struct { // isbranch bit, size uint15, extended uint24, _ [3]byte, ver int64
		b    Back
		page int64
		ver  int64
		free *FreeList

		//	meta *treemeta

		ro bool
	}

	KVLayout struct { // base [2]byte, keys [size]int16, data []byte
		BaseLayout
	}

	FixedLayout struct { // base [2]byte, _ [14]byte, keyval []{int64,int64}
		BaseLayout
		k, v, kv, pm int
		p            int64
	}
)

func NewPageLayout(b Back, psize, ver int64, free *FreeList) BaseLayout {
	return BaseLayout{
		b:    b,
		page: psize,
		ver:  ver,
		free: free,
	}
}

func (l *BaseLayout) NKeys(off int64) (r int) {
	l.b.Access(off, 0x10, func(p []byte) {
		r = int(p[0])&0x7f<<8 | int(p[1])
	})
	return
}

func (l *BaseLayout) IsLeaf(off int64) (r bool) {
	l.b.Access(off, 0x10, func(p []byte) {
		r = p[0]&0x80 == 0
	})
	return
}

func (l *BaseLayout) Reclaim(off int64) error {
	if l.free == nil {
		return nil
	}

	var ver int64
	var n int
	l.b.Access(off, 0x10, func(p []byte) {
		ver = l.getver(p)
		n = l.extended(p)
	})

	return l.free.Reclaim(n, off, ver)
}

func (l *BaseLayout) alloc(nold, nnew int, off, ver int64) (noff int64, err error) {
	noff, err = l.free.Alloc(nnew)
	if err != nil {
		return
	}

	if off == NilPage {
		return noff, nil
	}

	err = l.free.Reclaim(nold, off, ver)
	if err != nil {
		return
	}

	min := nold
	if nnew < min {
		min = nnew
	}
	err = l.b.Copy(noff, off, int64(min)*l.page)
	if err != nil {
		return
	}

	return noff, nil
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

func (l *BaseLayout) extended(p []byte) int {
	return (int(p[2])<<16 | int(p[3])<<8 | int(p[4])) + 1
}

func (l *BaseLayout) setsize(p []byte, n int) {
	p[0] = p[0]&0x80 | byte(n>>8&0x7f)
	p[1] = byte(n)
}

func (l *BaseLayout) setextended(p []byte, n int) {
	n--
	p[2] = byte(n >> 16)
	p[3] = byte(n >> 8)
	p[4] = byte(n)
}

func (l *FixedLayout) alloc(off, ver int64) (_ int64, err error) {
	return l.BaseLayout.alloc(l.pm, l.pm, off, ver)
}

func (l *FixedLayout) setheader(p []byte) {
	l.setver(p, l.ver)
	l.setextended(p, l.pm)
}

func (l *FixedLayout) AllocRoot() (int64, error) {
	off, err := l.alloc(NilPage, 0)
	if err != nil {
		return NilPage, err
	}
	l.b.Access(off, 0x10, func(p []byte) {
		p[0] = 0x80
		l.setsize(p, 0)
		l.setheader(p)
	})
	return off, nil
}

func (l *FixedLayout) KeyCmp(off int64, i int, k []byte) (r int) {
	if k == nil {
		return 1
	}
	if len(k) != l.k {
		panic(len(k))
	}

	l.b.Access(off, l.p, func(p []byte) {
		v := l.v
		if p[0]&0x80 != 0 {
			v = 8
		}
		s := 16 + i*(l.k+v)
		r = bytes.Compare(p[s:s+l.k], k)
	})

	return
}

func (l *FixedLayout) Key(off int64, i int) (r []byte) {
	if !l.ro {
		r = make([]byte, l.k)
	}

	l.b.Access(off, l.p, func(p []byte) {
		v := l.v
		if p[0]&0x80 != 0 {
			v = 8
		}
		s := 16 + i*(l.k+v)

		if l.ro {
			r = p[s : s+l.k]
		} else {
			copy(r, p[s:s+l.k])
		}
	})

	return
}

func (l *FixedLayout) LastKey(off int64) (r []byte) {
	if !l.ro {
		r = make([]byte, l.v)
	}

	l.b.Access(off, l.p, func(p []byte) {
		v := l.v
		if p[0]&0x80 != 0 {
			v = 8
		}
		i := l.nkeys(p) - 1
		s := 16 + i*(l.k+v)

		if l.ro {
			r = p[s : s+l.k]
		} else {
			copy(r, p[s:s+l.k])
		}
	})

	return
}

func (l *FixedLayout) Value(off int64, i int) (r []byte) {
	if !l.ro {
		v := l.v
		if v < 8 {
			v = 8
		}
		r = make([]byte, v)
	}

	l.b.Access(off, l.p, func(p []byte) {
		v := l.v
		if p[0]&0x80 != 0 {
			v = 8
		}
		s := 16 + i*(l.k+v) + l.k

		if l.ro {
			r = p[s : s+v]
		} else {
			copy(r, p[s:s+v])
		}
	})

	return
}

func (l *FixedLayout) Int64(off int64, i int) (r int64) {
	l.b.Access(off, l.p, func(p []byte) {
		v := 8
		if p[0]&0x80 == 0 {
			v = l.v
			if v < 8 {
				panic(l.v)
			}
		}
		s := 16 + i*(l.k+v) + l.k

		r = int64(binary.BigEndian.Uint64(p[s : s+v]))
	})

	return
}

func (l *FixedLayout) Del(off int64, i int) (_ int64, err error) {
	var ver int64
	var alloc bool
again:
	l.b.Access(off, l.p, func(p []byte) {
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
		if p[0]&0x80 != 0 {
			kv = l.k + 8
		}

		n := l.nkeys(p)
		st := 16 + i*kv
		end := 16 + n*kv

		copy(p[st:], p[st+kv:end])
		l.setsize(p, n-1)
	})
	if alloc {
		off, err = l.alloc(off, ver)
		if err != nil {
			return
		}
		goto again
	}

	return off, nil
}

func (l *FixedLayout) Put(off int64, i int, k, v []byte) (loff, roff int64, err error) {
	var ver int64
	var alloc, split bool
again:
	l.b.Access(off, l.p, func(p []byte) {
		if alloc {
			l.setheader(p)
			alloc = false
		} else {
			ver = l.getver(p)
			alloc = ver != l.ver
		}

		kv := l.kv
		if p[0]&0x80 != 0 {
			kv = l.k + 8
		}
		n := l.nkeys(p)
		st := 16 + n*kv

		if st < len(p) {
			if ver == l.ver {
				l.putPage(p, kv, i, n, k, v)
			}
		} else {
			split = true
		}
	})
	if !alloc && !split {
		return off, NilPage, nil
	}
	if !split {
		off, err = l.alloc(off, ver)
		if err != nil {
			return
		}
		goto again
	}

	loff = off
	if alloc {
		loff, err = l.alloc(off, ver)
		if err != nil {
			return
		}
	}

	roff, err = l.alloc(NilPage, 0)
	if err != nil {
		return
	}

	l.b.Access2(loff, l.p, roff, l.p, func(lp, rp []byte) {
		rp[0] = lp[0]
		l.setheader(lp)
		l.setheader(rp)

		kv := l.kv
		if lp[0]&0x80 != 0 {
			kv = l.k + 8
		}

		n := l.nkeys(lp)
		m := n / 2
		if i > m {
			m = (n + 1) / 2
		}

		l.setsize(lp, m)
		l.setsize(rp, n-m)

		copy(rp[16:], lp[16+m*kv:16+n*kv])

		if i <= m {
			l.putPage(lp, kv, i, m, k, v)
		} else {
			l.putPage(rp, kv, i-m, n-m, k, v)
		}
	})

	return
}

func (l *FixedLayout) putPage(p []byte, kv, i, n int, k, v []byte) {
	st := 16 + i*kv
	end := 16 + n*kv
	copy(p[st+kv:], p[st:end])
	copy(p[st:], k)
	copy(p[st+l.k:], v)
	l.setsize(p, n+1)
}

func (l *FixedLayout) PutInt64(off int64, i int, k []byte, v int64) (loff, roff int64, err error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	return l.Put(off, i, k, buf[:])
}

func (l *FixedLayout) NeedRebalance(off int64) (r bool) {
	l.b.Access(off, l.p, func(p []byte) {
		n := l.nkeys(p)
		end := 16 + n*16
		r = end < len(p)/2
	})
	return
}

func (l *FixedLayout) Siblings(off int64, i int, poff int64) (li int, loff, roff int64) {
	l.b.Access(off, l.p, func(p []byte) {
		n := l.nkeys(p)
		if i+1 < n && i&1 == 0 {
			li = i
			loff = poff
			roff = l.Int64(off, i+1)
		} else {
			li = i - 1
			loff = l.Int64(off, i)
			roff = poff
		}
	})
	return
}

func (l *FixedLayout) Rebalance(lpoff, rpoff int64) (loff, roff int64, err error) {
	loff, roff = lpoff, rpoff
	var lalloc, ralloc bool
	var lver, rver int64
again:
	l.b.Access2(loff, l.p, roff, l.p, func(lp, rp []byte) {
		kv := l.kv
		if lp[0]&0x80 != 0 {
			kv = l.k + 8
		}

		ln := l.nkeys(lp)
		rn := l.nkeys(rp)
		sum := ln + rn
		rend := 16 + rn*kv
		lend := 16 + ln*kv

		if lalloc {
			l.setheader(lp)
			lalloc = false
		} else {
			lver = l.getver(lp)
			if lver != l.ver {
				lalloc = true
			}
		}

		if ralloc {
			l.setheader(rp)
			ralloc = false
		} else {
			rver = l.getver(rp)
			if rver != l.ver {
				ralloc = true
			}
		}

		if 16+sum*kv <= len(lp) {
			ralloc = false
		} else {
			d := ln - rn
			if d < 0 {
				d = -d
			}
			if d <= 1 {
				return // do not rebalance if no profit
			}
		}

		if lalloc || ralloc {
			return
		}

		if 16+sum*kv <= len(lp) {
			copy(lp[lend:], rp[16:rend])
			l.setsize(lp, sum)

			roff = NilPage
			return
		}

		m := (sum + 1) / 2
		end := 16 + m*16

		if ln > rn {
			diff := lend - end
			copy(rp[16+diff:], rp[16:rend])
			copy(rp[16:], lp[end:lend])
			l.setsize(lp, m)
			l.setsize(rp, sum-m)
		} else {
			diff := rend - end
			copy(lp[lend:], rp[16:16+diff])
			copy(rp[16:], rp[16+diff:rend])
			l.setsize(rp, m)
			l.setsize(lp, sum-m)
		}
	})
	if lalloc {
		loff, err = l.alloc(loff, lver)
		if err != nil {
			return
		}
	}
	if ralloc {
		roff, err = l.alloc(roff, rver)
		if err != nil {
			return
		}
	}
	if lalloc || ralloc {
		goto again
	}

	return
}
