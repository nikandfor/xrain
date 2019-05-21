package xrain

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"sort"
)

const NilPage = -1

type (
	PageLayout interface {
		Serializer

		Alloc(leaf bool) (int64, error)
		Free(p int64, recursive bool) error

		NKeys(p int64) int
		IsLeaf(p int64) bool
		SetLeaf(p int64, y bool)

		Search(p int64, k []byte) (i int, eq bool)
		Key(p int64, i int) []byte
		LastKey(p int64) []byte

		Value(p int64, i int, f func(v []byte, f int))
		ValueCopy(p int64, i int) []byte
		Int64(p int64, i int) int64

		Insert(p int64, i, kl, vl, flags int, f func(k, v []byte)) (l, r int64, _ error)

		Put(p int64, i int, k, v []byte) (loff, roff int64, _ error)
		PutInt64(p int64, i int, k []byte, v int64) (loff, roff int64, _ error)
		Del(p int64, i int) (int64, error)

		NeedRebalance(p int64) bool
		Siblings(p int64, i int, pi int64) (li int, l, r int64)
		Rebalance(l, r int64) (nl, nr int64, _ error)

		SetVer(ver int64)
		SetFreelist(fl Freelist)
	}

	BaseLayout struct { // crc32 uint32, isbranch bit, size uint15, extended uint16, ver int64
		b    Back
		page int64
		ver  int64
		free Freelist
	}

	KVLayout struct { // base [16]byte, keys [size]int16, data []byte
		BaseLayout
	}

	FixedLayout struct { // base [16]byte, _ [14]byte, keyval []{int64,int64}
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

func (l *BaseLayout) SetVer(ver int64) {
	l.ver = ver
}

func (l *BaseLayout) SetFreelist(fl Freelist) {
	l.free = fl
}

func (l *BaseLayout) NKeys(off int64) (r int) {
	l.b.Access(off, 0x10, func(p []byte) {
		r = int(p[4])&^0x80<<8 | int(p[5])
	})
	return
}

func (l *BaseLayout) IsLeaf(off int64) (r bool) {
	l.b.Access(off, 0x10, func(p []byte) {
		r = l.isleaf(p)
	})
	return
}

func (l *BaseLayout) SetLeaf(off int64, y bool) {
	l.b.Access(off, 0x10, func(p []byte) {
		l.setleaf(p, y)
	})
}

func (l *BaseLayout) Free(off int64, r bool) error {
	if l.free == nil {
		return nil
	}
	if r {
		panic("not supported")
	}

	var ver int64
	var n int
	l.b.Access(off, 0x10, func(p []byte) {
		ver = l.getver(p)
		n = l.extended(p)
	})

	return l.free.Free(n, off, ver)
}

func (l *BaseLayout) alloc(nold, nnew int, off, ver int64, f func(noff, off int64)) (noff int64, err error) {
	noff, err = l.free.Alloc(nnew)
	if err != nil {
		return
	}
	if off == NilPage {
		return noff, nil
	}

	if f != nil {
		f(noff, off)
	}

	err = l.free.Free(nold, off, ver)
	if err != nil {
		return
	}

	return noff, nil
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

func (l *BaseLayout) extended(p []byte) int {
	return (int(p[6])<<8 | int(p[7])) + 1
}

func (l *BaseLayout) setsize(p []byte, n int) {
	p[4] = p[4]&0x80 | byte(n>>8&^0x80)
	p[5] = byte(n)
}

func (l *BaseLayout) setextended(p []byte, n int) {
	n--
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

func (*FixedLayout) SerializerName() string { return "FixedLayout" }

func (*FixedLayout) Deserialize(ctx *SerializeContext, p []byte) (interface{}, int, error) {
	l := NewFixedLayout(ctx.Back, ctx.Page, ctx.Freelist)

	s := 0
	k, n := binary.Uvarint(p[s:])
	s += n
	v, n := binary.Uvarint(p[s:])
	s += n
	pm, n := binary.Uvarint(p[s:])
	s += n

	l.SetKVSize(int(k), int(v), int(pm))

	return l, s, nil
}

func (l *FixedLayout) Serialize(p []byte) int {
	s := 0
	s += binary.PutUvarint(p[s:], uint64(l.k))
	s += binary.PutUvarint(p[s:], uint64(l.v))
	s += binary.PutUvarint(p[s:], uint64(l.pm))
	return s
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
	l.b.Access(off, l.p, func(p []byte) {
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
	})

	for _, off := range sub {
		err = l.BaseLayout.Free(off, true)
		if err != nil {
			return
		}
	}

	return l.BaseLayout.Free(off, false)
}

func (l *FixedLayout) setheader(p []byte) {
	l.setver(p, l.ver)
	l.setextended(p, l.pm)
}

func (l *FixedLayout) alloc(off, ver int64, f func(noff, off int64)) (_ int64, err error) {
	return l.BaseLayout.alloc(l.pm, l.pm, off, ver, f)
}

func (l *FixedLayout) Alloc(leaf bool) (int64, error) {
	off, err := l.alloc(NilPage, 0, nil)
	if err != nil {
		return NilPage, err
	}
	l.b.Access(off, 0x10, func(p []byte) {
		l.setleaf(p, leaf)
		l.setsize(p, 0)
		l.setheader(p)
	})
	return off, nil
}

func (l *FixedLayout) Search(off int64, k []byte) (i int, eq bool) {
	l.b.Access(off, l.p, func(p []byte) {
		ln := l.nkeys(p)
		keycmp := func(i int) int {
			v := l.v
			if !l.isleaf(p) {
				v = 8
			}
			s := 16 + i*(l.k+v)

			return bytes.Compare(p[s:s+l.k], k)
		}
		i = sort.Search(ln, func(i int) bool {
			return keycmp(i) >= 0
		})
		eq = i < ln && keycmp(i) == 0
	})
	return
}

func (l *FixedLayout) Key(off int64, i int) (r []byte) {
	r = make([]byte, l.k)

	l.b.Access(off, l.p, func(p []byte) {
		v := l.v
		if !l.isleaf(p) {
			v = 8
		}
		s := 16 + i*(l.k+v)

		copy(r, p[s:s+l.k])
	})

	return
}

func (l *FixedLayout) LastKey(off int64) (r []byte) {
	r = make([]byte, l.v)

	l.b.Access(off, l.p, func(p []byte) {
		v := l.v
		if !l.isleaf(p) {
			v = 8
		}
		i := l.nkeys(p) - 1
		s := 16 + i*(l.k+v)

		copy(r, p[s:s+l.k])
	})

	return
}

func (l *FixedLayout) Value(off int64, i int, f func(v []byte, flags int)) {
	v := l.v

	l.b.Access(off, l.p, func(p []byte) {
		if !l.isleaf(p) {
			v = 8
		}
		s := 16 + i*(l.k+v) + l.k

		f(p[s:s+v], 0)
	})

	return
}

func (l *FixedLayout) Int64(off int64, i int) (r int64) {
	l.Value(off, i, func(v []byte, flags int) {
		r = int64(binary.BigEndian.Uint64(v))
	})
	return
}

func (l *FixedLayout) ValueCopy(off int64, i int) (r []byte) {
	v := l.v
	if v < 8 {
		v = 8
	}
	r = make([]byte, v)
	l.Value(off, i, func(v []byte, flags int) {
		copy(r, v)
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
		if !l.isleaf(p) {
			kv = l.k + 8
		}

		n := l.nkeys(p)
		st := 16 + i*kv
		end := 16 + n*kv

		copy(p[st:], p[st+kv:end])
		l.setsize(p, n-1)
	})
	if alloc {
		off, err = l.alloc(off, ver, l.copyPage)
		if err != nil {
			return
		}
		goto again
	}

	return off, nil
}

func (l *FixedLayout) Insert(off int64, i, _, _, flags int, f func(k, v []byte)) (loff, roff int64, err error) {
	loff = off
	var ver int64
	var alloc, split bool
again:
	l.b.Access(loff, l.p, func(p []byte) {
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

		if st < len(p) {
			if ver == l.ver {
				l.insertPage(p, i, n, f)
			}
		} else {
			split = true
		}
	})
	if !alloc && !split {
		return loff, NilPage, nil
	}
	if alloc {
		loff, err = l.alloc(loff, ver, l.copyPage)
		if err != nil {
			return
		}
	}
	if !split {
		goto again
	}

	roff, err = l.alloc(NilPage, 0, nil)
	if err != nil {
		return
	}

	l.b.Access2(loff, l.p, roff, l.p, func(lp, rp []byte) {
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

		l.setsize(lp, m)
		l.setsize(rp, n-m)

		copy(rp[16:], lp[16+m*kv:16+n*kv])

		if i <= m {
			l.insertPage(lp, i, m, f)
		} else {
			l.insertPage(rp, i-m, n-m, f)
		}
	})

	return
}

func (l *FixedLayout) insertPage(p []byte, i, n int, f func(k, v []byte)) {
	st := 16 + i*l.kv
	end := 16 + n*l.kv
	copy(p[st+l.kv:], p[st:end])
	f(p[st:st+l.k], p[st+l.k:st+l.kv])
	l.setsize(p, n+1)
}

func (l *FixedLayout) Put(off int64, i int, k, v []byte) (loff, roff int64, err error) {
	return l.Insert(off, i, 0, 0, 0, func(km, vm []byte) {
		copy(km, k)
		copy(vm, v)
	})
}

func (l *FixedLayout) PutInt64(off int64, i int, k []byte, v int64) (loff, roff int64, err error) {
	return l.Insert(off, i, 0, 0, 0, func(km, vm []byte) {
		copy(km, k)
		binary.BigEndian.PutUint64(vm, uint64(v))
	})
}

func (l *FixedLayout) NeedRebalance(off int64) (r bool) {
	l.b.Access(off, l.p, func(p []byte) {
		n := l.nkeys(p)
		end := 16 + n*16
		r = end < len(p)*2/5
	})
	return
}

func (l *FixedLayout) Siblings(off int64, i int, poff int64) (li int, loff, roff int64) {
	readoff := func(p []byte, i int) int64 {
		s := 16 + i*(l.k+8) + l.k
		return int64(binary.BigEndian.Uint64(p[s:]))
	}

	l.b.Access(off, l.p, func(p []byte) {
		n := l.nkeys(p)
		if i+1 < n && i&1 == 0 {
			li = i
			loff = poff
			roff = readoff(p, i+1)
		} else {
			li = i - 1
			loff = readoff(p, i-1)
			roff = poff
		}
	})
	return
}

func (l *FixedLayout) Rebalance(lpoff, rpoff int64) (loff, roff int64, err error) {
	loff, roff = lpoff, rpoff
	var lalloc, ralloc bool
	var rfree bool
	var lver, rver int64
again:
	l.b.Access2(loff, l.p, roff, l.p, func(lp, rp []byte) {
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
				return // do not rebalance if no profit
			}
		}

		if lalloc || ralloc {
			return
		}

		if 16+sum*kv <= len(lp) {
			copy(lp[lend:], rp[16:rend])
			l.setsize(lp, sum)

			rfree = true
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
		loff, err = l.alloc(loff, lver, l.copyPage)
		if err != nil {
			return
		}
	}
	if ralloc {
		roff, err = l.alloc(roff, rver, l.copyPage)
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

func (l *FixedLayout) copyPage(noff, off int64) {
	l.b.Access2(noff, l.p, off, l.p, func(n, o []byte) {
		copy(n, o)
	})
}
