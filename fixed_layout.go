package xrain

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

type (
	FixedLayout struct {
		BaseLayout2
		ff, k, v, kv, fkv, pm int
		p                     int64
	}
)

const fixedIndexStart = 0x10

var _ Layout = &FixedLayout{}

func NewFixedLayout(c *Common) *FixedLayout {
	l := &FixedLayout{
		BaseLayout2: BaseLayout2{
			Common: c,
		},
	}

	l.SetKVSize(0, 8, 8, 1)

	return l
}

func (l *FixedLayout) dataoff(leaf bool, i int) int {
	if leaf {
		return fixedIndexStart + i*l.fkv
	} else {
		return fixedIndexStart + i*(l.k+8)
	}
}

func (l *FixedLayout) datakeyoff(leaf bool, i int) int {
	if leaf {
		return fixedIndexStart + i*l.fkv + l.f
	} else {
		return fixedIndexStart + i*(l.k+8)
	}
}

func (l *FixedLayout) datavaloff(leaf bool, i int) int {
	if leaf {
		return fixedIndexStart + i*l.fkv + l.f + l.k
	} else {
		return fixedIndexStart + i*(l.k+8) + l.k
	}
}

func (l *FixedLayout) pagelink(p []byte, i int) (off int64) {
	var buf [8]byte

	isleaf := l.isleaf(p)
	st := l.datavaloff(isleaf, i)

	off = int64(binary.BigEndian.Uint64(p[st:]))

	return
}

func (l *FixedLayout) SetKVSize(ff, k, v, pm int) {
	l.ff = ff
	l.k = k
	l.v = v
	l.fkv = ff + k + v
	l.kv = k + v
	l.pm = pm
	l.p = l.Page * int64(pm)
}

func (l *FixedLayout) Free(off int64) error {
	p := l.Access(off, 0x10)

	pages := 1 + l.overflow(p)
	ver := l.pagever(p)

	var sub []int64
	if !l.isleaf() {
		n := l.nkeys(p)
		sub = make([]int64, n)

		for i := range sub {
			sub[i] = l.pagelink(p, i)
		}
	}

	l.Unlock(p)

	err := l.Freelist.Free(off, ver, pages)
	if err != nil {
		return err
	}

	for _, off := range sub {
		err = l.Free(off)
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *FixedLayout) Flags(st Stack) (ff int) {
	off, i := st.LastOffIndex(l.Mask)

	p := l.Access(off, l.p)

	isleaf := l.isleaf(p)
	dst := l.dataoff(isleaf, i)

	for j := l.ff - 1; j >= 0; j-- {
		ff = ff<<8 | int(p[dst+j])
	}

	l.Unlock(p)

	return
}

func (l *FixedLayout) Key(st Stack, buf []byte) (k []byte, ff int) {
	off, i := st.LastOffIndex(l.Mask)

	p := l.Access(off, l.p)

	isleaf := l.isleaf(p)
	dst := l.dataoff(isleaf, i)

	if isleaf {
		for j := l.ff - 1; j >= 0; j-- {
			ff = ff<<8 | int(p[dst+j])
		}

		dst += l.ff
	}

	k = append(buf, p[dst:dst+l.k]...)

	l.Unlock(p)

	return
}

func (l *FixedLayout) Value(st Stack, buf []byte) (v []byte) {
	off, i := st.LastOffIndex(l.Mask)
	return l.value(off, i, buf)
}

func (l *FixedLayout) value(off int64, i int, buf []byte) (v []byte) {
	p := l.Access(off, l.p)

	isleaf := l.isleaf(p)
	st := l.dataoff(isleaf, i)

	lv := 8
	if isleaf {
		st += l.ff
		lv = l.v
	}
	st += l.k

	v = append(buf, p[st:st+lv]...)

	//	tl.Printf("value %x %d -> % 2x\n%v", off, i, v, hex.Dump(p))

	l.Unlock(p)

	return
}

func (l *FixedLayout) link(st Stack) (off int64) {
	var buf [8]byte

	l.Value(st, buf[:0])

	off = int64(binary.BigEndian.Uint64(buf[:]))

	//	tl.Printf("link: %x %d -> %x %q", st[len(st)-1].Off(l.Mask), st[len(st)-1].Index(l.Mask), off, buf[:])

	return off
}

func (l *FixedLayout) Seek(st Stack, root int64, k []byte) (_ Stack, eq bool) {
	st = st[:0]

	if root == NilPage {
		return nil, false
	}

	off := root
	var isleaf bool
	var i, n, d int

	for {
		st = append(st, OffIndex(off))

		prev := off
		i, n, off, eq, isleaf = l.search(off, k)
		tl.V("seek").Printf("search %4x %q -> %3x %2d/%2d eq %5v leaf %5v  st %v", prev, k, off, i, n, eq, isleaf, st)

		if isleaf {
			st[d] |= OffIndex(i)
			break
		}

		if i == n {
			i--
		}

		st[d] |= OffIndex(i)
		d++
	}
	//	log.Printf("seek      %q -> %x %v", k, st, eq)

	return st, eq
}

func (l *FixedLayout) search(off int64, k []byte) (i, n int, coff int64, eq, isleaf bool) {
	pages := 1

again:
	p := l.Access(off, l.Page*int64(pages))
	if o := l.overflow(p); o+1 != pages {
		l.Unlock(p)

		pages = 1 + o

		goto again
	}

	keycmp := func(i int) int {
		isleaf := l.isleaf(p)

		ff := 0
		if isleaf {
			ff = l.ff
		}

		dst := l.dataoff(isleaf, i)
		ik := p[dst+ff : dst+ff+l.k]

		return bytes.Compare(ik, k)
	}

	n = l.nkeys(p)

	i = sort.Search(n, func(i int) bool {
		return keycmp(i) >= 0
	})

	isleaf = l.isleaf(p)

	if isleaf {
		eq = i < n && keycmp(i) == 0
	} else {
		if i == n {
			i--
		}

		dst := l.dataoff(isleaf, i) + l.k
		v := p[dst : dst+8]

		coff = int64(binary.BigEndian.Uint64(v))

		tl.V("search").Printf("found link %x at %x[%x:] on page\n%v", coff, off, dst, hex.Dump(p))
	}

	l.Unlock(p)

	return
}

func (l *FixedLayout) Step(st Stack, root int64, back bool) Stack {
	if len(st) == 0 {
		return l.firstLast(st, root, back)
	}

	last := len(st) - 1
	off, i := st[last].OffIndex(l.Mask)

	if back {
		if i > 0 {
			i--
			st[last] = MakeOffIndex(off, i)
			return st
		}
	} else {
		n := l.nKeys(off)
		i++
		if i < n {
			st[last] = MakeOffIndex(off, i)
			return st
		}
	}

	if last == 0 {
		return nil
	}

	par := l.Step(st[:last], root, back)
	if par == nil {
		return nil
	}

	off = l.link(par)

	if back {
		i = l.nKeys(off) - 1
	} else {
		i = 0
	}

	st[last] = MakeOffIndex(off, i)

	return st
}

func (l *FixedLayout) firstLast(st Stack, root int64, back bool) Stack {
	if root == NilPage {
		return nil
	}

	off := root
	isleaf := false
	var i int

	for {
		//	tl.Printf("here, off %x\n%v", off, l.dumpPage(off))
		p := l.Access(off, l.p)
		func() {
			n := l.nkeys(p)
			if n == 0 {
				st = nil
				return
			}

			if back {
				i = n - 1
			} else {
				i = 0
			}

			st = append(st, MakeOffIndex(off, i))

			isleaf = l.isleaf(p)
			if isleaf {
				return
			}

			dst := l.dataoff(isleaf, i)
			dst += l.k

			v := p[dst : dst+l.v]
			off = int64(binary.BigEndian.Uint64(v))
		}()
		l.Unlock(p)

		if isleaf || st == nil {
			return st
		}
	}
}

func (l *FixedLayout) Insert(st Stack, ff int, k, v []byte) (_ Stack, err error) {
	off, i := st.LastOffIndex(l.Mask)

	pages := l.pm

	var di int
	var off1 int64 = NilPage
	var ver int64
	var alloc, split bool

again:
	p := l.Access(off, int64(pages)*l.Page)
	func() {
		ver := l.pagever(p)
		alloc = ver != l.Ver

		n := l.nkeys(p)
		split = l.dataoff(true, n+1) > int(l.Page)

		if alloc || split && pages == l.pm {
			return
		}

		if !split {
			l.pageInsert(p, i, n, ff, k, v)

			return
		}

		m := l.pageSplit(p[:l.Page], p[l.Page:], n)

		if i <= m {
			l.pageInsert(p[:l.Page], i, m, ff, k, v)

			st[len(st)-1] = MakeOffIndex(off, i)
		} else {
			l.pageInsert(p[l.Page:], i-m, n-m, ff, k, v)

			st[len(st)-1] = MakeOffIndex(off+l.p, i-m)
			di = 1
		}

		split = false
	}()
	l.Unlock(p)

	if split {
		pages = l.pm * 2

		tl.V("split").Printf("split %x (%v) by put %q -> %q", off, st, k, v)

		off, err = l.realloc(off, ver, l.pm, pages)
		if err != nil {
			return
		}

		off1 = off + l.p

		goto again
	}

	if alloc {
		off, err = l.realloc(off, ver, l.pm, l.pm)
		if err != nil {
			return
		}

		goto again
	}

	return l.out(st, off, off1, di)
}

func (l *FixedLayout) pageInsert(p []byte, i, n, ff int, k, v []byte) {
	isleaf := l.isleaf(p)

	dst := l.dataoff(isleaf, i)

	if i < n {
		kv := l.fkv
		if !l.isleaf(p) {
			kv = l.kv
		}
		copy(p[dst+kv:], p[dst:l.Page])
	}

	p[dst] = byte(ff)
	if isleaf {
		dst += l.ff
	}

	copy(p[dst:], k)
	copy(p[dst+l.k:], v)

	l.setnkeys(p, n+1)
}

func (l *FixedLayout) pageSplit(p, r []byte, n int) int {
	isleaf := l.isleaf(p)

	m := (n + 1) / 2

	dst := l.dataoff(isleaf, m)
	dend := l.dataoff(isleaf, n)

	st := l.dataoff(isleaf, 0)

	copy(r[st:], p[dst:dend])

	l.setnkeys(p, m)
	l.setnkeys(r, n-m)

	l.setoverflow(p, l.pm-1)
	l.setoverflow(r, l.pm-1)

	l.setver(p, l.Ver)
	l.setver(r, l.Ver)

	return m
}

func (l *FixedLayout) out(s Stack, off0, off1 int64, di int) (_ Stack, err error) {
	var stop bool

	tl.V("").Printf("out %v  %x %x %d", s, off0, off1, di)

	for d := len(s) - 2; d >= 0; d-- {
		off, i := s[d].OffIndex(l.Mask)

		var alloc, split bool
		var ver int64

	again:
		p, cp := l.Access2(off, l.p, off0, l.p)
		func() {
			ver = l.pagever(p)
			alloc = ver != l.Ver

			//	tl.V("").Printf("out pages %x.%d -> %x  root ver %d\n%v\n%v", off, i, off0, ver, hex.Dump(p), hex.Dump(cp))

			n := l.nkeys(p)
			split = off1 != NilPage && l.dataoff(false, n+1) >= int(l.p)

			st := l.dataoff(false, i)
			cn := l.nkeys(cp)
			cleaf := l.isleaf(cp)
			cst := l.dataoff(cleaf, cn-1)
			if cleaf {
				cst += l.ff
			}

			oldoff := int64(binary.BigEndian.Uint64(p[st+l.k : st+l.k+8]))
			stop = bytes.Equal(p[st:st+l.k], cp[cst:cst+l.k]) && oldoff == off0

			tl.V("out").Printf("out %x.%d -> %x (%x) st %x cst %x  cleaf %v", off, i, off0, off1, st, cst, cleaf)

			if stop {
				return
			}

			if alloc {
				return
			}

			copy(p[st:st+l.k], cp[cst:])
			binary.BigEndian.PutUint64(p[st+l.k:], uint64(off0))

			//	tl.V("out").Printf("res pages %x.%d -> %x  root ver %d\n%v", off, i, off0, ver, hex.Dump(p))

			s[d] = MakeOffIndex(off, i+di)
		}()
		l.Unlock2(p, cp)

		if stop {
			break
		}

		pages := l.pm
		if alloc && !split {
			off, err = l.realloc(off, ver, l.pm, l.pm)
			if err != nil {
				return
			}

			goto again
		}

		if off1 == NilPage {
			off0 = off
			di = 0
			continue
		}

		if split {
			pages *= 2

			off, err = l.realloc(off, ver, l.pm, pages)
			if err != nil {
				return
			}
		}

		var shift int64

		p, cp = l.Access2(off, int64(pages)*l.p, off1, l.p)
		func() {
			n := l.nkeys(p)

			if split {
				m := l.pageSplit(p[:l.p], p[l.p:], n)

				if i+1 <= m {
					n = m

					di = 0
				} else {
					shift = l.p
					i -= m
					n -= m

					s[d] = MakeOffIndex(off+shift, i+di)

					di = 1
				}
			}

			st := l.dataoff(false, i+1)
			cn := l.nkeys(cp)
			cleaf := l.isleaf(cp)
			cst := l.dataoff(cleaf, cn-1)
			if cleaf {
				cst += l.ff
			}

			kv := cp[cst : cst+l.k+8]

			tl.V("out").Printf("out %x.%d -> %x (%x) st %x cst %x  cleaf %v  split %v\n%v\n%v", off, i, off1, off0, st, cst, cleaf, split, hex.Dump(p), hex.Dump(cp))

			l.pageInsert(p[shift:], i+1, n, 0, kv[:l.k], kv[l.k:])

			binary.BigEndian.PutUint64(p[st+l.k:], uint64(off1))
		}()
		l.Unlock2(p, cp)

		off0 = off
		if split {
			off1 = off + shift
		} else {
			off1 = NilPage
		}
	}

	if off1 != NilPage {
		root, err := l.Freelist.Alloc(l.pm)
		if err != nil {
			return nil, err
		}

		l.appendLink(root, 0, off0)
		l.appendLink(root, 1, off1)

		s = append(s, 0)
		copy(s[1:], s)
		s[0] = MakeOffIndex(root, di)

		tl.V("").Printf("out grow %x -> %x %x   : %v", root, off0, off1, s)
		if tl.V("dump") != nil {
			p := l.Access(root, l.p)
			tl.Printf("dump %x\n%v", root, hex.Dump(p))
			l.Unlock(p)
		}
	} else if !stop {
		s[0] = MakeOffIndex(off0, s[0].Index(l.Mask)+di)
	}

	return s, err
}

func (l *FixedLayout) appendLink(root int64, i int, off int64) {
	p, cp := l.Access2(root, l.p, off, l.p)
	func() {
		l.setleaf(p, false)
		l.setver(p, l.Ver)

		st := l.dataoff(false, i)
		cleaf := l.isleaf(cp)
		cst := l.dataoff(cleaf, l.nkeys(cp)-1)

		ff := 0
		if cleaf {
			ff = l.ff
		}

		copy(p[st:], cp[cst+ff:cst+ff+l.k])

		binary.BigEndian.PutUint64(p[st+l.k:], uint64(off))

		l.setnkeys(p, i+1)

		tl.V("grow").Printf("out %x to %x of\n%v", off, st+l.k, hex.Dump(p))
	}()
	l.Unlock2(p, cp)
}

func (l *FixedLayout) Delete(st Stack) (_ Stack, err error) {

	off, i := st.LastOffIndex(l.Mask)

	var rebalance, alloc bool
	var ver int64

again:
	p := l.Access(off, l.p)
	func() {
		defer l.Unlock(p)

		n := l.nkeys(p)
		isleaf := l.isleaf(p)

		ver = l.pagever(p)
		alloc = ver != l.Ver

		rebalance = (n-1)*l.fkv < int(l.p)*2/5 && len(st) > 1

		if alloc {
			return
		}

		l.pageDel(p, isleaf, i, n)
	}()

	if alloc {
		off, err = l.realloc(off, ver, l.pm, l.pm)
		if err != nil {
			return
		}

		goto again
	}

	return l.outDel(st, off, rebalance)
}

func (l *FixedLayout) pageDel(p []byte, isleaf bool, i, n int) {
	dst := l.dataoff(isleaf, i)

	fkv := l.fkv
	if !isleaf {
		fkv = l.kv
	}

	copy(p[dst:], p[dst+fkv:])

	l.setnkeys(p, n-1)
}

func (l *FixedLayout) outDel(s Stack, coff int64, rebalance bool) (_ Stack, err error) {
	var reduce, stop bool
	var ver int64

	tl.V("out").Printf("outDel st %v coff %x rebalance %v", s, coff, rebalance)

	d := len(s) - 1
	for d--; d >= 0; d-- {
		var di int = -1

		if rebalance {
			di, err = l.rebalance(s[:d+2], coff)
			if err != nil {
				return
			}
		}

		off, i := s[d].OffIndex(l.Mask)

		var alloc bool

	again:
		p, cp := l.Access2(off, l.p, coff, l.p)
		func() {
			defer l.Unlock2(p, cp)

			ver = l.pagever(p)
			alloc = ver != l.Ver

			st := l.dataoff(false, i)
			cn := l.nkeys(cp)
			cleaf := l.isleaf(cp)
			cst := l.dataoff(cleaf, cn-1)
			if cleaf {
				cst += l.ff
			}

			oldoff := int64(binary.BigEndian.Uint64(p[st+l.k : st+l.k+8]))
			stop = bytes.Equal(p[st:st+l.k], cp[cst:cst+l.k]) && oldoff == coff && di == -1

			tl.V("out").Printf("out %x.%d -> %x  st %x cst %x  cleaf %v", off, i, coff, st, cst, cleaf)

			if stop {
				return
			}

			if alloc {
				return
			}

			copy(p[st:st+l.k], cp[cst:])
			binary.BigEndian.PutUint64(p[st+l.k:], uint64(coff))

			if di == -1 {
				rebalance = false
				return
			}

			n := l.nkeys(p)

			l.pageDel(p, false, di, n)
			n--

			if di < i {
				s[d] = MakeOffIndex(off, i-1)
			}

			rebalance = true

			if d == 0 && n == 1 {
				reduce = true
			}
		}()

		if stop {
			break
		}

		if alloc {
			off, err = l.realloc(off, ver, l.pm, l.pm)
			if err != nil {
				return
			}

			goto again
		}

		coff = off
	}

	if !stop {
		s[0] = MakeOffIndex(coff, s[0].Index(l.Mask))
	}

	if reduce {
		root := s[0].Off(l.Mask)

		err = l.Freelist.Free(root, ver, l.pm)
		if err != nil {
			return
		}

		copy(s, s[1:])
		s = s[:len(s)-1]
	}

	return s, nil
}

func (l *FixedLayout) rebalance(st Stack, off int64) (di int, err error) {
	off0, off1, i0, i1 := l.sibling(st, off)

	var merge bool
	var ver int64

	p0, p1 := l.Access2(off0, l.p, off1, l.p)
	func() {
		defer l.Unlock2(p0, p1)

		isleaf := l.isleaf(p0)

		n0 := l.nkeys(p0)
		n1 := l.nkeys(p1)

		merge = l.dataoff(isleaf, n0+n1-1) <= int(l.p)

		if !merge {
			di = -1

			return
		}

		if off == off0 {
			st := l.dataoff(isleaf, n0)
			sst := l.dataoff(isleaf, 0)
			send := l.dataoff(isleaf, n1)

			copy(p0[st:], p1[sst:send])

			l.setnkeys(p0, n0+n1)

			ver = l.pagever(p1)
			di = i1
		} else {
			st := l.dataoff(isleaf, 0)
			end := l.dataoff(isleaf, n0)
			size := end - st

			end1 := l.dataoff(isleaf, n1)

			copy(p1[st+size:], p1[st:end1])
			copy(p1[st:], p0[st:end])

			l.setnkeys(p1, n0+n1)

			ver = l.pagever(p0)
			di = i0
		}
	}()

	if !merge {
		return -1, nil
	}

	if off == off0 {
		err = l.Freelist.Free(off1, ver, l.pm)
	} else {
		err = l.Freelist.Free(off0, ver, l.pm)
	}
	if err != nil {
		return
	}

	return di, nil
}

func (l *FixedLayout) sibling(st Stack, off int64) (off0, off1 int64, i0, i1 int) {
	poff, pi := st[len(st)-2].OffIndex(l.Mask)

	p := l.Access(poff, l.p)
	func() {
		defer l.Unlock(p)

		n := l.nkeys(p)

		if pi&1 == 1 || pi == n-1 {
			i0 = pi - 1
			i1 = pi
			off1 = off

			dst := l.dataoff(false, i0) + l.k
			off0 = int64(binary.BigEndian.Uint64(p[dst:]))
		} else {
			i0 = pi
			i1 = pi + 1
			off0 = off

			dst := l.dataoff(false, i1) + l.k
			off1 = int64(binary.BigEndian.Uint64(p[dst:]))
		}
	}()

	return
}

func (l *FixedLayout) dumpPage(off int64) string {
	var buf bytes.Buffer

	var isleaf bool
	var n int

	p := l.Access(off, l.Page)
	{
		isleaf = l.isleaf(p)
		tp := 'B'
		if isleaf {
			tp = 'D'
		}
		ver := l.pagever(p)
		over := l.overflow(p)
		n = l.nkeys(p)
		fmt.Fprintf(&buf, "%4x: %c over %2d ver %3d  nkeys %4d  ", off, tp, over, ver, n)
		fmt.Fprintf(&buf, "datasize %3x free space %3x\n", n*16, len(p)-n*16-16)
	}
	l.Unlock(p)

	st := Stack{0}

	for i := 0; i < n; i++ {
		st[0] = MakeOffIndex(off, i)
		k, _ := l.Key(st, nil)

		if isleaf {
			v := l.Value(st, nil)
			fmt.Fprintf(&buf, "    %2x -> %2x  | %q -> %q\n", k, v, k, v)
		} else {
			v := l.link(st)
			fmt.Fprintf(&buf, "    %2x -> %4x  | %q\n", k, v, k)
		}
	}

	return buf.String()
}

func (l *FixedLayout) dumpFile() string {
	var buf strings.Builder

	b := l.Back
	off := int64(0)

	p := b.Access(off, 0x10)
	if bytes.HasPrefix(p, []byte("xrain")) {
		off = 4 * l.Page
	}
	b.Unlock(p)

	for off < b.Size() {
		s := l.dumpPage(off)

		buf.WriteString(s)

		off += l.p
	}

	return buf.String()
}
