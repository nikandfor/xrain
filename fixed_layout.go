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
		return fixedIndexStart + i*l.fkv + l.ff
	} else {
		return fixedIndexStart + i*(l.k+8)
	}
}

func (l *FixedLayout) datavaloff(leaf bool, i int) int {
	if leaf {
		return fixedIndexStart + i*l.fkv + l.ff + l.k
	} else {
		return fixedIndexStart + i*(l.k+8) + l.k
	}
}

func (l *FixedLayout) pagerowsize(isleaf bool) int {
	if isleaf {
		return l.fkv
	} else {
		return l.k + 8
	}
}

func (l *FixedLayout) valsize(isleaf bool) int {
	if isleaf {
		return l.v
	} else {
		return 8
	}
}

func (l *FixedLayout) pagelink(p []byte, i int) (off int64) {
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

func (l *FixedLayout) Alloc() (int64, error) {
	off, err := l.Freelist.Alloc(l.pm)
	if err != nil {
		return NilPage, err
	}

	tl.V("lalloc").Printf("layout alloc %3x %d", off, l.pm)

	p := l.Access(off, 0x10)
	l.setleaf(p, true)
	l.setnkeys(p, 0)
	l.setoverflow(p, l.pm-1)
	l.setver(p, l.Ver)
	l.Unlock(p)

	return off, nil
}

func (l *FixedLayout) Free(off int64) error {
	p := l.Access(off, 0x10)

	pages := 1 + l.overflow(p)
	ver := l.pagever(p)

	var sub []int64
	if !l.isleaf(p) {
		n := l.nkeys(p)
		sub = make([]int64, n)

		for i := range sub {
			sub[i] = l.pagelink(p, i)
		}
	}

	l.Unlock(p)

	tl.V("lalloc").Printf("layout free  %x %d", off, pages)

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
	defer l.Unlock(p)

	isleaf := l.isleaf(p)
	dst := l.dataoff(isleaf, i)

	for j := 0; j < l.ff; j++ {
		ff |= int(p[dst+j]) << uint(j*8)
	}

	return
}

func (l *FixedLayout) Key(st Stack, buf []byte) (k []byte, ff int) {
	off, i := st.LastOffIndex(l.Mask)

	p := l.Access(off, l.p)
	defer l.Unlock(p)

	isleaf := l.isleaf(p)
	dst := l.dataoff(isleaf, i)

	if isleaf {
		for j := l.ff - 1; j >= 0; j-- {
			ff = ff<<8 | int(p[dst+j])
		}

		dst += l.ff
	}

	k = append(buf, p[dst:dst+l.k]...)

	return
}

func (l *FixedLayout) lastKey(off int64, buf []byte) []byte {
	p := l.Access(off, l.p)
	defer l.Unlock(p)

	isleaf := l.isleaf(p)
	n := l.nkeys(p)

	st := l.datakeyoff(isleaf, n-1)

	return append(buf, p[st:st+l.k]...)
}

func (l *FixedLayout) Value(st Stack, buf []byte) (v []byte) {
	off, i := st.LastOffIndex(l.Mask)
	return l.value(off, i, buf)
}

func (l *FixedLayout) value(off int64, i int, buf []byte) (v []byte) {
	p := l.Access(off, l.p)

	isleaf := l.isleaf(p)
	st := l.datavaloff(isleaf, i)

	len := l.valsize(isleaf)

	v = append(buf, p[st:st+len]...)

	//	tl.Printf("value %x %d -> % 2x\n%v", off, i, v, hex.Dump(p))

	l.Unlock(p)

	return
}

func (l *FixedLayout) link(st Stack) (off int64) {
	off, i := st.LastOffIndex(l.Mask)

	p := l.Access(off, l.p)
	off = l.pagelink(p, i)
	l.Unlock(p)

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
	var i, n int
	var coff int64

	for !isleaf {
		i, n, coff, eq, isleaf = l.search(off, k)

		if tl.V("seek") != nil {
			tl.If(isleaf).Printf("seek root %3x  off %3x -> i %2d / %2d  eq %5v  - leaf", root, off, i, n, eq)
			tl.If(!isleaf).Printf("seek root %3x  off %3x -> %3x  i %2d / %2d  - branch", root, off, coff, i, n)
		}

		if !isleaf && i == n {
			i--
		}

		st = append(st, MakeOffIndex(off, i))

		off = coff
	}

	if tl.V("seek,seek_res") != nil {
		tl.Printf("seek root %3x -> %v  eq %5v by %q [% 2x]", root, st, eq, k, k)
	}

	return st, eq
}

func (l *FixedLayout) search(off int64, k []byte) (i, n int, coff int64, eq, isleaf bool) {
	p := l.Access(off, l.p)
	defer l.Unlock(p)

	keycmp := func(i int) int {
		isleaf := l.isleaf(p)

		st := l.datakeyoff(isleaf, i)
		end := l.datavaloff(isleaf, i)

		return bytes.Compare(p[st:end], k)
	}

	n = l.nkeys(p)
	isleaf = l.isleaf(p)

	i = sort.Search(n, func(i int) bool {
		return keycmp(i) >= 0
	})

	if isleaf {
		eq = i < n && keycmp(i) == 0
	} else {
		if i == n {
			i--
		}

		coff = l.pagelink(p, i)
	}

	return
}

func (l *FixedLayout) firstLast(st Stack, off int64, back bool) Stack {
	if off == NilPage {
		return nil
	}

	st = st[:0]

	var stop bool
	var i int

	for !stop {
		p := l.Access(off, l.p)
		func() {
			defer l.Unlock(p)

			n := l.nkeys(p)

			if stop = n == 0; stop {
				st = nil
				return
			}

			if back {
				i = n - 1
			} else {
				i = 0
			}

			st = append(st, MakeOffIndex(off, i))

			if stop = l.isleaf(p); stop {
				return
			}

			off = l.pagelink(p, i)
		}()
	}

	return st
}

func (l *FixedLayout) Step(st Stack, root int64, back bool) Stack {
	if tl.V("step") != nil {
		defer func() {
			tl.Printf("step root %3x -> %v  (back %v)", root, st, back)
		}()
	}

	if len(st) == 0 {
		return l.firstLast(st, root, back)
	}

	q := len(st) - 1
	off, i := st[q].OffIndex(l.Mask)

	var n int

	if back && i > 0 {
		i--
		goto fin
	}

	n = l.nKeys(off)

	if i+1 < n {
		i++
		goto fin
	}

	if l.Step(st[:q], NilPage, back) == nil {
		return nil
	}

	off = l.link(st[:q])

	if back {
		i = l.nKeys(off) - 1
	} else {
		i = 0
	}

fin:
	st[q] = MakeOffIndex(off, i)

	return st
}

func (l *FixedLayout) Insert(st Stack, ff int, k, v []byte) (_ Stack, err error) {
	off, i := st.LastOffIndex(l.Mask)

	off0, off1, di, err := l.insert(off, i, ff, k, v)
	if err != nil {
		return
	}

	if di == 0 {
		st[len(st)-1] = MakeOffIndex(off0, i)
	} else {
		st[len(st)-1] = MakeOffIndex(off1, i-di)
		di = 1
	}

	return l.out(st, off0, off1, di, false)
}

func (l *FixedLayout) insert(off int64, i int, ff int, k, v []byte) (off0, off1 int64, di int, err error) {
	var alloc, split bool
	var ver int64

again:
	p := l.Access(off, l.p)
	func() {
		defer l.Unlock(p)

		ver = l.pagever(p)
		alloc = ver != l.Ver

		isleaf := l.isleaf(p)
		n := l.nkeys(p)
		split = l.dataoff(isleaf, n+1) > int(l.p)

		if split && tl.V("insert,split") != nil {
			tl.Printf("split %3x  %d / %d  by %q %q  free %x / %x", off, i, n, k, v, int(l.p)-l.dataoff(isleaf, n), l.p)
		}

		if alloc || split {
			return
		}

		l.pageInsert(p, i, n, ff, k, v)
	}()

	if alloc && !split {
		off, err = l.realloc(off, ver, l.pm, l.pm)
		if err != nil {
			return
		}

		goto again
	}

	if !split {
		return off, NilPage, 0, nil
	}

	return l.insertSplit(off, ver, i, ff, k, v)
}

func (l *FixedLayout) insertSplit(off, ver int64, i, ff int, k, v []byte) (off0, off1 int64, di int, err error) {
	off, err = l.realloc(off, ver, l.pm, 2*l.pm)
	if err != nil {
		return
	}

	p := l.Access(off, 2*l.p)
	func() {
		defer l.Unlock(p)

		n := l.nkeys(p[:l.p])

		m := l.pageSplit(p[:l.p], p[l.p:], n)

		if i <= m {
			l.pageInsert(p[:l.p], i, m, ff, k, v)
		} else {
			l.pageInsert(p[l.p:], i-m, n-m, ff, k, v)
			di = m
		}
	}()

	return off, off + l.p, di, nil
}

func (l *FixedLayout) pageInsert(p []byte, i, n, ff int, k, v []byte) {
	isleaf := l.isleaf(p)

	dst := l.dataoff(isleaf, i)

	if i < n {
		size := l.pagerowsize(isleaf)
		copy(p[dst+size:], p[dst:l.p])
	}

	if isleaf {
		for j := 0; j < l.ff; j++ {
			p[dst] = byte(ff >> uint(j*8))
		}
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

	l.setleaf(r, l.isleaf(p))

	l.setnkeys(p, m)
	l.setnkeys(r, n-m)

	l.setoverflow(p, l.pm-1)
	l.setoverflow(r, l.pm-1)

	l.setver(p, l.Ver)
	l.setver(r, l.Ver)

	return m
}

func (l *FixedLayout) out(s Stack, off0, off1 int64, di int, rebalance bool) (_ Stack, err error) {
	var bufdata [30]byte
	buf := bufdata[:]

	for d := len(s) - 2; d >= 0; d-- {
		off, i := s[d].OffIndex(l.Mask)

		if tl.V("out") != nil {
			tl.Printf("out d %d  %3x %d  -> %3x %3x  di %d  reb %v   st %v", d, off, i, off0, off1, di, rebalance, s)
		}

		if off1 == NilPage && di != 0 || off1 != NilPage && rebalance {
			tl.Printf("bad situation: par %x %d off %x %x di %d rebalance %v st %v", off, i, off0, off1, di, rebalance, s)
			panic("bad")
		}

		if rebalance {
			di, err = l.rebalance(s[:d+2], off0)
			if err != nil {
				return
			}

			if di != -1 {
				if tl.V("out") != nil {
					tl.Printf("out merged %3x %d + %d  (%3x)", off, i, di, off0)
				}

				off, rebalance, err = l.delete(off, di)
				if err != nil {
					return
				}

				if di < i {
					i--
				}
			}
		}

		off, err = l.updatePageLink(off, i, off0)
		if err != nil {
			return
		}

		if off1 == NilPage {
			s[d] = MakeOffIndex(off, i)

			off0 = off
			di = 0

			continue
		}

		k := l.lastKey(off1, buf[:0])

		buf = append(k, 0, 0, 0, 0, 0, 0, 0, 0)

		v := buf[len(k):]
		binary.BigEndian.PutUint64(v, uint64(off1))

		var split int
		off0, off1, split, err = l.insert(off, i+1, 0, k, v)
		if err != nil {
			return
		}

		if tl.V("out").If(off1 != NilPage) != nil {
			tl.Printf("out split %x -> %x %x  split %d", off, off0, off1, split)
		}

		if split == 0 {
			s[d] = MakeOffIndex(off0, i+di)
			di = 0
		} else {
			s[d] = MakeOffIndex(off1, i+di-split)
			di = 1
		}
	}

	if rebalance && len(s) > 1 {
		off, i := s[0].OffIndex(l.Mask)

		if i == 0 {
			p := l.Access(off, l.p)
			ver := l.pagever(p)
			n := l.nkeys(p)
			l.Unlock(p)

			if n == 1 {
				copy(s, s[1:])
				s = s[:len(s)-1]

				l.Freelist.Free(off, ver, l.pm)

				if tl.V("out") != nil {
					tl.Printf("out pop  root %x -> %v", off, s)
				}
			}
		}
	}

	if off1 != NilPage {
		root, err := l.Freelist.Alloc(l.pm)
		if err != nil {
			return nil, err
		}

		if tl.V("out") != nil {
			tl.Printf("out push root %x <- %x, %x", root, off0, off1)
		}

		l.rootAppendLink(root, 0, off0)
		l.rootAppendLink(root, 1, off1)

		s = append(s, 0)
		copy(s[1:], s)
		s[0] = MakeOffIndex(root, di)
	}

	return s, nil
}

func (l *FixedLayout) updatePageLink(off int64, i int, coff int64) (_ int64, err error) {
	var alloc bool
	var ver int64

again:
	p, cp := l.Access2(off, l.p, coff, l.p)
	func() {
		defer l.Unlock2(p, cp)

		ver = l.pagever(p)
		alloc = ver != l.Ver

		isleaf := l.isleaf(p)
		cisleaf := l.isleaf(cp)
		cn := l.nkeys(cp)

		st := l.datakeyoff(isleaf, i)
		end := l.datavaloff(isleaf, i)
		cst := l.datakeyoff(cisleaf, cn-1)
		cend := l.datavaloff(cisleaf, cn-1)

		oldlink := l.pagelink(p, i)

		if bytes.Equal(p[st:end], cp[cst:cend]) && oldlink == coff {
			alloc = false
			return
		}

		if alloc {
			return
		}

		copy(p[st:end], cp[cst:])

		binary.BigEndian.PutUint64(p[end:], uint64(coff))
	}()

	if alloc {
		off, err = l.realloc(off, ver, l.pm, l.pm)
		if err != nil {
			return
		}

		goto again
	}

	return off, nil
}

func (l *FixedLayout) rootAppendLink(root int64, i int, off int64) {
	p, cp := l.Access2(root, l.p, off, l.p)
	func() {
		l.setleaf(p, false)
		l.setver(p, l.Ver)
		l.setoverflow(p, l.pm-1)

		st := l.dataoff(false, i)
		cleaf := l.isleaf(cp)
		cst := l.datakeyoff(cleaf, l.nkeys(cp)-1)

		copy(p[st:], cp[cst:cst+l.k])

		binary.BigEndian.PutUint64(p[st+l.k:], uint64(off))

		l.setnkeys(p, i+1)

		if tl.V("grow") != nil {
			tl.Printf("out %x to %x of\n%v", off, st+l.k, hex.Dump(p))
		}
	}()
	l.Unlock2(p, cp)
}

func (l *FixedLayout) Delete(st Stack) (_ Stack, err error) {
	off, i := st.LastOffIndex(l.Mask)

	var rebalance bool
	off, rebalance, err = l.delete(off, i)
	if err != nil {
		return
	}

	st[len(st)-1] = MakeOffIndex(off, i)

	return l.out(st, off, NilPage, 0, rebalance)
}

func (l *FixedLayout) delete(off int64, i int) (_ int64, _ bool, err error) {
	var rebalance, alloc bool
	var ver int64

again:
	p := l.Access(off, l.p)
	func() {
		defer l.Unlock(p)

		ver = l.pagever(p)
		alloc = ver != l.Ver

		n := l.nkeys(p)
		isleaf := l.isleaf(p)

		end := l.dataoff(isleaf, n-1)

		rebalance = end < int(l.p)*2/5

		if rebalance && tl.V("delete,rebalance") != nil {
			tl.Printf("rebalance %3x  %d / %d   used %3x / %3x", off, i, n, l.dataoff(isleaf, n), l.p)
		}

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

	return off, rebalance, nil
}

func (l *FixedLayout) pageDel(p []byte, isleaf bool, i, n int) {
	st := l.dataoff(isleaf, i)
	end := l.dataoff(isleaf, i+1)

	copy(p[st:], p[end:])

	l.setnkeys(p, n-1)
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

		merge = l.dataoff(isleaf, n0+n1) <= int(l.p)

		if !merge {
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

			end1 := l.dataoff(isleaf, n1)

			copy(p1[end:], p1[st:end1])
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
			off0 = l.pagelink(p, i0)
			off1 = off
		} else {
			i0 = pi
			i1 = pi + 1
			off0 = off
			off1 = l.pagelink(p, i1)
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
		fmt.Fprintf(&buf, "%4x: %c over %2x ver %3x  nkeys %3x  ", off, tp, over, ver, n)
		fmt.Fprintf(&buf, "datasize %3x free space %3x\n", n*16, len(p)-n*16-16)
	}
	l.Unlock(p)

	st := Stack{0}

	for i := 0; i < n; i++ {
		st[0] = MakeOffIndex(off, i)
		k, _ := l.Key(st, nil)

		if isleaf {
			v := l.Value(st, nil)
			fmt.Fprintf(&buf, "    %2x -> %2x    | %q -> %q\n", k, v, k, v)
		} else {
			v := l.link(st)
			fmt.Fprintf(&buf, "    %2x -> %4x    | %q\n", k, v, k)
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
