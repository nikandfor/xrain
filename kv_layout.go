package xrain

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
)

type (
	Layout interface {
		SetCommon(*Common)

		Alloc() (int64, error)
		Free(root int64) error

		Seek(s Stack, root int64, k []byte) (Stack, bool) // TODO: add value
		Step(s Stack, root int64, back bool) Stack

		Key(s Stack, buf []byte) ([]byte, int)
		Value(s Stack, buf []byte) []byte

		Insert(st Stack, ff int, k, v []byte) (Stack, error)
		Delete(st Stack) (Stack, error)
	}

	FlagsSupported interface {
		Flags(Stack) int
	}

	BaseLayout2 struct {
		*Common
		linkf      func(off int64, i int) int64
		searchf    func(off int64, k []byte) (i, n int, coff int64, eq, isleaf bool)
		firstLastf func(st Stack, off int64, back bool) Stack
	}

	KVLayout2 struct {
		BaseLayout2
	}

	Stack []OffIndex

	OffIndex int64

	fileDumper interface {
		dumpFile() string
	}

	pageDumper interface {
		dumpPage(off int64) string
	}
)

const NilPage = -1

const kvIndexStart = 0x10
const kvMaxBranckKeylen = 100

var _ Layout = &KVLayout2{}

func (l *BaseLayout2) isleaf(p []byte) bool {
	return p[4]&0x80 == 0
}

func (l *BaseLayout2) setleaf(p []byte, y bool) {
	if y {
		p[4] &^= 0x80
	} else {
		p[4] |= 0x80
	}
}

func (l *BaseLayout2) nkeys(p []byte) int {
	return int(p[4])&^0x80<<8 | int(p[5])
}

func (l *BaseLayout2) nKeys(off int64) (n int) {
	p := l.Access(off, 0x10)
	n = l.nkeys(p)
	l.Unlock(p)

	return n
}

func (l *BaseLayout2) setnkeys(p []byte, n int) {
	p[4] = p[4]&0x80 | byte(n>>8&^0x80)
	p[5] = byte(n)
}

func (l *BaseLayout2) overflow(p []byte) int {
	return (int(p[6])<<8 | int(p[7]))
}

func (l *BaseLayout2) setoverflow(p []byte, n int) {
	p[6] = byte(n >> 8)
	p[7] = byte(n)
}

func (l *BaseLayout2) pagever(p []byte) int64 {
	return int64(p[8])<<56 | int64(p[9])<<48 | int64(p[10])<<40 | int64(p[11])<<32 | int64(p[12])<<24 | int64(p[13])<<16 | int64(p[14])<<8 | int64(p[15])
}

func (l *BaseLayout2) setver(p []byte, v int64) {
	p[8] = byte(v >> 56)
	p[9] = byte(v >> 48)
	p[10] = byte(v >> 40)
	p[11] = byte(v >> 32)
	p[12] = byte(v >> 24)
	p[13] = byte(v >> 16)
	p[14] = byte(v >> 8)
	p[15] = byte(v)
}

func (l *BaseLayout2) crccheck(p []byte) bool {
	sum := crc32.ChecksumIEEE(p[4:])
	exp := binary.BigEndian.Uint32(p)
	return sum == exp
}

func (l *BaseLayout2) crccalc(p []byte) {
	sum := crc32.ChecksumIEEE(p[4:])
	binary.BigEndian.PutUint32(p, sum)
}

func (l *BaseLayout2) realloc(off int64, oldn, n int) (noff int64, err error) {
	noff, err = l.Freelist.Alloc(n)
	if err != nil {
		return
	}

	var ver int64
	func() {
		d, s := l.Access2(noff, int64(n)*l.Page, off, int64(oldn)*l.Page)
		defer l.Unlock2(d, s)

		copy(d, s)

		ver = l.pagever(s)
		l.setver(d, l.Ver)
	}()

	err = l.Freelist.Free(off, ver, oldn)
	if err != nil {
		return
	}

	if tl.V("realloc") != nil {
		tl.Printf("realloc %3x <- %3x  size %x <- %x", noff, off, n, oldn)
	}

	return
}

func (l *BaseLayout2) SetCommon(c *Common) { l.Common = c }

func (l *BaseLayout2) Alloc() (int64, error) {
	off, err := l.Freelist.Alloc(1)
	if err != nil {
		return NilPage, err
	}

	if tl.V("lalloc") != nil {
		tl.Printf("layout alloc %3x %d", off, 1)
	}

	p := l.Access(off, 0x10)
	l.setleaf(p, true)
	l.setnkeys(p, 0)
	l.setoverflow(p, 0)
	l.setver(p, l.Ver)
	l.Unlock(p)

	return off, nil
}

func (l *BaseLayout2) Seek(st Stack, root int64, k []byte) (_ Stack, eq bool) {
	st = st[:0]

	if root == NilPage {
		return nil, false
	}

	off := root
	var isleaf bool
	var i, n int
	var coff int64

	for !isleaf {
		i, n, coff, eq, isleaf = l.searchf(off, k)

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

func (l *BaseLayout2) Step(st Stack, root int64, back bool) Stack {
	if tl.V("step") != nil {
		defer func() {
			tl.Printf("step root %3x -> %v  (back %v)", root, st, back)
		}()
	}

	if len(st) == 0 {
		return l.firstLastf(st, root, back)
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

	off = l.linkf(st[q-1].OffIndex(l.Mask))

	if back {
		i = l.nKeys(off) - 1
	} else {
		i = 0
	}

fin:
	st[q] = MakeOffIndex(off, i)

	return st
}

func NewKVLayout2(c *Common) *KVLayout2 {
	var l KVLayout2

	l.Common = c
	l.linkf = l.link
	l.searchf = l.search
	l.firstLastf = l.firstLast

	return &l
}

func (l *KVLayout2) setdataoff(p []byte, i, off int) {
	st := kvIndexStart + 2*i
	p[st] = byte(off >> 8)
	p[st+1] = byte(off)
}

func (l *KVLayout2) dataoff(p []byte, i int) int {
	if i < 0 {
		panic(i)
	}
	st := kvIndexStart + 2*i
	return int(p[st])<<8 | int(p[st+1])
}

func (l *KVLayout2) dataend(p []byte, i int) int {
	if i < 0 {
		panic(i)
	}
	if i == 0 {
		return (1 + l.overflow(p)) * int(l.Page)
	}
	i--
	st := kvIndexStart + 2*i
	return int(p[st])<<8 | int(p[st+1])
}

func (l *KVLayout2) pagefree(p []byte, n int) int {
	if n == 0 {
		return int(l.Page)*(1+l.overflow(p)) - kvIndexStart
	}
	fst := kvIndexStart + 2*int(n)
	dst := int(p[fst-2])<<8 | int(p[fst-1])
	return dst - fst
}

func (l *KVLayout2) pagedatasize(p []byte, i, n int) int {
	if i == n {
		return 0
	}
	nst := kvIndexStart + 2*n
	dst := int(p[nst-2])<<8 | int(p[nst-1])
	var dend int
	if i == 0 {
		dend = (1 + l.overflow(p)) * int(l.Page)
	} else {
		ist := kvIndexStart + 2*i
		dend = int(p[ist-2])<<8 | int(p[ist-1])
	}
	return (dend - dst) + 2*(n-i)
}

func (l *KVLayout2) expectedsize(p []byte, isleaf bool, i, n, ff int, k, v []byte) int {
	if isleaf {
		// link + flags + keylen + key + value
		return 2 + 1 + varlen(len(k)) + len(k) + len(v)
	}

	kl := len(k)
	if kl > kvMaxBranckKeylen {
		kl = kvMaxBranckKeylen
	}
	// link + keylen + key + value
	return 2 + 1 + kl + len(v)
}

func (l *KVLayout2) pagelink(p []byte, i int) (off int64) {
	st := l.dataend(p, i) - 8
	return int64(binary.BigEndian.Uint64(p[st:]))
}

func (l *KVLayout2) link(off int64, i int) int64 {
	var buf [8]byte

	l.value(off, i, buf[:0])

	return int64(binary.BigEndian.Uint64(buf[:]))
}

func (l *KVLayout2) Free(off int64) error {
	p := l.Access(off, l.Page)
	pages := 1 + l.overflow(p)
	ver := l.pagever(p)
	rec := !l.isleaf(p)
	var sub []int64
	if rec {
		n := l.nkeys(p)
		sub = make([]int64, n)
		for i := range sub {
			v := l.value(off, i, nil)
			sub[i] = int64(binary.BigEndian.Uint64(v))
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

func (l *KVLayout2) Flags(st Stack) (ff int) {
	off, i := st.LastOffIndex(l.Mask)

	pg := l.Page

again:
	remount := false
	func() {
		p := l.Access(off, pg)
		defer l.Unlock(p)

		if !l.isleaf(p) {
			return
		}

		dst := l.dataoff(p, i)

		if dst < len(p) {
			ff = int(p[dst])
			return
		}

		remount = true
		pg = l.Page * int64(1+l.overflow(p))
	}()

	if remount {
		goto again
	}

	return
}

func (l *KVLayout2) Key(st Stack, buf []byte) (key []byte, ff int) {
	off, i := st.LastOffIndex(l.Mask)

	pg := l.Page

again:
	remount := false

	func() {
		p := l.Access(off, pg)
		defer l.Unlock(p)

		pg = l.Page * int64(1+l.overflow(p))

		dst := l.dataoff(p, i)

		if dst+2 > len(p) {
			remount = true
			return
		}

		if l.isleaf(p) {
			ff = int(p[dst])
			dst++
		}

		kl := int(p[dst])
		dst++

		if dst+kl >= len(p) {
			remount = true
			return
		}

		key = append(buf, p[dst:dst+kl]...)
	}()

	if remount {
		goto again
	}

	return
}

func (l *KVLayout2) Value(st Stack, buf []byte) []byte {
	off, i := st.LastOffIndex(l.Mask)
	return l.value(off, i, nil)
}

func (l *KVLayout2) value(off int64, i int, buf []byte) (val []byte) {
	pg := l.Page

again:
	remount := false

	func() {
		p := l.Access(off, pg)
		defer l.Unlock(p)

		pg = l.Page * int64(1+l.overflow(p))

		dst := l.dataoff(p, i)
		dend := l.dataend(p, i)

		if dend > len(p) {
			remount = true
			return
		}

		if l.isleaf(p) {
			dst++ // flags
		}

		kl := int(p[dst])
		dst += 1 + kl

		val = append(buf, p[dst:dend]...)
	}()

	if remount {
		goto again
	}

	return
}

func (l *KVLayout2) search(off int64, k []byte) (i, n int, coff int64, eq, isleaf bool) {
	keycmp := func(p []byte, i int, isleaf bool) int {
		st := l.dataoff(p, i)
		if isleaf {
			st++
		}

		kl := int(p[st])
		st++

		ik := p[st : st+kl]

		return bytes.Compare(ik, k)
	}

	pg := l.Page

again:
	p := l.Access(off, pg)

	if ps := int64(1+l.overflow(p)) * l.Page; ps != pg {
		pg = ps
		l.Unlock(p)
		goto again
	} else {
		defer l.Unlock(p)
	}

	n = l.nkeys(p)
	isleaf = l.isleaf(p)

	if n == 0 {
		return 0, 0, 0, false, isleaf
	}

	i = sort.Search(n, func(i int) bool {
		return keycmp(p, i, isleaf) >= 0
	})

	if isleaf {
		eq = i < n && keycmp(p, i, isleaf) == 0
	} else {
		if i == n {
			i--
		}

		coff = l.pagelink(p, i)
	}

	return
}

func (l *KVLayout2) firstLast(st Stack, off int64, back bool) Stack {
	if off == NilPage {
		return nil
	}

	st = st[:0]

	var stop bool
	var i int

	for !stop {
		func() {
			p := l.Access(off, l.Page)
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

func (l *KVLayout2) Insert(st Stack, ff int, k, v []byte) (_ Stack, err error) {
	off, i := st.LastOffIndex(l.Mask)

	off0, off1, off2, di, err := l.insert(off, i, ff, k, v)
	if err != nil {
		return
	}

	if di == 0 {
		st[len(st)-1] = MakeOffIndex(off0, i)
	} else {
		st[len(st)-1] = MakeOffIndex(off1, i-di)
		di = 1
	}

	return l.out(st, off0, off1, off2, di, false)
}

func (l *KVLayout2) insert(off int64, i, ff int, k, v []byte) (off0, off1, off2 int64, di int, err error) {
	var alloc, split, triple bool
	ps := 1

again:
	func() {
		p := l.Access(off, int64(ps)*l.Page)
		defer l.Unlock(p)

		ps = 1 + l.overflow(p)

		isleaf := l.isleaf(p)
		alloc = l.pagever(p) != l.Ver

		n := l.nkeys(p)

		free := l.pagefree(p, n)
		exp := l.expectedsize(p, isleaf, i, n, ff, k, v)

		split = exp > free

		//                      link + flags + keylen + key + value
		triple = int64(kvIndexStart+2+1+varlen(len(k))+len(k)+len(v)) > 4*l.Page

		if alloc || split || triple {
			return
		}

		l.pageInsert(p, i, n, ff, k, v)
	}()

	switch {
	case triple:
		return l.insertTriple(off, i, ff, k, v, ps)
	case split:
		return l.insertSplit(off, i, ff, k, v, ps)
	case alloc:
		off, err = l.realloc(off, ps, ps)
		if err != nil {
			return
		}

		goto again
	}

	return off, NilPage, NilPage, 0, nil
}

func (l *KVLayout2) insertSplit(off int64, i, ff int, k, v []byte, oldpages int) (off0, off1, off2 int64, di int, err error) {
	var ver int64
	var lpage, rpage int

	off0 = NilPage
	ps := 0

again:
	func() {
		p, p0 := l.Access2(off, int64(oldpages)*l.Page, off0, int64(ps)*l.Page)
		defer l.Unlock2(p, p0)

		ver = l.pagever(p)
		isleaf := l.isleaf(p)
		n := l.nkeys(p)

		exp := l.expectedsize(p, isleaf, i, n, ff, k, v)
		tot := l.pagedatasize(p, 0, i) + exp + l.pagedatasize(p, i, n)

		var m, left int
		for m = 0; m < i; m++ {
			left = l.pagedatasize(p, 0, m)

			if left > tot/2 {
				break
			}
		}
		for ; m < n; m++ {
			left = l.pagedatasize(p, 0, m)

			if left+exp > tot/2 {
				break
			}
		}

		if tl.V("insert") != nil {
			tl.Printf("insert %d / %d split at %d  exp %3x  left %3x / %3x", i, n, m, exp, left, tot)
		}

		switch {
		case i < m:
		case i > m:
			di = m
		case i == m:
			if left < tot-left {
				left += exp
			} else {
				di = m
			}
		}

		lpage = l.pages(left)
		rpage = l.pages(tot - left)

		ps = (lpage + rpage) / int(l.Page)

		if off0 == NilPage {
			return
		}

		l.pageSplit(p, p0[:lpage], p0[lpage:], m, n)

		if di == 0 {
			l.pageInsert(p0[:lpage], i, m, ff, k, v)
		} else {
			l.pageInsert(p0[lpage:], i-m, n-m, ff, k, v)
		}
	}()

	if off0 == NilPage {
		off0, err = l.Freelist.Alloc(ps)
		if err != nil {
			return
		}

		goto again
	} else {
		err = l.Freelist.Free(off, ver, oldpages)
		if err != nil {
			return
		}
	}

	return off0, off0 + int64(lpage), NilPage, di, nil
}

func (l *KVLayout2) insertTriple(off int64, i, ff int, k, v []byte, oldpages int) (off0, off1, off2 int64, di int, err error) {
	var ver int64
	var page0, page1, page2 int

	off0, off1, off2 = NilPage, NilPage, NilPage
	ps := 0

again:
	func() {
		p, p0 := l.Access2(off, int64(oldpages)*l.Page, off0, int64(ps)*l.Page)
		defer l.Unlock2(p, p0)

		isleaf := l.isleaf(p)
		n := l.nkeys(p)

		exp := l.expectedsize(p, isleaf, 0, 0, ff, k, v)

		switch {
		case i == 0:
			page0 = exp
			page1 = oldpages * int(l.Page)
		case i == n:
			page0 = oldpages * int(l.Page)
			page1 = exp
		default:
			page0 = l.pagedatasize(p, 0, i)
			page1 = exp
			page2 = l.pagedatasize(p, i, n)
		}

		page0 = l.pages(page0)
		page1 = l.pages(page1)
		page2 = l.pages(page2)

		ps = (page0 + page1 + page2) / int(l.Page)

		if off0 == NilPage {
			return
		}

		l.pageSplit(p, p0[:page0], p0[page0+page1:], i, n)
		off1 = off0 + int64(page0)

		switch {
		case i == 0:
			l.pageInsert(p0[:page0], 0, 0, ff, k, v)
		case i == n:
			l.pageInsert(p0[page0:], 0, 0, ff, k, v)
		default:
			l.pagesetheaders(p0[page0:], isleaf, 0)
			l.pageInsert(p0[page0:], 0, 0, ff, k, v)

			off2 = off1 + int64(page1)
		}

		di = i
	}()

	if off0 == NilPage {
		off0, err = l.Freelist.Alloc(ps)
		if err != nil {
			return
		}

		goto again
	} else {
		err = l.Freelist.Free(off, ver, oldpages)
		if err != nil {
			return
		}
	}

	return
}

func (l *KVLayout2) pageInsert(p []byte, i, n, ff int, k, v []byte) {
	isleaf := l.isleaf(p)

	exp := 0
	if isleaf {
		exp = 1 + varlen(len(k)) + len(k) + len(v) // flags + keylen + key + value
	} else {
		kl := len(k)
		if kl > kvMaxBranckKeylen {
			kl = kvMaxBranckKeylen
		}

		exp = 1 + kl + len(v) // keylen + key prefix + value
	}

	dend := l.dataend(p, i)

	if i < n {
		dst := l.dataoff(p, n-1)

		if tl.V("insert") != nil {
			tl.Printf("pageInsert %d / %d move %x - %x on %x", i, n, dst, dend, exp)
		}

		copy(p[dst-exp:], p[dst:dend])

		for j := n - 1; j >= i; j-- {
			off := l.dataoff(p, j)
			l.setdataoff(p, j+1, off-exp)
		}
	}

	dst := dend - exp

	if tl.V("insert") != nil {
		tl.Printf("pageInsert %d / %d to   %x - %x (%x)", i, n, dst, dend, exp)
	}

	l.setdataoff(p, i, dst)

	if isleaf {
		p[dst] = byte(ff)
		dst++
	}
	p[dst] = byte(len(k))
	dst++
	copy(p[dst:], k)
	copy(p[dst+len(k):], v)

	l.setnkeys(p, n+1)
}

func (l *KVLayout2) pageInsertFrom(r, p []byte, ri, rn, i, n int) {
	st := l.dataoff(p, n-1)
	end := l.dataend(p, i)
	size := end - st

	rend := l.dataend(r, ri)
	if ri < rn {
		rst := l.dataoff(r, rn-1)

		copy(r[rst-size:], r[rst:rend])

		di := n - i
		for j := rn - 1; j >= ri; j-- {
			off := l.dataoff(r, j)
			l.setdataoff(r, j+di, off-size)
		}
	}

	rst := rend - size

	copy(r[rst:], p[st:end])

	di := ri - i
	diff := rend - end
	for j := i; j < n; j++ {
		off := l.dataoff(p, j)
		l.setdataoff(r, j+di, off+diff)
	}

	l.setnkeys(r, rn+(n-i))
}

func (l *KVLayout2) pageSplit(p, p0, p1 []byte, m, n int) {
	// p1 page

	dst := l.dataoff(p, n-1)
	dend := l.dataend(p, m)
	dsize := dend - dst

	if tl.V("split") != nil {
		//	tl.Printf("split  %d / %d move %x - %x <- %x - %x", m, n, len(r)-dsize, len(r), dst, dend)
	}

	if dsize != 0 {
		copy(p1[len(p1)-dsize:], p[dst:dend])

		diff := len(p1) - dend
		for j := m; j < n; j++ {
			off := l.dataoff(p, j)
			l.setdataoff(p1, j-m, off+diff)
		}
	}

	// p0 page

	dst = l.dataend(p, m)
	dend = l.dataend(p, 0)
	dsize = dend - dst

	if tl.V("split") != nil {
		//	tl.Printf("split inside page  move %x - %x <- %x - %x", len(p)-dsize, len(p), dst, dend)
	}

	if dsize != 0 {
		copy(p0[len(p0)-dsize:], p[dst:dend])

		diff := len(p0) - dend
		for j := 0; j < m; j++ {
			off := l.dataoff(p, j)
			l.setdataoff(p0, j, off+diff)
		}
	}

	// headers

	isleaf := l.isleaf(p)

	l.pagesetheaders(p0, isleaf, m)
	l.pagesetheaders(p1, isleaf, n-m)

	if tl.V("split").V("dump") != nil {
		tl.Printf("split %d / %d  dump\n%v\n%v\n%v", m, n, hex.Dump(p), hex.Dump(p0), hex.Dump(p1))
	}
}

func (l *KVLayout2) pagesetheaders(p []byte, isleaf bool, n int) {
	l.setleaf(p, isleaf)
	l.setnkeys(p, n)
	l.setoverflow(p, len(p)/int(l.Page)-1)
	l.setver(p, l.Ver)
}

func (l *KVLayout2) Delete(st Stack) (_ Stack, err error) {
	off, i := st.LastOffIndex(l.Mask)

	var rebalance bool
	off, rebalance, err = l.delete(off, i)
	if err != nil {
		return
	}

	st[len(st)-1] = MakeOffIndex(off, i)

	return l.out(st, off, NilPage, NilPage, 0, rebalance)
}

func (l *KVLayout2) delete(off int64, i int) (_ int64, _ bool, err error) {
	var alloc, rebalance bool
	ps, oldps := 1, 1

again:
	remount := false

	func() {
		p := l.Access(off, int64(ps)*l.Page)
		defer l.Unlock(p)

		ps = 1 + l.overflow(p)

		if alloc = l.pagever(p) != l.Ver; alloc {
			return
		} else if remount = ps != oldps; remount {
			return
		}

		n := l.nkeys(p)

		l.pageDelete(p, i, n)

		rebalance = l.pagedatasize(p, 0, n-1) < ps*int(l.Page)*3/5
	}()

	if alloc {
		off, err = l.realloc(off, ps, ps)
		if err != nil {
			return
		}

		goto again
	}

	if remount {
		oldps = ps
		goto again
	}

	return off, rebalance, nil
}

func (l *KVLayout2) pageDelete(p []byte, i, n int) {
	if i+1 < n {
		st := l.dataoff(p, n-1)
		end := l.dataoff(p, i)

		dend := l.dataend(p, i)
		diff := dend - end

		if tl.V("delete") != nil {
			tl.Printf("delete %d / %d  move %x-%x <- %x-%x%v", i, n, st+diff, st+diff+end-st, st, end, tl.VArg("dump", "\n"+hex.Dump(p)))
		}

		copy(p[st+diff:], p[st:end])

		for j := i; j+n < n; j++ {
			off := l.dataoff(p, j+1)
			l.setdataoff(p, j, off+diff)
		}
	}

	l.setnkeys(p, n-1)
}

func (l *KVLayout2) out(s Stack, off0, off1, off2 int64, di int, rebalance bool) (_ Stack, err error) {
	for d := len(s) - 2; d >= 0; d-- {
		off, i := s[d].OffIndex(l.Mask)

		if tl.V("out") != nil {
			tl.Printf("out d %d  %3x %d  -> %3x %3x %3x  di %d  reb %v   st %v", d, off, i, off0, off1, off2, di, rebalance, s)
		}

		if off1 == NilPage && di != 0 || off1 != NilPage && rebalance {
			tl.Printf("bad situation: par %x %d off %x %x di %d rebalance %v st %v", off, i, off0, off1, di, rebalance, s)
			panic("bad")
		}

		if rebalance {
			coff0, coff1, i0, i1 := l.sibling(s[:d+2], off0)

			di, err = l.rebalance(off0, coff0, coff1, i0, i1)
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

				tl.Printf("deleted rebalanced sibling: off %x\n%v", off, l.dumpPage(off))

				if di < i {
					i--
				}
			}

			di = 0
		}

		var split int
		poff := [4]int64{off}
		coff := [3]int64{off0, off1, off2}
		pi := 0

		// update/insert page link
		for ci := 0; ci < 3 && coff[ci] != NilPage; ci++ {
			poff[pi], poff[pi+1], split, err = l.updatePageLink(off, i+ci, coff[ci], ci == 0)
			if err != nil {
				return
			}

			if split != 0 {
				pi++
				i -= split
				split = 1
			} else {
				poff[pi+2] = poff[pi+1]
			}

			off = poff[pi]
		}

		if poff[pi+2] == poff[pi+1] {
			poff[pi+2] = NilPage
		}

		s[d] = MakeOffIndex(off, i+di)

		off0, off1, off2 = poff[0], poff[1], poff[2]
		di = split
	}

	if rebalance && len(s) > 1 {
		off, i := s[0].OffIndex(l.Mask)

		if i == 0 {
			p := l.Access(off, 0x10)
			ver := l.pagever(p)
			n := l.nkeys(p)
			l.Unlock(p)

			if n == 1 {
				copy(s, s[1:])
				s = s[:len(s)-1]

				err = l.Freelist.Free(off, ver, 1)
				if err != nil {
					return
				}

				if tl.V("out") != nil {
					tl.Printf("out pop  root %x -> %v", off, s)
				}
			}
		}
	}

	if off1 != NilPage {
		var root int64

		root, err = l.Freelist.Alloc(1)
		if err != nil {
			return nil, err
		}

		p := l.Access(root, l.Page)
		l.pagesetheaders(p, false, 0)
		l.Unlock(p)

		if tl.V("out") != nil {
			tl.Printf("out push root %x <- %x, %x, %x", root, off0, off1, off2)
		}

		root, _, _, err = l.updatePageLink(root, 0, off0, false)
		if err != nil {
			return
		}

		root, _, _, err = l.updatePageLink(root, 1, off1, false)
		if err != nil {
			return
		}

		if off2 != NilPage {
			root, _, _, err = l.updatePageLink(root, 2, off2, false)
			if err != nil {
				return
			}
		}

		//	tl.Printf("out dump %x\n%v", root, l.dumpPage(root))

		s = append(s, 0)
		copy(s[1:], s)
		s[0] = MakeOffIndex(root, di)
	}

	return s, nil
}

func (l *KVLayout2) updatePageLink(off int64, i int, coff int64, del bool) (off0, off1 int64, split int, err error) {
	cps := 1
	var k []byte

again:
	remount := false

	func() {
		p, cp := l.Access2(off, l.Page, coff, int64(cps)*l.Page)
		defer l.Unlock2(p, cp)

		cps = 1 + l.overflow(cp)

		cn := l.nkeys(cp)
		cst := l.dataoff(cp, cn-1)

		if cst+2 > len(cp) {
			remount = true
			return
		}

		if l.isleaf(cp) {
			cst++ // flags
		}

		ckl := int(cp[cst])
		cst++

		if cst+ckl > len(cp) {
			remount = true
			return
		}

		ck := cp[cst : cst+ckl]

		//	tl.Printf("uppl %x %d cp %x  ckey: %q", off, i, coff, ck)

		n := l.nkeys(p)
		if i < n {
			st := l.dataoff(p, i)
			kl := int(p[st])
			st++

			if l.pagelink(p, i) == coff && bytes.Equal(p[st:st+kl], ck) {
				return
			}
		}

		k = append([]byte{}, ck...)
	}()

	if remount {
		goto again
	}

	if k == nil {
		return off, NilPage, 0, nil
	}

	if del {
		off, _, err = l.delete(off, i)
		if err != nil {
			return
		}
	}

	v := append(k, 0, 0, 0, 0, 0, 0, 0, 0)
	v = v[len(k):]
	binary.BigEndian.PutUint64(v, uint64(coff))

	off0, off1, _, split, err = l.insert(off, i, 0, k, v)
	return
}

func (l *KVLayout2) rebalance(off, off0, off1 int64, i0, i1 int) (di int, err error) {
	var merge bool
	var ver int64

	if tl.V("rebalance") != nil {
		tl.Printf("rebalance %3x - %3x %3x  %2d %2d", off, off0, off1, i0, i1)
	}

	ps0, ps1 := 1, 1

	func() {
		p0, p1 := l.Access2(off0, int64(ps0)*l.Page, off1, int64(ps1)*l.Page)
		defer l.Unlock2(p0, p1)

		ps0 = 1 + l.overflow(p0)
		ps1 = 1 + l.overflow(p1)

		n0 := l.nkeys(p0)
		n1 := l.nkeys(p1)

		s0 := l.pagedatasize(p0, 0, n0)
		s1 := l.pagedatasize(p1, 0, n1)

		merge = kvIndexStart+s0+s1 <= int(l.Page)

		if !merge {
			return
		}

		if off == off0 {
			l.pageInsertFrom(p0, p1, n0, n0, 0, n1)

			ver = l.pagever(p1)
			di = i1
		} else {
			l.pageInsertFrom(p1, p0, 0, n1, 0, n0)

			ver = l.pagever(p0)
			di = i0
		}
	}()

	if !merge {
		return -1, nil
	}

	if off == off0 {
		err = l.Freelist.Free(off1, ver, ps1)
	} else {
		err = l.Freelist.Free(off0, ver, ps0)
	}
	if err != nil {
		return
	}

	return
}

func (l *KVLayout2) sibling(st Stack, off int64) (off0, off1 int64, i0, i1 int) {
	poff, pi := st[len(st)-2].OffIndex(l.Mask)

	if tl.V("sibling") != nil {
		tl.Printf("sibling for %x in %v  par %x %d", off, st, poff, pi)
	}

	p := l.Access(poff, l.Page)
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

//

func MakeOffIndex(off int64, i int) OffIndex {
	return OffIndex(off) | OffIndex(i)
}

func (l OffIndex) Off(mask int64) int64 {
	return int64(l) &^ mask
}

func (l OffIndex) Index(mask int64) int {
	return int(int64(l) & mask)
}

func (l OffIndex) OffIndex(m int64) (int64, int) {
	return l.Off(m), l.Index(m)
}

func (st Stack) LastOffIndex(m int64) (int64, int) {
	last := st[len(st)-1]
	return last.Off(m), last.Index(m)
}

func (st Stack) String() string {
	return fmt.Sprintf("%3x", []OffIndex(st))
}

func (l *KVLayout2) dumpPage(off int64) string {
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
		fmt.Fprintf(&buf, "datasize %3x free space %3x\n", l.pagedatasize(p, 0, n), l.pagefree(p, n))
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
			v := l.link(st.LastOffIndex(l.Mask))
			fmt.Fprintf(&buf, "    %2x -> %16x  | %q\n", k, v, k)
		}
	}

	return buf.String()
}

func (l *KVLayout2) dumpFile() string {
	var buf strings.Builder

	b := l.Back
	off := int64(0)

	p := b.Access(off, 0x10)
	if bytes.HasPrefix(p, []byte("xrain")) {
		off = 4 * l.Page
	}
	b.Unlock(p)

	for off < b.Size() {
		p := b.Access(off, 0x10)
		ps := 1 + l.overflow(p)
		b.Unlock(p)

		s := l.dumpPage(off)

		buf.WriteString(s)

		off += int64(ps) * l.Page
	}

	return buf.String()
}

func (l *KVLayout2) pages(s int) int {
	p := int(l.Page)

	return (s + p - 1) / p * p
}

func varlen(x int) (n int) {
	n = 1
	for x >= 0x80 {
		n++
		x >>= 7
	}
	return
}
