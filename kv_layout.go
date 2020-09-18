package xrain

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"

	"github.com/nikandfor/tlog/ext/tlargs"
)

type (
	Layout interface {
		SetMeta(*Meta)

		Alloc() (int64, error)
		Free(root int64) error

		Seek(s Stack, root int64, k, v []byte) (Stack, bool)
		Step(s Stack, root int64, back bool) Stack

		Flags(s Stack) int
		Key(s Stack, buf []byte) ([]byte, int)
		Value(s Stack, buf []byte) []byte

		Int64(s Stack) int64
		SetInt64(s Stack, v int64) (old int64, err error)
		AddInt64(s Stack, v int64) (new int64, err error)

		Insert(st Stack, ff int, k, v []byte) (Stack, error)
		Delete(st Stack) (Stack, error)
	}

	FlagsSupported interface {
		Flags(Stack) int
	}

	BaseLayout2 struct { // crc32 uint32, isbranch bit, size uint15, overflow uint32, ver int47
		*Meta
		Compare    func(a, b []byte) int
		linkf      func(off int64, i int) int64
		searchf    func(off int64, k, v []byte) (i, n int, coff int64, eq, isleaf bool)
		firstLastf func(st Stack, off int64, back bool) Stack

		stbuf Stack

		kDataBytes []byte
		kFreeBytes []byte
	}

	KVLayout2 struct { // base [16]byte, offsets [size]int16, data []{...}
		BaseLayout2
		maxrow int
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

/*
	Key Value Pair

	Total len of the struct must not exceed ((page_size - header_size) / 0x10) bytes.

	// index
	offset int16

	// data
	flags  byte
	keylen byte // could be prefix
	key    []byte
	value  []byte
*/

const NilPage = -1

const kvIndexStart = 0x10
const kvMaxBranckKeylen = 0x50

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
	return int(p[6])<<24 | int(p[7])<<16 | int(p[8])<<8 | int(p[9])
}

func (l *BaseLayout2) setoverflow(p []byte, n int) {
	p[6] = byte(n >> 24)
	p[7] = byte(n >> 16)
	p[8] = byte(n >> 8)
	p[9] = byte(n)
}

func (l *BaseLayout2) pagever(p []byte) (ver int64) {
	ver = int64(p[10])<<40 | int64(p[11])<<32 | int64(p[12])<<24 | int64(p[13])<<16 | int64(p[14])<<8 | int64(p[15])
	ver = ver << 16 >> 16
	return
}

func (l *BaseLayout2) setver(p []byte, v int64) {
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

func (l *BaseLayout2) metric(k []byte, d int) {
	if l.Meta == nil || l.Meta.Meta.Layout == nil {
		return
	}

	_, l.stbuf, _ = l.Meta.Meta.AddInt64(k, int64(d), l.stbuf)
}

func (l *BaseLayout2) SetMeta(m *Meta) { l.Meta = m }

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

func (l *BaseLayout2) Seek(st Stack, root int64, k, v []byte) (_ Stack, eq bool) {
	st = st[:0]

	if root == NilPage {
		return nil, false
	}

	off := root
	var isleaf bool
	var i, n int
	var coff int64

	for !isleaf {
		i, n, coff, eq, isleaf = l.searchf(off, k, v)

		if tl.V("seek") != nil {
			if isleaf {
				tl.Printf("seek root %3x  off %3x ->      i %2d / %2d  eq %5v  - leaf", root, off, i, n, eq)
			} else {
				tl.Printf("seek root %3x  off %3x -> %3x  i %2d / %2d            - branch", root, off, coff, i, n)
			}
		}

		if !isleaf && i == n {
			i--
		}

		st = append(st, MakeOffIndex(off, i))

		off = coff
	}

	if tl.V("seek,seek_res") != nil {
		tl.Printf("seek root %3x -> %v  eq %5v by %.20q [% 2.20x]", root, st, eq, k, k)
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

	if back {
		if i > 0 {
			i--
			goto fin
		}
	} else {
		n := l.nKeys(off)

		if i+1 < n {
			i++
			goto fin
		}
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

func NewKVLayout2(m *Meta) *KVLayout2 {
	l := &KVLayout2{
		BaseLayout2: BaseLayout2{
			kDataBytes: []byte("stats.data"),
			kFreeBytes: []byte("stats.free"),
		},
	}

	l.Meta = m
	l.linkf = l.link
	l.searchf = l.search
	l.firstLastf = l.firstLast

	l.init()

	return l
}

func (l *KVLayout2) SetMeta(m *Meta) { l.Meta = m; l.init() }

func (l *KVLayout2) init() {
	if l.Meta == nil {
		return
	}

	l.maxrow = kvMaxBranckKeylen
	if m := int(l.Page / 4); l.maxrow > m {
		l.maxrow = m
	}
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
	if kl > l.maxrow {
		kl = l.maxrow
	}
	// link + keylen + key + value
	return 2 + 1 + kl + len(v)
}

func (l *KVLayout2) pagelink(p []byte, i int) (off int64) {
	isleaf := l.isleaf(p)
	st := l.dataoff(p, i)
	end := l.dataend(p, i)

	var kl int
	if isleaf {
		st++ // flags

		var ll int
		kl, ll = decodevarlen(p[st:])
		st += ll + kl
	} else {
		kl = int(p[st])
		kl &^= 0x80
		st += 1 + kl
	}

	var buf [8]byte
	var v []byte
	sz := end - st

	if sz >= 8 {
		v = p[end-8 : end]
	} else {
		copy(buf[8-sz:], p[end-sz:end])
		v = buf[:]
	}

	off = int64(binary.BigEndian.Uint64(v))

	if sz < 8 {
		s := uint(8-sz) * 8
		off = off << s >> s
	}

	return
}

func (l *KVLayout2) pagesetlink(p []byte, i int, off int64) {
	isleaf := l.isleaf(p)
	st := l.dataoff(p, i)
	end := l.dataend(p, i)

	if isleaf {
		st++ // flags
	}

	kl := int(p[st])
	st += 1 + kl

	var buf [8]byte
	sz := end - st

	binary.BigEndian.PutUint64(buf[:], uint64(off))

	if sz >= 8 {
		copy(p[end-8:], buf[:])
	} else {
		copy(p[end-sz:], buf[8-sz:])
	}
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

	ps := 1

again:
	remount := false

	func() {
		p := l.Access(off, int64(ps)*l.Page)
		defer l.Unlock(p)

		ps = 1 + l.overflow(p)

		dst := l.dataoff(p, i)

		if dst+10 > len(p) && ps*int(l.Page) != len(p) { // do not remount if we already are and key+value is really short
			remount = true
			return
		}

		var kl int
		if l.isleaf(p) {
			ff = int(p[dst])
			dst++

			var ll int
			kl, ll = decodevarlen(p[dst:])
			dst += ll
		} else {
			kl = int(p[dst])
			dst++
			if kl >= 0x80 {
				kl = kl&^0x80 - 8
			}
		}

		if dst+kl > len(p) {
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
	return l.value(off, i, buf)
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

		var kl int
		if l.isleaf(p) {
			dst++ // flags

			var ll int
			kl, ll = decodevarlen(p[dst:])
			dst += ll
		} else {
			kl = int(p[dst])
			kl &^= 0x80
			dst++
		}

		dst += kl

		val = append(buf, p[dst:dend]...)
	}()

	if remount {
		goto again
	}

	return
}

func (l *KVLayout2) Int64(st Stack) (v int64) {
	off, i := st.LastOffIndex(l.Mask)

	ps := 1

again:
	remount := false

	func() {
		p := l.Access(off, int64(ps)*l.Page)
		defer l.Unlock(p)

		dend := l.dataend(p, i)
		if remount = dend > len(p); remount {
			return
		}

		v = l.pagelink(p, i)
	}()

	if remount {
		goto again
	}

	return
}

func (l *KVLayout2) SetInt64(st Stack, v int64) (old int64, err error) {
	off, i := st.LastOffIndex(l.Mask)

	var alloc bool
	ps := 1

again:
	remount := false

	func() {
		p := l.Access(off, int64(ps)*l.Page)
		defer l.Unlock(p)

		ps = 1 + l.overflow(p)

		if alloc = l.pagever(p) != l.Ver; alloc {
			return
		}

		dend := l.dataend(p, i)
		if remount = dend > len(p); remount {
			return
		}

		old = l.pagelink(p, i)

		l.pagesetlink(p, i, v)
	}()

	if alloc {
		off, err = l.realloc(off, ps, ps)
		if err != nil {
			return
		}

		goto again
	}

	if remount {
		goto again
	}

	return
}

func (l *KVLayout2) AddInt64(st Stack, v int64) (new int64, err error) {
	off, i := st.LastOffIndex(l.Mask)

	var alloc bool
	ps := 1

again:
	remount := false

	func() {
		p := l.Access(off, int64(ps)*l.Page)
		defer l.Unlock(p)

		ps = 1 + l.overflow(p)

		if alloc = l.pagever(p) != l.Ver; alloc {
			return
		}

		dend := l.dataend(p, i)
		if remount = dend > len(p); remount {
			return
		}

		new = l.pagelink(p, i) + v

		l.pagesetlink(p, i, v)
	}()

	if alloc {
		off, err = l.realloc(off, ps, ps)
		if err != nil {
			return
		}

		goto again
	}

	if remount {
		goto again
	}

	return
}

func (l *KVLayout2) search(off int64, k, v []byte) (i, n int, coff int64, eq, isleaf bool) {
	cmp := l.Compare
	if cmp == nil {
		cmp = bytes.Compare
	}

	if tl.V("search") != nil {
		tl.Printf("search %4x %.20q", off, k)
	}

	ps := 1

again:
	remount := false

	func() {
		p0 := l.Access(off, int64(ps)*l.Page)
		defer func() {
			if p0 != nil {
				l.Unlock(p0)
			}
		}()

		p := p0

		if nps := 1 + l.overflow(p); nps != ps {
			ps = nps
			remount = true
			return
		}

		hardcase := func(i, st, kl int) (c int) {
			kl &^= 0x80

			ik := p[st : st+kl-10]

			kp := k
			if len(ik) < len(k) {
				kp = k[:len(ik)]
			}

			c = cmp(ik, kp)
			//	tl.Printf("hadcase %x  i %d  %x  c %d ik %x %.20q  k %x %.20q", off, i, st, c, len(ik), ik, len(kp), kp)
			if c != 0 {
				return
			}

			if p0 != nil {
				p = make([]byte, len(p0))
				copy(p, p0)

				l.Unlock(p0)

				p0 = nil
			}

			lps := int(p[st+kl-10])<<8 | int(p[st+kl-9])
			link, li := OffIndex(binary.BigEndian.Uint64(p[st+kl-8:])).OffIndex(l.Mask)

			//	tl.Printf("hardcase link %x %x ps %x", link, li, lps)

			lp := l.Access(link, int64(lps)*l.Page)
			defer l.Unlock(lp)

			lst := l.dataoff(lp, li)
			lst++ // flags

			kl, ll := decodevarlen(lp[lst:])
			lst += ll

			ik = lp[lst : lst+kl]

			c = cmp(ik, k)
			//	tl.Printf("hadcas2 %x  i %d  %x  c %d ik %x %.20q  k %.20q", off, i, st, c, kl, ik, k)
			return c
		}

		keycmp := func(i int, cmpval bool) (c int) {
			st := l.dataoff(p, i)

			var kl int
			if isleaf {
				st++ // flags

				var ll int
				kl, ll = decodevarlen(p[st:])
				st += ll
			} else {
				kl = int(p[st])
				st++
			}

			if !isleaf && kl >= 0x80 {
				return hardcase(i, st, kl)
			}

			ik := p[st : st+kl]

			c = cmp(ik, k)
			//	tl.Printf("cmp %2d <= %.20q (%x) %.20q (%x)  - cmpval %v", c, ik, kl, k, len(k), cmpval)
			if !cmpval || c != 0 {
				return
			}

			st += kl
			end := l.dataend(p, i)

			iv := p[st:end]

			return cmp(iv, v)
		}

		n = l.nkeys(p)
		isleaf = l.isleaf(p)

		i = sort.Search(n, func(i int) bool {
			return keycmp(i, true) >= 0
		})

		if isleaf {
			eq = i < n && keycmp(i, false) == 0
		} else {
			if i == n {
				i--
			}

			coff = l.pagelink(p, i)
		}
	}()

	if remount {
		goto again
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
	var alloc, split, triple, addbig bool
	ps := 1

again:
	func() {
		p := l.Access(off, int64(ps)*l.Page)
		defer l.Unlock(p)

		ps = 1 + l.overflow(p)

		isleaf := l.isleaf(p)
		alloc = l.pagever(p) != l.Ver // TODO: ver = l.pagever(p); alloc = !(ver == l.Ver || ver < l.Keep)

		n := l.nkeys(p)

		free := l.pagefree(p, n)
		exp := l.expectedsize(p, isleaf, i, n, ff, k, v)

		split = exp > free

		triple = int64(kvIndexStart+exp) > 4*l.Page

		addbig = triple && (i == 0 || i == n)

		if alloc || split || triple || addbig {
			return
		}

		l.pageInsert(p, i, n, ff, k, v)
	}()

	switch {
	case addbig:
		return l.insertAddBig(off, i, ff, k, v, ps)
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

		if off0 == NilPage && tl.V("insert") != nil {
			tl.Printf("insert %2d / %2d split at %2d  exp %3x  left %3x / %3x of %3x", i, n, m, exp, left, tot, len(p))
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
		case n == 0:
			page0 = exp
		case i == 0:
			page0 = exp
			page1 = l.pagedatasize(p, 0, n)
		case i == n:
			page0 = l.pagedatasize(p, 0, n)
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

		if tl.V("insert,triple") != nil {
			tl.Printf("insert %2d / %2d  to %3x triple %x %x %x  from %x + %x", i, n, off, page0, page1, page2, len(p), exp)
		}

		if page2 != 0 {
			l.pageSplit(p, p0[:page0], p0[page0+page1:], i, n)
		}

		if page1 != 0 {
			off1 = off0 + int64(page0)
		}

		switch {
		case n == 0:
			l.pagesetheaders(p0, isleaf, 0)
			l.pageInsert(p0, 0, 0, ff, k, v)
		case i == 0:
			l.pagesetheaders(p0[:page0], isleaf, 0)
			l.pageInsert(p0[:page0], 0, 0, ff, k, v)
		case i == n:
			l.pagesetheaders(p0[page0:], isleaf, 0)
			l.pageInsert(p0[page0:], 0, 0, ff, k, v)
		default:
			l.pagesetheaders(p0[page0:page0+page1], isleaf, 0)
			l.pageInsert(p0[page0:], 0, 0, ff, k, v)

			off2 = off1 + int64(page1)
		}

		if tl.V("triple_dump2") != nil {
			tl.Printf("triple dump %x + %x + %x\n%v", page0, page1, page2, hex.Dump(p0))
		}
		if tl.V("triple_dump") != nil {
			tl.Printf("triple dump %x + %x + %x.  dump %x\n%v", page0, page1, page2, off0, l.dumpPage(off0))
			if off1 != NilPage {
				tl.Printf("dump %x\n%v", off1, l.dumpPage(off1))
			}
			if off2 != NilPage {
				tl.Printf("dump %x\n%v", off2, l.dumpPage(off2))
			}
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

func (l *KVLayout2) insertAddBig(off int64, i, ff int, k, v []byte, oldpages int) (off0, off1, off2 int64, di int, err error) {
	var alloc, isleaf bool
	var n int
	off0, off1, off2 = NilPage, NilPage, NilPage
	ps := oldpages

again:
	func() {
		p := l.Access(off, int64(ps)*l.Page)
		defer l.Unlock(p)

		if off0 == NilPage {
			isleaf = l.isleaf(p)
			n = l.nkeys(p)

			exp := l.expectedsize(p, isleaf, i, n, ff, k, v)
			ps = l.pages(exp) / int(l.Page)

			alloc = l.pagever(p) != l.Ver

			if n == 0 && !alloc && ps == oldpages {
				off0 = off
			} else {
				return
			}

			if tl.V("insert,addbig") != nil {
				tl.Printf("insert %2d / %2d  off %4x -> %4x %4x  sz %4x = %3x * %4x = %4x", i, n, off, off0, off1, l.pages(exp), ps, l.Page, len(p))
			}
		}

		i = 0
		n = 0

		l.pagesetheaders(p, isleaf, 0)
		l.pageInsert(p, i, n, ff, k, v)
	}()

	if off0 == NilPage {
		switch {
		case n == 0:
			off, err = l.realloc(off, oldpages, ps)
			off0 = off
		case i == 0:
			off1 = off
			off, err = l.Freelist.Alloc(ps)
			off0 = off
		case i == n:
			off0 = off
			off, err = l.Freelist.Alloc(ps)
			off1 = off
			di = n
		}
		if err != nil {
			return
		}

		goto again
	}

	return
}

func (l *KVLayout2) pageInsert(p []byte, i, n, ff int, k, v []byte) {
	isleaf := l.isleaf(p)

	exp := l.expectedsize(p, isleaf, i, n, ff, k, v) - 2 // -link

	dend := l.dataend(p, i)

	if i < n {
		dst := l.dataoff(p, n-1)

		if tl.V("insert") != nil {
			tl.Printf("insert %2d / %2d move  %3x - %3x on %3x    %2x %.20q %.30q", i, n, dst, dend, exp, ff, k, v)
		}

		copy(p[dst-exp:], p[dst:dend])

		for j := n - 1; j >= i; j-- {
			off := l.dataoff(p, j)
			l.setdataoff(p, j+1, off-exp)
		}
	}

	dst := dend - exp

	if tl.V("insert") != nil {
		tl.Printf("insert %2d / %2d to    %3x - %3x   (%3x)   %2x %.20q %.30q", i, n, dst, dend, exp, ff, k, v)
	}

	l.setdataoff(p, i, dst)

	if isleaf {
		p[dst] = byte(ff)
		dst++
	}

	dst += encodevarlen(p[dst:], len(k))

	copy(p[dst:], k)
	copy(p[dst+len(k):], v)

	l.setnkeys(p, n+1)
}

func (l *KVLayout2) pageInsertPageLink(p []byte, i, n, cps int, k []byte, coff int64, link OffIndex) {
	var size int
	size = 1 + len(k) + 8
	if link != NilPage {
		size += 10
	}

	//	tl.Printf("dump before\n%v", hex.Dump(p))

	dend := l.dataend(p, i)

	if i < n {
		dst := l.dataoff(p, n-1)

		if tl.V("insert,pagelink") != nil {
			tl.Printf("pagelk %2d / %2d move %3x - %3x on %3x  of %3x  %q -> cps %2x link %4x off %4x", i, n, dst, dend, size, len(p), k, cps, link, coff)
		}

		copy(p[dst-size:], p[dst:dend])

		for j := n - 1; j >= i; j-- {
			off := l.dataoff(p, j)
			l.setdataoff(p, j+1, off-size)
		}
	}

	dst := dend - size

	if tl.V("insert,pagelink") != nil {
		tl.Printf("pagelk %2d / %2d to   %3x - %3x   (%3x) of %3x  %q -> cps %2x link %4x off %4x", i, n, dst, dend, size, len(p), k, cps, link, coff)
	}

	l.setdataoff(p, i, dst)

	if link == NilPage {
		p[dst] = byte(len(k))
		dst++
		dst += copy(p[dst:], k)
	} else {
		p[dst] = byte(len(k)+10) | 0x80
		dst++
		dst += copy(p[dst:], k)

		p[dst] = byte(cps << 8)
		dst++
		p[dst] = byte(cps)
		dst++

		binary.BigEndian.PutUint64(p[dst:], uint64(link))
		dst += 8
	}

	binary.BigEndian.PutUint64(p[dst:], uint64(coff))

	//	tl.Printf("dump after\n%v", hex.Dump(p))

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

	if tl.V("split_dump") != nil {
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

		if tl.If("delete") {
			tl.Printf("delete %d / %d  move %x-%x <- %x-%x %v", i, n, st+diff, st+diff+end-st, st, end, tlargs.IfV(tl, "delete_dump", "\n"+hex.Dump(p), ""))
		}

		copy(p[st+diff:], p[st:end])

		for j := i; j < n; j++ {
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

				// tl.Printf("deleted rebalanced sibling: off %x\n%v", off, l.dumpPage(off))

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

		if tl.V("out") != nil {
			tl.Printf("out push root %x <- %x, %x, %x", root, off0, off1, off2)
		}

		p := l.Access(root, l.Page)
		l.pagesetheaders(p, false, 0)
		l.Unlock(p)

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

func (l *KVLayout2) updatePageLink(off int64, i int, coff int64, del bool) (off0, off1 int64, splitm int, err error) {
	var alloc, split bool
	ps, cps := 1, 1

	off0, off1 = off, NilPage

	if tl.V("pagelink") != nil {
		tl.Printf("pagelink  %3x %d  ->  %3x %v", off, i, coff, del)
	}

again:
	remount := false

	func() {
		p, cp := l.Access2(off0, int64(ps)*l.Page, coff, int64(cps)*l.Page)
		defer l.Unlock2(p, cp)

		alloc = l.pagever(p) != l.Ver

		cps = 1 + l.overflow(cp)
		cisleaf := l.isleaf(cp)
		cn := l.nkeys(cp)
		cst := l.dataoff(cp, cn-1)

		if del && !cisleaf {
			st := l.dataoff(p, i)
			end := l.dataend(p, i)
			k := p[st : end-8]

			cend := l.dataend(cp, cn-1)

			ck := cp[cst : cend-8]

			if bytes.Equal(k, ck) && l.pagelink(p, i) == coff {
				return
			}
		}

		link := OffIndex(NilPage)
		kl := 0
		if cisleaf {
			if remount = cst+10 > len(cp) && cps*int(l.Page) != len(cp); remount { // do not remount if we already are and key+value is really short
				return
			}

			cst++ // flags

			var ll int
			kl, ll = decodevarlen(cp[cst:])
			cst += ll

			if kl >= l.maxrow {
				kl = l.maxrow - 10

				link = MakeOffIndex(coff, cn-1)
			}

			if remount = cst+kl > len(cp); remount {
				return
			}
		} else {
			kl = int(cp[cst])
			cst++
		}

		k := cp[cst : cst+kl]

		n := l.nkeys(p)

		exp := 1 + kl + 8

		usage := 0
		if del {
			usage = l.pagedatasize(p, 0, i) + exp + l.pagedatasize(p, i+1, n)
		} else {
			usage = l.pagedatasize(p, 0, n) + exp
		}

		split = kvIndexStart+usage > int(l.Page)

		if alloc || split && ps == 1 {
			return
		}

		if tl.V("pagelink") != nil {
			tl.Printf("pagelk %q (%x) cisleaf %5v link %x del %v  split %v", k, kl, cisleaf, link, del, split)
			tl.Printf("coff %x cst %x of %x", coff, cst, len(cp))
		}

		if del {
			l.pageDelete(p, i, n)
			n--
		}

		ip := p[:l.Page]

		if split {
			m := (n + 1) / 2

			l.pageSplit(p, p[:l.Page], p[l.Page:], m, n)

			if i >= m {
				ip = p[l.Page:]
				splitm = m
				i -= m
				n -= m
			} else {
				n = m
			}

			off1 = off0 + l.Page

			split = false
		}

		l.pageInsertPageLink(ip, i, n, cps, k, coff, link)
	}()

	if alloc || split {
		if split {
			ps = 2
		}

		off0, err = l.realloc(off0, 1, ps)
		if err != nil {
			return
		}

		goto again
	}

	if remount {
		goto again
	}

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
		k, ff := l.Key(st, nil)

		if isleaf {
			v := l.Value(st, nil)
			fmt.Fprintf(&buf, "    %-20.10x -> %2x  %-12.6x  | %-22.20q -> %-.30q\n", k, ff, v, k, v)
		} else {
			v := l.link(st.LastOffIndex(l.Mask))
			fmt.Fprintf(&buf, "    %-20.10x -> %16x  | %-22.20q\n", k, v, k)
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
	if s == 0 {
		return 0
	}

	s += kvIndexStart

	p := int(l.Page)

	return (s + p - 1) / p * p
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

func varlen(x int) (n int) {
	n = 1
	for x >= 0x80 {
		n++
		x >>= 7
	}
	return
}

func encodevarlen(p []byte, x int) (i int) {
	for x >= 0x80 {
		p[i] = byte(x) | 0x80
		x >>= 7
		i++
	}

	p[i] = byte(x)

	return i + 1
}

func decodevarlen(p []byte) (x, i int) {
	for _, b := range p {
		x |= int(b) &^ 0x80 << uint(i*7)
		i++

		if b < 0x80 {
			break
		}
	}

	return
}
