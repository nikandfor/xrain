package xrain

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"sort"
)

type (
	Layout interface {
		Seek(s Stack, root int64, k []byte) (Stack, bool)
		Step(s Stack, root int64, back bool) Stack

		Key(s Stack, buf []byte) ([]byte, int)
		Value(s Stack, buf []byte) []byte

		Insert(st Stack, ff int, k, v []byte) (Stack, error)
		Delete(st Stack) (Stack, error)
	}

	Common struct {
		Back

		Page, Mask int64
		Ver, Keep  int64

		Freelist

		Meta Layout
	}

	BaseLayout2 struct {
		*Common
	}

	KVLayout2 struct {
		BaseLayout2
	}
)

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

func (l *BaseLayout2) realloc(off, ver int64, oldn, n int) (noff int64, err error) {
	err = l.Free(oldn, off, ver)
	if err != nil {
		return
	}

	noff, err = l.Alloc(n)
	if err != nil {
		return
	}

	d, s := l.Access2(noff, l.Page*int64(n), off, l.Page*int64(oldn))
	func() {
		copy(d, s)

		l.setver(d, l.Ver)
	}()
	l.Unlock2(d, s)

	return
}

func NewKVLayout2(c *Common) *KVLayout2 {
	var l KVLayout2
	l.Common = c
	return &l
}

func (l *KVLayout2) setdataoff(p []byte, i, off int) {
	st := kvIndexStart + 2*i
	p[st] = byte(off >> 8)
	p[st+1] = byte(off)
}

func (l *KVLayout2) dataoff(p []byte, i int) int {
	if i == -1 {
		panic(i)
	}
	st := kvIndexStart + 2*i
	return int(p[st])<<8 | int(p[st+1])
}

func (l *KVLayout2) dataend(p []byte, i int) int {
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
	dend := len(p)
	if i != 0 {
		ist := kvIndexStart + 2*i
		dend = int(p[ist-2])<<8 | int(p[ist-1])
	}
	return (dend - dst) + 2*(n-i)
}

func (l *KVLayout2) expectedsize(p []byte, i, n int, k, v []byte) int {
	vallink := 0 // in case we moved value to overflow page
	// link + flags + keylen + key + value + vallink
	return 2 + 1 + 1 + len(k) + len(v) + vallink
}

func (l *KVLayout2) nKeys(off int64) (n int) {
	p := l.Access(off, 0x10)
	n = l.nkeys(p)
	l.Unlock(p)

	return n
}

func (l *KVLayout2) link(st Stack) (off int64) {
	var buf [8]byte

	l.Value(st, buf[:])

	off = int64(binary.BigEndian.Uint64(buf[:]))

	return off
}

func (l *KVLayout2) Key(st Stack, buf []byte) ([]byte, int) {
	return buf, 0
}

func (l *KVLayout2) Value(st Stack, buf []byte) []byte {
	return buf
}

func (l *KVLayout2) Insert(st Stack, ff int, k, v []byte) (_ Stack, err error) {
	off, i := st.LastOffIndex(l.Mask)

	var ver int64
	var alloc, split, remount bool
	page := l.Page

again:
	p := l.Access(off, page)
	func() {
		ver = l.pagever(p)
		n := l.nkeys(p)

		exp := l.expectedsize(p, i, n, k, v)
		free := l.pagefree(p, n)

		if free < exp {
			split = true
			return
		}

		alloc = l.pagever(p) != l.Ver

		dend := l.dataend(p, i)
		if len(p) < dend {
			page = l.Page * int64(1+l.overflow(p))
			remount = true
			return
		}

		if alloc {
			return
		}

		l.pageInsert(p, i, n, ff, k, v)
	}()
	l.Unlock(p)

	if split {
		return l.insertSplit(st, ff, k, v)
	}

	if alloc {
		pages := int(page / l.Page)

		off, err = l.realloc(off, ver, pages, pages)
		if err != nil {
			return
		}

		goto again
	}

	if remount {
		goto again
	}

	st[len(st)-1] = MakeOffIndex(off, i)

	return l.out(st, off, NilPage)
}

func (l *KVLayout2) insertSplit(st Stack, ff int, k, v []byte) (_ Stack, err error) {
	off, i := st.LastOffIndex(l.Mask)

	var lpage, rpage int
	var left bool
	var m int

	var ver int64
	var oldpages int
	alloc := true

	page := l.Page

again:
	p := l.Access(off, page)
	func() {
		n := l.nkeys(p)

		if lpage == 0 {
			ver = l.pagever(p)
			oldpages = 1 + l.overflow(p)

			exp := l.expectedsize(p, i, n, k, v)
			half := (l.pagedatasize(p, 0, n) + exp) / 2

			m = 0
			for m < n {
				lsize := l.pagedatasize(p, 0, m)
				if lsize > half || m == i && lsize+exp > half {
					break
				}

				m++
			}

			lpage = l.pagedatasize(p, 0, m)
			rpage = l.pagedatasize(p, m, n)

			switch {
			case i < m:
				lpage += exp
				left = true
			case i > m:
				rpage += exp
			case lpage <= rpage:
				lpage += exp
				left = true
			default:
				rpage += exp
			}

			lpage = (lpage + int(l.Page) - 1) / int(l.Page) * int(l.Page)
			rpage = (rpage + int(l.Page) - 1) / int(l.Page) * int(l.Page)

			tl.Printf("split %x -> %x %x  data %x  left %v", int(l.Page)*oldpages, lpage, rpage, exp, left)

			return
		}

		l.pageSplit(p[:lpage], p[lpage:], m, n)

		if left {
			l.pageInsert(p[:lpage], i, m, ff, k, v)

			st[len(st)-1] = MakeOffIndex(off, i)
		} else {
			l.pageInsert(p[lpage:], i-m, n-m, ff, k, v)

			st[len(st)-1] = MakeOffIndex(off+int64(lpage), i-m)
		}
	}()
	l.Unlock(p)

	if alloc {
		page = int64(lpage + rpage)
		pages := int(page / l.Page)

		off, err = l.realloc(off, ver, oldpages, pages)
		if err != nil {
			return
		}

		alloc = false

		goto again
	}

	return l.out(st, off, off+int64(lpage))
}

func (l *KVLayout2) pageInsert(p []byte, i, n, ff int, k, v []byte) {
	exp := 2 + len(k) + len(v)
	dend := l.dataend(p, i)

	if i < n {
		dst := l.dataoff(p, n-1)

		copy(p[dst-exp:], p[dst:dend])

		for j := n - 1; j >= i; j-- {
			off := l.dataoff(p, j)
			l.setdataoff(p, j+1, off-exp)
		}
	}

	dst := dend - exp

	tl.Printf("pageInsert %d / %d to %x - %x (%x)", i, n, dst, dend, exp)

	p[dst] = byte(ff)
	p[dst+1] = byte(len(k))
	copy(p[dst+2:], k)
	copy(p[dst+2+len(k):], v)

	l.setdataoff(p, i, dst)

	l.setnkeys(p, n+1)
}

func (l *KVLayout2) pageSplit(p, r []byte, m, n int) {
	// r page

	dst := l.dataoff(p, n-1)
	dend := l.dataend(p, m)
	dsize := dend - dst

	tl.Printf("split  %d / %d move %x - %x <- %x - %x", m, n, len(r)-dsize, len(r), dst, dend)

	copy(r[len(r)-dsize:], p[dst:dend])

	diff := len(r) - dend
	for j := m; j < n; j++ {
		off := l.dataoff(p, j)
		l.setdataoff(r, j-m, off+diff)
	}

	// p page
	if len(p) != l.dataend(p, 0) {
		dst = l.dataoff(p, m-1)
		dend = l.dataend(p, 0)
		dsize = dend - dst

		tl.Printf("split inside page  move %x - %x <- %x - %x", len(p)-dsize, len(p), dst, dend)

		copy(p[len(p)-dsize:], p[dst:dend])

		diff = len(p) - dend
		for j := 0; j < m; j++ {
			off := l.dataoff(p, j)
			l.setdataoff(p, j, off+diff)
		}
	}

	r[4] = p[4] // isleaf

	l.setnkeys(p, m)
	l.setnkeys(r, n-m)

	l.setoverflow(p, len(p)/int(l.Page)-1)
	l.setoverflow(r, len(r)/int(l.Page)-1)

	l.setver(p, l.Ver)
	l.setver(r, l.Ver)

	tl.Printf("split dump\n%v\n%v", hex.Dump(p), hex.Dump(r))
}

func (l *KVLayout2) Delete(st Stack) (_ Stack, err error) {
	return
}

func (l *KVLayout2) delete(off int64, i int) {}

func (l *KVLayout2) pageDelete(p []byte, i, n int) {}

func (l *KVLayout2) Seek(st Stack, root int64, k []byte) (_ Stack, eq bool) {
	st = st[:0]

	off := root
	var isleaf bool
	var i, n, d int

	search := func(off int64, k []byte) {
		pages := 1

	again:
		p := l.Access(off, l.Page*int64(pages))
		if o := l.overflow(p); o+1 != pages {
			l.Unlock(p)

			pages = 1 + o

			goto again
		}

		n = l.nkeys(p)

		keycmp := func(i int) int {
			dst := l.dataoff(p, i)
			//	iF := int(p[dst])
			kl := int(p[dst+1])
			dst += 2

			ik := p[dst : dst+kl]

			return bytes.Compare(ik, k)
		}

		i := sort.Search(n, func(i int) bool {
			return keycmp(i) >= 0
		})

		eq = i < n && keycmp(i) == 0
		isleaf = l.isleaf(p)

		if !isleaf {
			dst := l.dataoff(p, i)
			dend := l.dataend(p, i)
			kl := int(p[dst+1])
			dst += 2

			v := p[dst+kl : dend]
			off = int64(binary.BigEndian.Uint64(v))
		}

		l.Unlock(p)

		return
	}

	for {
		st = append(st, OffIndex(off))

		search(off, k)
		//	log.Printf("search %2x %q -> %x %v", off, k, i, eq)

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

func (l *KVLayout2) firstLast(st Stack, root int64, back bool) Stack {
	off := root
	isleaf := false
	var i int

	for {
		pages := 1
		reaccess := false

	again:
		p := l.Access(off, l.Page*int64(pages))

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

			dst := l.dataoff(p, i)
			dend := l.dataend(p, i)

			if dend > len(p) {
				if o := l.overflow(p); o+1 != pages {
					pages = 1 + o

					reaccess = true
				} else {
					panic("must not be so")
				}

				return
			}

			kl := int(p[dst+1])
			dst += 2

			v := p[dst+kl : dend]
			off = int64(binary.BigEndian.Uint64(v))
		}()

		l.Unlock(p)

		if reaccess {
			goto again
		}

		if isleaf {
			return st
		}
	}
}

func (l *KVLayout2) Step(st Stack, root int64, back bool) Stack {
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

func (l *KVLayout2) out(st Stack, off0, off1 int64) (_ Stack, err error) {
	var reduce bool

	return st, nil

	for d := len(st) - 2; d >= 0; d-- {
		poff, i := st[d].OffIndex(l.Mask)

		var ver int64
		oldpages, pages := 1, 1
		alloc := false
		coff := off0

	again:
		par, cp := l.Access2(poff, l.Page*int64(pages), coff, l.Page)
		func() {
			p := par[:l.Page]

			if !alloc {
				ver = l.pagever(p)
				alloc = ver != l.Ver
				oldpages = 1 + l.overflow(p)
			}

			dst := l.dataoff(cp, l.nkeys(cp)-1)
			ff := 0
			kl := int(cp[dst+1])
			dst += 2
			lkey := cp[dst : dst+kl]

			var vbuf [8]byte
			binary.BigEndian.PutUint64(vbuf[:], uint64(coff))

			n := l.nkeys(p)

			exp := l.expectedsize(p, i, n, lkey, vbuf[:])

			if l.pagefree(p, n) >= exp { // fit into the same page
				if !alloc {
					if coff == off0 {
						l.pageDelete(p, i, n)
					}
					l.pageInsert(p, i, n-1, ff, lkey, vbuf[:])

					reduce = l.nkeys(p) == 0
				}
				return
			}

			if pages == 1 {
				pages = 2
				alloc = true

				return
			}

			itop := i
			if coff == off0 {
				itop--
			}

			l.pageSplit(p, par[l.Page:], i, n)

			if l.pagefree(p, i-1) >= exp {
				l.pageInsert(p, itop, itop, ff, lkey, vbuf[:])

				st[d] = MakeOffIndex(poff, itop)

				i = itop + 1
			} else {
				l.setnkeys(p, itop)
				l.pageInsert(par[l.Page:], 0, n-i, ff, lkey, vbuf[:])

				st[d] = MakeOffIndex(poff+l.Page, 0)

				poff += l.Page
				i = 1
			}

			reduce = false
		}()
		l.Unlock2(par, cp)

		if alloc {
			poff, err = l.realloc(poff, ver, oldpages, pages)
			if err != nil {
				return
			}

			alloc = false

			goto again
		}

		if coff == off0 && off1 != NilPage {
			coff = off1
			goto again
		}

		off0 = poff
		if pages != 1 {
			off1 = poff + l.Page
		}
	}

	if off1 != NilPage {
		off, err := l.Alloc(1)
		if err != nil {
			return nil, err
		}

		p, cp := l.Access2(off, l.Page, off0, l.Page)
		func() {
			dst := l.dataoff(cp, l.nkeys(cp)-1)
			ff := 0
			kl := int(cp[dst+1])
			dst += 2
			lkey := cp[dst : dst+kl]

			var vbuf [8]byte
			binary.BigEndian.PutUint64(vbuf[:], uint64(off0))

			l.pageInsert(p, 0, 0, ff, lkey, vbuf[:])
		}()
		l.Unlock2(p, cp)

		p, cp = l.Access2(off, l.Page, off1, l.Page)
		func() {
			dst := l.dataoff(cp, l.nkeys(cp)-1)
			ff := 0
			kl := int(cp[dst+1])
			dst += 2
			lkey := cp[dst : dst+kl]

			var vbuf [8]byte
			binary.BigEndian.PutUint64(vbuf[:], uint64(off0))

			l.pageInsert(p, 1, 1, ff, lkey, vbuf[:])
		}()
		l.Unlock2(p, cp)

		st = append(st, 0)
		copy(st[1:], st)
		st[0] = 0
	} else if reduce {
		l := len(st) - 1
		copy(st, st[1:])
		st = st[:l]
	}

	return st, nil
}
