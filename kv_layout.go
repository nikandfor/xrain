package xrain

import (
	"bytes"
	"encoding/binary"
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

func (l *BaseLayout2) realloc(off, ver int64, nold, n int) (noff int64, err error) {
	noff, err = l.Alloc(n)
	if err != nil {
		return
	}

	min := n
	if nold < min {
		min = nold
	}

	l.Copy(noff, off, int64(min)*l.Page)

	err = l.Free(nold, off, ver)

	return
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
		return len(p)
	}
	i--
	st := kvIndexStart + 2*i
	return int(p[st])<<8 | int(p[st+1])
}

func (l *KVLayout2) pagefree(p []byte, n int) int64 {
	if n == 0 {
		return int64(len(p)) - kvIndexStart
	}
	fst := kvIndexStart + 2*int64(n)
	dst := int64(p[fst-2])<<8 | int64(p[fst-1])
	return dst - fst
}

func (l *KVLayout2) pagedatasize(p []byte, i, n int) int64 {
	if i == n {
		return 0
	}
	nst := kvIndexStart + 2*n
	dst := int64(p[nst-2])<<8 | int64(p[nst-1])
	dend := int64(len(p))
	if i != 0 {
		ist := kvIndexStart + 2*i
		dend = int64(p[ist-2])<<8 | int64(p[ist-1])
	}
	return (dend - dst) + 2*int64(n-i)
}

func (l *KVLayout2) expectedsize(p []byte, i int, k, v []byte) int64 {
	vallink := 0 // in case we moved value to overflow page
	// link + flags + keylen + key + value + vallink
	return 2 + 1 + 1 + int64(len(k)) + int64(len(v)+vallink)
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

	pages := 1

	minpage := func(ds int64) int64 {
		p := l.Page

		for p < kvIndexStart+ds {
			p *= 2
		}

		return p
	}

	var off0, off1, off2 int64 = off, NilPage, NilPage
	var ver int64
	var alloc bool
	var newpages int = pages

again:
	p0 := l.Access(off0, l.Page*int64(newpages))
	func() {
		p := p0[:l.Page*int64(pages)]

		if alloc {
			l.setver(p, l.Ver)
			l.setoverflow(p, newpages-1)
			ver = l.Ver
			alloc = false
		} else {
			ver = l.pagever(p)
			pages = 1 + l.overflow(p)
			alloc = ver != l.Ver
		}

		n := l.nkeys(p)

		exp := l.expectedsize(p, i, k, v)

		if l.pagefree(p, n) >= exp { // fit into the same page
			if alloc {
				newpages = pages
			} else {
				l.pageInsert(p, i, n, ff, k, v)
			}
			return
		}

		lsize := l.pagedatasize(p, 0, i)
		rsize := l.pagedatasize(p, i, n)

		lpage := minpage(lsize)
		rpage := minpage(rsize)

		mpage := int64(0)

		if lsize+exp <= lpage {
			//
		} else if rsize+exp <= rpage {
			//
		} else {
			mpage = minpage(exp)
		}

		if alloc {
			newpages = int((lpage + mpage + rpage) / l.Page)
			return
		}

		l.pageSplit(p, p0[lpage+mpage:], i, n)

		if lsize+exp <= lpage {
			l.pageInsert(p, i, n, ff, k, v)
			off1 = off0 + lpage

			off = off1
		} else if mpage != 0 {
			l.pageInsert(p0[lpage:lpage+mpage], 0, 0, ff, k, v)
			off1 = off0 + lpage
			off2 = off1 + mpage

			off = off1
			i = 0
		} else {
			l.pageInsert(p0[lpage:], 0, n-i, ff, k, v)
			off1 = off0 + lpage

			off = off1
			i = 0
		}
	}()
	l.Unlock(p0)

	if alloc {
		off0, err = l.realloc(off0, ver, pages, newpages)
		if err != nil {
			return
		}

		goto again
	}

	st[len(st)-1] = NewKeylink(off, i)

	for d := len(st) - 2; d >= 0; d-- {
		poff, i := st[d].OffIndex(l.Mask)

		newpages = 1
		alloc = false
		coff := off0

		par, cp := l.Access2(poff, l.Page*int64(newpages), coff, l.Page)
		func() {
			p := par[:l.Page]

			if alloc {
				l.setver(p, l.Ver)
				ver = l.Ver
				alloc = false
			} else {
				ver = l.pagever(p)
				alloc = ver != l.Ver
			}

			dst := l.dataoff(cp, l.nkeys(cp)-1)
			ff := 0
			kl := int(cp[dst+1])
			dst += 2
			lkey := cp[dst : dst+kl]

			var vbuf [8]byte
			binary.BigEndian.PutUint64(vbuf[:], uint64(coff))

			exp := l.expectedsize(p, i, lkey, vbuf[:])

			n := l.nkeys(p)

			if l.pagefree(p, n) >= exp { // fit into the same page
				if !alloc {
					l.pageDelete(p, i, n)
					l.pageInsert(p, i, n-1, ff, k, v)
				}
				return
			}

			if newpages == 1 {
				newpages = 2
				alloc = true
			}

			if alloc {
				return
			}

			itop := i
			if coff == off0 {
				itop--
			}

			l.pageSplit(p, par[l.Page:], i, n)

			if l.pagefree(p, i-1) >= exp {
				l.pageInsert(p, itop, itop, ff, lkey, vbuf[:])

				st[d] = NewKeylink(poff, itop)

				i = itop + 1
			} else {
				l.setnkeys(p, itop)
				l.pageInsert(par[l.Page:], 0, n-i, ff, lkey, vbuf[:])

				st[d] = NewKeylink(poff+l.Page, 0)

				poff += l.Page
				i = 1
			}
		}()
		l.Unlock2(par, cp)

		switch {
		case coff == off0:
			if off1 != NilPage {
				coff = off1
				goto again
			}
		case coff == off1:
			if off2 != NilPage {
				coff = off2
				goto again
			}
		}

		off0 = poff
		if newpages != 1 {
			off1 = poff + l.Page
		}
		off2 = NilPage
	}

	if off1 != NilPage {
		off, err = l.Alloc(1)
		if err != nil {
			return
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
	}

	return st, nil
}

func (l *KVLayout2) pageInsert(p []byte, i, n, ff int, k, v []byte) {}

func (l *KVLayout2) pageSplit(p, r []byte, m, n int) {}

func (l *KVLayout2) pageDelete(p []byte, i, n int) {}

func (l *KVLayout2) delete(off int64, i int) {}

func (l *KVLayout2) Delete(st Stack) (_ Stack, err error) {
	return
}

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
		st = append(st, Keylink(off))

		search(off, k)
		//	log.Printf("search %2x %q -> %x %v", off, k, i, eq)

		if isleaf {
			st[d] |= Keylink(i)
			break
		}

		if i == n {
			i--
		}

		st[d] |= Keylink(i)
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

			st = append(st, Keylink(off)|Keylink(i))

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
			st[last] = NewKeylink(off, i)
			return st
		}
	} else {
		n := l.nKeys(off)
		i++
		if i < n {
			st[last] = NewKeylink(off, i)
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

	st[last] = NewKeylink(off, i)

	return st
}
