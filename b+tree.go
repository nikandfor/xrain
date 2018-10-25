package xrain

import (
	"errors"
	"log"
	"sort"
)

type (
	tree struct {
		a Allocator

		root     int64
		pagemask int64

		buf [8]byte
		err error
	}

	keylink int64 // 1 byte is an overflow indicator, 51 bytes is for page offset, 12 bytes is for index at page (for 4kiB pages)
)

func NewBPTree(root int64, a Allocator) (*tree, error) {
	mask := a.Page() - 1
	t := &tree{
		a:        a,
		root:     root,
		pagemask: mask,
	}

	return t, nil
}

func (t *tree) Put(k, v []byte) {
	if t.err != nil {
		return
	}
	var s [32]keylink

	mask := t.pagemask
	kl := keylink(t.root)

	var d, i int
	var p []byte
	var err error
	var eq bool
	for {
		s[d] = kl

		p, err = t.a.Read(kl.Off(mask))
		if err != nil {
			t.err = err
			return
		}
		if p == nil {
			//	break
			t.err = errors.New("no page")
			return
		}

		i, eq = t.search(p, k)

		log.Printf("put s[%d] = %#x; i: %d  k '%s' eq %v", d, kl, i, k, eq)

		if !t.pageflag(p, fBranch) {
			break
		}

		if i == t.pagesize(p) {
			i--
		}
		s[d] |= keylink(i)
		d++

		kl = keylink(t.pagelink(p, i))
	}
	// we at leaf
	off := s[d].Off(mask)

	log.Printf("put d=%d off=%#4x i=%v eq=%v", d, off, i, eq)

	if eq {
		off, p, _, err = t.pagedel(off, p, i)
		if err != nil {
			t.err = err
			return
		}
	}

	loff, roff, l, r, err := t.pageput(off, p, i, k, v)
	if err != nil {
		t.err = err
		return
	}
	log.Printf("put %#x + %#x", loff, roff)

	for d--; d >= 0; d-- {
		reb := t.pageneedrebalance(l)
		_ = reb

		par := s[d]
		i = par.Index(mask)
		off, p, err = t.a.Write(par.Off(mask), nil)
		if err != nil {
			t.err = err
			return
		}
		log.Printf("d: %d  off %#x (%#x) i %d   l %#x r %#x  reb %v", d, off, s[d], i, loff, roff, reb)

		t.pagedel(off, p, i)

		lk := t.pagelastkey(l)
		var rpoff int64
		var p2 []byte
		off, rpoff, p, p2, err = t.pageputlink(off, p, i, lk, loff)
		if err != nil {
			t.err = err
			return
		}

		log.Printf("replaced l link: %#x %#x", off, rpoff)

		if r == nil {
			r = p2
			roff = rpoff
			continue
		}

		i++
		rk := t.pagelastkey(r)
		log.Printf("insert r link t: %d of %d", i, t.pagesize(p))
		if m := t.pagesize(p); i <= m {
			loff, roff, l, r, err = t.pageputlink(off, p, i, rk, roff)
		} else {
			loff = off
			l = p
			var roff2 int64
			var r2 []byte
			roff, roff2, r, r2, err = t.pageputlink(rpoff, p2, i-m, rk, roff)
			if r2 != nil {
				panic(roff2)
			}
		}
		if err != nil {
			t.err = err
			return
		}

		log.Printf("inserted r link: %#x %#x", loff, roff)
	}

	log.Printf("put root  l %#x r %#x", loff, roff)

	if r != nil {
		off, p, err = t.a.Alloc()
		if err != nil {
			t.err = err
			return
		}

		t.pagesetflag(p, fBranch)

		lk := t.pagelastkey(l)
		rk := t.pagelastkey(r)

		t.pageinsertlink(p, 0, lk, loff)
		t.pageinsertlink(p, 1, rk, roff)

		loff = off
	}

	t.root = loff
}

func (t *tree) Del(k []byte) {
	if t.err != nil {
		return
	}
	var s [32]keylink

	mask := t.pagemask
	kl := keylink(t.root)

	var d, i int
	var p []byte
	var err error
	var eq, reb bool
	var off int64
	for {
		s[d] = kl
		off = kl.Off(mask)

		p, err = t.a.Read(off)
		if err != nil {
			t.err = err
			return
		}
		if p == nil {
			//	break
			t.err = errors.New("no page")
			return
		}

		i, eq = t.search(p, k)
		s[d] |= keylink(i)
		d++

		log.Printf("del s[%d] = %#x; i: %d  k '%s' eq %v", d-1, kl, i, k, eq)

		if !t.pageflag(p, fBranch) {
			break
		}

		kl = keylink(t.pagelink(p, i))
	}
	// we at leaf

	if !eq {
		return
	}

	d--
	off, p, err = t.a.Write(off, p)
	if err != nil {
		t.err = err
		return
	}
	off, p, reb, err = t.pagedel(off, p, i)
	if err != nil {
		t.err = err
		return
	}
	eq = false

	link := off
	chk := t.pagelastkey(p)

	for d--; d >= 0; d-- {
		off := s[d].Off(mask)
		off, p, err = t.a.Write(off, nil)
		if err != nil {
			t.err = err
			return
		}
		i = s[d].Index(mask)

		log.Printf("del d: %d  off %#4x (%#4x) i %d  reb %v chk %s %#4x", d, off, s[d], i, reb, chk, link)

		if !reb {
			off, p, _, err = t.pagedel(off, p, i)
			if err != nil {
				t.err = err
				return
			}

			off, _, p, _, err = t.pageputlink(off, p, i, chk, link)

			link = off
			chk = t.pagelastkey(p)
			continue
		}

		li, ri, loff, roff := t.pagesiblings(off, p, i)
		loff, roff, l, r, err := t.pagerebalance(loff, roff, nil, nil)
		if err != nil {
			t.err = err
			return
		}

		// change right
		off, p, _, err = t.pagedel(off, p, ri)
		if err != nil {
			t.err = err
			return
		}
		if r != nil {
			k := t.pagelastkey(r)
			off, _, p, _, err = t.pageputlink(off, p, ri, k, roff)
			if err != nil {
				t.err = err
				return
			}
		}

		// change left
		off, p, _, err = t.pagedel(off, p, li)
		if err != nil {
			t.err = err
			return
		}
		k := t.pagelastkey(l)
		off, _, p, _, err = t.pageputlink(off, p, li, k, loff)
		if err != nil {
			t.err = err
			return
		}

		reb = t.pageneedrebalance(p)
		link = off
		chk = t.pagelastkey(p)
	}

	if t.pagesize(p) == 1 && t.pageflag(p, fBranch) {
		link := t.pagelink(p, 0)
		t.root = link
	}
}

func (t *tree) Get(k []byte) []byte {
	if t.err != nil {
		return nil
	}

	off := t.root

	var p []byte
	var err error
	var eq bool
	var i int
	for {
		p, err = t.a.Read(off)
		if err != nil {
			t.err = err
			return nil
		}
		if p == nil {
			//	break
			t.err = errors.New("no page")
			return nil
		}

		i, eq = t.search(p, k)

		//	log.Printf("off %#x i %d eq %v", off, i, eq)

		if !t.pageflag(p, fBranch) {
			break
		}

		off = t.pagelink(p, i)
		//	log.Printf("link %#x", off)
	}

	if !eq {
		return nil
	}

	return t.pagevalue(p, i)
}

func (t *tree) Next(k []byte) []byte {
	if t.err != nil {
		return nil
	}
	var s [32]keylink

	mask := t.pagemask
	kl := keylink(t.root)

	var d, i int
	var p []byte
	var err error
	var eq, back bool
	for {
		s[d] = kl

		p, err = t.a.Read(kl.Off(mask))
		if err != nil {
			t.err = err
			return nil
		}
		if p == nil {
			//	break
			t.err = errors.New("no page")
			return nil
		}

		i, eq = t.search(p, k)
		log.Printf("nxt s[%d] = %#x; i: %d  k '%s' eq %v back %v", d, kl, i, k, eq, back)
		if back {
			if i == 0 {
				if d == 0 {
					return nil
				}
				d--
				kl = s[d]
				log.Printf("nxt back")
				continue
			}
			back = false
			i--
		}

		if !t.pageflag(p, fBranch) {
			if i == 0 {
				if d == 0 {
					return nil
				}
				d--
				log.Printf("nxt back - here")
				kl = s[d]
				back = true
				continue
			}
			i--
			break
		}

		if i == t.pagesize(p) {
			i--
		}
		s[d] |= keylink(i)
		d++

		kl = keylink(t.pagelink(p, i))
	}
	// we at leaf

	return t.pagekey(p, i)
}

func (t *tree) Prev(k []byte) []byte {
	if t.err != nil {
		return nil
	}
	var s [32]keylink

	mask := t.pagemask
	kl := keylink(t.root)

	var d, i int
	var p []byte
	var err error
	var eq, back bool
	for {
		s[d] = kl

		p, err = t.a.Read(kl.Off(mask))
		if err != nil {
			t.err = err
			return nil
		}
		if p == nil {
			//	break
			t.err = errors.New("no page")
			return nil
		}

		if k == nil {
			i, eq = 0, false
		} else {
			i, eq = t.search(p, k)
		}
		log.Printf("prv s[%d] = %#x; i: %d  k '%s' eq %v back %v", d, kl, i, k, eq, back)
		if back {
			i++
			if i == t.pagesize(p) {
				if d == 0 {
					return nil
				}
				d--
				kl = s[d]
				log.Printf("prv back")
				continue
			}
			back = false
		}

		if !t.pageflag(p, fBranch) {
			if eq {
				i++
				if i == t.pagesize(p) {
					if d == 0 {
						return nil
					}
					d--
					log.Printf("prv back - here")
					kl = s[d]
					back = true
					continue
				}
			}
			break
		}

		if i == t.pagesize(p) {
			i--
		}
		s[d] |= keylink(i)
		d++

		kl = keylink(t.pagelink(p, i))
	}
	// we at leaf

	if i == t.pagesize(p) {
		return nil
	}

	return t.pagekey(p, i)
}

func (t *tree) search(p []byte, k []byte) (int, bool) {
	ln := t.pagesize(p)
	i := sort.Search(ln, func(i int) bool {
		return t.pagekeycmp(p, i, k) <= 0
	})
	return i, i < ln && t.pagekeycmp(p, i, k) == 0
}

func (l keylink) Off(mask int64) int64 {
	return int64(l) &^ mask
}

func (l keylink) Index(mask int64) int {
	return int(int64(l) & mask)
}
