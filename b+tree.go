package xrain

import (
	"log"
	"sort"
)

type (
	Tree struct {
		p PageLayout

		root int64
		mask int64
	}

	Iterator struct {
		t  *Tree
		st []keylink
	}

	keylink int64
)

func NewTree(p PageLayout, root int64) *Tree {
	mask := p.PageSize()
	if mask&(mask-1) != 0 {
		panic(mask)
	}
	mask--
	return &Tree{
		p:    p,
		root: root,
		mask: mask,
	}
}

func (t *Tree) Put(k, v []byte) (err error) {
	st, eq := t.seek(nil, k)

	last := st[len(st)-1]
	off := last.Off(t.mask)
	i := last.Index(t.mask)

	if eq {
		off, err = t.p.Del(off, i)
		if err != nil {
			return err
		}
	}

	l, r, err := t.p.Put(off, i, k, v)
	if err != nil {
		return err
	}

	return t.out(st, l, r)
}

func (t *Tree) Del(k []byte) (err error) {
	st, eq := t.seek(nil, k)

	if !eq {
		return nil
	}

	last := st[len(st)-1]
	off := last.Off(t.mask)
	i := last.Index(t.mask)

	l, err := t.p.Del(off, i)
	if err != nil {
		return err
	}

	return t.out(st, l, NilPage)
}

func (t *Tree) Get(k []byte) (v []byte) {
	st, eq := t.seek(nil, k)

	if !eq {
		return nil
	}

	last := st[len(st)-1]
	off := last.Off(t.mask)
	i := last.Index(t.mask)

	return t.p.Value(off, i)
}

func (t *Tree) Next(k []byte) []byte {
	st := t.step(nil, k, false)
	if st == nil {
		return nil
	}

	last := st[len(st)-1]
	off := last.Off(t.mask)
	i := last.Index(t.mask)

	return t.p.Key(off, i)
}

func (t *Tree) Prev(k []byte) []byte {
	st := t.step(nil, k, true)
	if st == nil {
		return nil
	}

	last := st[len(st)-1]
	off := last.Off(t.mask)
	i := last.Index(t.mask)

	return t.p.Key(off, i)
}

func (t *Tree) seek(st []keylink, k []byte) (_ []keylink, eq bool) {
	off := t.root
	var i, d int
	for {
		st = append(st, keylink(off))

		i, eq = t.search(off, k)
		log.Printf("search %q -> %x %v", k, i, eq)

		if t.p.IsLeaf(off) {
			st[d] |= keylink(i)
			break
		}

		if t.p.NKeys(off) == i {
			i--
		}

		st[d] |= keylink(i)
		d++

		off = t.p.Int64(off, i)
	}
	log.Printf("seek   %q -> %x %v", k, st, eq)
	return st, eq
}

func (t *Tree) step(st []keylink, k []byte, back bool) (_ []keylink) {
	off := t.root
	mask := t.mask
	var i, d int
	var eq, up bool
	for {
		if up {
			if d == 0 {
				st = nil
				break
			}
			d--

			last := st[d]
			off = last.Off(mask)
			i = last.Index(mask)
			if back {
				i++
				if i == t.p.NKeys(off) {
					continue
				}
				st[d]++
			} else {
				if i == 0 {
					continue
				}
				i--
				st[d]--
			}

			up = false
			st = st[:d+1]
		} else {
			st = append(st, keylink(off))
			n := t.p.NKeys(off)

			if back && k == nil {
				i, eq = 0, false
			} else {
				i, eq = t.search(off, k)
			}
			log.Printf("search %4x %q -> %d/%d %v", off, k, i, n, eq)

			if t.p.IsLeaf(off) {
				if !back {
					i--
				} else if eq {
					i++
				}
				if i < 0 || i >= n {
					up = true
					continue
				}
				st[d] |= keylink(i)
				break
			}

			if n == i {
				i--
			}

			st[d] |= keylink(i)
		}

		d++
		off = t.p.Int64(off, i)
	}
	log.Printf("step   %q -> %x", k, st)
	return st
}

func (t *Tree) out(s []keylink, l, r int64) (err error) {
	mask := t.mask
	d := len(s)
	//	log.Printf("out d  %v", d)
	for d -= 2; d >= 0; d-- {
		par := s[d]
		off := par.Off(mask)
		i := par.Index(mask)
		choff := s[d+1].Off(mask)

		if r == NilPage && l == choff {
			// we only've updated one leaf page
			return nil
		}

		// delete old link
		off, err = t.p.Del(off, i)
		if err != nil {
			return err
		}

		// rebalance if needed
		if r == NilPage {
			if t.p.NeedRebalance(l) {
				i, l, r = t.p.Siblings(off, i)
				l, r, err = t.p.Rebalance(l, r)
				if err != nil {
					return err
				}
			} else {
				continue
			}
		}

		// put left new child
		lk := t.p.LastKey(l)
		pl, pr, err := t.p.PutInt64(off, i, lk, l)
		if err != nil {
			return err
		}

		// don't have right new child
		if r == NilPage {
			l, r = pl, pr
			continue
		}

		rk := t.p.LastKey(r)
		// we didn't split parent page yet
		if pr == NilPage {
			pl, pr, err = t.p.PutInt64(pl, i+1, rk, r)
			if err != nil {
				return err
			}
		}

		i++
		var p2 int64
		// at which page our index are?
		if m := t.p.NKeys(pl); i < m {
			pl, p2, err = t.p.PutInt64(pl, i, rk, r)
		} else {
			pr, p2, err = t.p.PutInt64(pr, i-m, rk, r)
		}
		if err != nil {
			return err
		}
		if p2 != NilPage {
			panic("double split")
		}

		l, r = pl, pr
	}

	if r != NilPage {
		off, err := t.p.AllocRoot()
		if err != nil {
			return err
		}

		lk := t.p.LastKey(l)
		rk := t.p.LastKey(r)

		off, _, err = t.p.PutInt64(off, 0, lk, l)
		if err != nil {
			return err
		}
		off, _, err = t.p.PutInt64(off, 1, rk, r)
		if err != nil {
			return err
		}

		l = off
	}

	t.root = l
	log.Printf("root   %4x", t.root)

	return nil
}

func (t *Tree) search(off int64, k []byte) (int, bool) {
	ln := t.p.NKeys(off)
	i := sort.Search(ln, func(i int) bool {
		return t.p.KeyCmp(off, i, k) <= 0
	})
	return i, i < ln && t.p.KeyCmp(off, i, k) == 0
}

func (l keylink) Off(mask int64) int64 {
	return int64(l) &^ mask
}

func (l keylink) Index(mask int64) int {
	return int(int64(l) & mask)
}
