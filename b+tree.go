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
		//	log.Printf("search %2x %q -> %x %v", off, k, i, eq)

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
	//	log.Printf("seek      %q -> %x %v", k, st, eq)
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
				if i == 0 {
					continue
				}
				i--
			} else {
				i++
				if i == t.p.NKeys(off) {
					continue
				}
			}

			up = false
			st[d] = keylink(off) | keylink(i)
			st = st[:d+1]
		} else {
			st = append(st, keylink(off))
			n := t.p.NKeys(off)

			if back && k == nil {
				i, eq = n, false
			} else {
				i, eq = t.search(off, k)
			}
			//	log.Printf("search %4x %q -> %d/%d %v", off, k, i, n, eq)

			if t.p.IsLeaf(off) {
				if back {
					i--
				} else {
					if eq {
						i++
					}
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
	//	log.Printf("step   %q -> %x", k, st)
	return st
}

func (t *Tree) out(s []keylink, l, r int64) (err error) {
	mask := t.mask
	d := len(s)
	for d -= 2; d >= 0; d-- {
		log.Printf("out d  %v %x  lr %x %x", d+2, s, l, r)
		par := s[d] // parent page
		off := par.Off(mask)
		i := par.Index(mask)
		var rdel bool
		if false {
			log.Printf("stage0 d %d off %3x i %d lr %3x %3x\n%v", d, off, i, l, r, dumpFile(t.p))
		}

		// rebalance if needed
		if r == NilPage && t.p.NeedRebalance(l) {
			i, l, r = t.p.Siblings(off, i)
			if r != NilPage {
				l, r, err = t.p.Rebalance(l, r)
				if err != nil {
					return err
				}
				log.Printf("rebalanced %x r %x n %d", l, r, t.p.NKeys(l))
				if r == NilPage {
					rdel = true
				}
			}
		}

		// delete old link
		off, err = t.p.Del(off, i)
		if err != nil {
			return err
		}

		if rdel {
			off, err = t.p.Del(off, i)
			if err != nil {
				return err
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

		log.Printf("stage1 d %d par %3x %3x i %d lr %3x %3x", d, pl, pr, i, l, r)

		rk := t.p.LastKey(r)
		// we didn't split parent page yet
		if pr == NilPage {
			pl, pr, err = t.p.PutInt64(pl, i+1, rk, r)
			if err != nil {
				return err
			}
			l, r = pl, pr
			continue
		}

		log.Printf("pl pr %x %x   lk %q rk %q", pl, pr, lk, rk)

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

	for r == NilPage && !t.p.IsLeaf(l) && t.p.NKeys(l) == 1 {
		err = t.p.Reclaim(l)
		if err != nil {
			return err
		}
		l = t.p.Int64(l, 0)
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
		r = NilPage
	}

	t.root = l
	log.Printf("root   %4x", t.root)

	return nil
}

func (t *Tree) search(off int64, k []byte) (int, bool) {
	ln := t.p.NKeys(off)
	i := sort.Search(ln, func(i int) bool {
		return t.p.KeyCmp(off, i, k) >= 0
	})
	return i, i < ln && t.p.KeyCmp(off, i, k) == 0
}

func (l keylink) Off(mask int64) int64 {
	return int64(l) &^ mask
}

func (l keylink) Index(mask int64) int {
	return int(int64(l) & mask)
}
