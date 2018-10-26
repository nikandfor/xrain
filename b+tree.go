package xrain

import (
	"sort"
)

const (
	fBranch = 1 << iota
)

type (
	Tree struct {
		p PageLayout

		root int64
		mask int64

		// find
		s  []keylink
		eq bool

		// out
		l, r int64 // modified pages

		sbuf [32]keylink
	}

	keylink int64
)

func NewTree(p PageLayout, root int64) *Tree {
	sz := p.PageSize()
	mask := sz - 1
	t := &Tree{
		p:    p,
		root: root,
		mask: mask,
	}
	t.s = t.sbuf[:0]
	return t
}

func (t *Tree) find(k []byte) (err error) {
	t.s = t.s[:0]
	t.l = NilPage
	t.r = NilPage
	off := t.root
	var i, d int
	var eq bool
	for {
		t.s = append(t.s, keylink(off))

		i, eq, err = t.search(off, k)
		if err != nil {
			return err
		}

		if s, err := t.p.Size(off); err != nil {
			return err
		} else if s == i {
			i--
		}

		t.s[d] |= keylink(i)
		d++

		if c, err := t.p.IsLeaf(off); err != nil {
			return err
		} else if c {
			break
		}

		off, err = t.p.Int64(off, i)
		if err != nil {
			return
		}
	}
	t.eq = eq
	return nil
}

func (t *Tree) out() (err error) {
	mask := t.mask
	d := len(t.s)
	s := t.s
	l, r := t.l, t.r
	for d -= 2; d >= 0; d-- {
		par := s[d]
		off := par.Off(mask)
		i := par.Index(mask)
		choff := s[d+1].Off(mask)

		if r == NilPage && l == choff {
			return nil
		}

		// delete old link
		off, err = t.p.Del(off, i)
		if err != nil {
			return err
		}

		// rebalance if needed
		if r == NilPage {
			if c, err := t.p.NeedRebalance(r); err != nil {
				return err
			} else if c {
				i, l, r, err = t.p.Siblings(off, i)
				if err != nil {
					return err
				}
				l, r, err = t.p.Rebalance(l, r)
				if err != nil {
					return err
				}
			}
			return nil
		}

		// put left new child
		lk, err := t.p.LastKey(l)
		if err != nil {
			return err
		}
		pl, pr, err := t.p.PutInt64(off, i, lk, int64(l))
		if err != nil {
			return err
		}

		// don't have right new child
		if r == NilPage {
			l, r = pl, pr
			continue
		}

		rk, err := t.p.LastKey(r)
		if err != nil {
			return err
		}
		// we didn't split parent page yet
		if pr == NilPage {
			pl, pr, err = t.p.PutInt64(pl, i+1, rk, int64(r))
			if err != nil {
				return err
			}
		}

		i++
		var p2 int64
		// at which page our index are?
		if m, err := t.p.Size(pl); err != nil {
			return err
		} else if i < m {
			pl, p2, err = t.p.PutInt64(pl, i, rk, int64(r))
		} else {
			pr, p2, err = t.p.PutInt64(pr, i-m, rk, int64(r))
		}
		if err != nil {
			return err
		}
		if p2 != NilPage {
			panic("double split")
		}

		l, r = pl, pr
	}
	return nil
}

func (t *Tree) Put(k, v []byte) (err error) {
	if err = t.find(k); err != nil {
		return err
	}

	last := t.s[len(t.s)-1]
	off := last.Off(t.mask)
	i := last.Index(t.mask)
	if t.eq {
		off, err = t.p.Del(off, i)
		if err != nil {
			return err
		}
	}

	t.l, t.r, err = t.p.Put(off, i, k, v)
	if err != nil {
		return err
	}

	return t.out()
}

func (t *Tree) Del(k []byte) (err error) {
	if err = t.find(k); err != nil {
		return err
	}

	if !t.eq {
		return nil
	}

	last := t.s[len(t.s)-1]
	off := last.Off(t.mask)
	i := last.Index(t.mask)

	t.l, err = t.p.Del(off, i)
	if err != nil {
		return err
	}

	return t.out()
}

func (t *Tree) Get(k []byte) (v []byte, err error) {
	if err = t.find(k); err != nil {
		return nil, err
	}

	last := t.s[len(t.s)-1]
	off := last.Off(t.mask)
	i := last.Index(t.mask)

	return t.p.Value(off, i)
}

func (t *Tree) search(off int64, k []byte) (int, bool, error) {
	ln, err := t.p.Size(off)
	if err != nil {
		return 0, false, err
	}
	i := sort.Search(ln, func(i int) bool {
		var c int
		c, err = t.p.KeyCmp(off, i, k)
		return c <= 0
	})
	if err != nil {
		return 0, false, err
	}
	var eq bool
	if i < ln {
		if c, err := t.p.KeyCmp(off, i, k); err != nil {
			return 0, false, err
		} else {
			eq = c == 0
		}
	}
	return i, eq, nil
}

func (l keylink) Off(mask int64) int64 {
	return int64(l) &^ mask
}

func (l keylink) Index(mask int64) int {
	return int(int64(l) & mask)
}
