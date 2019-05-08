package xrain

import (
	"log"
)

var checkTree func(t *Tree)

type (
	Tree struct {
		p PageLayout

		root int64
		mask int64

		meta *treemeta

		dd map[string]string
	}

	keylink int64
)

func NewTree(p PageLayout, root, page int64) *Tree {
	mask := page
	if mask&(mask-1) != 0 {
		panic(mask)
	}
	mask--
	t := &Tree{
		p:    p,
		root: root,
		mask: mask,
	}

	if checkTree != nil {
		t.dd = make(map[string]string)
		for k := t.Next(nil); k != nil; k = t.Next(k) {
			t.dd[string(k)] = string(t.Get(k))
		}
	}

	return t
}

func (t *Tree) Size() int {
	if t.meta == nil {
		return 0
	}
	return int(t.meta.n)
}

func (t *Tree) Put(k, v []byte) (old []byte, err error) {
	st, eq := t.seek(nil, k)

	//	log.Printf("root %x Put %x -> %x", t.root, k, v)

	last := st[len(st)-1]
	off := last.Off(t.mask)
	i := last.Index(t.mask)

	if eq {
		old = t.p.Value(off, i)

		off, err = t.p.Del(off, i)
		if err != nil {
			return
		}
	}

	l, r, err := t.p.Put(off, i, k, v)
	if err != nil {
		return
	}

	if checkTree != nil {
		t.dd[string(k)] = string(v)
	}

	if t.meta != nil && !eq {
		t.meta.n++
	}

	err = t.out(st, l, r)

	return
}

func (t *Tree) Del(k []byte) (old []byte, err error) {
	st, eq := t.seek(nil, k)

	//	log.Printf("root %x Del %x", t.root, k)

	if !eq {
		return
	}

	last := st[len(st)-1]
	off := last.Off(t.mask)
	i := last.Index(t.mask)

	old = t.p.Value(off, i)

	l, err := t.p.Del(off, i)
	if err != nil {
		return
	}

	if checkTree != nil {
		delete(t.dd, string(k))
	}

	if t.meta != nil {
		t.meta.n--
	}

	err = t.out(st, l, NilPage)
	return
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

	next := t.p.Key(off, i)

	//	log.Printf("root %x Nxt %x -> %x", t.root, k, next)

	return next
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

		i, eq = t.p.Search(off, k)
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
				i, eq = t.p.Search(off, k)
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
		//	log.Printf("out d  %v %x  lr %x %x", d+2, s, l, r)
		par := s[d] // parent page
		off := par.Off(mask)
		i := par.Index(mask)
		var rdel bool

		// rebalance if needed
		if r == NilPage && t.p.NeedRebalance(l) {
			i, l, r = t.p.Siblings(off, i, l)
			if i == -1 {
				panic(off)
			}
			if r != NilPage {
				l, r, err = t.p.Rebalance(l, r)
				if err != nil {
					return err
				}
				//	log.Printf("rebalanced %x r %x n %d", l, r, t.p.NKeys(l))
				rdel = true
			}
		}

		//	log.Printf("stage0 d %d off %3x i %d lr %3x %3x\n%v", d, off, i, l, r, dumpFile(t.p))

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

		//	log.Printf("stage1 d %d off %3x i %d lr %3x %3x\n%v", d, off, i, l, r, dumpFile(t.p))

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

		//	log.Printf("stage2 d %d par %3x %3x i %d lr %3x %3x", d, pl, pr, i, l, r)
		//	log.Printf("page now\n%v", dumpPage(t.p, off))

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

	if r == NilPage && !t.p.IsLeaf(l) && t.p.NKeys(l) == 1 {
		err = t.p.Free(l)
		if err != nil {
			return err
		}
		l = t.p.Int64(l, 0)

		if t.meta != nil {
			t.meta.depth--
		}
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

		if t.meta != nil {
			t.meta.depth++
		}
	}

	if r != NilPage {
		panic(r)
	}

	//	log.Printf("root   %4x <- %4x%v\n%v", l, t.root, callers(-1), dumpFile(t.p))
	t.root = l

	if checkTree != nil {
		checkTree(t)

		cnt := 0
		for k := t.Next(nil); k != nil; k = t.Next(k) {
			cnt++
			if v, ok := t.dd[string(k)]; !ok || v != string(t.Get(k)) {
				log.Fatalf("data mismatch (root %x): %x -> %x != %x (%v)", t.root, k, t.Get(k), []byte(v), ok)
			}
		}
		if cnt != len(t.dd) {
			log.Fatalf("data mismatch: expected %d keys, have %d", len(t.dd), cnt)
		}
	}

	return nil
}

func (l keylink) Off(mask int64) int64 {
	return int64(l) &^ mask
}

func (l keylink) Index(mask int64) int {
	return int(int64(l) & mask)
}
