package xrain

import (
	"encoding/binary"
)

type (
	Tree interface {
		Serializer

		Size() int

		Get(k []byte) ([]byte, int)

		Put(k, v []byte, F int) (old []byte, err error)
		Del(k []byte) (old []byte, err error)

		Seek(st Stack, k []byte) (_ Stack, eq bool)
		Step(st Stack, back bool) Stack
		Out(st Stack, l, r int64) error

		Root() int64
		SetRoot(int64)

		PageLayout() PageLayout
		SetPageLayout(PageLayout)

		SetPageSize(page int64)
		Copy() Tree
	}

	FileTree struct {
		p PageLayout

		root int64
		mask int64

		size  int
		depth int
	}

	Stack []keylink

	keylink int64
)

func NewTree(p PageLayout, root, page int64) *FileTree {
	mask := page
	if mask&(mask-1) != 0 {
		panic(mask)
	}
	mask--
	t := &FileTree{
		p:    p,
		root: root,
		mask: mask,
	}

	return t
}

func (t *FileTree) Serialize(p []byte) int {
	s := t.p.Serialize(p)

	if p == nil {
		return s + 8
	}

	binary.BigEndian.PutUint64(p[s:], uint64(t.root)|uint64(t.depth))
	s += 8
	binary.BigEndian.PutUint64(p[s:], uint64(t.size))
	s += 8

	return s
}

func (t *FileTree) Deserialize(p []byte) (int, error) {
	s, err := t.p.Deserialize(p)
	if err != nil {
		return s, err
	}

	root := int64(binary.BigEndian.Uint64(p[s:]))
	s += 8
	size := int64(binary.BigEndian.Uint64(p[s:]))
	s += 8

	t.root = root &^ 0xff
	t.size = int(size)
	t.depth = int(root & 0xff)

	return s, nil
}

func (t *FileTree) Size() int {
	return t.size
}

func (t *FileTree) Root() int64 { return t.root }

func (t *FileTree) SetRoot(r int64) { t.root = r }

func (t *FileTree) PageLayout() PageLayout { return t.p }

func (t *FileTree) SetPageLayout(pl PageLayout) { t.p = pl }

func (t *FileTree) SetPageSize(page int64) { t.mask = page - 1; t.p.SetPageSize(page) }

func (t *FileTree) Copy() Tree { cp := *t; return &cp }

func (t *FileTree) Put(k, v []byte, F int) (old []byte, err error) {
	st, eq := t.Seek(nil, k)

	//	log.Printf("root %x Put %x -> %x", t.root, k, v)

	off, i := st.OffIndex(t.mask)

	if eq {
		old = t.p.Value(off, i, nil)

		off, err = t.p.Delete(off, i)
		if err != nil {
			return
		}
	}

	l, r, err := t.p.Insert(off, i, F, k, v)
	if err != nil {
		return
	}

	if !eq {
		t.size++
	}

	err = t.Out(st, l, r)

	return
}

func (t *FileTree) Del(k []byte) (old []byte, err error) {
	st, eq := t.Seek(nil, k)

	//	log.Printf("root %x Del %x", t.root, k)

	if !eq {
		return
	}

	off, i := st.OffIndex(t.mask)

	old = t.p.Value(off, i, nil)

	l, err := t.p.Delete(off, i)
	if err != nil {
		return
	}

	t.size--

	err = t.Out(st, l, NilPage)
	return
}

func (t *FileTree) Get(k []byte) (v []byte, F int) {
	st, eq := t.Seek(nil, k)

	if !eq {
		return nil, 0
	}

	off, i := st.OffIndex(t.mask)

	_, F = t.p.Key(off, i, nil)

	return t.p.Value(off, i, nil), F
}

func (t *FileTree) Seek(st Stack, k []byte) (_ Stack, eq bool) {
	st = st[:0]

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

func (t *FileTree) Out(s Stack, l, r int64) (err error) {
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
			//	if r != NilPage {
			l, r, err = t.p.Rebalance(l, r)
			if err != nil {
				return err
			}
			//	log.Printf("rebalanced %x r %x n %d", l, r, t.p.NKeys(l))
			rdel = true
			//	}
		}

		//	log.Printf("stage0 d %d off %3x i %d lr %3x %3x\n%v", d, off, i, l, r, dumpFile(t.p))

		pl, pr, err := t.p.UpdatePageLink(off, i, l)
		if err != nil {
			return err
		}

		if r == NilPage {
			l, r = pl, pr
			continue
		}

		if pr == NilPage {
			if rdel {
				// update key
				pl, pr, err = t.p.UpdatePageLink(pl, i+1, r)
			} else {
				// insert
				pl, pr, err = t.p.InsertPageLink(pl, i+1, r)
			}
			if err != nil {
				return err
			}

			l, r = pl, pr
			continue
		}

		i++
		var p2 int64
		if m := t.p.NKeys(pl); i < m {
			if rdel {
				pl, p2, err = t.p.UpdatePageLink(pl, i, r)
			} else {
				pl, p2, err = t.p.InsertPageLink(pl, i, r)
			}
		} else {
			if rdel {
				pr, p2, err = t.p.UpdatePageLink(pr, i-m, r)
			} else {
				pr, p2, err = t.p.InsertPageLink(pr, i-m, r)
			}
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
		err = t.p.Free(l, false)
		if err != nil {
			return err
		}
		l = t.p.Int64(l, 0)

		t.depth--
	}

	if r != NilPage {
		off, err := t.p.Alloc(false)
		if err != nil {
			return err
		}

		off, _, err = t.p.InsertPageLink(off, 0, l)
		if err != nil {
			return err
		}
		off, _, err = t.p.InsertPageLink(off, 1, r)
		if err != nil {
			return err
		}

		l = off
		r = NilPage

		t.depth++
	}

	//	log.Printf("root   %4x <- %4x%v\n%v", l, t.root, callers(-1), dumpFile(t.p))
	t.root = l

	return nil
}

func (t *FileTree) Step(st Stack, back bool) Stack {
	var off int64
	var i int

	if len(st) == 0 {
		p := t.root
		for {
			n := t.p.NKeys(p)
			if n == 0 {
				return nil
			}

			if back {
				i = n - 1
			} else {
				i = 0
			}
			st = append(st, keylink(p)|keylink(i))

			if t.p.IsLeaf(p) {
				return st
			}

			p = t.p.Int64(p, i)
		}
	}

	l := len(st) - 1

	last := st[l]
	off = last.Off(t.mask)
	i = last.Index(t.mask)

	if back {
		if i > 0 {
			i--
			st[l] = keylink(off) | keylink(i)
			return st
		}
	} else {
		n := t.p.NKeys(off)
		if i+1 < n {
			i++
			st[l] = keylink(off) | keylink(i)
			return st
		}
	}

	if l == 0 {
		return nil
	}

	par := t.Step(st[:l], back)
	if par == nil {
		return nil
	}

	poff, pi := par.OffIndex(t.mask)

	off = t.p.Int64(poff, pi)

	if back {
		i = t.p.NKeys(off) - 1
	} else {
		i = 0
	}

	st[l] = keylink(off) | keylink(i)

	return st
}

func (l keylink) Off(mask int64) int64 {
	return int64(l) &^ mask
}

func (l keylink) Index(mask int64) int {
	return int(int64(l) & mask)
}

func (st Stack) OffIndex(m int64) (int64, int) {
	last := st[len(st)-1]
	return last.Off(m), last.Index(m)
}
