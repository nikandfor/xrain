package xrain

type (
	LayoutShortcut struct {
		Layout
		Root int64
		Mask int64
		st   Stack
	}
)

func NewLayoutShortcut(l Layout, root, mask int64) *LayoutShortcut {
	return &LayoutShortcut{
		Layout: l,
		Root:   root,
		Mask:   mask,
	}
}

func (t *LayoutShortcut) Get(k []byte) (v []byte, ff int) {
	var eq bool
	t.st, eq = t.Seek(t.st[:0], t.Root, k)
	if !eq {
		return nil, 0
	}

	if l, ok := t.Layout.(FlagsSupported); ok {
		ff = l.Flags(t.st)
	}

	v = t.Value(t.st, nil)

	return
}

func (t *LayoutShortcut) Put(ff int, k, v []byte) (err error) {
	if t.Root == NilPage {
		t.Root, err = t.Alloc()
		if err != nil {
			return
		}
	}

	t.st, _ = t.Seek(t.st[:0], t.Root, k)

	tl.V("stack").Printf("put to %v  %2x %2x", t.st, k, v)

	t.st, err = t.Insert(t.st, ff, k, v)

	if tl.V("root") != nil && t.st[0].Off(t.Mask) != t.Root {
		tl.Printf("root %x <- %x", t.st[0].Off(t.Mask), t.Root)
	}

	t.Root = t.st[0].Off(t.Mask)

	tl.V("dump").Printf("dump\n%v", t.Layout.(pageDumper).dumpPage(t.Root))

	return err
}

func (t *LayoutShortcut) Del(k []byte) (err error) {
	var eq bool
	t.st, eq = t.Seek(t.st[:0], t.Root, k)
	if !eq {
		return nil
	}

	t.st, err = t.Delete(t.st)

	if tl.V("root") != nil && t.st[0].Off(t.Mask) != t.Root {
		tl.Printf("root %x <- %x", t.st[0].Off(t.Mask), t.Root)
	}

	t.Root = t.st[0].Off(t.Mask)

	return err
}

func (t *LayoutShortcut) First(st Stack) Stack {
	return t.Step(st[:0], t.Root, false)
}

func (t *LayoutShortcut) Last(st Stack) Stack {
	return t.Step(st[:0], t.Root, true)
}

func (t *LayoutShortcut) Next(st Stack) Stack {
	return t.Step(st, t.Root, false)
}

func (t *LayoutShortcut) Prev(st Stack) Stack {
	return t.Step(st, t.Root, true)
}
