package xrain

type (
	LayoutShortcut struct {
		Layout
		Root int64
		Mask int64
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
	st, eq := t.Seek(nil, t.Root, k, nil)
	if !eq {
		return nil, 0
	}

	if l, ok := t.Layout.(FlagsSupported); ok {
		ff = l.Flags(st)
	}

	v = t.Layout.Value(st, nil)

	return
}

func (t *LayoutShortcut) Put(ff int, k, v []byte) (err error) {
	if t.Root == NilPage {
		t.Root, err = t.Alloc()
		if err != nil {
			return
		}
	}

	st, _ := t.Seek(nil, t.Root, k, nil)

	if tl.V("tree,put") != nil {
		tl.Printf("tree %3x  st %v  put %q %q ff %x", t.Root, st, k, v, ff)
	}

	st, err = t.Layout.Insert(st, ff, k, v)
	if err != nil {
		return
	}
	if tl.V("root").If(st[0].Off(t.Mask) != t.Root) != nil {
		tl.Printf("root %x <- %x", st[0].Off(t.Mask), t.Root)
	}

	t.Root = st[0].Off(t.Mask)

	return err
}

func (t *LayoutShortcut) Del(k []byte) (err error) {
	st, eq := t.Seek(nil, t.Root, k, nil)
	if !eq {
		return nil
	}

	if tl.V("tree,del") != nil {
		tl.Printf("tree %3x  st %v  del %q", t.Root, st, k)
	}

	st, err = t.Layout.Delete(st)

	if len(st) != 0 && st[0].Off(t.Mask) != t.Root && tl.V("root") != nil {
		tl.Printf("root %x <- %x", st[0].Off(t.Mask), t.Root)
	}

	t.Root = st[0].Off(t.Mask)

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

func (t *LayoutShortcut) Delete(st Stack) (_ Stack, err error) {
	st, err = t.Layout.Delete(st)
	if err != nil {
		return
	}

	t.Root = st[0].Off(t.Mask)

	return st, nil
}

func (t *LayoutShortcut) Insert(st Stack, ff int, k, v []byte) (_ Stack, err error) {
	st, err = t.Layout.Insert(st, ff, k, v)
	if err != nil {
		return
	}

	t.Root = st[0].Off(t.Mask)

	return st, nil
}
