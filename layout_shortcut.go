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

	v = t.Layout.Value(t.st, nil)

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

	t.st, err = t.Layout.Insert(t.st, ff, k, v)
	if err != nil {
		return
	}

	tl.V("tree,put").Printf("put %x %q %q to %3x : %v", ff, k, v, t.Root, t.st)
	tl.V("root").If(t.st[0].Off(t.Mask) != t.Root).Printf("root %x <- %x", t.st[0].Off(t.Mask), t.Root)

	t.Root = t.st[0].Off(t.Mask)

	return err
}

func (t *LayoutShortcut) Del(k []byte) (err error) {
	var eq bool
	t.st, eq = t.Seek(t.st[:0], t.Root, k)
	if !eq {
		return nil
	}

	t.st, err = t.Layout.Delete(t.st)

	tl.V("tree,del").Printf("del %v by %3x %q", t.st, t.Root, k)

	tl.V("root").If(t.st[0].Off(t.Mask) != t.Root).Printf("root %x <- %x", t.st[0].Off(t.Mask), t.Root)

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
