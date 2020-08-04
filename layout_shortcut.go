package xrain

type (
	LayoutShortcut struct {
		Layout
		Root int64
		Mask int64
	}

	FlagKeyValue struct {
		F          int
		Key, Value []byte
	}
)

func NewLayoutShortcut(l Layout, root, mask int64) *LayoutShortcut {
	t := &LayoutShortcut{
		Layout: l,
		Root:   root,
		Mask:   mask,
	}

	return t
}

func (t *LayoutShortcut) Fill(prefill []FlagKeyValue) (err error) {
	for _, p := range prefill {
		err = t.Put(p.F, p.Key, p.Value, nil)
		if err != nil {
			return
		}
	}

	return nil
}

func (t *LayoutShortcut) Get(k []byte, st Stack) (v []byte, ff int) {
	st, eq := t.Seek(k, nil, st)
	if !eq {
		return nil, 0
	}

	if l, ok := t.Layout.(FlagsSupported); ok {
		ff = l.Flags(st)
	}

	v = t.Layout.Value(st, nil)

	return
}

func (t *LayoutShortcut) Put(ff int, k, v []byte, st Stack) (err error) {
	if t.Root == NilPage {
		t.Root, err = t.Alloc()
		if err != nil {
			return
		}
	}

	st, _ = t.Seek(k, nil, st)

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

func (t *LayoutShortcut) Set(ff int, k, v []byte, st Stack) (err error) {
	if t.Root == NilPage {
		t.Root, err = t.Alloc()
		if err != nil {
			return
		}
	}

	st, eq := t.Seek(k, nil, st)

	if tl.V("tree,set") != nil {
		tl.Printf("tree %3x  st %v  set %q %q ff %x  eq %v", t.Root, st, k, v, ff, eq)
	}

	if eq {
		st, err = t.Layout.Delete(st)
		if err != nil {
			return
		}
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

func (t *LayoutShortcut) Del(k []byte, st Stack) (err error) {
	st, eq := t.Seek(k, nil, st)
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

func (t *LayoutShortcut) Int64(k []byte, s Stack) int64 {
	s, eq := t.Seek(k, nil, s)
	if !eq {
		return 0
	}
	return t.Layout.Int64(s)
}

func (t *LayoutShortcut) SetInt64(k []byte, v int64, s Stack) (_ int64, err error) {
	s, eq := t.Seek(k, nil, s)
	if !eq {
		s, err = t.Insert(s, 0, k, []byte{0, 0, 0, 0, 0, 0, 0, 0})
		if err != nil {
			return
		}
	}

	return t.Layout.SetInt64(s, v)
}

func (t *LayoutShortcut) AddInt64(k []byte, v int64, s Stack) (_ int64, err error) {
	s, eq := t.Seek(k, nil, s)
	if !eq {
		s, err = t.Insert(s, 0, k, []byte{0, 0, 0, 0, 0, 0, 0, 0})
		if err != nil {
			return
		}
	}

	return t.Layout.AddInt64(s, v)
}

func (t *LayoutShortcut) Seek(k, v []byte, st Stack) (Stack, bool) {
	return t.Layout.Seek(st, t.Root, k, v)
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
