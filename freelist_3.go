package xrain

import (
	"encoding/binary"
)

type (
	Freelist3 struct {
		l SubpageLayout
		//	t LayoutShortcut

		*Meta

		meta       SubpageLayout
		next, flen int64

		buf   []byte
		stbuf Stack

		nextkey []byte
		datakey []byte

		k, v int
	}
)

var _ Freelist = &Freelist3{}

func NewFreelist3(m *Meta, next int64) *Freelist3 {
	l := &Freelist3{
		l:       *NewSubpageLayout(nil),
		Meta:    m,
		next:    next,
		buf:     make([]byte, 16),
		stbuf:   Stack{0},
		nextkey: []byte("freelist3.next"),
		datakey: []byte("freelist3.data"),
		k:       6,
		v:       6,
	}

	l.init()

	return l
}

func (l *Freelist3) SetMeta(m *Meta) {
	l.Meta = m

	l.init()
}

func (l *Freelist3) init() {
	if l.Meta == nil {
		return
	}

	if l.Meta.Meta.Layout != nil {
		var eq bool
		l.stbuf, eq = l.Meta.Meta.Seek(l.nextkey, nil, l.stbuf)
		if eq {
			l.next = l.Meta.Meta.Layout.Int64(l.stbuf)
		}

		l.stbuf, eq = l.Meta.Meta.Seek(l.datakey, nil, l.stbuf)
		data := l.Meta.Meta.Value(l.stbuf, nil)
		if eq {
			l.l.SetBytes(data)
		}
	}

	//	l.t = NewLayoutShortcut(&l.l, 0, l.Mask)
	l.flen = l.Back.Size()
}

func (l *Freelist3) flush() (err error) {
	if l.Meta == nil || l.Meta.Meta.Layout == nil {
		return
	}

	_, l.stbuf, err = l.Meta.Meta.SetInt64(l.nextkey, l.next, l.stbuf[:0])
	if err != nil {
		return
	}

	l.stbuf, err = l.Meta.Meta.Set(0, l.datakey, l.l.Bytes(), l.stbuf[:0])
	if err != nil {
		return
	}

	return
}

func (l *Freelist3) Free(off, ver int64, n int) (err error) {
	if tl.V("free") != nil {
		tl.Printf("freei %3x %4x %4x", n, off, ver)
		defer func() {
			tl.Printf("freeo %3x %4x %4x", n, off, ver)
		}()
	}

	err = l.free(off, ver, n)
	if err != nil {
		return
	}

	err = l.shrinkFile()
	if err != nil {
		return
	}

	return l.flush()
}

func (l *Freelist3) free(off, ver int64, n int) (err error) {
	if ver == -1 {
		ver = -2
	}

	sz := nsize(n)
more:
	ps := l.Page << sz
	sib := off ^ ps

	if off&(ps-1) != 0 {
		panic(off)
	}

	binary.BigEndian.PutUint64(l.buf[:8], uint64(sib|int64(sz)))

	var eq bool
	l.stbuf, eq = l.l.Seek(l.stbuf, 0, l.buf[8-l.k:8], nil)
	if eq {
		v := l.l.Int64(l.stbuf)

		l.stbuf, err = l.l.Delete(l.stbuf)
		if err != nil {
			return
		}

		sz++
		off &= sib
		if v > ver {
			ver = v
		}

		goto more
	}

	l.buf = l.buf[:16]

	binary.BigEndian.PutUint64(l.buf[:8], uint64(off|int64(sz)))
	binary.BigEndian.PutUint64(l.buf[8:], uint64(ver))

	l.stbuf, _ = l.l.Seek(l.stbuf, 0, l.buf[8-l.k:8], nil)
	l.stbuf, err = l.l.Insert(l.stbuf, 0, l.buf[8-l.k:8], l.buf[16-l.v:16])

	return nil
}

func (l *Freelist3) Alloc(n int) (off int64, err error) {
	var ver int64

	if tl.V("alloc") != nil {
		tl.Printf("alloc %3x   ??   ??", n)
		defer func() {
			tl.Printf("alloc %3x %4x %4x", n, off, ver)
		}()
	}

	nsize := nsize(n)
	st := l.stbuf[:0]

next:
	st = l.l.Step(st, 0, false)
	if st == nil {
		err = l.grow(n)
		if err != nil {
			return
		}

		goto next
	}

	l.buf = l.buf[:8]
	copy(l.buf, zeros[:])

	l.buf, _ = l.l.Key(st, l.buf[:8-l.k])
	off = int64(binary.BigEndian.Uint64(l.buf))

	size := uint(off & l.Mask)

	if size < nsize {
		goto next
	}

	ver = l.l.Int64(st)
	if ver >= l.Keep && ver != l.Ver {
		goto next
	}

	st, err = l.l.Delete(st)
	if err != nil {
		return
	}

	off &^= l.Mask

	ps := l.Page << nsize
	l.buf = l.buf[:16]
	for nsize != size {
		binary.BigEndian.PutUint64(l.buf, uint64(off+ps|int64(nsize)))
		binary.BigEndian.PutUint64(l.buf[8:], uint64(ver))

		st, err = l.l.Insert(st, 0, l.buf[8-l.k:8], l.buf[16-l.v:16])
		if err != nil {
			return
		}

		ps *= 2
		nsize++
	}

	if st != nil {
		l.stbuf = st
	}

	err = l.flush()
	if err != nil {
		return
	}

	return
}

func (l *Freelist3) grow(n int) (err error) {
	p := l.Page << nsize(n)
	off := l.next
	l.next += int64(n) * l.Page
	if l.next&(p-1) != 0 {
		l.next &^= p - 1
		l.next += p
	}

	l.flen, err = growFile(l.Back, l.Page, l.next)
	if err != nil {
		return
	}

	for off != l.next {
		sz := nsize(n)

		for {
			p := l.Page << sz

			if off&(p-1) == 0 {
				break
			}

			sz--
		}

		err = l.free(off, -1, 1<<sz)
		if err != nil {
			return
		}

		off += l.Page << sz
	}

	return
}

func (l *Freelist3) shrinkFile() (err error) {
	fnext := l.next
	st := l.stbuf[:0]

	for {
		st = l.l.Step(st, 0, true)
		if st == nil {
			break
		}

		l.buf = l.buf[:8]
		copy(l.buf, zeros[:])

		l.buf, _ = l.l.Key(st, l.buf[:8-l.k])

		bst := int64(binary.BigEndian.Uint64(l.buf))
		bend := bst&^l.Mask + l.Page<<uint(bst&l.Mask)

		if tl.V("shrink") != nil {
			tl.Printf("shink check block %x - %x  of %x", bst, bend, fnext)
		}

		if bend != fnext {
			break
		}

		ver := l.l.Int64(st)
		if ver >= l.Keep && ver != l.Ver {
			break
		}

		_, err = l.l.Delete(st)
		if err != nil {
			return
		}

		fnext = bst &^ l.Mask
	}

	if fnext == l.next {
		if tl.V("shrink") != nil {
			tl.Printf("none was shrinked")
		}
		return
	}

	var truncate bool
	diff := l.flen - fnext
	switch {
	case l.flen < 8*l.Page:
	case l.flen < 64*KB:
		truncate = diff >= l.flen/2
	case l.flen < 100*MB:
		truncate = diff >= l.flen/4
	case l.flen < GB:
		truncate = diff >= l.flen/16
	default:
		truncate = diff >= GB/16
	}

	if truncate {
		if tl.V("shrink") != nil {
			tl.Printf("truncate file %x <- %x", fnext, l.flen)
		}

		err = l.Truncate(fnext)
		if err != nil {
			return
		}

		l.flen = fnext
	}

	if tl.V("shrink,shrink_yes") != nil {
		tl.Printf("file shrunk %x <- %x", fnext, l.next)
	}

	l.next = fnext

	return
}
