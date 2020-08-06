package xrain

import (
	"encoding/binary"

	"github.com/nikandfor/tlog"
)

const (
	B = 1 << (10 * iota)
	KB
	MB
	GB
	TB
)

type (
	Freelist interface {
		Alloc(n int) (int64, error)
		Free(off, ver int64, n int) error

		SetMeta(m *Meta)
	}

	Freelist2 struct {
		l Layout // off|size -> ver; size ::= log(n)
		t LayoutShortcut

		*Meta

		meta       SubpageLayout
		next, flen int64
		root       int64 // to keep it between constructor and SetMeta

		deferred         []kv2
		defi             int
		lock, shrinklock bool

		rootkey []byte
		nextkey []byte
		datakey []byte

		buf, buf2     []byte
		stbuf, stbuf2 Stack
	}

	GrowFreelist struct {
		*Meta
		next, flen int64
	}

	kv2 struct {
		k, v int64
	}
)

const flDelete = -1

var (
	_ Freelist = &Freelist2{}
	_ Freelist = &GrowFreelist{}
)

func NewFreelist2(m *Meta, l Layout, root, next int64) (_ *Freelist2, err error) {
	f := &Freelist2{
		l:       l,
		Meta:    m,
		root:    root,
		next:    next,
		rootkey: []byte("freelist2.root"),
		nextkey: []byte("freelist2.next"),
		datakey: []byte("freelist2.data"),

		buf2: make([]byte, 16),
	}

	f.init()

	return f, nil
}

func (f *Freelist2) SetMeta(m *Meta) {
	f.Meta = m
	f.l.SetMeta(m)

	f.init()
}

func (f *Freelist2) init() {
	if f.Meta == nil {
		return
	}

	if f.Meta.Meta.Layout != nil {
		st, eq := f.Meta.Meta.Seek(f.rootkey, nil, nil)
		if eq {
			f.root = f.Meta.Meta.Layout.Int64(st)
		}

		st, eq = f.Meta.Meta.Seek(f.nextkey, nil, st)
		if eq {
			f.next = f.Meta.Meta.Layout.Int64(st)
		}

		if l, ok := f.l.(*SubpageLayout); ok {
			st, eq = f.Meta.Meta.Seek(f.datakey, nil, st)
			data := f.Meta.Meta.Value(st, nil)
			if eq {
				l.SetBytes(data)
			}
		}
	}

	f.t = NewLayoutShortcut(f.l, f.root, f.Mask)
	f.flen = f.Back.Size()
}

func (f *Freelist2) flush() (err error) {
	if f.Meta == nil || f.Meta.Meta.Layout == nil {
		return
	}

	_, f.stbuf, err = f.Meta.Meta.SetInt64(f.rootkey, f.t.Root, f.stbuf[:0])
	if err != nil {
		return
	}
	_, f.stbuf, err = f.Meta.Meta.SetInt64(f.nextkey, f.next, f.stbuf[:0])
	if err != nil {
		return
	}

	if l, ok := f.l.(*SubpageLayout); ok {
		f.stbuf, err = f.Meta.Meta.Set(0, f.datakey, l.Bytes(), f.stbuf[:0])
		if err != nil {
			return
		}
	}

	return
}

func (f *Freelist2) Alloc(n int) (off int64, err error) {
	if tl.V("alloc") != nil {
		tl.Printf("alloc: %2x   ??   ??  ver %x/%x next %x  def %x[%d:] from %#v %#v", n, f.Ver, f.Keep, f.next, f.deferred, f.defi,
			tl.VArg("where,where2", tlog.StackTrace(1, 4), ""), tl.VArg("where2", tlog.StackTrace(5, 4), ""))

		defer func() {
			tl.Printf("alloc: %2x %4x   ??  ver %x/%x next %x  def %x[%d:]", n, off, f.Ver, f.Keep, f.next, f.deferred, f.defi)
		}()
	}

	nsize := nsize(n)
	// don't return blocks from f.deferred: they are still may be used by freelist tree

	var st Stack = f.stbuf2[:0]
next:
	st = f.t.Next(st)
	if st == nil {
		return f.allocGrow(n)
	}
	f.buf2, _ = f.l.Key(st, f.buf2[:0])

	off = int64(binary.BigEndian.Uint64(f.buf2))

	size := uint(off & f.Mask)
	if size < nsize {
		goto next
	}

	for _, kv := range f.deferred { // TODO: go from back to forth
		if kv.v == flDelete && kv.k == off {
			goto next
		}
	}

	ver := f.l.Int64(st)
	if ver >= f.Keep && ver != f.Ver {
		goto next
	}

	f.stbuf2 = st

	f.deferOp(off, flDelete)

	off &^= f.Mask

	ps := f.Page << nsize
	for nsize != size {
		//	log.Printf("took %x %d  put back %x %d", off, size, off+ps, nsize)
		f.deferOp(off+ps|int64(nsize), ver)
		ps *= 2
		nsize++
	}

	err = f.unlock()

	return
}

func (f *Freelist2) allocGrow(n int) (off int64, err error) {
	sz := nsize(n)
	p := f.Page << sz
	pm := p - 1
	next := f.next + p
	if next&pm != 0 { // use last blocks from tree
		next = next&^pm + p
	}
	f.flen, err = growFile(f.Back, f.Page, next)
	if err != nil {
		return
	}

	if tl.V("grow") != nil {
		tl.Printf("grow   % 4x n %d : %x -> %x  p %x", f.next, n, f.next, next, p)
	}

	off = f.next
	f.next = next

	for b, n := align(off, p, sz); b != 0; b, n = align(off, p, sz) {
		if tl.V("grow") != nil {
			tl.Printf("back   % 4x n %x", off, n)
		}

		err = f.Free(off, f.Keep-1, n)
		if err != nil {
			return
		}
		off += b
	}

	err = f.unlock()

	if tl.V("grow") != nil {
		tl.Printf("galloc % 4x n %d", off, n)
	}

	return
}

func (f *Freelist2) Free(off, ver int64, n int) (err error) {
	var sz uint
	if tl.V("free") != nil {
		tl.Printf("freei: %2x %4x %4x  ver %x/%x next %x  def %x[%d:]  from %#v %#v", n, off, ver, f.Ver, f.Keep, f.next, f.deferred, f.defi,
			tl.VArg("where,where2", tlog.StackTrace(1, 4), ""), tl.VArg("where2", tlog.StackTrace(5, 4), ""))

		defer func() {
			tl.Printf("freeo: %2x %4x %4x  ver %x/%x next %x  def %x[%d:]", 1<<sz, off, ver, f.Ver, f.Keep, f.next, f.deferred, f.defi)
		}()
	}

	if ver == flDelete { // special value
		ver = -2
	}

	buf := f.buf2
	var eq bool

	sz = nsize(n)
more:
	ps := f.Page << sz
	sib := off ^ ps

	if off&(ps-1) != 0 {
		panic(off)
	}

	for i := len(f.deferred) - 1; i >= 0; i-- {
		kv := f.deferred[i]
		if kv.k != sib|int64(sz) {
			continue
		}
		if kv.v == flDelete {
			goto fin
		}

		if tl.V("merge,sibling") != nil {
			tl.Printf("free   %4x n %2x sib %4x %4x  def %x", off, n, sib|int64(sz), kv.v, f.deferred)
		}
		f.deferOp(sib|int64(sz), flDelete)

		sz++
		off &= sib
		if kv.v > ver {
			ver = kv.v
		}

		goto more
	}

	binary.BigEndian.PutUint64(buf[:8], uint64(sib|int64(sz)))

	f.stbuf2, eq = f.t.Seek(buf[:8], nil, f.stbuf2)
	if eq {
		v := f.t.Layout.Int64(f.stbuf2)
		if tl.V("merge,sibling") != nil {
			tl.Printf("free   %4x n %2x sib %4x %4x  def %x", off, n, sib|int64(sz), v, f.deferred)
		}
		f.deferOp(sib|int64(sz), flDelete)

		sz++
		off &= sib
		if v > ver {
			ver = v
		}

		goto more
	}

fin:
	if n != 1<<sz && tl.V("merge") != nil {
		tl.Printf("free   merged %4x n %d   to logsize %x (size %d)  ver %x  def %x", off, n, sz, 1<<sz, ver, f.deferred)
	}
	f.deferOp(off|int64(sz), ver)

	err = f.unlock()

	return
}

func (f *Freelist2) unlock() (err error) {
	if tl.V("unlock") != nil {
		tl.Printf("unlock: next %x/%x  deff %x  ver %d/%d  lock %v  from %#v", f.next, f.flen, f.deferred, f.Ver, f.Keep, f.lock, tl.VArg("where", tlog.StackTrace(1, 2), ""))
	}

	if f.lock {
		return
	}
	f.lock = true

	if len(f.buf) < 16 {
		f.buf = make([]byte, 16)
	}
	buf := f.buf
	i := 0

	q := cap(f.stbuf)

more:
	for ; i < len(f.deferred); i++ { // for range is not applicable here
		kv := f.deferred[i]
		f.defi = i

		if tl.V("unlockop") != nil {
			tl.Printf("op     %3x %3x  el %2d of %x", kv.k, kv.v, i, f.deferred)
		}
		//	tl.V("dump").Printf("dump  fl root %x\n%v", f.t.Root, f.l.(fileDumper).dumpFile())

		binary.BigEndian.PutUint64(buf[:8], uint64(kv.k))
		if kv.v == flDelete {
			f.stbuf, err = f.t.Del(buf[:8], f.stbuf)
		} else {
			binary.BigEndian.PutUint64(buf[8:], uint64(kv.v))
			f.stbuf, err = f.t.Put(0, buf[:8], buf[8:16], f.stbuf)
		}

		if cap(f.stbuf) == 0 {
			tl.Printf("stbuf %x <- %x   %x[%d]", cap(f.stbuf), q, f.deferred, i)
		}
		if err != nil {
			return
		}
	}

	err = f.shrinkFile()
	if err != nil {
		return
	}

	if i < len(f.deferred) {
		goto more
	}

	f.deferred = f.deferred[:0]
	f.defi = -1
	f.lock = false

	err = f.flush()

	return
}

func (f *Freelist2) shrinkFile() (err error) {
	if f.shrinklock {
		tlog.Fatalf("here: %#v", tlog.StackTrace(1, 8))
		return nil
	}
	f.shrinklock = true
	defer func() {
		f.shrinklock = false
	}()

	fnext := f.next

	if tl.V("shrink") != nil {
		tl.Printf("try to shrink file ver/keep %x/%x fnext %x", f.Ver, f.Keep, fnext)
	}
	//	tl.V("shrink_dump").Printf("\n%v", f.l.(fileDumper).dumpFile())

	for {
		st := f.t.Last(f.stbuf)
		if st == nil {
			break
		}
		last, _ := f.l.Key(st, f.buf[:0])

		bst := int64(binary.BigEndian.Uint64(last))
		bend := bst&^f.Mask + f.Page<<uint(bst&f.Mask)

		if tl.V("shrink") != nil {
			tl.Printf("check last block %x - %x of %x", bst, bend, fnext)
		}

		if bend != fnext {
			break
		}

		ver := f.l.Int64(st)
		if ver >= f.Keep && ver != f.Ver {
			break
		}

		_, err = f.t.Delete(st)
		if err != nil {
			return
		}

		fnext = bst &^ f.Mask
	}

	if fnext == f.next {
		tl.V("shrink").Printf("none was shrinked")
		return
	}

	var truncate bool
	diff := f.flen - fnext
	switch {
	case f.flen < 8*f.Page:
	case f.flen < 64*KB:
		truncate = diff >= f.flen/2
	case f.flen < 100*MB:
		truncate = diff >= f.flen/4
	case f.flen < GB:
		truncate = diff >= f.flen/16
	default:
		truncate = diff >= GB/16
	}

	if truncate {
		tl.V("shrink").Printf("truncate file")

		err = f.Truncate(fnext)
		if err != nil {
			return
		}

		f.flen = fnext
	}

	if tl.V("shrink,shrink_yes") != nil {
		tl.Printf("file shrunk %x <- %x", fnext, f.next)
	}

	f.next = fnext

	return
}

func (f *Freelist2) deferOp(k, v int64) {
	ln := len(f.deferred) - 1
	if ln > f.defi && f.deferred[ln].k == k && (f.deferred[ln].v == flDelete) != (v == flDelete) {
		f.deferred = f.deferred[:ln]
		return
	}
	if tl.V("") != nil {
		tl.Printf("deferred %x %x on defi %d (ln %d) %x  (%v %v %v)", k, v, f.defi, ln, f.deferred, ln > f.defi, ln > f.defi && f.deferred[ln].k == k, ln > f.defi && (f.deferred[ln].v == flDelete) != (v == flDelete))
	}
	f.deferred = append(f.deferred, kv2{k, v})
}

func nsize(n int) (s uint) {
	n--
	if n == 0 {
		return 0
	}
	s = 1
	for n>>s != 0 {
		s++
	}
	return
}

func align(off, p int64, s uint) (b int64, n int) {
	pm := p - 1
	if off&pm == 0 {
		return
	}

	bs := s
	for off&pm != 0 {
		bs--
		pm >>= 1
	}

	return p >> (s - bs), 1 << bs
}

func NewEverGrowFreelist(m *Meta) *GrowFreelist {
	flen := m.Back.Size()

	f := &GrowFreelist{
		Meta: m,
		flen: flen,
	}

	return f
}

func (f *GrowFreelist) Alloc(n int) (off int64, err error) {
	off = f.next
	size := int64(n) * f.Page
	f.flen, err = growFile(f.Back, f.Page, off+size)
	if err != nil {
		return NilPage, err
	}
	f.next += size

	return off, nil
}

func (f *GrowFreelist) Free(off, ver int64, n int) error { return nil }

func growFile(b Back, page, sz int64) (flen int64, err error) {
	flen = b.Size()

	if sz <= flen {
		return
	}

	for flen < sz {
		switch {
		case flen < 4*page:
			flen = 4 * page
		case flen < 64*KB:
			flen *= 2
		case flen < 100*MB:
			flen += flen / 4 // at most 25 MB
		case flen < GB:
			flen += flen / 16 // at most 64 MB
		default:
			flen += GB / 16 // 64 MB
		}

		flen -= flen % page
	}

	err = b.Truncate(flen)
	if err != nil {
		return
	}

	return
}
