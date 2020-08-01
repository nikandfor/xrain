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
	}

	Freelist2 struct {
		l Layout // off|size -> ver; size ::= log(n)
		t *LayoutShortcut

		*Common

		flen int64

		deferred []kv2
		defi     int
		lock     bool
	}

	GrowFreelist struct {
		*Common
		flen int64
	}

	kv2 struct {
		k, v int64
	}
)

const flDelete = -1

func NewFreelist2(c *Common, l Layout, root int64) *Freelist2 {
	flen := c.Back.Size()

	return &Freelist2{
		l:      l,
		t:      NewLayoutShortcut(l, root, c.Mask),
		Common: c,
		flen:   flen,
	}
}

func (f *Freelist2) Alloc(n int) (off int64, err error) {
	tl.V("alloc,in").Printf("alloc: %2x   ??  ver %x/%x next %x def %x[%d:] from %v", n, f.Ver, f.Keep, f.FileNext, f.deferred, f.defi, tlog.StackTrace(1, 3))
	defer func() {
		tl.V("alloc,out").Printf("alloc: %2x %4x  ver %x/%x next %x def %x[%d:]", n, off, f.Ver, f.Keep, f.FileNext, f.deferred, f.defi)
	}()

	nsize := nsize(n)
	used := map[int64]struct{}{}
	for i := len(f.deferred) - 1; i >= 0; i-- {
		kv := f.deferred[i]
		if kv.v == flDelete {
			used[kv.k] = struct{}{}
			continue
		}

		if _, ok := used[kv.k]; ok {
			continue
		}
		if kv.v >= f.Keep && kv.v != f.Ver {
			continue
		}

		size := uint(kv.k & f.Mask)
		if size < nsize {
			continue
		}
		if size == nsize {
			//	log.Printf("asquired %d found %x %x  ver %x/%x def %x", n, kv.k, kv.v, f.ver, f.Keep, f.deferred)
			f.deferOp(kv.k, flDelete)
			return kv.k &^ f.Mask, nil
		}
	}

	var st Stack
next:
	st = f.t.Next(st)
	if st == nil {
		// TODO: could alloc less than full block plus align
		return f.allocGrow(n)
	}
	last, _ := f.l.Key(st, nil)

	off = int64(binary.BigEndian.Uint64(last))

	size := uint(off & f.Mask)
	if size < nsize {
		goto next
	}

	for _, kv := range f.deferred {
		if kv.v == flDelete && kv.k == off {
			goto next
		}
	}

	vbytes := f.l.Value(st, nil)
	ver := int64(binary.BigEndian.Uint64(vbytes))
	if ver >= f.Keep && ver != f.Ver {
		goto next
	}

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
	next := f.FileNext + p
	if next&pm != 0 {
		next = next&^pm + p
	}
	f.flen, err = growFile(f.Back, f.Page, next)
	if err != nil {
		return
	}

	tl.V("grow").Printf("grow   % 4x n %d : %x -> %x  p %x", f.FileNext, n, f.FileNext, next, p)

	off = f.FileNext
	f.FileNext = next

	for b, n := align(off, p, sz); b != 0; b, n = align(off, p, sz) {
		tl.V("grow").Printf("back   % 4x n %x", off, n)
		err = f.Free(off, f.Keep-1, n)
		if err != nil {
			return
		}
		off += b

		tl.V("grow").Printf("freed %x x %x  root %x\n%v", off, n, f.t.Root, f.l.(pageDumper).dumpPage(f.t.Root))
	}

	err = f.unlock()

	tl.V("grow").Printf("galloc % 4x n %d", off, n)

	return
}

func (f *Freelist2) Free(off, ver int64, n int) (err error) {
	var sz uint
	tl.V("free,in").Printf("freei: %2x %4x  ver %x/%x next %x  ver %x  def %x[%d:]  from %#v", n, off, f.Ver, f.Keep, f.FileNext, ver, f.deferred, f.defi, tlog.StackTrace(1, 4))
	defer func() {
		tl.V("free,out").Printf("freeo: %2x %4x  ver %x/%x next %x  ver %x  def %x[%d:]", 1<<sz, off, f.Ver, f.Keep, f.FileNext, ver, f.deferred, f.defi)
	}()

	if ver == flDelete { // special value
		ver = -2
	}

	var buf [8]byte

	sz = nsize(n)
more:
	ps := f.Page << sz
	sib := off ^ ps

	if off&(ps-1) != 0 { // TODO(nik): remove
		panic(off)
	}

	binary.BigEndian.PutUint64(buf[:8], uint64(sib|int64(sz)))

	for i := len(f.deferred) - 1; i >= 0; i-- {
		kv := f.deferred[i]
		if kv.k != sib|int64(sz) {
			continue
		}
		if kv.v == flDelete {
			goto fin
		}

		tl.V("merge,sibling").Printf("free   %x n %d sib %x  def %x", off, n, sib|int64(sz), f.deferred)
		f.deferOp(sib|int64(sz), flDelete)

		sz++
		off &= sib
		if kv.v < ver {
			ver = kv.v
		}

		goto more
	}

	if vbytes, _ := f.t.Get(buf[:8]); vbytes != nil {
		v := int64(binary.BigEndian.Uint64(vbytes))
		tl.V("merge,sibling").Printf("free   %x n %d sib %x  def %x", off, n, sib|int64(sz), f.deferred)
		f.deferOp(sib|int64(sz), flDelete)

		sz++
		off &= sib
		if v > ver {
			ver = v
		}

		goto more
	}

fin:
	tl.V("merge").Printf("free   merged %4x n %d ver %x  def %x", off, n, ver, f.deferred)
	f.deferOp(off|int64(sz), ver)

	err = f.unlock()

	return
}

func (f *Freelist2) unlock() (err error) {
	tl.V("unlock").Printf("unlock: next %x/%x deff %x ver %d/%d lock %v from %#v", f.FileNext, f.flen, f.deferred, f.Ver, f.Keep, f.lock, tlog.StackTrace(1, 2))
	if f.lock {
		return
	}
	f.lock = true

	var buf [16]byte

	for i := 0; i < len(f.deferred); i++ { // for range is not applicable here
		kv := f.deferred[i]
		f.defi = i

		tl.V("unlockop").Printf("op     %x %x  el %d of %x", kv.k, kv.v, i, f.deferred)

		binary.BigEndian.PutUint64(buf[:8], uint64(kv.k))
		if kv.v == flDelete {
			err = f.t.Del(buf[:8])
		} else {
			binary.BigEndian.PutUint64(buf[8:], uint64(kv.v))
			err = f.t.Put(0, buf[:8], buf[8:])
		}
		if err != nil {
			return
		}
	}

	f.deferred = f.deferred[:0]
	f.defi = -1
	f.lock = false

	err = f.shrinkFile()

	return
}

func (f *Freelist2) shrinkFile() (err error) {
	fend := f.FileNext

	//	tlog.Printf("try to shrinkFile %d/%d %x\n%v", f.ver, f.Keep, fend, dumpFile(f.pl))

	for {
		st := f.t.Prev(nil)
		if st == nil {
			break
		}
		last, _ := f.l.Key(st, nil)

		bst := int64(binary.BigEndian.Uint64(last))
		bend := bst&^f.Mask + f.Page<<uint(bst&f.Mask)

		//	tlog.Printf("check last block %x - %x of %x", bst, bend, fend)

		if bend != fend {
			break
		}

		vbytes := f.l.Value(st, nil)
		ver := int64(binary.BigEndian.Uint64(vbytes))
		if ver >= f.Keep && ver != f.Ver {
			break
		}

		err = f.t.Del(last)
		if err != nil {
			return
		}

		fend = bst &^ f.Mask
	}

	if fend == f.FileNext {
		return
	}

	var truncate bool
	diff := f.flen - fend
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
		err = f.Truncate(fend)
		if err != nil {
			return
		}

		f.flen = fend
	}

	//	log.Printf("file shrunk %x <- %x", fend, f.FileNext)

	f.FileNext = fend

	return
}

func (f *Freelist2) deferOp(k, v int64) {
	ln := len(f.deferred) - 1
	if ln > f.defi && f.deferred[ln].k == k && (f.deferred[ln].v == flDelete) != (v == flDelete) {
		f.deferred = f.deferred[:ln]
		return
	}
	tl.V("").Printf("deferred %x %x on defi %d (ln %d) %x  (%v %v %v)", k, v, f.defi, ln, f.deferred, ln > f.defi, ln > f.defi && f.deferred[ln].k == k, ln > f.defi && (f.deferred[ln].v == flDelete) != (v == flDelete))
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

func NewEverGrowFreelist(c *Common) *GrowFreelist {
	flen := c.Back.Size()

	f := &GrowFreelist{
		Common: c,
		flen:   flen,
	}

	return f
}

func (f *GrowFreelist) Alloc(n int) (off int64, err error) {
	off = f.FileNext
	size := int64(n) * f.Page
	f.flen, err = growFile(f.Back, f.Page, off+size)
	if err != nil {
		return NilPage, err
	}
	f.FileNext += size

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
