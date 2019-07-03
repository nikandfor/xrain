package xrain

import (
	"encoding/binary"
)

const (
	B = 1 << (10 * iota)
	KiB
	MiB
	GiB
	TiB
)

type (
	Freelist interface {
		Serializer

		Alloc(n int) (int64, error)
		Free(n int, off, ver int64) error
		SetVer(ver, keep int64)
	}

	Freelist2 struct {
		b  Back
		t  Tree // off|size -> ver; size ::= log(n)
		pl PageLayout

		page, mask int64
		ver, keep  int64

		next, flen int64

		deferred []kv2
		defi     int
		lock     bool
	}

	GrowFreelist struct {
		b          Back
		page       int64
		next, flen int64
	}

	kv2 struct {
		k, v int64
	}
)

func NewFreelist2(b Back, t Tree, next, page int64) *Freelist2 {
	if page&(page-1) != 0 {
		panic(page)
	}

	flen := b.Size()
	return &Freelist2{
		b:    b,
		t:    t,
		pl:   t.PageLayout(),
		page: page,
		mask: page - 1,
		next: next,
		flen: flen,
	}
}

func (*Freelist2) SerializerName() string { return "Freelist2" }

func (*Freelist2) Deserialize(ctx *SerializeContext, p []byte) (interface{}, int, error) {
	tr, s, err := Deserialize(ctx, p)
	if err != nil {
		return nil, s, err
	}

	next := int64(binary.BigEndian.Uint64(p[s:]))
	s += 8

	l := NewFreelist2(ctx.Back, tr.(Tree), next, ctx.Page)

	tr.(Tree).PageLayout().SetFreelist(l)

	return l, s, nil
}

func (l *Freelist2) Serialize(p []byte) int {
	s := Serialize(p, l.t)

	binary.BigEndian.PutUint64(p[s:], uint64(l.next))
	s += 8

	return s
}

func (l *Freelist2) Alloc(n int) (off int64, err error) {
	//	tlog.Printf("alloc: %2x       ver %d/%d next %x def %x", n, l.ver, l.keep, l.next, l.deferred)
	//	defer func() {
	//		tlog.Printf("alloc: %2x %4x  ver %d/%d next %x def %x", n, off, l.ver, l.keep, l.next, l.deferred)
	//	}()

	nsize := nsize(n)
	used := map[int64]struct{}{}
	for i := len(l.deferred) - 1; i >= 0; i-- {
		kv := l.deferred[i]
		if kv.v == 0 {
			used[kv.k] = struct{}{}
			continue
		}
		if _, ok := used[kv.k]; ok {
			continue
		}
		if kv.v >= l.keep && kv.v != l.ver {
			continue
		}

		size := uint(kv.k & l.mask)
		if size < nsize {
			continue
		}
		if size == nsize {
			//	log.Printf("asquired %d found %x %x  ver %x/%x def %x", n, kv.k, kv.v, l.ver, l.keep, l.deferred)
			l.deferOp(kv.k, 0)
			return kv.k &^ l.mask, nil
		}
	}

	var st Stack
next:
	st = l.t.Step(st, false)
	if st == nil {
		return l.allocGrow(n)
	}
	off, i := st.OffIndex(l.mask)
	last := l.pl.Key(off, i, nil)

	off = int64(binary.BigEndian.Uint64(last))

	size := uint(off & l.mask)
	if size < nsize {
		goto next
	}

	for _, kv := range l.deferred {
		if kv.v == 0 && kv.k == off {
			goto next
		}
	}

	key := make([]byte, len(last))
	copy(key, last)

	vbytes := l.t.Get(key)
	ver := int64(binary.BigEndian.Uint64(vbytes))
	if ver >= l.keep && ver != l.ver {
		goto next
	}

	l.deferOp(off, 0)

	off &^= l.mask

	ps := l.page << nsize
	for nsize != size {
		//	log.Printf("took %x %d  put back %x %d", off, size, off+ps, nsize)
		l.deferOp(off+ps|int64(nsize), ver)
		ps *= 2
		nsize++
	}

	err = l.unlock()

	return
}

func (l *Freelist2) allocGrow(n int) (off int64, err error) {
	sz := nsize(n)
	p := l.page << sz
	pm := p - 1
	next := l.next + p
	if l.next&pm != 0 {
		next += p - next&pm
	}
	l.flen, err = growFile(l.b, l.page, next)
	if err != nil {
		return
	}

	//	log.Printf("grow   % 16x x %d : %x -> %x  p %x", l.next, n, l.next, next, p)

	off = l.next
	l.next = next

	for b, n := align(off, p, sz); b != 0; b, n = align(off, p, sz) {
		//	log.Printf("back   % 16x n %x", off, n)
		err = l.Free(n, off, l.keep-1)
		if err != nil {
			return
		}
		off += b
	}

	err = l.unlock()

	//	log.Printf("grow   % 16x x %d", off, n)

	return
}

func (l *Freelist2) Free(n int, off, ver int64) (err error) {
	//	log.Printf("freei: %2x %4x  ver %d/%d next %x def %x", n, off, l.ver, l.keep, l.next, l.deferred)
	//	defer func() {
	//		log.Printf("freeo: %2x %4x  ver %d/%d next %x def %x", n, off, l.ver, l.keep, l.next, l.deferred)
	//	}()

	if ver == 0 { // 0 is a special value
		ver = -1
	}

	var buf [8]byte

	sz := nsize(n)
more:
	ps := l.page << sz
	sib := off ^ ps

	if off&(ps-1) != 0 { // TODO(nik): remove
		panic(off)
	}

	binary.BigEndian.PutUint64(buf[:8], uint64(sib|int64(sz)))

	for i := len(l.deferred) - 1; i >= 0; i-- {
		kv := l.deferred[i]
		if kv.k != sib|int64(sz) {
			continue
		}
		if kv.v == 0 {
			goto fin
		}

		//	log.Printf("free   %x n %d sib %x  def %x", off, n, sib|int64(sz), l.deferred)
		l.deferOp(sib|int64(sz), 0)

		sz++
		off &= sib
		if kv.v < ver {
			ver = kv.v
		}

		goto more
	}

	if vbytes := l.t.Get(buf[:8]); vbytes != nil {
		v := int64(binary.BigEndian.Uint64(vbytes))
		//	log.Printf("free   %x n %d sib %x  def %x", off, n, sib|int64(sz), l.deferred)
		l.deferOp(sib|int64(sz), 0)

		sz++
		off &= sib
		if v > ver {
			ver = v
		}

		goto more
	}

fin:
	//	log.Printf("free   merged %4x n %d  last %16x def %x", off, n, l.last, l.deferred)
	l.deferOp(off|int64(sz), ver)

	err = l.unlock()

	return
}

func (l *Freelist2) SetVer(ver, keep int64) {
	if keep == 0 {
		keep = -1
	}
	l.ver, l.keep = ver, keep
	l.t.PageLayout().SetVer(l.ver)
}

func (l *Freelist2) unlock() (err error) {
	//	log.Printf("unlock: next %x/%x last %x deff %x ver %d/%d lock %v", l.next, l.flen, l.last, l.deferred, l.ver, l.keep, l.lock)
	if l.lock {
		return
	}
	l.lock = true

	var buf [16]byte

	for i := 0; i < len(l.deferred); i++ { // for range is not applicable here
		kv := l.deferred[i]
		l.defi = i

		//	log.Printf("op     %x %x  el %d of %x", kv.k, kv.v, i, l.deferred)

		binary.BigEndian.PutUint64(buf[:8], uint64(kv.k))
		if kv.v == 0 {
			_, err = l.t.Del(buf[:8])
		} else {
			binary.BigEndian.PutUint64(buf[8:], uint64(kv.v))
			_, err = l.t.Put(buf[:8], buf[8:])
		}
		if err != nil {
			return
		}
	}

	l.deferred = l.deferred[:0]
	l.defi = -1
	l.lock = false

	err = l.shrinkFile()

	return
}

func (l *Freelist2) shrinkFile() (err error) {
	fend := l.next

	//	tlog.Printf("try to shrinkFile %d/%d %x\n%v", l.ver, l.keep, fend, dumpFile(l.pl))

	for {
		st := l.t.Step(nil, true)
		if st == nil {
			break
		}
		off, i := st.OffIndex(l.mask)
		last := l.pl.Key(off, i, nil)

		bst := int64(binary.BigEndian.Uint64(last))
		bend := bst&^l.mask + l.page<<uint(bst&l.mask)

		//	tlog.Printf("check last block %x - %x of %x", bst, bend, fend)

		if bend != fend {
			break
		}

		vbytes := l.t.Get(last)
		ver := int64(binary.BigEndian.Uint64(vbytes))
		if ver >= l.keep && ver != l.ver {
			break
		}

		_, err = l.t.Del(last)
		if err != nil {
			return
		}

		fend = bst &^ l.mask
	}

	if fend == l.next {
		return
	}
	// TODO(nik): shrink by big parts

	err = l.b.Truncate(fend)
	if err != nil {
		return
	}

	//	log.Printf("file shrunk %x <- %x", fend, l.next)

	l.next = fend
	l.flen = fend

	return
}

func (l *Freelist2) deferOp(k, v int64) {
	ln := len(l.deferred) - 1
	if ln > l.defi && l.deferred[ln].k == k && v == 0 {
		l.deferred = l.deferred[:ln]
		return
	}
	l.deferred = append(l.deferred, kv2{k, v})
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

func NewEverGrowFreelist(b Back, page, next int64) *GrowFreelist {
	flen := b.Size()

	l := &GrowFreelist{
		b:    b,
		page: page,
		next: flen,
		flen: flen,
	}

	return l
}

func (*GrowFreelist) SerializerName() string {
	return "GrowFreelist"
}

func (*GrowFreelist) Deserialize(ctx *SerializeContext, p []byte) (interface{}, int, error) {
	next := int64(binary.BigEndian.Uint64(p))
	l := NewEverGrowFreelist(ctx.Back, ctx.Page, next)
	return l, 8, nil
}

func (l *GrowFreelist) Serialize(p []byte) int {
	binary.BigEndian.PutUint64(p, uint64(l.next))
	return 8
}

func (l *GrowFreelist) SetVer(ver, keep int64) {}

func (l *GrowFreelist) Alloc(n int) (off int64, err error) {
	off = l.next
	size := int64(n) * l.page
	l.flen, err = growFile(l.b, l.page, off+size)
	if err != nil {
		return 0, err
	}
	l.next += size

	return off, nil
}

func (l *GrowFreelist) Free(n int, off, ver int64) error { return nil }

func growFile(b Back, page, sz int64) (flen int64, err error) {
	flen = b.Size()

	if sz <= flen {
		return
	}

	for flen < sz {
		switch {
		case flen < 4*page:
			flen = 4 * page
		case flen < 64*KiB:
			flen *= 2
		case flen < 100*MiB:
			flen += flen / 4
		case flen < GiB:
			flen += flen / 16
		default:
			flen += GiB / 16 // 64 MiB
		}

		flen -= flen % page
	}

	err = b.Truncate(flen)
	if err != nil {
		return
	}

	return
}
