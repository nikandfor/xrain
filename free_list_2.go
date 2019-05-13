package xrain

import (
	"encoding/binary"
)

type (
	Freelist2 struct {
		b Back
		t Tree // off|size -> ver; size :== log(n*page)-1

		page, mask int64
		ver, keep  int64

		next, flen int64

		//	last []byte

		deferred []kv2
		lock     bool
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
		page: page,
		mask: page - 1,
		next: next,
		flen: flen,
	}
}

func (l *Freelist2) Alloc(n int) (off int64, err error) {
	//	log.Printf("alloc: %2x       ver %d/%d next %x  def %x", n, l.ver, l.keep, l.next, l.deferred)
	//	defer func() {
	//		log.Printf("alloc: %2x %4x  ver %d/%d next %x", n, off, l.ver, l.keep, l.next)
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
		if kv.v != l.ver && kv.v >= l.keep {
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

	var last []byte
next:
	last = l.t.Next(last)
	if last == nil {
		return l.allocGrow(n)
	}

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
		l.Free(n, off, l.ver)
		off += b
	}

	err = l.unlock()

	//	log.Printf("grow   % 16x x %d", off, n)

	return
}

func (l *Freelist2) Free(n int, off, ver int64) (err error) {
	//	defer func() {
	//		log.Printf("free : %d %x  ver %d/%d next %x", n, off, l.ver, l.keep, l.next)
	//	}()

	if ver == 0 { // 0 is a special value
		ver = 1
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

	//	log.Printf("compare %x %x", buf[:8], l.last)
	//	if bytes.Compare(buf[:8], l.last) <= 0 {
	//		goto fin
	//	}
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
		if v < ver {
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
	l.ver, l.keep = ver, keep
	//	l.last = nil
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

		//	log.Printf("op     % 16x % 16x", kv.k, kv.v)

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
	l.lock = false
	//	l.last = nil

	return
}

func (l *Freelist2) deferOp(k, v int64) {
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
