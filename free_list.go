package xrain

import (
	"encoding/binary"
	"fmt"
	"log"
	"path"
	"runtime"
	"strings"
)

const (
	B = 1 << (10 * iota)
	KiB
	MiB
	GiB
	TiB
)

type (
	FreeList struct {
		ver, keep int64
		b         Back
		t0, t1    *Tree // get, put

		last       []byte
		page       int64
		next, flen int64
		deferred   []kv

		exht bool
		lock bool
	}

	kv struct {
		Key, Value [8]byte
		add        bool
	}
)

func NewFreeList(t0, t1 *Tree, next, page int64, ver, keep int64, b Back) *FreeList {
	if t0 == t1 {
		assert_(t0 != t1, "must be 2 distinct trees")
	}

	if t0.Size() < t1.Size() {
		t0, t1 = t1, t0
	}

	flen := b.Size()

	l := &FreeList{
		t0:   t0,
		t1:   t1,
		ver:  ver,
		keep: keep,
		b:    b,
		page: page,
		next: next,
		flen: flen,
	}

	return l
}

func NewFreeList_(root0, root1, next, page int64, ver, keep int64, b Back) *FreeList {
	if root0 == root1 {
		panic(root1)
	}

	flen := b.Size()

	l := &FreeList{
		ver:  ver,
		keep: keep,
		b:    b,
		page: page,
		next: next,
		flen: flen,
	}

	pl := &IntLayout{BaseLayout: NewPageLayout(b, page, ver, l)}
	t0 := NewTree(pl, root0, page)
	t1 := NewTree(pl, root1, page)

	size := func(t *Tree) (c int) {
		for k := t.Next(nil); k != nil; k = t.Next(k) {
			c++
		}
		return
	}

	if size(t0) < size(t1) {
		t0, t1 = t1, t0
	}

	l.t0 = t0
	l.t1 = t1

	return l
}

func NewNoRewriteFreeList(page int64, b Back) *FreeList {
	flen := b.Size()

	l := &FreeList{
		b:    b,
		page: page,
		next: flen,
		flen: flen,
		exht: true,
	}

	return l
}

func (l *FreeList) Alloc() (off int64, err error) {
	//	defer func(last []byte) {
	//		log.Printf("alloc  [%3x] %3x  (last %2x)%v", l.t0.root, off, last, callers(-1))
	//		log.Printf("freelist state %x %x defer %x\n%v", l.t0.root, l.t1.root, l.deferred, dumpFile(l.t0.p))
	//	}(l.last)
	/*
		log.Printf("alloc in:  root %x last %2x next %x", l.t.root, l.last, l.next)
		defer func() {
			log.Printf("alloc out: root %x last %2x next %x -> %x", l.t.root, l.last, l.next, off)
		}()
	*/

	if l.exht {
		return l.allocGrow()
	}

next:
	next := l.t0.Next(l.last)

	log.Printf("Alloc nxt %x <- %x   next %x", next, l.last, l.next)
	if next == nil {
		l.exht = true
		return l.allocGrow()
	}

	l.last = make([]byte, 8)
	copy(l.last, next)

	key := l.last
	off = int64(binary.BigEndian.Uint64(key))

	cp := make([]byte, len(key))
	copy(cp, key)

	vbytes := l.t0.Get(key)
	v := int64(binary.BigEndian.Uint64(vbytes))
	if v >= l.keep {
		goto next
	}

	if l.lock {
		l.deferred = append(l.deferred, newkv(key, nil))
		//log.Printf("alloc (defer) %x  now len %d%v", off, len(l.deferred), callers(0))
		return off, nil
	}

	l.lock = true

	err = l.t0.Del(key)
	if err != nil {
		return 0, err
	}

	err = l.unlock()
	if err != nil {
		return 0, err
	}

	return off, nil
}

func (l *FreeList) Reclaim(off, ver int64) error {
	//	defer func() {
	//		log.Printf("reclaim[%3x] %3x %d%v", l.t1.root, off, ver, callers(-1))
	//		log.Printf("freelist state %x %x defer %x\n%v", l.t0.root, l.t1.root, l.deferred, dumpFile(l.t0.p))
	//	}()

	if l.t1 == nil {
		return nil
	}

	kv := newkvint(off, ver, true)

	if l.lock {
		l.deferred = append(l.deferred, kv)
		//log.Printf("free  (defer) %x  now len %d%v", off, len(l.deferred), callers(0))
		return nil
	}

	l.lock = true

	err := l.t1.Put(kv.Key[:], kv.Value[:])
	if err != nil {
		return err
	}

	return l.unlock()
}

func (l *FreeList) allocGrow() (int64, error) {
	off := l.next
	if err := l.growFile(off + l.page); err != nil {
		return 0, err
	}
	l.next += l.page

	return off, nil
}

func (l *FreeList) growFile(sz int64) error {
	if sz <= l.flen {
		return nil
	}

	for l.flen < sz {
		if l.flen < 4*l.page {
			l.flen = 4 * l.page
		} else if l.flen < 64*KiB {
			l.flen *= 2
		} else if l.flen < 100*MiB {
			l.flen += l.flen / 4
		} else if l.flen < GiB {
			l.flen += l.flen / 16
		} else {
			l.flen += GiB / 16 // 64 MiB
		}

		l.flen -= l.flen % l.page
	}

	err := l.b.Truncate(l.flen)
	if err != nil {
		return err
	}

	return nil
}

func (l *FreeList) unlock() (err error) {
	//	log.Printf("unlock: root %x %x  %x\n%v%v", l.t0.root, l.t1.root, l.deferred, dumpFile(l.t0.p), callers(0))

	for i := 0; i < len(l.deferred); i++ {
		kv := l.deferred[i]
		if kv.add {
			err = l.t1.Put(kv.Key[:], kv.Value[:])
		} else {
			err = l.t0.Del(kv.Key[:])
		}
		if err != nil {
			return
		}
		//	log.Printf("deferred(%d)'ve done: %x -> %x on root %x %x ver %d  defer %x\n%v", i, kv.Key, kv.Value, l.t0.root, l.t1.root, l.ver, l.deferred[i+1:], "" /*dumpFile(l.t0.p)*/)
	}

	l.deferred = l.deferred[:0]
	l.lock = false

	return nil
}

func newkv(k, v []byte) kv {
	r := kv{}
	copy(r.Key[:], k)
	if v != nil {
		copy(r.Value[:], v)
		r.add = true
	}
	return r
}

func newkvint(k, v int64, add bool) kv {
	r := kv{add: add}
	binary.BigEndian.PutUint64(r.Key[:], uint64(k))
	binary.BigEndian.PutUint64(r.Value[:], uint64(v))
	return r
}

func callers(skip int) string {
	if skip < 0 {
		return ""
	}

	var pc [100]uintptr
	n := runtime.Callers(2+skip, pc[:])

	frames := runtime.CallersFrames(pc[:n])

	var buf strings.Builder
	buf.WriteString("\n")

	for {
		f, more := frames.Next()
		if !strings.Contains(f.File, "/xrain/") {
			break
		}
		fmt.Fprintf(&buf, "    %-20s at %s:%d\n", path.Ext(f.Function)[1:], path.Base(f.File), f.Line)
		if !more {
			break
		}
	}

	return buf.String()
}