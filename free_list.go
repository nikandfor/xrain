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
		ver, keep  int64
		b          Back
		t          *Tree
		last       []byte
		page       int64
		next, flen int64
		deferred   []kv
		exht       bool
		lock       bool
	}

	kv struct {
		Key, Value []byte
	}
)

func NewFreeList(root, next, page int64, ver, keep int64, b Back) *FreeList {
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
	t := NewTree(pl, root)

	l.t = t

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
	//	defer func() {
	//		log.Printf("alloc %x%v", off, callers(1))
	//	}()
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
	l.last = l.t.Next(l.last)
	if l.last == nil {
		l.exht = true
		return l.allocGrow()
	}

	key := l.last
	off = int64(binary.BigEndian.Uint64(key))

	vbytes := l.t.Get(key)
	v := int64(binary.BigEndian.Uint64(vbytes))
	if v >= l.keep {
		goto next
	}

	if l.lock {
		l.deferred = append(l.deferred, kv{Key: key})
		return off, nil
	}

	l.lock = true

	err = l.t.Del(key)
	if err != nil {
		return 0, err
	}

	err = l.unlock()
	if err != nil {
		return 0, err
	}

	return off, nil
}

func (l *FreeList) allocGrow() (int64, error) {
	off := l.next
	if err := l.growFile(off + l.page); err != nil {
		return 0, err
	}
	l.next += l.page

	return off, nil
}

func (l *FreeList) Reclaim(off, ver int64) error {
	//	defer func() {
	//		log.Printf("reclaim %x %d%v", off, ver, callers(1))
	//	}()

	if l.t == nil {
		return nil
	}

	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], uint64(off))
	binary.BigEndian.PutUint64(buf[8:], uint64(ver))

	if l.lock {
		l.deferred = append(l.deferred, kv{Key: buf[:8], Value: buf[8:16]})
		return nil
	}

	l.lock = true

	err := l.t.Put(buf[:8], buf[8:16])
	if err != nil {
		return err
	}

	return l.unlock()
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
		} else if l.flen < 1*GiB {
			l.flen += l.flen / 16
		} else {
			l.flen += 64 * MiB
		}
	}

	err := l.b.Truncate(l.flen)
	if err != nil {
		return err
	}

	return nil
}

func (l *FreeList) unlock() (err error) {
	log.Printf("unlock: %q   root %x\n%v%v", l.deferred, l.t.root, dumpFile(l.t.p), callers(0))

	for i := 0; i < len(l.deferred); i++ {
		kv := l.deferred[i]
		//	log.Printf("do deferred: %x -> %x on root %x ver %d\n%v", kv.Key, kv.Value, l.t.root, l.ver, dumpFile(l.t.p))
		if kv.Value == nil {
			err = l.t.Del(kv.Key)
		} else {
			err = l.t.Put(kv.Key, kv.Value)
		}
		if err != nil {
			return
		}
	}

	l.deferred = l.deferred[:0]
	l.lock = false

	return nil
}

func callers(skip int) string {
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
