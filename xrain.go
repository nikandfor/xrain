package xrain

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"path"
	"runtime"
	"strings"
	"sync"
)

var (
	DefaultPageSize int64 = 0x1000 // 4 KiB
)

type (
	DB struct {
		b  Back
		pl PageLayout

		page int64

		mu   sync.Mutex
		ver  int64
		keep int64
		root [2]rootpage

		wmu sync.Mutex
	}

	Tx struct {
		d        *DB
		t        *Tree
		writable bool
	}

	Config struct {
		PageSize int64
	}

	rootpage struct {
		data         int64
		free0, free1 int64
		next         int64
		datameta     treemeta
		free0meta    treemeta
		free1meta    treemeta
	}

	treemeta struct {
		n        int64
		depth    byte
		_        [3]byte
		pages    int32
		datasize int64
	}
)

func NewDB(b Back, c *Config) (*DB, error) {
	d := &DB{
		b: b,
	}

	if c != nil {
		if c.PageSize != 0 {
			if (c.PageSize-1)&c.PageSize != 0 {
				return nil, errors.New("page size must be power of 2")
			}
			if c.PageSize < 0x100 {
				return nil, errors.New("too small page size")
			}
			d.page = c.PageSize
		}
	}

	if b.Size() == 0 {
		err := d.initEmpty()
		if err != nil {
			return nil, err
		}
	} else {
		d.initExisting()
	}

	return d, nil
}

func (d *DB) View(f func(tx *Tx) error) error {
	d.mu.Lock()
	ver := d.ver
	rp := d.root[ver%2]
	d.mu.Unlock()

	kvl := NewFixedLayout(d.b, d.page, ver, nil)

	t := NewTree(kvl, rp.data, d.page)
	t.meta = &rp.datameta

	tx := &Tx{
		t: t,
	}

	err := f(tx)
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) UpdateNoBatching(f func(tx *Tx) error) error {
	defer d.wmu.Unlock()
	d.wmu.Lock()

	d.mu.Lock()

	ver := d.ver + 1
	rp := &d.root[ver%2]

	*rp = d.root[(ver-1)%2]

	d.mu.Unlock()

	fpl0 := NewFixedLayout(d.b, d.page, ver, nil)
	fpl0.meta = &rp.free0meta
	fpl1 := NewFixedLayout(d.b, d.page, ver, nil)
	fpl1.meta = &rp.free0meta

	f0 := NewTree(fpl0, rp.free0, d.page)
	f1 := NewTree(fpl1, rp.free1, d.page)

	f0.meta = &rp.free0meta
	f1.meta = &rp.free1meta

	fl := NewTreeFreelist(d.b, f0, f1, rp.next, d.page, d.keep)
	fpl0.SetFreeList(fl)
	fpl1.SetFreeList(fl)

	kvl := NewFixedLayout(d.b, d.page, ver, fl)
	kvl.meta = &rp.datameta

	t := NewTree(kvl, rp.data, d.page)
	t.meta = &rp.datameta

	tx := &Tx{
		t:        t,
		writable: true,
	}

	err := f(tx)
	if err != nil {
		return err
	}

	rp.data = t.root
	rp.free0 = f0.root
	rp.free1 = f1.root
	rp.next = fl.next

	d.writeRoot(ver)

	err = d.b.Sync()
	if err != nil {
		return err
	}

	d.mu.Lock()
	d.ver++
	d.mu.Unlock()

	return nil
}

func (d *DB) Update(f func(tx *Tx) error) error {
	return nil
}

func (d *DB) writeRoot(ver int64) {
	n := ver % 2
	rp := &d.root[n]

	d.b.Access(n*d.page, d.page, func(p []byte) {
		s := 0x10
		binary.BigEndian.PutUint64(p[s:], uint64(d.page))
		s += 0x8
		binary.BigEndian.PutUint64(p[s:], uint64(ver))
		s += 0x8
		binary.BigEndian.PutUint64(p[s:], uint64(rp.next))
		s += 0x8
		binary.BigEndian.PutUint64(p[s:], uint64(rp.data))
		s += 0x8
		binary.BigEndian.PutUint64(p[s:], uint64(rp.free0))
		s += 0x8
		binary.BigEndian.PutUint64(p[s:], uint64(rp.free1))
		s += 0x8

		for _, m := range []*treemeta{&rp.datameta, &rp.free0meta, &rp.free1meta} {
			binary.BigEndian.PutUint64(p[s:], uint64(m.n))
			s += 0x8
			p[s] = byte(m.depth)
			binary.BigEndian.PutUint32(p[s+4:], uint32(m.pages))
			s += 0x8
			binary.BigEndian.PutUint64(p[s:], uint64(m.datasize))
			s += 0x8
		}
	})
}

func (d *DB) initEmpty() (err error) {
	if d.page == 0 {
		d.page = DefaultPageSize
	}

	err = d.b.Truncate(8 * d.page)
	if err != nil {
		return
	}

	h0 := fmt.Sprintf("xrain000%7x\n", d.page)
	if len(h0) != 16 {
		panic(len(h0))
	}

	d.b.Access(0, 2*d.page, func(p []byte) {
		copy(p, h0)
		copy(p[d.page:], h0)

		for i := 0; i < 2; i++ {
			rp := &d.root[i]
			rp.data = 2 * d.page
			rp.free0 = 3 * d.page
			rp.free1 = 4 * d.page
			rp.next = 5 * d.page

			for _, m := range []*treemeta{&rp.datameta, &rp.free0meta, &rp.free1meta} {
				m.depth = 1
				m.pages = 1
			}
		}
	})

	d.writeRoot(0)

	return d.b.Sync()
}

func (d *DB) initExisting() {
	if d.page == 0 {
		d.page = 0x100
	}

again:
	retry := false
	d.b.Access(0, 2*d.page, func(p []byte) {
		page := int64(binary.BigEndian.Uint64(p[0x10:]))
		if page != d.page {
			d.page = page
			retry = true
			return
		}

		for _, off := range []int64{0, d.page} {
			s := off + 0x10 + 0x8 // header + page
			ver := int64(binary.BigEndian.Uint64(p[s:]))
			s += 0x8
			if ver > d.ver {
				d.ver = ver
			}
			rp := &d.root[off/d.page]
			rp.next = int64(binary.BigEndian.Uint64(p[s:]))
			s += 0x8
			rp.data = int64(binary.BigEndian.Uint64(p[s:]))
			s += 0x8
			rp.free0 = int64(binary.BigEndian.Uint64(p[s:]))
			s += 0x8
			rp.free1 = int64(binary.BigEndian.Uint64(p[s:]))
			s += 0x8

			for _, m := range []*treemeta{&rp.datameta, &rp.free0meta, &rp.free1meta} {
				m.n = int64(binary.BigEndian.Uint64(p[s:]))
				s += 0x8
				m.depth = p[s]
				m.pages = int32(binary.BigEndian.Uint32(p[s+4:]))
				s += 0x8
				m.datasize = int64(binary.BigEndian.Uint64(p[s:]))
				s += 0x8
			}
		}
	})
	if retry {
		goto again
	}
}

//
func checkPage(l PageLayout, off int64) {
	n := l.NKeys(off)
	var prev []byte
	for i := 0; i < n; i++ {
		k := l.Key(off, i)
		if bytes.Compare(prev, k) != -1 {
			log.Fatalf("at page %x of size %d  %2x goes before %2x", off, n, prev, k)
		}
		prev = k
	}
}

func checkFile(l PageLayout) {
	var b Back
	var page int64
	switch l := l.(type) {
	//	case LogLayout:
	//		checkFile(l.PageLayout)
	//		return
	//	case *KVLayout:
	//		b = l.b
	//		page = l.page
	case *FixedLayout:
		b = l.b
		page = l.p
	default:
		panic(fmt.Sprintf("layout type %T", l))
	}

	b.Sync()
	sz := b.Size()
	for off := int64(0); off < sz; off += page {
		checkPage(l, off)
	}
}

func dumpPage(l PageLayout, off int64) (string, int64) {
	var b Back
	var base *BaseLayout
	var kvl *KVLayout
	var fl *FixedLayout
	var page int64
	switch l := l.(type) {
	//	case *KVLayout:
	//		b = l.b
	//		base = &l.BaseLayout
	//		kvl = l
	//		page = l.page
	case *FixedLayout:
		b = l.b
		base = &l.BaseLayout
		fl = l
		page = l.page
	default:
		panic(fmt.Sprintf("layout type %T", l))
	}

	var buf bytes.Buffer

	var size int

	b.Access(off, page, func(p []byte) {
		tp := 'B'
		if l.IsLeaf(off) {
			tp = 'D'
		}
		ver := base.getver(p)
		size = base.extended(p)
		n := l.NKeys(off)
		fmt.Fprintf(&buf, "%4x: %c ext %2d ver %3d  nkeys %4d  ", off, tp, size, ver, n)
		if kvl != nil {
			//	fmt.Fprintf(&buf, "datasize %3x free space %3x\n", kvl.datasize(p), len(p)-kvl.datasize(p)-16)
		} else {
			fmt.Fprintf(&buf, "datasize %3x free space %3x\n", n*16, len(p)-n*16-16)
		}
		if fl != nil {
			for i := 0; i < n; i++ {
				k := l.Key(off, i)
				v := l.Int64(off, i)
				fmt.Fprintf(&buf, "    %2x -> %4x\n", k, v)
			}
		} else {
			if l.IsLeaf(off) {
				for i := 0; i < n; i++ {
					k := l.Key(off, i)
					v := l.Value(off, i)
					fmt.Fprintf(&buf, "    %q -> %q\n", k, v)
				}
			} else {
				for i := 0; i < n; i++ {
					k := l.Key(off, i)
					v := l.Int64(off, i)
					fmt.Fprintf(&buf, "    %4x <- % 2x (%q)\n", v, k, k)
				}
			}
		}
	})

	return buf.String(), base.page * int64(size)
}

func dumpFile(l PageLayout) string {
	var b Back
	var page int64
	switch l := l.(type) {
	//	case *KVLayout:
	//		b = l.b
	//		page = l.page
	case *FixedLayout:
		b = l.b
		page = l.page
	default:
		panic(fmt.Sprintf("layout type %T", l))
	}

	var buf strings.Builder
	b.Sync()
	sz := b.Size()
	off := int64(0)
	if sz > 0 {
		b.Access(0, 0x10, func(p []byte) {
			if bytes.HasPrefix(p, []byte("xrain")) {
				off = 2 * page
			}
		})
	}

	for off < sz {
		s, l := dumpPage(l, off)
		buf.WriteString(s)
		off += l
	}
	return buf.String()
}

func assert0(c bool, f string, args ...interface{}) {
	if c {
		return
	}

	panic(fmt.Sprintf(f, args...))
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
