package xrain

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"log"
	"path"
	"runtime"
	"strings"
	"sync"
)

const Version = "000"

var (
	DefaultPageSize int64 = 0x1000 // 4 KiB

	CRCTable = crc64.MakeTable(crc64.ECMA)

	ErrPageChecksum = errors.New("page checksum mismatch")
)

/*
	Root page layout
00: xrainVVVPPPPPPP\n // VVV - Version, PPPPPPP - page size in hex
10: <crc64> <page>
20: <ver>   _
30: <freelist>
xx: <tree>
*/

type (
	DB struct {
		b     Back
		fl    Freelist
		t, tr Tree

		conf *Config

		page int64

		mu   sync.Mutex
		ver  int64
		keep int64

		wmu sync.Mutex
	}

	Config struct {
		PageSize int64

		Freelist   Freelist
		PageLayout PageLayout
		Tree       Tree
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
		d.conf = c
	}

	var err error
	if b.Size() == 0 {
		err = d.initEmpty()
	} else {
		err = d.initExisting()
	}
	if err != nil {
		return nil, err
	}

	return d, nil
}

func (d *DB) View(f func(tx *Tx) error) error {
	d.mu.Lock()
	tr := d.tr
	d.mu.Unlock()

	tx := newTx(d, tr, false)

	return f(tx)
}

func (d *DB) UpdateNoBatching(f func(tx *Tx) error) error {
	defer d.wmu.Unlock()
	d.wmu.Lock()

	d.mu.Lock()
	ver, keep := d.ver, d.keep
	d.mu.Unlock()
	ver++

	d.fl.SetVer(ver, keep)
	d.t.SetVer(ver)

	tx := newTx(d, d.t, true)

	err := f(tx)
	if err != nil {
		return err
	}

	d.writeRoot(ver)

	tr := tx.t.Copy()

	d.mu.Lock()
	d.ver++
	d.tr = tr
	d.mu.Unlock()

	return nil
}

func (d *DB) Update(f func(tx *Tx) error) error {
	return d.UpdateNoBatching(f)
}

func (d *DB) writeRoot(ver int64) {
	n := ver % 2

	d.b.Access(n*d.page, d.page, func(p []byte) {
		binary.BigEndian.PutUint64(p[0x20:], uint64(ver))

		s := 0x30
		s += Serialize(p[s:], d.fl)
		s += Serialize(p[s:], d.t)
		_ = s

		sum := crc64.Checksum(p[0x18:], CRCTable)
		binary.BigEndian.PutUint64(p[0x10:], sum)
	})
}

func (d *DB) initParts0() {
	if d.conf != nil && d.conf.Freelist != nil {
		d.fl = d.conf.Freelist
	} else {
		pl := NewFixedLayout(d.b, d.page, nil)
		tr := NewTree(pl, 2*d.page, d.page)
		d.fl = NewFreelist2(d.b, tr, 4*d.page, d.page)
		pl.SetFreelist(d.fl)
	}
}

func (d *DB) initParts1() {
	if d.conf != nil && d.conf.Tree != nil {
		d.t = d.conf.Tree
	} else {
		pl := NewFixedLayout(d.b, d.page, d.fl)
		d.t = NewTree(pl, 3*d.page, d.page)
		d.tr = d.t.Copy()
	}
}

func (d *DB) initEmpty() (err error) {
	if d.page == 0 {
		d.page = DefaultPageSize
	}

	err = d.b.Truncate(4 * d.page)
	if err != nil {
		return
	}

	d.initParts0()
	d.initParts1()

	h0 := fmt.Sprintf("xrain%3s%7x\n", Version, d.page)
	if len(h0) != 16 {
		panic(len(h0))
	}

	d.b.Access(0, 2*d.page, func(p []byte) {
		copy(p, h0)
		copy(p[d.page:], h0)

		for _, off := range []int64{0, d.page} {
			s := off + 0x18
			binary.BigEndian.PutUint64(p[s:], uint64(d.page))
		}
	})

	d.writeRoot(0)

	return d.b.Sync()
}

func (d *DB) initExisting() (err error) {
	if d.page == 0 {
		d.page = 0x100
	}

	d.initParts1()

again:
	retry := false
	d.b.Access(0, 2*d.page, func(p []byte) {
		d.ver = int64(binary.BigEndian.Uint64(p[0x20:]))
		if ver := int64(binary.BigEndian.Uint64(p[d.page+0x20:])); ver > d.ver {
			d.ver = ver
			p = p[d.page:]
		} else {
			p = p[:d.page]
		}

		page := int64(binary.BigEndian.Uint64(p[0x18:]))
		if page != d.page {
			d.page = page
			retry = true
			return
		}

		esum := crc64.Checksum(p[0x18:], CRCTable)
		sum := binary.BigEndian.Uint64(p[0x10:])
		if sum != esum {
			err = ErrPageChecksum
			return
		}

		// p is last root page
		s := 0x30
		ctx := &SerializeContext{Back: d.b, Page: d.page}

		var fl interface{}
		var ss int
		fl, ss, err = Deserialize(ctx, p[s:])
		if err != nil {
			return
		}
		d.fl = fl.(Freelist)
		ctx.Freelist = d.fl
		s += ss

		var tr interface{}
		tr, ss, err = Deserialize(ctx, p[s:])
		if err != nil {
			return
		}
		d.t = tr.(Tree)
		d.tr = d.t.Copy()
		s += ss

		_ = s
	})
	if retry {
		goto again
	}

	return
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

	_ = b.Sync()
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
	_ = b.Sync()
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
