package xrain

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"log"
	"strings"
	"sync"
)

const Version = "000"

var (
	DefaultPageSize int64 = 0x1000 // 4 KiB

	CRCTable = crc64.MakeTable(crc64.ECMA)
)

var (
	ErrPageChecksum        = errors.New("page checksum mismatch")
	ErrBucketAlreadyExists = errors.New("bucket already exists")
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
		batch *Batcher

		nb     NewBucketFunc
		NoSync bool

		page int64

		mu        sync.Mutex
		ver, verp int64
		keep      int64
		keepl     map[int64]int

		wmu sync.Mutex
	}

	Serializer interface {
		Serialize(p []byte) int
		Deserialize(p []byte) (int, error)
	}
)

func New(b Back, page int64) (*DB, error) {
	pl := NewFixedLayout(b, page, nil)
	t := NewTree(pl, 2*page, page)
	fl := NewFreelist2(b, t, 3*page, page)
	pl.SetFreelist(fl)

	return NewDB(b, page, t, fl)
}

func NewDB(b Back, page int64, t Tree, fl Freelist) (*DB, error) {
	d := &DB{
		b:     b,
		fl:    fl,
		t:     t,
		nb:    newBucket,
		page:  page,
		keepl: make(map[int64]int),
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

	d.batch = NewBatcher(&d.wmu, b.Sync)
	go d.batch.Run()

	return d, nil
}

func (d *DB) View(f func(tx *Tx) error) error {
	d.mu.Lock()
	tr := d.tr
	ver := d.ver
	d.keepl[ver]++
	//	tlog.Printf("View      %2d  %v", ver, d.keepl)
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		c := d.keepl[ver]
		if c == 1 {
			delete(d.keepl, ver)
		} else {
			d.keepl[ver]--
		}
		d.mu.Unlock()
	}()

	tx := newTx(d, tr, false)

	return f(&tx)
}

func (d *DB) Update(f func(tx *Tx) error) error {
	return d.update1(f)
}

func (d *DB) update0(f func(tx *Tx) error) (err error) {
	defer d.wmu.Unlock()
	d.wmu.Lock()

	d.mu.Lock()
	d.updateKeep()
	ver, keep := d.ver, d.keep
	d.mu.Unlock()
	ver++

	d.fl.SetVer(ver, keep)
	d.t.PageLayout().SetVer(ver)

	tx := newTx(d, d.t, true)

	err = f(&tx)
	if err != nil {
		return err
	}

	d.writeRoot(ver)

	tr := d.t.Copy()

	d.mu.Lock()
	d.ver++
	d.tr = tr
	d.mu.Unlock()

	if d.NoSync {
		return nil
	}

	err = d.b.Sync()
	if err != nil {
		return
	}

	return nil
}

func (d *DB) update1(f func(tx *Tx) error) (err error) {
	defer d.batch.Unlock()
	batch := d.batch.Lock()

	d.mu.Lock()
	d.updateKeep()
	d.verp++
	ver, keep := d.verp, d.keep
	d.mu.Unlock()

	d.fl.SetVer(ver, keep)
	d.t.PageLayout().SetVer(ver)

	tx := newTx(d, d.t, true)

	err = f(&tx)
	if err != nil {
		return err
	}

	//	tlog.Printf("Update %2d %2d  %2d\n%v", ver, keep, batch, dumpFile(d.t.PageLayout()))

	d.writeRoot(ver)

	err = d.batch.Wait(batch)
	if err != nil {
		return err
	}

	d.mu.Lock()
	//	tlog.Printf("Update %2d %2d  %2d exit", ver, d.ver, batch)
	if ver > d.ver {
		d.ver = ver
		d.tr = d.t.Copy()
	}
	d.mu.Unlock()

	return nil
}

func (d *DB) updateKeep() {
	min := d.ver
	for k := range d.keepl {
		if k < min {
			min = k
		}
	}
	d.keep = min
}

func (d *DB) writeRoot(ver int64) {
	n := ver % 2

	p := d.b.Access(n*d.page, d.page)

	binary.BigEndian.PutUint64(p[0x20:], uint64(ver))

	s := 0x30
	s += d.fl.Serialize(p[s:])
	s += d.t.Serialize(p[s:])
	_ = s

	binary.BigEndian.PutUint64(p[0x10:], 0)

	sum := crc64.Checksum(p, CRCTable)
	binary.BigEndian.PutUint64(p[0x10:], sum)

	d.b.Unlock(p)
}

func (d *DB) initEmpty() (err error) {
	if d.page == 0 {
		d.page = DefaultPageSize
	}

	d.fl.SetPageSize(d.page)
	d.t.SetPageSize(d.page)

	d.tr = d.t.Copy()

	err = d.b.Truncate(4 * d.page)
	if err != nil {
		return
	}

	h0 := fmt.Sprintf("xrain%3s%7x\n", Version, d.page)
	if len(h0) != 16 {
		panic(len(h0))
	}

	p := d.b.Access(0, 2*d.page)

	copy(p, h0)
	copy(p[d.page:], h0)

	for _, off := range []int64{0, d.page} {
		s := off + 0x18
		binary.BigEndian.PutUint64(p[s:], uint64(d.page))
	}

	d.b.Unlock(p)

	d.writeRoot(0)

	return d.b.Sync()
}

func (d *DB) initExisting() (err error) {
	if d.page == 0 {
		d.page = 0x100
	}

	var zeros [8]byte

again:
	retry := false
	p := d.b.Access(0, 2*d.page)
	func() {
		page := int64(binary.BigEndian.Uint64(p[0x18:]))
		if page != d.page {
			d.page = page
			retry = true
			return
		}

		d.ver = int64(binary.BigEndian.Uint64(p[0x20:]))
		if ver := int64(binary.BigEndian.Uint64(p[d.page+0x20:])); ver > d.ver {
			d.ver = ver
			p = p[d.page:]
		} else {
			p = p[:d.page]
		}

		d.verp = d.ver

		sum := crc64.Update(0, CRCTable, p[:0x10])
		sum = crc64.Update(sum, CRCTable, zeros[:])
		sum = crc64.Update(sum, CRCTable, p[0x18:])
		rsum := binary.BigEndian.Uint64(p[0x10:])
		if sum != rsum {
			err = ErrPageChecksum
			return
		}

		d.fl.SetPageSize(d.page)
		d.t.SetPageSize(d.page)

		// p is last root page
		s := 0x30
		ss, err := d.fl.Deserialize(p[s:])
		if err != nil {
			return
		}
		s += ss

		ss, err = d.t.Deserialize(p[s:])
		if err != nil {
			return
		}
		s += ss
		d.tr = d.t.Copy()

		_ = s
	}()
	d.b.Unlock(p)
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
		k := l.Key(off, i, nil)
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
	case *KVLayout:
		b = l.b
		base = &l.BaseLayout
		kvl = l
		page = l.page
	case *FixedLayout:
		b = l.b
		base = &l.BaseLayout
		fl = l
		page = l.page
	default:
		panic(fmt.Sprintf("layout type %T", l))
	}

	var buf bytes.Buffer

	var size, n int

	p := b.Access(off, page)
	{
		tp := 'B'
		if base.isleaf(p) {
			tp = 'D'
		}
		ver := base.getver(p)
		over := base.overflow(p)
		size = over + 1
		n = base.nkeys(p)
		fmt.Fprintf(&buf, "%4x: %c over %2d ver %3d  nkeys %4d  ", off, tp, over, ver, n)
		if kvl != nil {
			fmt.Fprintf(&buf, "datasize %3x free space %3x\n", kvl.pagedatasize(p, n), kvl.pagefree(p, n))
		} else {
			fmt.Fprintf(&buf, "datasize %3x free space %3x\n", n*16, len(p)-n*16-16)
		}
	}
	b.Unlock(p)
	if fl != nil {
		for i := 0; i < n; i++ {
			k := l.Key(off, i, nil)
			v := l.Int64(off, i)
			fmt.Fprintf(&buf, "    %2x -> %4x\n", k, v)
		}
	} else {
		if l.IsLeaf(off) {
			for i := 0; i < n; i++ {
				k := l.Key(off, i, nil)
				v := l.Value(off, i, nil)
				fmt.Fprintf(&buf, "    %q -> %q\n", k, v)
			}
		} else {
			for i := 0; i < n; i++ {
				k := l.Key(off, i, nil)
				v := l.Int64(off, i)
				fmt.Fprintf(&buf, "    %2x | %q -> %4x | %q\n", k, k, v, v)
			}
		}
	}

	return buf.String(), base.page * int64(size)
}

func dumpFile(l PageLayout) string {
	var b Back
	var page int64
	switch l := l.(type) {
	case *KVLayout:
		b = l.b
		page = l.page
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
		p := b.Access(0, 0x10)
		if bytes.HasPrefix(p, []byte("xrain")) {
			off = 2 * page
		}
		b.Unlock(p)
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
