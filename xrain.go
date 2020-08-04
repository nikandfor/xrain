package xrain

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"sync"

	"github.com/nikandfor/tlog"
)

const Version = "000"

var (
	DefaultPageSize int64 = 4 * KB

	CRCTable = crc64.MakeTable(crc64.ECMA)

	zeros [8]byte

	tl *tlog.Logger = tlog.DefaultLogger // test logger
)

var ( // errors
	ErrPageChecksum = errors.New("page checksum mismatch")
)

/*
	Root page layout

	00: xrainVVVPPPPPPP\n // VVV - Version, PPPPPPP - page size in hex
	10: <crc64> <page>
	20: <ver>   <root>
	30: <metalen> <meta>
	40: ...
*/

type (
	DB struct {
		b     Back
		batch *Batcher

		l    Layout
		root int64
		Meta

		metaLayout *SubpageLayout

		NoSync bool

		mu    sync.Mutex
		okver int64
		keepl map[int64]int

		safe, write int64

		wmu sync.Mutex
	}

	Meta struct {
		Back

		Page, Mask int64
		Ver, Keep  int64

		Freelist

		Meta *LayoutShortcut
	}

	Serializer interface {
		Serialize(p []byte) int
		Deserialize(p []byte) (int, error)
	}
)

func NewDB(b Back, page int64, l Layout) (_ *DB, err error) {
	if page&(page-1) != 0 || page != 0 && page < 0x100 {
		panic(page)
	}

	if l == nil {
		l = NewKVLayout2(nil)
	}

	d := &DB{
		b: b,
		l: l,
		Meta: Meta{
			Back: b,
			Page: page,
			Mask: page - 1,
			Keep: -1,
		},
		keepl: make(map[int64]int),
	}

	// root = 4 * page

	fll := NewFixedLayout(nil)
	d.Freelist, err = NewFreelist2(nil, fll, 5*page, 6*page)
	if err != nil {
		return
	}

	d.metaLayout = NewSubpageLayout(nil)

	if b.Size() == 0 {
		err = d.initEmpty()
	} else {
		err = d.initExisting()
	}
	if err != nil {
		return nil, err
	}

	d.Meta.Meta = NewLayoutShortcut(d.metaLayout, 0, d.Mask)

	l.SetMeta(&d.Meta)
	d.Freelist.SetMeta(&d.Meta)

	d.batch = NewBatcher(&d.wmu, d.sync)
	go d.batch.Run()

	return d, nil
}

func (d *DB) View(f func(tx *Tx) error) error {
	d.mu.Lock()
	root := d.root
	ver := d.okver
	d.keepl[ver]++
	//	tlog.Printf("View      %2d  %v", ver, d.keepl)
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		d.keepl[ver]--
		if d.keepl[ver] == 0 {
			delete(d.keepl, ver)
		}
		d.mu.Unlock()
	}()

	tx := newTx(d, d.l, root, false)

	return f(&tx)
}

func (d *DB) Update(f func(tx *Tx) error) error {
	if d.NoSync {
		return d.update0(f)
	} else {
		return d.update1(f)
	}
}

// synchronized calls
func (d *DB) update0(f func(tx *Tx) error) (err error) {
	defer d.wmu.Unlock()
	d.wmu.Lock()

	d.mu.Lock()
	d.updateKeep()
	d.Ver++
	write := d.write
	d.mu.Unlock()

	tx := newTx(d, d.l, d.root, true)

	err = f(&tx)
	if err != nil {
		return err
	}

	err = d.writeRoot(write, d.Ver, tx.oldroot)
	if err != nil {
		return
	}

	d.mu.Lock()
	d.okver = d.Ver
	d.root = tx.oldroot
	d.write++
	d.safe++
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

// synchronized updates, parallel Sync
func (d *DB) update1(f func(tx *Tx) error) (err error) {
	defer d.batch.Unlock()
	batch := d.batch.Lock()

	d.mu.Lock()
	d.updateKeep()
	d.Ver++
	write := d.write
	d.mu.Unlock()

	tx := newTx(d, d.l, d.root, true)

	err = f(&tx)
	if err != nil {
		return err
	}

	//	tlog.Printf("Update %2d %2d  %2d\n%v", ver, keep, batch, dumpFile(d.t.PageLayout()))

	err = d.writeRoot(write, d.Ver, tx.oldroot)
	if err != nil {
		return
	}

	d.mu.Lock()
	d.okver = d.Ver
	d.root = tx.oldroot
	d.mu.Unlock()

	err = d.batch.Wait(batch)
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) sync() error {
	d.mu.Lock()
	d.write++
	d.mu.Unlock()

	err := d.b.Sync()
	if err != nil {
		return err
	}

	d.mu.Lock()
	d.safe++
	d.mu.Unlock()

	return nil
}

func (d *DB) updateKeep() {
	min := d.okver
	for k := range d.keepl {
		if k < min {
			min = k
		}
	}
	d.Keep = min
}

func (d *DB) writeRoot(writepage, ver, root int64) (err error) {
	n := writepage & 0x3

	m := d.metaLayout.Bytes()

	if len(m) > int(d.Page)-0x38 {
		panic("fix me")
	}

	p := d.b.Access(n*d.Page, d.Page)

	binary.BigEndian.PutUint64(p[0x20:], uint64(ver))
	binary.BigEndian.PutUint64(p[0x28:], uint64(root))

	binary.BigEndian.PutUint64(p[0x30:], uint64(len(m)))
	copy(p[0x38:], m)

	//

	binary.BigEndian.PutUint64(p[0x10:], 0)

	sum := crc64.Checksum(p, CRCTable)
	binary.BigEndian.PutUint64(p[0x10:], sum)

	d.b.Unlock(p)

	return
}

func (d *DB) initEmpty() (err error) {
	if d.Page == 0 {
		d.Page = DefaultPageSize
	}

	d.root = 4 * d.Page

	err = d.b.Truncate(6 * d.Page)
	if err != nil {
		return
	}

	h0 := fmt.Sprintf("xrain%3s%7x\n", Version, d.Page)
	if len(h0) != 16 {
		panic(len(h0))
	}

	p := d.b.Access(0, 4*d.Page)

	for i := 0; i < 4; i++ {
		off := int64(i) * d.Page

		copy(p[off:], h0)

		s := off + 0x18
		binary.BigEndian.PutUint64(p[s:], uint64(d.Page))
	}

	d.b.Unlock(p)

	err = d.writeRoot(0, 0, 0)
	if err != nil {
		return
	}

	return d.b.Sync()
}

func (d *DB) initExisting() (err error) {
	if d.Page == 0 {
		d.Page = 0x100
	}

again:
	retry := false

	func() {
		p := d.b.Access(0, 4*d.Page)
		defer d.b.Unlock(p)

		page := int64(binary.BigEndian.Uint64(p[0x18:]))
		if page != d.Page {
			d.Page = page
			retry = true
			return
		}

		d.Ver = 0
		var latest int64
		for i := 0; i < 4; i++ {
			off := int64(i) * d.Page

			ver := int64(binary.BigEndian.Uint64(p[off+0x20:]))
			if ver > d.Ver {
				latest = off
				d.Ver = ver
				d.safe = int64(i)
				d.write = d.safe + 1
			}
		}
		p = p[latest : latest+d.Page]

		d.okver = d.Ver
		d.Keep = d.Ver

		sum := crc64.Update(0, CRCTable, p[:0x10])
		sum = crc64.Update(sum, CRCTable, zeros[:])
		sum = crc64.Update(sum, CRCTable, p[0x18:])
		rsum := binary.BigEndian.Uint64(p[0x10:])
		if sum != rsum {
			err = ErrPageChecksum
			return
		}

		d.Mask = d.Page - 1

		d.root = int64(binary.BigEndian.Uint64(p[0x28:]))
		metalen := int64(binary.BigEndian.Uint64(p[0x30:]))

		m := make([]byte, metalen)
		copy(m, p[0x38:0x38+metalen])

		d.metaLayout.SetBytes(m)
	}()

	if retry {
		goto again
	}

	return
}

/*

func checkPage(l PageLayout, off int64) {
	n := l.NKeys(off)
	var prev []byte
	for i := 0; i < n; i++ {
		k, _ := l.Key(off, i, nil)
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
	for off := 4 * page; off < sz; off += page {
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
			k, _ := l.Key(off, i, nil)
			v := l.Int64(off, i)
			fmt.Fprintf(&buf, "    %2x -> %4x\n", k, v)
		}
	} else {
		if l.IsLeaf(off) {
			for i := 0; i < n; i++ {
				k, F := l.Key(off, i, nil)
				v := l.Value(off, i, nil)
				fmt.Fprintf(&buf, "    %q %x -> %q\n", k, F, v)
			}
		} else {
			for i := 0; i < n; i++ {
				k, _ := l.Key(off, i, nil)
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
			off = 4 * page
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
*/
