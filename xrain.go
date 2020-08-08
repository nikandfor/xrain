package xrain

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
	"testing"

	"github.com/nikandfor/tlog"
)

const Version = "000"

var (
	DefaultPageSize int64 = 4 * KB

	//	CRCTable = crc64.MakeTable(crc64.ECMA)
	//	CRCTable = crc32.IEEETable
	//	Checksum = crc32.ChecksumIEEE
	ChecksumUpdate = func(s uint32, p []byte) uint32 {
		return crc32.Update(s, crc32.IEEETable, p)
	}

	zeros [8]byte

	tl *tlog.Logger // = tlog.DefaultLogger // test logger
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

		metaLayout SubpageLayout

		NoSync bool

		mu    sync.Mutex
		okver int64
		keepl map[int64]int

		safe, write int64

		kRTx     []byte
		kRWTx    []byte
		kRTxErr  []byte
		kRWTxErr []byte

		stbuf Stack

		wmu sync.Mutex
	}

	Meta struct {
		Back

		Page, Mask int64
		Ver, Keep  int64

		Freelist

		Meta LayoutShortcut
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
		},
		keepl:    make(map[int64]int),
		kRTx:     []byte("stats.rtx"),
		kRWTx:    []byte("stats.rwtx"),
		kRTxErr:  []byte("stats.rtxerr"),
		kRWTxErr: []byte("stats.rwtxerr"),
	}

	// root = NilPage

	d.metaLayout = NewSubpageLayout(nil)

	if b.Size() == 0 {
		err = d.initEmpty()
	} else {
		err = d.initExisting()
	}
	if err != nil {
		return nil, err
	}

	d.Meta.Meta = NewLayoutShortcut(&d.metaLayout, 0, d.Mask)

	//	fll := NewFixedLayout(&d.Meta)
	//	d.Freelist, err = NewFreelist2(&d.Meta, fll, 5*d.Page, 6*d.Page)
	//	if err != nil {
	//		return
	//	}
	d.Freelist = NewFreelist3(&d.Meta, 4*d.Page)

	l.SetMeta(&d.Meta)

	d.batch = NewBatcher(&d.wmu, d.sync)
	go d.batch.Run()

	return d, nil
}

func (d *DB) View(f func(tx *Tx) error) (err error) {
	d.mu.Lock()
	ver := d.okver
	d.keepl[ver]++
	//	tlog.Printf("View      %2d  %v", ver, d.keepl)

	l, root := d.rootBucket()
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		d.keepl[ver]--
		if d.keepl[ver] == 0 {
			delete(d.keepl, ver)
		}
		d.mu.Unlock()
	}()

	tx := newTx(d, l, root, false)

	err = f(tx)

	tx.free()

	d.mu.Lock()
	d.metric(d.kRTx, 1)
	if err != nil {
		d.metric(d.kRTxErr, 1)
	}
	d.mu.Unlock()

	return err
}

func (d *DB) Update(f func(tx *Tx) error) (err error) {
	if d.NoSync {
		err = d.update0(f)
	} else {
		err = d.update1(f)
	}

	d.mu.Lock()
	d.metric(d.kRWTx, 1)
	if err != nil {
		d.metric(d.kRWTxErr, 1)
	}
	d.mu.Unlock()

	return
}

// synchronized calls
func (d *DB) update0(f func(tx *Tx) error) (err error) {
	defer d.wmu.Unlock()
	d.wmu.Lock()

	d.mu.Lock()
	d.updateKeep()
	d.Ver++
	write := d.write

	l, root := d.rootBucket()
	d.mu.Unlock()

	tx := newTx(d, l, root, true)

	err = f(tx)
	if err != nil {
		return err
	}

	tx.free()

	err = d.writeRoot(write, d.Ver, tx.rootbuf)
	if err != nil {
		return
	}

	if !d.NoSync {
		err = d.b.Sync()
		if err != nil {
			return
		}
	}

	d.mu.Lock()
	d.okver = d.Ver
	d.root = tx.rootbuf
	d.safe = write
	d.write = d.safe + 1
	d.mu.Unlock()

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

	l, root := d.rootBucket()
	d.mu.Unlock()

	tx := newTx(d, l, root, true)

	err = f(tx)
	if err != nil {
		return err
	}

	d.mu.Lock()
	tx.free()

	//	tlog.Printf("Update %2d %2d  %2d\n%v", ver, keep, batch, dumpFile(d.t.PageLayout()))

	err = d.writeRoot(write, d.Ver, tx.rootbuf)
	d.mu.Unlock()
	if err != nil {
		return
	}

	d.mu.Lock()
	d.okver = d.Ver
	d.root = tx.rootbuf
	d.mu.Unlock()

	err = d.batch.Wait(batch)
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) rootBucket() (Layout, int64) {
	if d.root != NilPage {
		return d.l, d.root
	}

	v := bsPool.Get().([]byte)[:0]

	var eq bool
	d.stbuf, eq = d.Meta.Meta.Layout.Seek(d.stbuf, 0, []byte("root"), nil)
	if eq {
		v = d.Meta.Meta.Layout.Value(d.stbuf, v)
	}

	l := NewSubpageLayout(v)

	return &l, 0
}

func (d *DB) releaseRootBucket(l Layout, root int64) {
}

func (d *DB) metric(k []byte, v int) {
	_, d.stbuf, _ = d.Meta.Meta.AddInt64(k, int64(v), d.stbuf)
}

func (d *DB) sync() error {
	err := d.b.Sync()
	if err != nil {
		return err
	}

	d.mu.Lock()
	d.safe++
	d.write = d.safe + 1
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

	if tl.V("dbroot") != nil {
		tl.Printf("dbroot root %5x  ver %5x  wrpage %5x %x  safe %5x", root, ver, writepage, writepage&0x3, d.safe)
	}

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

	sum := ChecksumUpdate(0, p)
	binary.BigEndian.PutUint32(p[0x10:], sum)

	d.b.Unlock(p)

	return
}

func (d *DB) initEmpty() (err error) {
	if d.Page == 0 {
		d.Page = DefaultPageSize
	}

	d.root = NilPage
	d.Mask = d.Page - 1

	err = d.b.Truncate(4 * d.Page)
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

		d.Mask = d.Page - 1

		sum := ChecksumUpdate(0, p[:0x10])
		sum = ChecksumUpdate(sum, zeros[:8])
		sum = ChecksumUpdate(sum, p[0x18:])

		rsum := binary.BigEndian.Uint32(p[0x10:])
		if sum != rsum {
			err = ErrPageChecksum
			return
		}

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

func DumpDB(d *DB) string {
	return fmt.Sprintf("root %6x page %5x %5x ver %6x %6x okver %6x", d.root, d.Page, d.Mask, d.Ver, d.Keep, d.okver)
}

func DumpBucket(b *SimpleBucket) string {
	return fmt.Sprintf("root %6x %6x name %q", b.rootbuf, b.t.Root, b.name)
}

func DumpPage(l Layout, off int64) string {
	d, ok := l.(pageDumper)
	if !ok {
		return ""
	}

	return d.dumpPage(off)
}

func InitTestLogger(t testing.TB, v string, tostderr bool) *tlog.Logger {
	tl = tlog.NewTestLogger(t, v, tostderr)
	return tl
}
