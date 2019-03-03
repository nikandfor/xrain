package xrain

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
)

var (
	DefaultPageSize int64 = 0x1000 // 4 KiB
)

type (
	DB struct {
		b  Back
		pl PageLayout

		page int64

		ver  int64
		keep int64
		root [2]rootpage
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
	ver := d.ver
	rp := &d.root[ver%2]

	kvl := &KVLayout{BaseLayout: BaseLayout{
		b:    d.b,
		page: d.page,
		ver:  ver,
	}}

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
	ver := d.ver + 1
	rp := &d.root[ver%2]
	rp1 := &d.root[(ver+1)%2]

	fpl0 := &IntLayout{BaseLayout: BaseLayout{
		b:    d.b,
		page: d.page,
		ver:  ver,
		meta: &rp.free0meta,
	}}
	fpl1 := &IntLayout{BaseLayout: BaseLayout{
		b:    d.b,
		page: d.page,
		ver:  ver,
		meta: &rp.free1meta,
	}}

	f0 := NewTree(fpl0, rp.free0, d.page)
	f1 := NewTree(fpl1, rp.free1, d.page)

	f0.meta = &rp.free0meta
	f1.meta = &rp.free1meta

	fl := NewFreeList(f0, f1, rp1.next, d.page, ver, d.keep, d.b)
	fpl0.free = fl
	fpl1.free = fl

	kvl := &KVLayout{BaseLayout: BaseLayout{
		b:    d.b,
		page: d.page,
		ver:  ver,
		free: fl,
	}}

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

	d.ver++

	d.writeRoot()

	return d.b.Sync()
}

func (d *DB) writeRoot() {
	n := d.ver % 2
	rp := &d.root[d.ver%2]

	p := d.b.Load(n*d.page, d.page)

	s := 0x10
	binary.BigEndian.PutUint64(p[s:], uint64(d.page))
	s += 0x8
	binary.BigEndian.PutUint64(p[s:], uint64(d.ver))
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

	log.Printf("s %x", s)
}

func (d *DB) initEmpty() (err error) {
	if d.page == 0 {
		d.page = DefaultPageSize
	}

	err = d.b.Truncate(8 * d.page)
	if err != nil {
		return
	}

	p := d.b.Load(0, 2*d.page)

	h0 := fmt.Sprintf("xrain000%7x\n", d.page)
	if len(h0) != 16 {
		panic(len(h0))
	}

	copy(p, h0)
	copy(p[d.page:], h0)

	for i := 0; i < 2; i++ {
		rp := &d.root[i]
		rp.data = 2 * d.page
		rp.free0 = 3 * d.page
		rp.free1 = 4 * d.page
		rp.next = 5 * d.page
	}

	d.writeRoot()

	return d.b.Sync()
}

func (d *DB) initExisting() {
	if d.page == 0 {
		d.page = 0x100
	}

again:
	p := d.b.Load(0, 2*d.page)

	page := int64(binary.BigEndian.Uint64(p[0x10:]))
	if page != d.page {
		d.page = page
		goto again
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
}

func (d *DB) init(p []byte) (err error) {

	return
}

func assert_(c bool, f string, args ...interface{}) {
	if c {
		return
	}

	panic(fmt.Sprintf(f, args...))
}
