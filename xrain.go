package xrain

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
)

const (
	DBHeaderSize = 0x20
	MinPageSize  = 0x40
)

type (
	Tx struct {
		writable bool
		db       *DB
		t        *tree
		err      error
	}

	DB struct {
		b Back

		page int64
		ver  int64
		root int64
		data int64
		free int64

		pbuf []byte
	}

	Config struct {
		PageSize int64
	}
)

func NewDB(b Back, c *Config) (*DB, error) {
	d := &DB{
		b: b,
	}
	if c != nil {
		if c.PageSize != 0 {
			if c.PageSize < 0x40 {
				return nil, errors.New("too small page size")
			}
			d.page = c.PageSize
		}
	}
	err := d.init()
	if err != nil {
		return nil, err
	}
	return d, nil
}

func (d *DB) init() error {
	l, err := d.b.Len()
	if err != nil {
		return err
	}
	if l == 0 {
		return d.initEmpty()
	} else {
		return d.initExisting()
	}
}

func (d *DB) initEmpty() error {
	if d.page == 0 {
		d.page = int64(os.Getpagesize())
	}

	p := make([]byte, d.page)
	for i := 1; i < 4; i++ {
		if err := d.b.Write(int64(i)*d.page, p); err != nil {
			return err
		}
	}

	str := fmt.Sprintf("xrain000 page %x\n", d.page)
	if len(str) > DBHeaderSize {
		panic("too big header")
	}
	copy(p, []byte(str))

	d.ver = 0
	d.free = 2 * d.page
	d.data = 3 * d.page

	d.pbuf = p
	d.root = d.page

	return d.swapRoot()
}

func (d *DB) initExisting() error {
	if d.page == 0 {
		d.page = MinPageSize
	}

again:
	p, err := d.b.Read(0, 2*d.page)
	if err != nil {
		return err
	}
	if !bytes.Equal(p[:5], []byte("xrain")) {
		return errors.New("not an xrain db file")
	}
	page := int64(binary.BigEndian.Uint64(p[DBHeaderSize:]))
	if page != d.page {
		d.page = page
		goto again
	}

	v1 := int64(binary.BigEndian.Uint64(p[DBHeaderSize+8:]))
	v2 := int64(binary.BigEndian.Uint64(p[d.page+DBHeaderSize+8:]))
	if v1 > v2 {
		d.root = 0
	} else {
		d.root = d.page
	}

	p, err = d.b.Read(d.root, d.page)
	if err != nil {
		return err
	}

	d.pbuf = p

	d.free = int64(binary.BigEndian.Uint64(p[DBHeaderSize+0x10:]))
	d.data = int64(binary.BigEndian.Uint64(p[DBHeaderSize+0x18:]))

	return nil
}

func (d *DB) swapRoot() error {
	d.root = d.page - d.root
	d.ver++

	binary.BigEndian.PutUint64(d.pbuf[DBHeaderSize:], uint64(d.page))
	binary.BigEndian.PutUint64(d.pbuf[DBHeaderSize+8:], uint64(d.ver))
	binary.BigEndian.PutUint64(d.pbuf[DBHeaderSize+0x10:], uint64(d.free))
	binary.BigEndian.PutUint64(d.pbuf[DBHeaderSize+0x18:], uint64(d.data))

	err := d.b.Write(d.root, d.pbuf)
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) View(f func(tx *Tx) error) error {
	a := NewNoAlloc(d.b, d.page)
	t, err := NewBPTree(d.data, a, BytesPage{a})
	if err != nil {
		return err
	}
	tx := &Tx{
		db: d,
		t:  t,
	}
	return f(tx)
}

func (d *DB) Update(f func(tx *Tx) error) error {
	a, err := NewTreeAlloc(d.b, d.page, d.free)
	if err != nil {
		return err
	}
	t, err := NewBPTree(d.data, a, BytesPage{a})
	if err != nil {
		return err
	}
	tx := &Tx{
		writable: true,
		db:       d,
		t:        t,
	}

	err = f(tx)

	log.Printf("ci tx")

	if err == nil {
		d.data, err = t.root, t.err
	}

	if err == nil {
		d.free, err = a.Commit()
	} else {
		_ = a.Abort()
	}

	if err == nil {
		err = d.swapRoot()
	}

	return err
}

func (d *DB) Close() error {
	return nil
}
