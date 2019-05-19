package xrain

import (
	"encoding/binary"
	"errors"
)

var ErrBucketAlreadyExists = errors.New("bucket already exists")

type (
	Tx struct {
		d *DB
		SimpleBucket
		writable bool
	}

	SimpleBucket struct {
		tx   *Tx
		name string
		par  *SimpleBucket
		t    Tree
		root int64
		sub  map[string]*SimpleBucket
		del  bool
	}
)

func newTx(d *DB, t Tree, w bool) *Tx {
	tx := &Tx{
		d: d,
		SimpleBucket: SimpleBucket{
			t:    t,
			root: t.Root(),
		},
		writable: w,
	}
	tx.tx = tx

	return tx
}

func (b *SimpleBucket) Put(k, v []byte) error {
	if !b.allowed(true) {
		panic("not allowed")
	}

	_, err := b.t.Put(k, v)
	if err != nil {
		return err
	}

	return b.propagate()
}

func (b *SimpleBucket) Get(k []byte) []byte {
	if !b.allowed(false) {
		panic("not allowed")
	}

	return b.t.Get(k)
}

func (b *SimpleBucket) Del(k []byte) error {
	if !b.allowed(true) {
		panic("not allowed")
	}

	_, err := b.t.Del(k)
	if err != nil {
		return err
	}

	return b.propagate()
}

func (b *SimpleBucket) Next(k []byte) []byte {
	if !b.allowed(false) {
		panic("not allowed")
	}

	return b.t.Next(k)
}

func (b *SimpleBucket) Prev(k []byte) []byte {
	if !b.allowed(false) {
		panic("not allowed")
	}

	return b.t.Prev(k)
}

func (b *SimpleBucket) Bucket(k []byte) *SimpleBucket {
	if !b.allowed(false) {
		panic("not allowed")
	}

	n := string(k)
	if len(b.sub) != 0 {
		if sub, ok := b.sub[n]; ok {
			return sub
		}
	}

	v := b.t.Get(k)
	if v == nil {
		return nil
	}

	off := int64(binary.BigEndian.Uint64(v))

	t := b.t.Copy()
	t.SetRoot(off)

	sub := &SimpleBucket{
		tx:   b.tx,
		name: n,
		par:  b,
		t:    t,
		root: off,
	}

	if b.sub == nil {
		b.sub = make(map[string]*SimpleBucket)
	}
	b.sub[n] = sub

	return sub
}

func (b *SimpleBucket) PutBucket(k []byte) (*SimpleBucket, error) {
	if !b.allowed(false) {
		panic("not allowed")
	}

	n := string(k)
	if len(b.sub) != 0 {
		if _, ok := b.sub[n]; ok {
			return nil, ErrBucketAlreadyExists
		}
	}

	v := b.t.Get(k)
	if v != nil {
		return nil, ErrBucketAlreadyExists
	}

	t := b.t.Copy()
	pl := t.PageLayout()
	off, err := pl.Alloc(true)
	if err != nil {
		return nil, err
	}
	t.SetRoot(off)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(off))

	_, err = b.t.Put(k, buf[:])
	if err != nil {
		return nil, err
	}
	err = b.propagate()
	if err != nil {
		return nil, err
	}

	sub := &SimpleBucket{
		tx:   b.tx,
		name: n,
		par:  b,
		t:    t,
		root: off,
	}

	if b.sub == nil {
		b.sub = make(map[string]*SimpleBucket)
	}
	b.sub[n] = sub

	return sub, nil
}

func (b *SimpleBucket) DelBucket(k []byte) error {
	if !b.allowed(false) {
		panic("not allowed")
	}

	if len(b.sub) != 0 {
		if sub, ok := b.sub[string(k)]; ok {
			sub.del = true
		}
	}

	v := b.t.Get(k)
	if v == nil {
		return nil
	}

	pl := b.t.PageLayout()
	off := int64(binary.BigEndian.Uint64(v))

	err := pl.Free(off, true)
	if err != nil {
		return err
	}

	_, err = b.t.Del(k)
	if err != nil {
		return err
	}
	err = b.propagate()
	if err != nil {
		return err
	}

	return nil
}

func (b *SimpleBucket) propagate() error {
	root := b.t.Root()
	//	log.Printf("propagate bucket %s (par %p)  %x <- %x", b.name, b.par, root, b.root)
	if b.par == nil || root == b.root {
		return nil
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(root))
	err := b.par.Put([]byte(b.name), buf[:])
	if err != nil {
		return err
	}

	b.root = root

	return nil
}

func (b *SimpleBucket) allowed(w bool) bool {
	return !b.del && (b.tx.writable || !w)
}
