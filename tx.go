package xrain

import (
	"encoding/binary"
	"errors"
)

type (
	Tx struct {
		d *DB
		*SimpleBucket
		writable bool
	}

	SimpleBucket struct {
		tx    *Tx
		name  []byte
		par   *SimpleBucket
		l     Layout
		c     Common
		t     *LayoutShortcut
		root  int64
		sub   map[string]*SimpleBucket
		del   bool
		flags FlagsSupported
	}
)

var (
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrTypeMismatch        = errors.New("value type mismatch")
)

func newTx(d *DB, l Layout, root int64, w bool) Tx {
	tx := Tx{
		d:        d,
		writable: w,
	}
	tx.SimpleBucket = newBucket(&tx, l, root, nil, nil)

	return tx
}

func newBucket(tx *Tx, l Layout, root int64, name []byte, par *SimpleBucket) *SimpleBucket {
	ff, _ := l.(FlagsSupported)

	return &SimpleBucket{
		tx:    tx,
		name:  name,
		par:   par,
		l:     l,
		t:     NewLayoutShortcut(l, root, tx.c.Mask),
		root:  root,
		flags: ff,
	}
}

func (b *SimpleBucket) Put(k, v []byte) error {
	return b.put(0, k, v)
}

func (b *SimpleBucket) put(F int, k, v []byte) error {
	if !b.allowed(true) {
		panic("not allowed")
	}
	if err := b.allocRoot(); err != nil {
		return err
	}

	ov, oF := b.t.Get(k)
	if ov != nil && oF != F && b.flags != nil {
		return ErrTypeMismatch
	}

	err := b.t.Put(F, k, v)
	if err != nil {
		return err
	}

	return b.propagate()
}

func (b *SimpleBucket) Get(k []byte) []byte {
	if !b.allowed(false) {
		panic("not allowed")
	}
	if b.t.Root == NilPage {
		return nil
	}

	v, F := b.t.Get(k)

	if F != 0 {
		return nil
	}

	return v
}

func (b *SimpleBucket) Del(k []byte) error {
	if !b.allowed(true) {
		panic("not allowed")
	}
	if b.t.Root == NilPage {
		return nil
	}

	_, F := b.t.Get(k)
	if F != 0 {
		return ErrTypeMismatch
	}

	err := b.t.Del(k)
	if err != nil {
		return err
	}

	return b.propagate()
}

func (b *SimpleBucket) Bucket(k []byte) *SimpleBucket {
	if !b.allowed(false) {
		panic("not allowed")
	}
	if b.t.Root == NilPage {
		return nil
	}

	n := string(k)
	if len(b.sub) != 0 {
		if sub, ok := b.sub[n]; ok {
			return sub
		}
	}

	st, eq := b.l.Seek(nil, b.t.Root, k, nil)
	if !eq {
		return nil
	}

	F := 0
	if b.flags != nil {
		F = b.flags.Flags(st)
		if F == 0 {
			return nil
		}
	}

	v := b.l.Value(st, nil)
	off := int64(binary.BigEndian.Uint64(v))

	sub := newBucket(b.tx, b.l, off, k, b)

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
		if sub, ok := b.sub[n]; ok {
			return sub, nil
		}
	}

	v, F := b.t.Get(k)
	if v != nil {
		if F == 0 && b.flags != nil {
			return nil, ErrTypeMismatch
		}
	}

	var buf [8]byte
	err := b.put(1, k, buf[:])
	if err != nil {
		return nil, err
	}

	sub := newBucket(b.tx, b.l, NilPage, k, b)

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

	v, F := b.t.Get(k)
	if v == nil {
		return nil
	}
	if F == 0 && b.flags != nil {
		return ErrTypeMismatch
	}

	off := int64(binary.BigEndian.Uint64(v))

	err := b.delBucket(off)
	if err != nil {
		return err
	}

	err = b.t.Del(k)
	if err != nil {
		return err
	}
	err = b.propagate()
	if err != nil {
		return err
	}

	return nil
}

func (b *SimpleBucket) delBucket(root int64) error {
	if b.flags == nil {
		return b.l.Free(root)
	}

	for st := b.l.Step(nil, b.t.Root, false); st != nil; st = b.l.Step(st, b.t.Root, false) {
		F := b.flags.Flags(st)
		if F == 0 {
			continue
		}

		v := b.l.Value(st, nil)
		sub := int64(binary.BigEndian.Uint64(v))

		err := b.delBucket(sub)
		if err != nil {
			return err
		}
	}

	err := b.l.Free(b.t.Root)
	if err != nil {
		return err
	}

	return nil
}

func (b *SimpleBucket) allocRoot() error {
	if b.t.Root != NilPage {
		return nil
	}

	off, err := b.l.Alloc()
	if err != nil {
		return err
	}

	b.t.Root = off

	return nil
}

//func (b *SimpleBucket) Cursor() Cursor { return nil }

func (b *SimpleBucket) SetLayout(l Layout) {
	b.l = l
	b.t.Layout = l
}

func inc(k []byte) {
	for i := len(k) - 1; i >= 0; i-- {
		k[i]++
		if k[i] != 0 {
			break
		}
	}
}

func (b *SimpleBucket) propagate() error {
	root := b.t.Root
	if b.par == nil || root == b.root {
		b.root = root
		return nil
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(root))
	err := b.par.put(1, []byte(b.name), buf[:])
	if err != nil {
		return err
	}

	b.root = root

	return nil
}

func (b *SimpleBucket) allowed(w bool) bool {
	return !b.del && (!w || b.tx.writable)
}

func (tx *Tx) IsWritable() bool { return tx.writable }

/*
func (tx *Tx) Get(k []byte) []byte                 { return tx.b.Get(k) }
func (tx *Tx) Put(k, v []byte) error               { return tx.b.Put(k, v) }
func (tx *Tx) Del(k []byte) error                  { return tx.b.Del(k) }
func (tx *Tx) Bucket(k []byte) *SimpleBucket             { return tx.b.Bucket(k) }
func (tx *Tx) PutBucket(k []byte) (*Bucket, error) { return tx.b.PutBucket(k) }
func (tx *Tx) DelBucket(k []byte) error            { return tx.b.DelBucket(k) }
*/
