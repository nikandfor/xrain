package xrain

import (
	"encoding/binary"
	"errors"
	"sync"
	"unsafe"
)

type (
	Tx struct {
		d *DB
		*SimpleBucket
		writable bool
		stbuf    Stack

		buckets []*SimpleBucket
	}

	SimpleBucket struct {
		tx      *Tx
		par     *SimpleBucket
		name    []byte
		l       Layout
		t       LayoutShortcut
		oldroot int64
		flags   FlagsSupported

		stbuf Stack
	}
)

var (
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrTypeMismatch        = errors.New("value type mismatch")
	ErrDeletedBucket       = errors.New("deleted bucket used")
	ErrNotAllowed          = errors.New("operation is not allowed")
)

var ( // pools
	bkPool = sync.Pool{New: func() interface{} {
		return &SimpleBucket{stbuf: make(Stack, 8)}
	}}

	txPool = sync.Pool{New: func() interface{} {
		return &Tx{
			buckets: make([]*SimpleBucket, 10),
		}
	}}
)

func newTx(d *DB, l Layout, root int64, w bool) *Tx {
	tx := txPool.Get().(*Tx)

	*tx = Tx{
		d:        d,
		writable: w,
		buckets:  tx.buckets[:0],
	}
	tx.SimpleBucket = newBucket(tx, l, root, nil, nil)

	return tx
}

func newBucket(tx *Tx, l Layout, root int64, name []byte, par *SimpleBucket) *SimpleBucket {
	ff, _ := l.(FlagsSupported)

	b := bkPool.Get().(*SimpleBucket)

	*b = SimpleBucket{
		tx:      tx,
		par:     par,
		name:    name,
		l:       l,
		t:       NewLayoutShortcut(l, root, tx.d.Mask),
		oldroot: root,
		flags:   ff,
		stbuf:   b.stbuf,
	}

	tx.buckets = append(tx.buckets, b)

	return b
}

func (tx *Tx) free() {
	for _, b := range tx.buckets {
		bkPool.Put(b)
	}

	txPool.Put(tx)
}

func (b *SimpleBucket) Put(k, v []byte) error {
	return b.put(0, k, v)
}

func (b *SimpleBucket) put(ff int, k, v []byte) (err error) {
	if err = b.allowed(true); err != nil {
		return
	}
	if err = b.allocRoot(); err != nil {
		return err
	}

	st := b.stbuf
	defer func() {
		b.stbuf = st
	}()

	st, eq := b.t.Seek(k, nil, st)
	if eq && b.flags != nil && b.flags.Flags(st) != ff {
		return ErrTypeMismatch
	}

	if eq {
		st, err = b.t.Delete(st)
		if err != nil {
			return err
		}
	}

	st, err = b.t.Insert(st, ff, k, v)
	if err != nil {
		return err
	}

	return b.propagate()
}

func (b *SimpleBucket) Get(k []byte) []byte {
	if err := b.allowed(false); err != nil {
		panic(err)
	}
	if b.t.Root == NilPage {
		return nil
	}

	v, F := b.t.Get(k, nil)

	if F != 0 {
		return nil
	}

	return v
}

func (b *SimpleBucket) Del(k []byte) (err error) {
	if err = b.allowed(true); err != nil {
		return
	}
	if b.t.Root == NilPage {
		return nil
	}

	_, F := b.t.Get(k, nil)
	if F != 0 {
		return ErrTypeMismatch
	}

	_, err = b.t.Del(k, nil)
	if err != nil {
		return err
	}

	return b.propagate()
}

func (b *SimpleBucket) Bucket(k []byte) *SimpleBucket {
	if err := b.allowed(false); err != nil {
		panic(err)
	}
	if b.t.Root == NilPage {
		return nil
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

	return sub
}

func (b *SimpleBucket) PutBucket(k []byte) (_ *SimpleBucket, err error) {
	if err = b.allowed(false); err != nil {
		return
	}

	st := b.stbuf
	defer func() {
		b.stbuf = st
	}()

	st, eq := b.t.Seek(k, nil, st)
	if eq {
		if b.flags != nil && b.flags.Flags(st) != 1 {
			return nil, ErrTypeMismatch
		}

		off := b.t.Layout.Int64(st)

		return newBucket(b.tx, b.l, off, k, b), nil
	}

	var buf [8]byte
	off := int64(NilPage)
	binary.BigEndian.PutUint64(buf[:], uint64(off))

	err = b.put(1, k, buf[:])
	if err != nil {
		return nil, err
	}

	sub := newBucket(b.tx, b.l, off, k, b)

	return sub, nil
}

func (b *SimpleBucket) DelBucket(k []byte) (err error) {
	if err = b.allowed(false); err != nil {
		panic(err)
	}

	st, eq := b.t.Seek(k, nil, nil)
	if !eq {
		return nil
	}

	if b.flags != nil && b.flags.Flags(st) != 1 {
		return ErrTypeMismatch
	}

	off := b.t.Layout.Int64(st)

	err = b.delBucket(off)
	if err != nil {
		return err
	}

	_, err = b.t.Delete(st)
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
	if root == NilPage {
		return nil
	}

	if b.flags == nil {
		return b.l.Free(root)
	}

	for st := b.l.Step(nil, root, false); st != nil; st = b.l.Step(st, root, false) {
		F := b.flags.Flags(st)
		if F == 0 {
			continue
		}

		sub := b.l.Int64(st)

		err := b.delBucket(sub)
		if err != nil {
			return err
		}
	}

	err := b.l.Free(root)
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

func (b *SimpleBucket) checkNotDeleted() (err error) {
	if b.par == nil {
		return nil
	}

	err = b.par.checkNotDeleted()
	if err != nil {
		return
	}

	st := b.stbuf
	defer func() {
		b.stbuf = st
	}()

	st, eq := b.par.t.Seek(b.name, nil, st)
	if !eq {
		return ErrDeletedBucket
	}

	if b.par.flags != nil && b.par.flags.Flags(st) != 1 {
		return ErrTypeMismatch
	}

	if b.par.t.Layout.Int64(st) != b.oldroot {
		return ErrDeletedBucket
	}

	return nil
}

func (b *SimpleBucket) propagate() error {
	root := b.t.Root
	//	tl.Printf("propogate: %x <- %x  par %v", root, b.oldroot, b.par)
	if b.par == nil || root == b.oldroot {
		b.oldroot = root
		return nil
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(root))

	buf1 := buf[:]
	buf2 := *(*[]byte)(noescape(unsafe.Pointer(&buf1)))

	err := b.par.put(1, []byte(b.name), buf2)
	if err != nil {
		return err
	}

	b.oldroot = root

	return nil
}

func (b *SimpleBucket) allowed(w bool) (err error) {
	if err = b.checkNotDeleted(); err != nil {
		return
	}

	if w && !b.tx.writable {
		return ErrNotAllowed
	}

	return nil
}

func (b *SimpleBucket) Tree() *LayoutShortcut { return &b.t }

func (b *SimpleBucket) Layout() Layout { return b.l }

func (b *SimpleBucket) Tx() *Tx { return b.tx }

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

func (tx *Tx) IsWritable() bool { return tx.writable }

func (tx *Tx) Meta() Meta { return tx.d.Meta }

/*
func (tx *Tx) Get(k []byte) []byte                 { return tx.b.Get(k) }
func (tx *Tx) Put(k, v []byte) error               { return tx.b.Put(k, v) }
func (tx *Tx) Del(k []byte) error                  { return tx.b.Del(k) }
func (tx *Tx) Bucket(k []byte) *SimpleBucket             { return tx.b.Bucket(k) }
func (tx *Tx) PutBucket(k []byte) (*Bucket, error) { return tx.b.PutBucket(k) }
func (tx *Tx) DelBucket(k []byte) error            { return tx.b.DelBucket(k) }
*/
