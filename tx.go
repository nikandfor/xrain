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
		t       LayoutShortcut
		rootbuf int64

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

	bsPool = sync.Pool{New: func() interface{} {
		return make([]byte, 0x1000)
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
	b := bkPool.Get().(*SimpleBucket)

	*b = SimpleBucket{
		tx:      tx,
		par:     par,
		name:    name,
		t:       NewLayoutShortcut(l, root, tx.d.Mask),
		rootbuf: root,
		stbuf:   b.stbuf,
	}

	tx.buckets = append(tx.buckets, b)

	return b
}

func (tx *Tx) free() {
	if l, ok := tx.SimpleBucket.t.Layout.(*SubpageLayout); ok {
		if tx.writable {
			tx.rootbuf = NilPage

			_, _ = tx.d.Meta.Meta.Set(0, []byte("root"), l.Bytes(), nil)
		}

		bsPool.Put(l.Bytes())
		tx.SimpleBucket.t.Layout = nil
	}

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
	if eq && b.t.Layout.Flags(st) != ff {
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

	st, eq := b.t.Layout.Seek(nil, b.t.Root, k, nil)
	if !eq {
		return nil
	}

	if b.t.Layout.Flags(st) == 0 {
		return nil
	}

	v := b.t.Layout.Value(st, nil)
	off := int64(binary.BigEndian.Uint64(v))

	sub := newBucket(b.tx, b.tx.d.l, off, k, b)

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
		if b.t.Layout.Flags(st) != 1 {
			return nil, ErrTypeMismatch
		}

		off := b.t.Layout.Int64(st)

		return newBucket(b.tx, b.tx.d.l, off, k, b), nil
	}

	var buf [8]byte
	off := int64(NilPage)
	binary.BigEndian.PutUint64(buf[:], uint64(off))

	err = b.put(1, k, buf[:])
	if err != nil {
		return nil, err
	}

	sub := newBucket(b.tx, b.tx.d.l, off, k, b)

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

	if b.t.Layout.Flags(st) != 1 {
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

	l := b.tx.d.l

	for st := l.Step(nil, root, false); st != nil; st = l.Step(st, root, false) {
		if l.Flags(st) == 0 {
			continue
		}

		sub := l.Int64(st)

		err := b.delBucket(sub)
		if err != nil {
			return err
		}
	}

	err := l.Free(root)
	if err != nil {
		return err
	}

	return nil
}

func (b *SimpleBucket) allocRoot() error {
	if b.t.Root != NilPage {
		return nil
	}

	off, err := b.t.Layout.Alloc()
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

	if b.par.t.Layout.Flags(st) != 1 {
		return ErrTypeMismatch
	}

	if b.par.t.Layout.Int64(st) != b.rootbuf {
		return ErrDeletedBucket
	}

	return nil
}

func (b *SimpleBucket) propagate() error {
	root := b.t.Root

	//	tl.Printf("propogate: %x <- %x  par %v  layout %T", root, b.rootbuf, b.par, b.t.Layout)
	if b.par == nil || root == b.rootbuf {
		b.rootbuf = root
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

	b.rootbuf = root

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

func (b *SimpleBucket) Layout() Layout { return b.t.Layout }

func (b *SimpleBucket) Tx() *Tx { return b.tx }

func (b *SimpleBucket) SetLayout(l Layout) {
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
