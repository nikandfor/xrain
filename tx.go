package xrain

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type (
	Tx struct {
		d        *DB
		b        *Bucket
		writable bool
	}

	Bucket struct {
		tx          *Tx
		name        string
		par         *Bucket
		t           Tree
		pl          PageLayout
		root        int64
		page        int64
		sub         map[string]*Bucket
		del         bool
		flagSupport bool
	}
)

var (
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrTypeMismatch        = errors.New("value type mismatch")
)

func newTx(d *DB, t Tree, w bool) Tx {
	tx := Tx{
		d:        d,
		writable: w,
	}
	tx.b = newBucket(&tx, t)

	return tx
}

func newBucket(tx *Tx, t Tree) *Bucket {
	return &Bucket{
		tx:          tx,
		t:           t,
		pl:          t.PageLayout(),
		root:        t.Root(),
		page:        tx.d.page,
		flagSupport: t.PageLayout().Supports(Flags),
	}
}

func (b *Bucket) Put(k, v []byte) error {
	return b.put(k, v, 0)
}

func (b *Bucket) put(k, v []byte, F int) error {
	if !b.allowed(true) {
		panic("not allowed")
	}

	ov, oF := b.t.Get(k)
	if ov != nil && oF != F && b.flagSupport {
		return ErrTypeMismatch
	}

	_, err := b.t.Put(k, v, F)
	if err != nil {
		return err
	}

	return b.propagate()
}

func (b *Bucket) Get(k []byte) []byte {
	if !b.allowed(false) {
		panic("not allowed")
	}

	v, F := b.t.Get(k)

	if F != 0 {
		return nil
	}

	return v
}

func (b *Bucket) Del(k []byte) error {
	if !b.allowed(true) {
		panic("not allowed")
	}

	_, F := b.t.Get(k)
	if F != 0 {
		return ErrTypeMismatch
	}

	_, err := b.t.Del(k)
	if err != nil {
		return err
	}

	return b.propagate()
}

func (b *Bucket) Bucket(k []byte) *Bucket {
	if !b.allowed(false) {
		panic("not allowed")
	}

	n := string(k)
	if len(b.sub) != 0 {
		if sub, ok := b.sub[n]; ok {
			return sub
		}
	}

	v, F := b.t.Get(k)
	if v == nil || F == 0 && b.flagSupport {
		return nil
	}

	off := int64(binary.BigEndian.Uint64(v))

	t := NewTree(b.pl, off, b.page)

	sub := &Bucket{
		tx:   b.tx,
		name: n,
		par:  b,
		t:    t,
		root: off,
	}

	if b.sub == nil {
		b.sub = make(map[string]*Bucket)
	}
	b.sub[n] = sub

	return sub
}

func (b *Bucket) PutBucket(k []byte) (*Bucket, error) {
	if !b.allowed(false) {
		panic("not allowed")
	}

	n := string(k)
	if len(b.sub) != 0 {
		if _, ok := b.sub[n]; ok {
			return nil, ErrBucketAlreadyExists
		}
	}

	v, F := b.t.Get(k)
	if v != nil {
		if F == 0 {
			return nil, ErrTypeMismatch
		} else {
			return nil, ErrBucketAlreadyExists
		}
	}

	off, err := b.pl.Alloc(true)
	if err != nil {
		return nil, err
	}

	t := NewTree(b.pl, off, b.page)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(off))

	_, err = b.t.Put(k, buf[:], 1)
	if err != nil {
		return nil, err
	}
	err = b.propagate()
	if err != nil {
		return nil, err
	}

	sub := &Bucket{
		tx:   b.tx,
		name: n,
		par:  b,
		t:    t,
		root: off,
	}

	if b.sub == nil {
		b.sub = make(map[string]*Bucket)
	}
	b.sub[n] = sub

	return sub, nil
}

func (b *Bucket) DelBucket(k []byte) error {
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
	if F == 0 && b.flagSupport {
		return ErrTypeMismatch
	}

	off := int64(binary.BigEndian.Uint64(v))

	err := b.delBucket(off, b.page)
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

func (b *Bucket) delBucket(root, page int64) error {
	pl := b.pl
	t := NewTree(pl, root, page)
	mask := page - 1

	for st := t.Step(nil, false); st != nil; st = t.Step(st, false) {
		off, i := st.OffIndex(mask)
		_, F := pl.Key(off, i, nil)
		if F == 0 {
			continue
		}

		v := pl.Value(off, i, nil)
		sub := int64(binary.BigEndian.Uint64(v))

		err := b.delBucket(sub, page)
		if err != nil {
			return err
		}
	}

	err := pl.Free(t.Root(), true)
	if err != nil {
		return err
	}

	return nil
}

//func (b *Bucket) Cursor() Cursor { return nil }

func (b *Bucket) NextUniqueKey(k []byte) []byte {
	st, eq := b.t.Seek(nil, k)
	if !eq {
		return k
	}

	mask := b.page - 1
	n := make([]byte, len(k))
	copy(n, k)

	for {
		inc(n)

		st := b.t.Step(st, false)
		if st == nil {
			break
		}

		off, i := st.OffIndex(mask)

		q, _ := b.pl.Key(off, i, nil)

		if !bytes.Equal(n, q) {
			break
		}
	}

	return n
}

func inc(k []byte) {
	for i := len(k) - 1; i >= 0; i-- {
		k[i]++
		if k[i] != 0 {
			break
		}
	}
}

func (b *Bucket) propagate() error {
	root := b.t.Root()
	//	log.Printf("propagate bucket %s (par %p)  %x <- %x", b.name, b.par, root, b.root)
	if b.par == nil || root == b.root {
		return nil
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(root))
	err := b.par.put([]byte(b.name), buf[:], 1)
	if err != nil {
		return err
	}

	b.root = root

	return nil
}

func (b *Bucket) allowed(w bool) bool {
	return !b.del && (b.tx.IsWritable() || !w)
}

func (tx *Tx) IsWritable() bool                    { return tx.writable }
func (tx *Tx) Get(k []byte) []byte                 { return tx.b.Get(k) }
func (tx *Tx) Put(k, v []byte) error               { return tx.b.Put(k, v) }
func (tx *Tx) Del(k []byte) error                  { return tx.b.Del(k) }
func (tx *Tx) Bucket(k []byte) *Bucket             { return tx.b.Bucket(k) }
func (tx *Tx) PutBucket(k []byte) (*Bucket, error) { return tx.b.PutBucket(k) }
func (tx *Tx) DelBucket(k []byte) error            { return tx.b.DelBucket(k) }
