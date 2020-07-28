package xrain

import (
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
		name        []byte
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
	tx.b = newBucket(&tx, t, nil, nil)

	return tx
}

func newBucket(tx *Tx, t Tree, name []byte, par *Bucket) *Bucket {
	return &Bucket{
		tx:          tx,
		name:        name,
		par:         par,
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
	if err := b.allocRoot(); err != nil {
		return err
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
	if b.root == NilPage {
		return nil
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
	if b.root == NilPage {
		return nil
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
	if b.root == NilPage {
		return nil
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

	sub := newBucket(b.tx, t, k, b)

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
		if sub, ok := b.sub[n]; ok {
			return sub, nil
		}
	}

	v, F := b.t.Get(k)
	if v != nil {
		if F == 0 && b.flagSupport {
			return nil, ErrTypeMismatch
		}
	}

	var buf [8]byte
	err := b.put(k, buf[:], 1)
	if err != nil {
		return nil, err
	}

	t := NewTree(b.pl, NilPage, b.page)

	sub := newBucket(b.tx, t, k, b)

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
		off, i := st.LastOffIndex(mask)
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

func (b *Bucket) allocRoot() error {
	if b.root != NilPage {
		return nil
	}

	off, err := b.pl.Alloc(true)
	if err != nil {
		return err
	}

	b.t.SetRoot(off)

	return nil
}

//func (b *Bucket) Cursor() Cursor { return nil }

func (b *Bucket) SetPageLayout(p PageLayout) {
	b.pl = p
	b.t.SetPageLayout(p)
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
	if b.par == nil || root == b.root {
		b.root = root
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
