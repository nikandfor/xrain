package xrain

import "nikand.dev/go/xrain/back"

type (
	DB struct {
		b back.Back
	}

	Tx struct {
		d    *DB
		root int64
	}

	Bucket struct {
		tx   *Tx
		root int64
	}

	Cursor struct {
		b *Bucket
		s []offindex
	}

	offindex int64
)

func NewDB() (*DB, error) {
}

func (d *DB) Tx(w bool) (*Tx, error) {
}

func (tx *Tx) Commit() error {
	return nil
}

func (tx *Tx) Rollback() error {
	return nil
}

func (tx *Tx) Bucket(key []byte) *Bucket {
}

func (tx *Tx) CreateBucket(key []byte) (*Bucket, error) {
}

func (b *Bucket) Bucket(key []byte) *Bucket {
}

func (b *Bucket) CreateBucket(key []byte) (*Bucket, error) {
}

func (b *Bucket) Get(key []byte) []byte {
}

func (b *Bucket) Put(key []byte) []byte {
}

func (b *Bucket) Cursor() *Cursor {
}
