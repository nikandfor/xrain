package xrain

import (
	"encoding/hex"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTxBucket(t *testing.T) {
	const Page = 0x100

	b := NewMemBack(0)

	db, err := NewDB(b, &Config{
		PageSize: Page,
	})
	if !assert.NoError(t, err) {
		return
	}

	err = db.Update(func(tx *Tx) error {
		b := tx.Bucket([]byte("bucket00"))
		assert.Nil(t, b)

		b0, err := tx.PutBucket([]byte("bucket00"))
		assert.NoError(t, err)
		assert.NotNil(t, b0)

		err = b0.Put([]byte("key_aaaa"), []byte("value_00"))
		assert.NoError(t, err)

		b1, err := tx.PutBucket([]byte("bucket01"))
		assert.NoError(t, err)
		assert.NotNil(t, b1)

		err = b1.Put([]byte("key_aaaa"), []byte("value_01"))
		assert.NoError(t, err)

		return nil
	})
	assert.NoError(t, err)

	err = db.Update(func(tx *Tx) error {
		b0 := tx.Bucket([]byte("bucket00"))
		assert.NotNil(t, b0)

		assert.Equal(t, []byte("value_00"), b0.Get([]byte("key_aaaa")))
		err = b0.Put([]byte("key_aaaa"), []byte("value_10"))
		assert.NoError(t, err)

		b1 := tx.Bucket([]byte("bucket01"))
		assert.NotNil(t, b1)

		assert.Equal(t, []byte("value_01"), b1.Get([]byte("key_aaaa")))
		err = b1.Put([]byte("key_aaaa"), []byte("value_11"))
		assert.NoError(t, err)

		return nil
	})
	assert.NoError(t, err)

	err = db.Update(func(tx *Tx) error {
		b0 := tx.Bucket([]byte("bucket00"))
		assert.NotNil(t, b0)

		assert.Equal(t, []byte("value_10"), b0.Get([]byte("key_aaaa")))
		err = tx.DelBucket([]byte("bucket00"))
		assert.NoError(t, err)

		b1 := tx.Bucket([]byte("bucket01"))
		assert.NotNil(t, b1)

		assert.Equal(t, []byte("value_11"), b1.Get([]byte("key_aaaa")))
		err = tx.DelBucket([]byte("bucket01"))
		assert.NoError(t, err)

		return nil
	})
	assert.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("bucket00"))
		assert.Nil(t, b)

		b = tx.Bucket([]byte("bucket01"))
		assert.Nil(t, b)

		return nil
	})
	assert.NoError(t, err)

	l, r := b.Access2(0, 0x40, Page, 0x40)
	log.Printf("header pages:\n%v%v", hex.Dump(l), hex.Dump(r))
	b.Unlock2(l, r)
	log.Printf("dump ver %x/%x root %x free %x next %x\n%v", db.ver, db.keep, db.t.Root(), db.fl.(*Freelist2).t.Root(), db.fl.(*Freelist2).next, dumpFile(db.t.PageLayout()))
}
