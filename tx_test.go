package xrain

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTxBucket(t *testing.T) {
	initLogger(t)

	const Page = 0x100

	b := NewMemBack(0)

	l := NewFixedLayout(nil)
	l.SetKVSize(1, 7, 8, 1)

	db, err := NewDB(b, Page, l)
	if !assert.NoError(t, err) {
		return
	}

	err = db.Update(func(tx *Tx) error {
		b := tx.Bucket([]byte("bucket0"))
		assert.Nil(t, b)

		b0, err := tx.PutBucket([]byte("bucket0"))
		assert.NoError(t, err)
		assert.NotNil(t, b0)

		err = b0.Put([]byte("key_aaa"), []byte("value_00"))
		assert.NoError(t, err)

		b1, err := tx.PutBucket([]byte("bucket1"))
		assert.NoError(t, err)
		assert.NotNil(t, b1)

		err = b1.Put([]byte("key_aaa"), []byte("value_01"))
		assert.NoError(t, err)

		return nil
	})
	assert.NoError(t, err)

	tl.Printf("dump ver %x/%x  root %x  fnext %x\n%v\n%v", db.Ver, db.Keep, db.root, db.Freelist.(*Freelist3).next, db.Meta.Meta.Layout.(*SubpageLayout).dump(), db.l.(fileDumper).dumpFile())

	err = db.Update(func(tx *Tx) error {
		b0 := tx.Bucket([]byte("bucket0"))
		assert.NotNil(t, b0)

		assert.Equal(t, []byte("value_00"), b0.Get([]byte("key_aaa")))
		err = b0.Put([]byte("key_aaa"), []byte("value_10"))
		assert.NoError(t, err)

		b1 := tx.Bucket([]byte("bucket1"))
		assert.NotNil(t, b1)

		assert.Equal(t, []byte("value_01"), b1.Get([]byte("key_aaa")))
		err = b1.Put([]byte("key_aaa"), []byte("value_11"))
		assert.NoError(t, err)

		return nil
	})
	assert.NoError(t, err)

	tl.Printf("dump ver %x/%x  root %x  fnext %x\n%v\n%v", db.Ver, db.Keep, db.root, db.Freelist.(*Freelist3).next, db.Meta.Meta.Layout.(*SubpageLayout).dump(), db.l.(fileDumper).dumpFile())

	err = db.Update(func(tx *Tx) error {
		b0 := tx.Bucket([]byte("bucket0"))
		assert.NotNil(t, b0)

		assert.Equal(t, []byte("value_10"), b0.Get([]byte("key_aaa")))
		err = tx.DelBucket([]byte("bucket0"))
		assert.NoError(t, err)

		b1 := tx.Bucket([]byte("bucket1"))
		assert.NotNil(t, b1)

		assert.Equal(t, []byte("value_11"), b1.Get([]byte("key_aaa")))
		err = tx.DelBucket([]byte("bucket1"))
		assert.NoError(t, err)

		return nil
	})
	assert.NoError(t, err)

	tl.Printf("dump ver %x/%x  root %x  fnext %x\n%v\n%v", db.Ver, db.Keep, db.root, db.Freelist.(*Freelist3).next, db.Meta.Meta.Layout.(*SubpageLayout).dump(), db.l.(fileDumper).dumpFile())

	err = db.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("bucket0"))
		assert.Nil(t, b)

		b = tx.Bucket([]byte("bucket1"))
		assert.Nil(t, b)

		return nil
	})
	assert.NoError(t, err)

	off0, off1 := b.Access2(0, 0x40, Page, 0x40)
	tl.Printf("header pages 0, 1:\n%v\n%v", hex.Dump(off0), hex.Dump(off1))
	b.Unlock2(off0, off1)

	off0, off1 = b.Access2(2*Page, 0x40, 3*Page, 0x40)
	tl.Printf("header pages 2, 3:\n%v\n%v", hex.Dump(off0), hex.Dump(off1))
	b.Unlock2(off0, off1)

	tl.Printf("dump ver %x/%x  root %x  fnext %x\n%v\n%v", db.Ver, db.Keep, db.root, db.Freelist.(*Freelist3).next, db.Meta.Meta.Layout.(*SubpageLayout).dump(), db.l.(fileDumper).dumpFile())
}
