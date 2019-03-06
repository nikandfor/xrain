package xrain

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestXRainSmoke(t *testing.T) {
	const Page = 0x100

	b := NewMemBack(0)
	kvl := NewFixedLayout(b, Page, 0, nil)

	db, err := NewDB(b, &Config{PageSize: Page})
	assert.NoError(t, err)

	err = db.UpdateNoBatching(func(tx *Tx) error {
		log.Printf("put %+v", tx.t)
		err := tx.Put([]byte("key_aaaa"), []byte("value_aa"))
		log.Printf("put %+v", tx.t)
		return err
	})
	assert.NoError(t, err)

	log.Printf("dump %+v\n%v", db, dumpFile(kvl))

	db, err = NewDB(b, nil)
	assert.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		v := tx.Get([]byte("key_aaaa"))
		assert.Equal(t, []byte("value_aa"), v)
		return nil
	})
	assert.NoError(t, err)

	err = db.UpdateNoBatching(func(tx *Tx) error {
		log.Printf("del %+v", tx.t)
		err := tx.Del([]byte("key_aaaa"))
		log.Printf("del %+v", tx.t)
		return err
	})
	assert.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		v := tx.Get([]byte("key_aaaa"))
		assert.Equal(t, []byte(nil), v)
		return nil
	})
	assert.NoError(t, err)

	log.Printf("dump %+v\n%v", db, dumpFile(kvl))
}
