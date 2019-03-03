package xrain

import (
	"encoding/hex"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestXRainSmoke(t *testing.T) {
	const Page = 0x100

	b := NewMemBack(0)

	db, err := NewDB(b, &Config{PageSize: Page})
	assert.NoError(t, err)

	err = db.UpdateNoBatching(func(tx *Tx) error {
		return tx.t.Put([]byte("key_aaaa"), []byte("value_aaaa"))
	})
	assert.NoError(t, err)

	log.Printf("dump\n%v", hex.Dump(b.Load(0, 7*Page)))

	db, err = NewDB(b, nil)
	assert.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		v := tx.t.Get([]byte("key_aaaa"))
		assert.Equal(t, []byte("value_aaaa"), v)
		return nil
	})
	assert.NoError(t, err)
}
