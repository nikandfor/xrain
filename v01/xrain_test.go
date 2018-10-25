package xrain

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestXRainOpen(t *testing.T) {
	const Page = 0x40
	b := NewMemBack(0 * Page)

	db, err := NewDB(b, &Config{
		PageSize: Page,
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), db.root)

	err = db.Update(func(tx *Tx) error {
		tx.Put([]byte("key1"), []byte("val___1"))
		return nil
	})
	assert.NoError(t, err)

	err = db.Close()
	assert.NoError(t, err)

	db, err = NewDB(b, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(Page), db.root)

	err = db.View(func(tx *Tx) error {
		v := tx.Get([]byte("key1"))
		assert.Equal(t, []byte("val___1"), v)
		return nil
	})
	assert.NoError(t, err)

	err = db.Update(func(tx *Tx) error {
		v := tx.Get([]byte("key1"))
		assert.Equal(t, []byte("val___1"), v)
		v = tx.Get([]byte("key2"))
		assert.Equal(t, []byte(nil), v)

		tx.Put([]byte("key1"), []byte("val__11"))

		v = tx.Get([]byte("key1"))
		assert.Equal(t, []byte("val__11"), v)

		tx.Put([]byte("key2"), []byte("val___2"))

		v = tx.Get([]byte("key2"))
		assert.Equal(t, []byte("val___2"), v)
		return nil
	})
	assert.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		i := 0
		for k := tx.Next(nil); k != nil; k = tx.Next(k) {
			t.Logf("%d: %s -> %s", i, k, tx.Get(k))
			i++
		}
		return nil
	})

	p0, _ := b.Read(0, int64(len(b.buf)))
	t.Logf("dump\n%v", hex.Dump(p0))
}
