package xrain

import (
	"encoding/binary"
	"encoding/hex"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newIntTree(n int, psize int64) (*Tree, Back) {
	b := NewMemBack(int64(n) * psize)
	pl := &IntLayout{BaseLayout: NewPageLayout(b, psize, 1, 0, nil)}
	tr := NewTree(pl, 0)
	tr.p = LogLayout{PageLayout: tr.p, Logger: log.New(os.Stderr, "", log.LstdFlags)}
	return tr, b
}

func TestIntPut2Get(t *testing.T) {
	const Page = 0x40

	tr, b := newIntTree(1, Page)

	err := tr.Put(tb(1), []byte("    key1"))
	assert.NoError(t, err)

	err = tr.Put(tb(2), []byte("    key2"))
	assert.NoError(t, err)

	err = b.Sync()
	assert.NoError(t, err)

	p := b.Load(0, b.Size())
	log.Printf("dump %p\n%v", &p[0], hex.Dump(p))

	v := tr.Get(tb(1))
	assert.Equal(t, []byte("    key1"), v)

	v = tr.Get(tb(2))
	assert.Equal(t, []byte("    key2"), v)
}

func TestIntPut2DelGet(t *testing.T) {
	const Page = 0x40

	tr, b := newIntTree(1, Page)

	err := tr.Put(tb(1), []byte("    key1"))
	assert.NoError(t, err)

	err = tr.Put(tb(2), []byte("    key2"))
	assert.NoError(t, err)

	err = b.Sync()
	assert.NoError(t, err)

	p := b.Load(0, b.Size())
	log.Printf("dump %p\n%v", &p[0], hex.Dump(p))

	err = tr.Del(tb(1))
	assert.NoError(t, err)

	err = tr.Del(tb(1))
	assert.NoError(t, err)

	err = tr.Del(tb(2))
	assert.NoError(t, err)

	p = b.Load(0, b.Size())
	log.Printf("dump %p\n%v", &p[0], hex.Dump(p))

	v := tr.Get(tb(1))
	assert.Nil(t, v)

	v = tr.Get(tb(2))
	assert.Nil(t, v)
}

func TestIntPut2Next(t *testing.T) {
	const Page = 0x40

	tr, b := newIntTree(1, Page)

	err := tr.Put(tb(1), []byte("    key1"))
	assert.NoError(t, err)

	err = tr.Put(tb(2), []byte("    key2"))
	assert.NoError(t, err)

	err = b.Sync()
	assert.NoError(t, err)

	p := b.Load(0, b.Size())
	log.Printf("dump %p\n%v", &p[0], hex.Dump(p))

	k := []byte(nil)

	k = tr.Next(k)
	assert.Equal(t, tb(1), k)

	k = tr.Next(k)
	assert.Equal(t, tb(2), k)

	k = tr.Next(k)
	assert.Equal(t, []byte(nil), k)
}

func TestIntPut4Next(t *testing.T) {
	const Page = 0x40

	tr, b := newIntTree(1, Page)

	err := tr.Put(tb(1), []byte("    key1"))
	assert.NoError(t, err)

	err = tr.Put(tb(2), []byte("    key2"))
	assert.NoError(t, err)

	err = tr.Put(tb(3), []byte("    key3"))
	assert.NoError(t, err)

	err = tr.Put(tb(4), []byte("    key4"))
	assert.NoError(t, err)

	err = b.Sync()
	assert.NoError(t, err)

	p := b.Load(0, b.Size())
	log.Printf("dump %p\n%v", &p[0], hex.Dump(p))

	k := []byte(nil)

	k = tr.Next(k)
	assert.Equal(t, tb(1), k)

	k = tr.Next(k)
	assert.Equal(t, tb(2), k)

	k = tr.Next(k)
	assert.Equal(t, tb(3), k)

	k = tr.Next(k)
	assert.Equal(t, tb(4), k)

	k = tr.Next(k)
	assert.Equal(t, []byte(nil), k)
}

func TestIntPut4Prev(t *testing.T) {
	const Page = 0x40

	tr, b := newIntTree(1, Page)

	err := tr.Put(tb(1), []byte("    key1"))
	assert.NoError(t, err)

	err = tr.Put(tb(2), []byte("    key2"))
	assert.NoError(t, err)

	err = tr.Put(tb(3), []byte("    key3"))
	assert.NoError(t, err)

	err = tr.Put(tb(4), []byte("    key4"))
	assert.NoError(t, err)

	err = b.Sync()
	assert.NoError(t, err)

	p := b.Load(0, b.Size())
	log.Printf("dump %p\n%v", &p[0], hex.Dump(p))

	k := []byte(nil)

	k = tr.Prev(k)
	assert.Equal(t, tb(4), k)

	k = tr.Prev(k)
	assert.Equal(t, tb(3), k)

	k = tr.Prev(k)
	assert.Equal(t, tb(2), k)

	k = tr.Prev(k)
	assert.Equal(t, tb(1), k)

	k = tr.Prev(k)
	assert.Equal(t, []byte(nil), k)
}

func tb(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}
