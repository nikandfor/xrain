// +build ignore

package xrain

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newIntTree(n int, psize int64) (*Tree, Back) {
	b := NewMemBack(int64(n) * psize)
	free := NewNoRewriteFreeList(psize, b)
	pl := &IntLayout{BaseLayout: NewPageLayout(b, psize, 0, free)}
	tr := NewTree(pl, 0, psize)
	tr.p = LogLayout{PageLayout: tr.p, Logger: log.New(os.Stderr, "", log.LstdFlags)}
	return tr, b
}

func newKVTree(n int, psize int64) (*Tree, Back) {
	b := NewMemBack(int64(n) * psize)
	free := NewNoRewriteFreeList(psize, b)
	pl := &KVLayout{BaseLayout: NewPageLayout(b, psize, 0, free)}
	tr := NewTree(pl, 0, psize)
	tr.p = LogLayout{PageLayout: tr.p, Logger: log.New(os.Stderr, "", log.LstdFlags)}
	return tr, b
}

func newKVTreeFL(psize int64, ver, keep int64) (*Tree, *FreeList, Back) {
	b := NewMemBack(2 * psize)

	fl := &IntLayout{BaseLayout: BaseLayout{b: b, page: psize}}

	f0 := NewTree(fl, 0, psize)
	f1 := NewTree(fl, psize, psize)

	free := NewFreeList(f0, f1, 2*psize, psize, ver, keep, b)

	pl := &KVLayout{BaseLayout: NewPageLayout(b, psize, ver, free)}
	tr := NewTree(pl, psize, psize)
	tr.p = LogLayout{PageLayout: tr.p, Logger: log.New(os.Stderr, "kv: ", log.LstdFlags)}
	return tr, free, b
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

func TestIntPut2DblGet(t *testing.T) {
	const Page = 0x40

	tr, b := newIntTree(1, Page)

	err := tr.Put(tb(1), []byte("    key1"))
	assert.NoError(t, err)

	err = tr.Put(tb(2), []byte("    key2"))
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

func TestKVPutManyGet(t *testing.T) {
	const Page = 0x40
	const N = 8
	var err error

	tr, b := newKVTree(1, Page)

	mod := int64(11)
	k := mod % N
	for i := 0; i < N; i++ {
		//	log.Printf("==== === Put k %v", k)
		err = tr.Put(tb7(k), []byte(fmt.Sprintf("val_%2d", k)))
		assert.NoError(t, err)

		if false {
			err = b.Sync()
			assert.NoError(t, err)

			p := b.Load(0, b.Size())
			log.Printf("dump (put %v) root %4x pages %d\n%v", k, tr.root, len(p)/Page, hex.Dump(p))
		}

		k = (k + mod) % N
	}

	err = b.Sync()
	assert.NoError(t, err)

	p := b.Load(0, b.Size())
	log.Printf("dump all %d keys added, %d pages, root %x\n%v", N, b.Size()/Page, tr.root, hex.Dump(p))

	//	return

	mod = 13
	k = mod % N
	for i := 0; i < N; i++ {
		v := tr.Get(tb7(k))
		e := []byte(fmt.Sprintf("val_%2d", k))
		assert.Equal(t, e, v, "key: %d", k)
		k = (k + mod) % N
	}
}

func TestKVPutManyGetDel(t *testing.T) {
	const Page = 0x80
	const N = 10
	var err error

	tr, b := newKVTree(1, Page)

	mod := int64(11)
	k := mod % N
	for i := 0; i < N; i++ {
		err = tr.Put(tb7(k), []byte(fmt.Sprintf("val_%2d", k)))
		assert.NoError(t, err)

		if false {
			err = b.Sync()
			assert.NoError(t, err)

			p := b.Load(0, b.Size())
			log.Printf("dump (put %v) root %4x pages %d\n%v", k, tr.root, len(p)/Page, hex.Dump(p))
		}

		k = (k + mod) % N
	}

	log.Printf("dump all %d keys added, %d pages, root %x\n%v", N, b.Size()/Page, tr.root, dumpFile(tr.p))

	//	return

	mod = 13
	k = mod % N
	for i := 0; i < N; i++ {
		v := tr.Get(tb7(k))
		e := []byte(fmt.Sprintf("val_%2d", k))
		assert.Equal(t, e, v, "key: %d", k)
		k = (k + mod) % N
	}

	mod = 17
	k = mod % N
	for i := 0; i < N; i++ {
		log.Printf("==== === Del %d", k)
		tr.Del(tb7(k))

		if true {
			log.Printf("dump (del %v) root %4x pages %d\n%v", k, tr.root, b.Size()/Page, dumpFile(tr.p))
		}

		k = (k + mod) % N
	}

	first := tr.Next(nil)
	assert.Nil(t, first)

	log.Printf("dump all %d keys deleted, %d pages, root %x\n%v", N, b.Size()/Page, tr.root, dumpFile(tr.p))
}

func TestKVPutManyNext(t *testing.T) {
	const Page = 0x40
	const N = 10
	var err error

	tr, b := newKVTree(1, Page)

	mod := int64(7)
	k := mod % N
	for i := 0; i < N; i++ {
		err = tr.Put(tb7(k), []byte(fmt.Sprintf("val_%2d", k)))
		assert.NoError(t, err)
		k = (k + mod) % N
	}

	err = b.Sync()
	assert.NoError(t, err)

	p := b.Load(0, b.Size())
	log.Printf("dump\n%v", hex.Dump(p))

	bk := []byte(nil)
	for i := 0; i < N; i++ {
		nk := tr.Next(bk)
		assert.True(t, bytes.Compare(bk, nk) < 0, "%q !< %q (%v %v)", bk, nk, bk == nil, nk == nil)
		bk = nk
	}

	nk := tr.Next(bk)
	assert.Nil(t, nk)
}

func TestKVPutManyPrev(t *testing.T) {
	const Page = 0x40
	const N = 10
	var err error

	tr, b := newKVTree(1, Page)

	mod := int64(7)
	k := mod % N
	for i := 0; i < N; i++ {
		err = tr.Put(tb7(k), []byte(fmt.Sprintf("val_%2d", k)))
		assert.NoError(t, err)
		k = (k + mod) % N
	}

	err = b.Sync()
	assert.NoError(t, err)

	p := b.Load(0, b.Size())
	log.Printf("dump\n%v", hex.Dump(p))

	bk := tr.Prev(nil)
	for i := 0; i < N; i++ {
		nk := tr.Prev(bk)
		assert.True(t, bytes.Compare(nk, bk) < 0, "%q !< %q (%v %v)", nk, bk, nk == nil, bk == nil)
		bk = nk
	}
}

func TestKVOutParentSplit(t *testing.T) {
	const Page = 0x80
	const N = 10
	var err error

	tr, b := newKVTree(1, Page)
	_ = b

	valbuf := make([]byte, 0x10-1)

	for i := 0; i < 14; i++ {
		key := fmt.Sprintf("k%3dc", i)
		if i == 11 {
			key = fmt.Sprintf("k%3dc__________________", i)
		}
		copy(valbuf, fmt.Sprintf("val_% 4d", i))
		err = tr.Put([]byte(key), valbuf)
		assert.NoError(t, err)
	}

	log.Printf("before root %x\n%v", tr.root, dumpFile(tr.p))

	copy(valbuf, fmt.Sprintf("newnewnew"))
	err = tr.Put([]byte(fmt.Sprintf("k%3dq", 17)), valbuf)
	assert.NoError(t, err)

	log.Printf("after root %x\n%v", tr.root, dumpFile(tr.p))
}

func TestKVReclaimReuse(t *testing.T) {
	const Page = 0x80
	const N = 10
	var err error

	tr, free, b := newKVTreeFL(Page, 0, -1)
	_ = tr
	_ = err
	_ = free
	_ = b

}

func TestIntPutManyGetDel(t *testing.T) {
	const Page = 0x40
	const N = 4
	var err error

	tr, b := newIntTree(1, Page)

	mod := int64(11)
	k := mod % N
	for i := 0; i < N; i++ {
		err = tr.Put(tb(k), tb(k))
		assert.NoError(t, err)

		log.Printf("dump (put %v) root %4x pages %d\n%v", k, tr.root, b.Size()/Page, dumpFile(tr.p))

		k = (k + mod) % N
	}

	log.Printf("dump all %d keys added, %d pages, root %x\n%v", N, b.Size()/Page, tr.root, dumpFile(tr.p))

	//	return

	mod = 13
	k = mod % N
	for i := 0; i < N; i++ {
		v := tr.Get(tb(k))
		assert.Equal(t, tb(k), v, "key: %d", k)
		k = (k + mod) % N
	}

	mod = 17
	k = mod % N
	for i := 0; i < N; i++ {
		log.Printf("==== === Del %d", k)
		tr.Del(tb(k))

		log.Printf("dump (del %v) root %4x pages %d\n%v", k, tr.root, b.Size()/Page, dumpFile(tr.p))

		k = (k + mod) % N
	}

	first := tr.Next(nil)
	assert.Nil(t, first)

	log.Printf("dump all %d keys deleted, %d pages, root %x\n%v", N, b.Size()/Page, tr.root, dumpFile(tr.p))
}

func tb(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func tb7(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b[1:]
}
