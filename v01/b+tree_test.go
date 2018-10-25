package xrain

import (
	"encoding/hex"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTreePut1(t *testing.T) {
	const Page = 0x40
	tr := newTestBPTree(6, Page)

	tr.Put([]byte("key1"), []byte("val__1"))
	tr.Put([]byte("key2"), []byte("val__2"))

	p0, _ := tr.a.(*SeqAlloc).b.Read(0, 1*Page)
	t.Logf("dump root:%#x\n%v", tr.root, hex.Dump(p0))

	tr.Put([]byte("key4"), []byte("val__4"))

	p0, _ = tr.a.(*SeqAlloc).b.Read(0, 3*Page)
	t.Logf("dump root:%#x\n%v", tr.root, hex.Dump(p0))
	for i := 0; i < 3; i++ {
		t.Logf("page %d %#4x: %v", i, i*Page, tr.p.(BytesPage).Dump(p0[i*Page:(i+1)*Page]))
	}

	v := tr.Get([]byte("key1"))
	assert.Equal(t, []byte("val__1"), v)

	v = tr.Get([]byte("key2"))
	assert.Equal(t, []byte("val__2"), v)

	v = tr.Get([]byte("key4"))
	assert.Equal(t, []byte("val__4"), v)

	v = tr.Get([]byte("key22"))
	assert.Equal(t, []byte(nil), v)

	log.Printf("put key3")

	tr.Put([]byte("key3"), []byte("val__3"))

	p0, _ = tr.a.(*SeqAlloc).b.Read(0, 6*Page)
	t.Logf("err: %v", tr.err)
	t.Logf("dump root:%#x\n%v", tr.root, hex.Dump(p0))
	for i := 0; i < 6; i++ {
		t.Logf("page %d %#4x: %v", i, i*Page, tr.p.(BytesPage).Dump(p0[i*Page:(i+1)*Page]))
	}

	v = tr.Get([]byte("key1"))
	assert.Equal(t, []byte("val__1"), v)

	v = tr.Get([]byte("key2"))
	assert.Equal(t, []byte("val__2"), v)

	v = tr.Get([]byte("key3"))
	assert.Equal(t, []byte("val__3"), v)

	v = tr.Get([]byte("key4"))
	assert.Equal(t, []byte("val__4"), v)
}

func TestTreeDel(t *testing.T) {
	const Page = 0x40
	tr := newTestBPTree(6, Page)

	tr.Put([]byte("key1"), []byte("val__1"))
	tr.Put([]byte("key2"), []byte("val__2"))
	tr.Put([]byte("key3"), []byte("val__3"))
	tr.Put([]byte("key4"), []byte("val__4"))

	log.Printf("put key5")

	tr.Put([]byte("key5"), []byte("val__5"))

	p0, _ := tr.a.(*SeqAlloc).b.Read(0, 3*Page)
	t.Logf("err: %v", tr.err)
	//	t.Logf("dump root:%#x\n%v", tr.root, hex.Dump(p0))
	for i := 0; i < 6; i++ {
		t.Logf("page %d %#4x: %v", i, i*Page, tr.p.(BytesPage).Dump(p0[i*Page:(i+1)*Page]))
	}

	log.Printf("del key1")
	tr.Del([]byte("key1"))

	p0, _ = tr.a.(*SeqAlloc).b.Read(0, 6*Page)
	t.Logf("err: %v", tr.err)
	//	t.Logf("dump root:%#x\n%v", tr.root, hex.Dump(p0))
	t.Logf("root: %d %#4x", tr.root/Page, tr.root)
	for i := 0; i < 6; i++ {
		t.Logf("page %d %#4x: %v", i, i*Page, tr.p.(BytesPage).Dump(p0[i*Page:(i+1)*Page]))
	}
}

func TestTreePutReverse(t *testing.T) {
	const Page = 0x40
	tr := newTestBPTree(6, Page)

	tr.Put([]byte("key4"), []byte("val__4"))
	tr.Put([]byte("key3"), []byte("val__3"))
	tr.Put([]byte("key2"), []byte("val__2"))

	p0, _ := tr.a.(*SeqAlloc).b.Read(0, 3*Page)
	t.Logf("root: %d %#4x", tr.root/Page, tr.root)
	for i := 0; i < 6; i++ {
		t.Logf("page %d %#4x: %v", i, i*Page, tr.p.(BytesPage).Dump(p0[i*Page:(i+1)*Page]))
	}

	tr.Put([]byte("key1"), []byte("val__1"))

	p0, _ = tr.a.(*SeqAlloc).b.Read(0, 3*Page)
	t.Logf("root: %d %#4x", tr.root/Page, tr.root)
	for i := 0; i < 6; i++ {
		t.Logf("page %d %#4x: %v", i, i*Page, tr.p.(BytesPage).Dump(p0[i*Page:(i+1)*Page]))
	}
}

func TestTreeNext(t *testing.T) {
	const Page = 0x40
	tr := newTestBPTree(6, Page)

	assert.Nil(t, tr.Next(nil))
	assert.NoError(t, tr.err)

	tr.Put([]byte("key4"), []byte("val__4"))
	tr.Put([]byte("key3"), []byte("val__3"))
	tr.Put([]byte("key2"), []byte("val__2"))
	tr.Put([]byte("key1"), []byte("val__1"))

	p0, _ := tr.a.(*SeqAlloc).b.Read(0, 3*Page)
	t.Logf("root: %d %#4x", tr.root/Page, tr.root)
	for i := 0; i < 6; i++ {
		t.Logf("page %d %#4x: %v", i, i*Page, tr.p.(BytesPage).Dump(p0[i*Page:(i+1)*Page]))
	}

	k := []byte(nil)
	k = tr.Next(k)
	assert.Equal(t, []byte("key1"), k)

	k = tr.Next(k)
	assert.Equal(t, []byte("key2"), k)

	k = tr.Next(k)
	assert.Equal(t, []byte("key3"), k)

	k = tr.Next(k)
	assert.Equal(t, []byte("key4"), k)

	k = tr.Next(k)
	assert.Equal(t, []byte(nil), k)

	k = tr.Next([]byte("k"))
	assert.Equal(t, []byte("key1"), k)

	k = tr.Next([]byte("key44"))
	assert.Equal(t, []byte(nil), k)
}

func TestTreePrev(t *testing.T) {
	const Page = 0x40
	tr := newTestBPTree(6, Page)

	assert.Nil(t, tr.Prev(nil))
	assert.NoError(t, tr.err)

	tr.Put([]byte("key4"), []byte("val__4"))
	tr.Put([]byte("key3"), []byte("val__3"))
	tr.Put([]byte("key2"), []byte("val__2"))
	tr.Put([]byte("key1"), []byte("val__1"))

	p0, _ := tr.a.(*SeqAlloc).b.Read(0, 3*Page)
	t.Logf("root: %d %#4x", tr.root/Page, tr.root)
	for i := 0; i < 6; i++ {
		t.Logf("page %d %#4x: %v", i, i*Page, tr.p.(BytesPage).Dump(p0[i*Page:(i+1)*Page]))
	}

	k := []byte(nil)
	k = tr.Prev(k)
	assert.Equal(t, []byte("key4"), k)

	k = tr.Prev(k)
	assert.Equal(t, []byte("key3"), k)

	k = tr.Prev(k)
	assert.Equal(t, []byte("key2"), k)

	k = tr.Prev(k)
	assert.Equal(t, []byte("key1"), k)

	k = tr.Prev(k)
	assert.Equal(t, []byte(nil), k)

	k = tr.Prev([]byte("k"))
	assert.Equal(t, []byte(nil), k)

	k = tr.Prev([]byte("key22"))
	assert.Equal(t, []byte("key2"), k)
}
