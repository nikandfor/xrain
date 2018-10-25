package xrain

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newTestBPTree(n int, Page int64) *tree {
	b := NewMemBack(int64(n) * Page)
	a := NewSeqAlloc(b, Page, 0)
	tr, _ := NewBPTree(0, a, BytesPage{a})
	return tr
}

func TestPageInsert1(t *testing.T) {
	const Page = 0x40
	tr := newTestBPTree(2, Page)

	_, p, _ := tr.a.Alloc()
	if !assert.NotNil(t, p) {
		return
	}

	n, sp := tr.p.(BytesPage).sizespace(p)
	assert.Equal(t, 0, n)
	assert.Equal(t, Page-pHead, sp)

	tr.p.(BytesPage).insert(p, 0, []byte("key1"), []byte("val__11"))

	tr.p.(BytesPage).insert(p, 1, []byte("key2"), []byte("val__22"))

	n, sp = tr.p.(BytesPage).sizespace(p)
	assert.Equal(t, 2, n)
	assert.Equal(t, 34, sp)

	t.Logf("dump\n%v", hex.Dump(p))
}

func TestPageInsert2(t *testing.T) {
	const Page = 0x40
	tr := newTestBPTree(2, Page)

	_, p, _ := tr.a.Alloc()
	if !assert.NotNil(t, p) {
		return
	}

	n, sp := tr.p.(BytesPage).sizespace(p)
	assert.Equal(t, 0, n)
	assert.Equal(t, Page-pHead, sp)

	tr.p.(BytesPage).insert(p, 0, []byte("key2"), []byte("val__22"))

	t.Logf("dump\n%v", hex.Dump(p))

	tr.p.(BytesPage).insert(p, 0, []byte("key1"), []byte("val__11"))

	t.Logf("dump\n%v", hex.Dump(p))

	k, v := tr.p.KeyValue(p, 0)
	assert.Equal(t, []byte("key1"), k)
	assert.Equal(t, []byte("val__11"), v)

	k, v = tr.p.KeyValue(p, 1)
	assert.Equal(t, []byte("key2"), k)
	assert.Equal(t, []byte("val__22"), v)
}

func TestPageUninsert1(t *testing.T) {
	const Page = 0x40
	tr := newTestBPTree(2, Page)

	_, p, _ := tr.a.Alloc()
	if !assert.NotNil(t, p) {
		return
	}

	tr.p.(BytesPage).insert(p, 0, []byte("key1"), []byte("val__11"))
	tr.p.(BytesPage).insert(p, 1, []byte("key2"), []byte("val__22"))

	t.Logf("dump\n%v", hex.Dump(p))

	tr.p.(BytesPage).uninsert(p, 1)
	t.Logf("dump\n%v", hex.Dump(p))

	tr.p.(BytesPage).uninsert(p, 0)
	t.Logf("dump\n%v", hex.Dump(p))
}

func TestPageUninsert2(t *testing.T) {
	const Page = 0x40
	tr := newTestBPTree(2, Page)

	_, p, _ := tr.a.Alloc()
	if !assert.NotNil(t, p) {
		return
	}

	tr.p.(BytesPage).insert(p, 0, []byte("key1"), []byte("val_1"))
	tr.p.(BytesPage).insert(p, 1, []byte("key2"), []byte("val___2"))

	t.Logf("dump\n%v", hex.Dump(p))

	tr.p.(BytesPage).uninsert(p, 0)
	t.Logf("dump\n%v", hex.Dump(p))

	tr.p.(BytesPage).uninsert(p, 0)
	t.Logf("dump\n%v", hex.Dump(p))
}

func TestPageMove(t *testing.T) {
	const Page = 0x40
	tr := newTestBPTree(4, Page)

	soff, s, _ := tr.a.Alloc()
	if !assert.NotNil(t, s) {
		return
	}

	roff, r, _ := tr.a.Alloc()
	if !assert.NotNil(t, r) {
		return
	}

	assert.NotEqual(t, soff, roff)

	tr.p.(BytesPage).insert(s, 0, []byte("key1"), []byte("val___11"))
	tr.p.(BytesPage).insert(s, 1, []byte("key2"), []byte("val_22"))

	tr.p.(BytesPage).move(r, s, 0, 0, 1)
	tr.p.(BytesPage).setsize(r, 1)

	t.Logf("dump s\n%v", hex.Dump(s))
	t.Logf("dump r0->0\n%v", hex.Dump(r))

	tr.p.(BytesPage).move(r, s, 0, 1, 2)
	tr.p.(BytesPage).setsize(r, 1)

	t.Logf("dump r1->0\n%v", hex.Dump(r))

	tr.p.(BytesPage).move(r, s, 0, 0, 2)
	tr.p.(BytesPage).setsize(r, 2)

	assert.Equal(t, s, r)

	t.Logf("dump r0->0 :2\n%v", hex.Dump(r))

	tr.p.(BytesPage).move(r, s, 1, 0, 1)

	t.Logf("dump r0->1\n%v", hex.Dump(r))

	k, v := tr.p.KeyValue(r, 0)
	assert.Equal(t, []byte("key1"), k)
	assert.Equal(t, []byte("val___11"), v)

	k, v = tr.p.KeyValue(r, 1)
	assert.Equal(t, []byte("key1"), k)
	assert.Equal(t, []byte("val___11"), v)

	tr.p.(BytesPage).move(r, s, 0, 1, 2)
	tr.p.(BytesPage).setsize(r, 1)
	t.Logf("dump r1->0\n%v", hex.Dump(r))

	k, v = tr.p.KeyValue(r, 0)
	assert.Equal(t, []byte("key2"), k)
	assert.Equal(t, []byte("val_22"), v)
}

func TestPagePut(t *testing.T) {
	const Page = 0x40
	tr := newTestBPTree(4, Page)

	root, _ := tr.a.Read(0)

	loff, _, l, r, _ := tr.p.Put(0, root, 0, []byte("key1"), []byte("val_1_______________"))
	assert.Nil(t, r)
	assert.Equal(t, int64(0), loff)

	t.Logf("dump\n%v", hex.Dump(l))

	loff, _, l, r, _ = tr.p.Put(loff, l, 1, []byte("key3"), []byte("val_333_______________"))
	assert.Nil(t, r)
	assert.Equal(t, int64(0), loff)

	t.Logf("dump\n%v", hex.Dump(l))

	n, sp := tr.p.(BytesPage).sizespace(l)
	assert.Equal(t, 2, n)
	assert.Equal(t, 6, sp)

	loff, roff, l, r, _ := tr.p.Put(loff, l, 1, []byte("key2"), []byte("val_22"))
	assert.NotNil(t, r)
	assert.Equal(t, int64(0), loff)
	assert.Equal(t, int64(Page), roff)

	t.Logf("dump l\n%v", hex.Dump(l))
	t.Logf("dump r\n%v", hex.Dump(r))

	k, v := tr.p.KeyValue(l, 0)
	assert.Equal(t, []byte("key1"), k)
	assert.Equal(t, []byte("val_1_______________"), v)

	k, v = tr.p.KeyValue(l, 1)
	assert.Equal(t, []byte("key2"), k)
	assert.Equal(t, []byte("val_22"), v)

	k, v = tr.p.KeyValue(r, 0)
	assert.Equal(t, []byte("key3"), k)
	assert.Equal(t, []byte("val_333_______________"), v)
}

func TestPageDel(t *testing.T) {
	const Page = 0x40
	tr := newTestBPTree(4, Page)

	_, p, _ := tr.a.Alloc()
	if !assert.NotNil(t, p) {
		return
	}

	tr.p.(BytesPage).insert(p, 0, []byte("key1"), []byte("val____________1"))
	tr.p.(BytesPage).insert(p, 1, []byte("key2"), []byte("val_____________________2"))

	t.Logf("dump\n%v", hex.Dump(p))

	loff, p, reb, _ := tr.p.Del(0, p, 0)
	assert.False(t, reb)
	assert.Equal(t, int64(0), loff)

	t.Logf("dump\n%v", hex.Dump(p))

	loff, p, reb, _ = tr.p.Del(0, p, 0)
	assert.True(t, reb)
	assert.Equal(t, int64(0), loff)

	t.Logf("dump\n%v", hex.Dump(p))
}

func TestPageLinkInsertGet(t *testing.T) {
	const Page = 0x40
	p := make([]byte, Page)
	tr := &tree{p: BytesPage{}}

	tr.p.(BytesPage).insertlink(p, 0, []byte("key2"), 0x2222)
	tr.p.(BytesPage).insertlink(p, 1, []byte("key1"), 0x1111)

	t.Logf("dump\n%v", hex.Dump(p))

	off := tr.p.Int64(p, 0)
	assert.Equal(t, int64(0x2222), off, "got %#x", off)

	off = tr.p.Int64(p, 1)
	assert.Equal(t, int64(0x1111), off, "got %#x", off)
}
