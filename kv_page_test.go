package xrain

import (
	"encoding/hex"
	"log"
	"testing"

	"github.com/nikandfor/tlog"
	"github.com/stretchr/testify/assert"
)

func TestPageKVIsLeaf(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, nil)

	pl.SetLeaf(0, true)
	pl.SetLeaf(Page, false)

	assert.Equal(t, true, pl.IsLeaf(0))
	assert.Equal(t, false, pl.IsLeaf(Page))
}

func TestPageKVAllocFree(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	off, err := pl.Alloc(false)
	assert.NoError(t, err)

	assert.Equal(t, false, pl.IsLeaf(off))
	assert.Equal(t, 0, pl.NKeys(off))

	err = pl.Free(off, true)
	assert.NoError(t, err)
}

func TestPageKVPageDataoff(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	p := b.Access(0, Page)

	pl.setnkeys(p, 2)
	pl.setdataoff(p, 0, Page-0x10)
	pl.setdataoff(p, 1, Page-0x10-0x18)

	assert.Panics(t, func() { pl.dataoff(p, -1) })
	assert.Equal(t, Page-0x10, pl.dataoff(p, 0))
	assert.Equal(t, Page-0x28, pl.dataoff(p, 1))
	assert.Equal(t, 0, pl.dataoff(p, 2))

	assert.Equal(t, Page, pl.dataend(p, 0))
	assert.Equal(t, Page-0x10, pl.dataend(p, 1))
	assert.Equal(t, Page-0x28, pl.dataend(p, 2))
	assert.Equal(t, 0, pl.dataend(p, 3))

	assert.Equal(t, Page-0x28-kvIndexStart-2*2, pl.pagefree(p, 2))

	if t.Failed() {
		t.Logf("page:\n%v", hex.Dump(p))
	}

	b.Unlock(p)
}

func TestPageKVPageInsert(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	p := b.Access(0, Page)

	assert.Equal(t, 0, pl.nkeys(p))

	pl.pageInsert(p, 0, 0, []byte("key2"), []byte("value2"))
	pl.pageInsert(p, 0, 1, []byte("key1"), []byte("value1"))
	pl.pageInsert(p, 2, 2, []byte("key3"), []byte("value3"))

	t.Logf("page %x\n%v", 0, hex.Dump(p))

	b.Unlock(p)
}

func TestPageKVPutOnePage8(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	testPagePutOnePage8(t, pl)

	if t.Failed() {
		tlog.Printf("dump\n%v", hex.Dump(b.d))
	}
}

func TestPageKVPutOnePageAlloc8(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))
	pl.SetVer(1)

	testPagePutOnePage8(t, pl)

	if t.Failed() {
		log.Printf("dump\n%v", hex.Dump(b.d))
	}
}

func TestPageKVPutSplit8(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	testPagePutSplit8(t, pl, 0xc)
}

func TestPageKVPutSplitAlloc8(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))
	pl.SetVer(1)

	testPagePutSplit8(t, pl, 0xc)
}

func TestPageKVKeyCmpLast8(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	testPageKeyCmpLast8(t, pl)
}

func TestPageKVPutInt648(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	testPagePutInt648(t, pl)
}

func TestPageKVPutDelOnePage8(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	off := testPagePutOnePage8(t, pl)
	testPageDelOnePage8(t, off, pl)
}

func TestPageKVPutDelOnePageAlloc8(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	off := testPagePutOnePage8(t, pl)

	pl.ver++

	noff := testPageDelOnePage8(t, off, pl)

	assert.NotEqual(t, off, noff)
}

func TestPageKVNeedRebalance8(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	off := int64(0)

	off, _, _ = pl.Insert(off, 0, []byte("key_aaaa"), []byte("value_aa"))
	assert.Equal(t, true, pl.NeedRebalance(off))

	off, _, _ = pl.Insert(off, 1, []byte("key_aaaa"), []byte("value_aa"))
	off, _, _ = pl.Insert(off, 2, []byte("key_aaaa"), []byte("value_aa"))
	off, _, _ = pl.Insert(off, 3, []byte("key_aaaa"), []byte("value_aa"))

	assert.Equal(t, false, pl.NeedRebalance(off))
}

func TestPageKVSiblings8(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	off := int64(0)

	off, _, _ = pl.InsertInt64(off, 0, []byte("key_aaaa"), 10)
	off, _, _ = pl.InsertInt64(off, 1, []byte("key_bbbb"), 20)
	off, _, _ = pl.InsertInt64(off, 2, []byte("key_cccc"), 30)

	li, loff, roff := pl.Siblings(off, 0, 40)
	assert.EqualValues(t, 0, li)
	assert.EqualValues(t, 40, loff)
	assert.EqualValues(t, 20, roff)

	li, loff, roff = pl.Siblings(off, 1, 40)
	assert.EqualValues(t, 0, li)
	assert.EqualValues(t, 10, loff)
	assert.EqualValues(t, 40, roff)

	li, loff, roff = pl.Siblings(off, 2, 40)
	assert.EqualValues(t, 1, li)
	assert.EqualValues(t, 20, loff)
	assert.EqualValues(t, 40, roff)
}

func TestPageKVRebalance8(t *testing.T) {
	const Page = 0x100

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	var ver int64

	testPageRebalance8(t, pl, 1, 3, b, pl.free, &ver, false)

	testPageRebalance8(t, pl, 2, 6, b, pl.free, &ver, true)

	testPageRebalance8(t, pl, 6, 2, b, pl.free, &ver, true)
}

func TestPageKVFree(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	off, err := pl.Alloc(false)
	assert.NoError(t, err)

	err = pl.Free(off, false)
	assert.NoError(t, err)
}
