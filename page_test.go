package xrain

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"log"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPageFixedIsLeaf(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(2 * Page)
	pl := NewFixedLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	l, r := b.Access2(0, 0x10, Page, 0x10)
	l[4] = 0x00
	r[4] = 0x80
	b.Unlock2(l, r)

	assert.Equal(t, true, pl.IsLeaf(0))
	assert.Equal(t, false, pl.IsLeaf(Page))
}

func TestPageFixedAllocRoot(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(2 * Page)
	pl := NewFixedLayout(b, Page, NewEverGrowFreelist(b, Page, 0))
	pl.SetKVSize(8, 8, 2)
	pl.SetVer(3)

	off, err := pl.Alloc(false)
	assert.NoError(t, err)

	p := b.Access(off, 0x10)
	assert.Equal(t, 0, pl.nkeys(p))
	assert.Equal(t, int64(3), pl.getver(p))
	assert.Equal(t, 1, pl.overflow(p))
	b.Unlock(p)
}

func TestPageFixedPutOnePage8(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(2 * Page)
	pl := NewFixedLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	testPagePutOnePage8(t, pl)
}

func TestPageFixedPutOnePageAlloc8(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(2 * Page)
	pl := NewFixedLayout(b, Page, NewEverGrowFreelist(b, Page, 0))
	pl.SetVer(1)

	testPagePutOnePage8(t, pl)

	if t.Failed() {
		log.Printf("dump\n%v", hex.Dump(b.d))
	}
}

func TestPageFixedPutSplit8(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(2 * Page)
	pl := NewFixedLayout(b, Page, NewEverGrowFreelist(b, Page, 0))
	pl.SetKVSize(8, 0x10, 1)

	testPagePutSplit8(t, pl)
}

func TestPageFixedPutSplitAlloc8(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(2 * Page)
	pl := NewFixedLayout(b, Page, NewEverGrowFreelist(b, Page, 0))
	pl.SetKVSize(8, 0x10, 1)
	pl.SetVer(1)

	testPagePutSplit8(t, pl)
}

func TestPageFixedKeyCmpLast8(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(2 * Page)
	pl := NewFixedLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	testPageKeyCmpLast8(t, pl)
}

func TestPageFixedPutInt648(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(2 * Page)
	pl := NewFixedLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	testPagePutInt648(t, pl)
}

func TestPageFixedPutDelOnePage8(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(2 * Page)
	pl := NewFixedLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	off := testPagePutOnePage8(t, pl)
	testPageDelOnePage8(t, off, pl)
}

func TestPageFixedPutDelOnePageAlloc8(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(2 * Page)
	pl := NewFixedLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	off := testPagePutOnePage8(t, pl)

	pl.ver++

	noff := testPageDelOnePage8(t, off, pl)

	assert.NotEqual(t, off, noff)
}

func TestPageFixedNeedRebalance8(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(2 * Page)
	pl := NewFixedLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	off := int64(0)

	off, _, _ = pl.Insert(off, 0, []byte("key_aaaa"), []byte("value_aa"))
	assert.Equal(t, true, pl.NeedRebalance(off))

	off, _, _ = pl.Insert(off, 1, []byte("key_aaaa"), []byte("value_aa"))
	off, _, _ = pl.Insert(off, 2, []byte("key_aaaa"), []byte("value_aa"))
	off, _, _ = pl.Insert(off, 3, []byte("key_aaaa"), []byte("value_aa"))

	assert.Equal(t, false, pl.NeedRebalance(off))
}

func TestPageFixedSiblings8(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(2 * Page)
	pl := NewFixedLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

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

func TestPageFixedRebalance8(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(2 * Page)
	pl := NewFixedLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	var ver int64

	testPageRebalance8(t, pl, 1, 3, b, pl.free, &ver, false)

	testPageRebalance8(t, pl, 2, 6, b, pl.free, &ver, true)

	testPageRebalance8(t, pl, 6, 2, b, pl.free, &ver, true)
}

func TestPageFixedFree(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(2 * Page)
	pl := NewFixedLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	off, err := pl.Alloc(false)
	assert.NoError(t, err)

	err = pl.Free(off, false)
	assert.NoError(t, err)
}

func testPagePutOnePage8(t *testing.T, pl PageLayout) int64 {
	loff, roff, err := pl.Insert(0, 0, []byte("key_aaaa"), []byte("value_aa"))
	assert.NoError(t, err)
	assert.EqualValues(t, -1, roff)

	loff, roff, err = pl.Insert(loff, 1, []byte("key_cccc"), []byte("value_cc"))
	assert.NoError(t, err)
	assert.EqualValues(t, -1, roff)

	loff, roff, err = pl.Insert(loff, 1, []byte("key_bbbb"), []byte("value_bb"))
	assert.NoError(t, err)
	assert.EqualValues(t, -1, roff)

	assert.EqualValues(t, "key_aaaa", pl.Key(loff, 0, nil))
	assert.EqualValues(t, "key_bbbb", pl.Key(loff, 1, nil))
	assert.EqualValues(t, "key_cccc", pl.Key(loff, 2, nil))

	assert.EqualValues(t, "value_aa", pl.Value(loff, 0, nil))
	assert.EqualValues(t, "value_bb", pl.Value(loff, 1, nil))
	assert.EqualValues(t, "value_cc", pl.Value(loff, 2, nil))

	return loff
}

func testPagePutSplit8(t *testing.T, pl PageLayout) {
	loff, roff, err := pl.Insert(0, 0, []byte("key_aaaa"), longval(0x10, "value_aa"))
	assert.NoError(t, err)
	assert.EqualValues(t, -1, roff)

	loff, roff, err = pl.Insert(loff, 1, []byte("key_cccc"), longval(0x10, "value_cc"))
	assert.NoError(t, err)
	assert.EqualValues(t, -1, roff)

	loff, roff, err = pl.Insert(loff, 1, []byte("key_bbbb"), longval(0x10, "value_bb"))
	assert.NoError(t, err)

	assert.EqualValues(t, 2, pl.NKeys(loff))
	assert.EqualValues(t, 1, pl.NKeys(roff))

	assert.EqualValues(t, "key_aaaa", pl.Key(loff, 0, nil))
	assert.EqualValues(t, "key_bbbb", pl.Key(loff, 1, nil))
	assert.EqualValues(t, "key_cccc", pl.Key(roff, 0, nil))

	assert.Equal(t, longval(0x10, "value_aa"), pl.Value(loff, 0, nil))
	assert.Equal(t, longval(0x10, "value_bb"), pl.Value(loff, 1, nil))
	assert.Equal(t, longval(0x10, "value_cc"), pl.Value(roff, 0, nil))

	loff, roff, err = pl.Insert(roff, 1, []byte("key_dddd"), longval(0x10, "value_dd"))
	assert.NoError(t, err)
	assert.EqualValues(t, -1, roff)

	loff, roff, err = pl.Insert(loff, 2, []byte("key_eeee"), longval(0x10, "value_ee"))
	assert.NoError(t, err)

	assert.EqualValues(t, 1, pl.NKeys(loff))
	assert.EqualValues(t, 2, pl.NKeys(roff))

	assert.EqualValues(t, "key_cccc", pl.Key(loff, 0, nil))
	assert.EqualValues(t, "key_dddd", pl.Key(roff, 0, nil))
	assert.EqualValues(t, "key_eeee", pl.Key(roff, 1, nil))

	assert.Equal(t, longval(0x10, "value_cc"), pl.Value(loff, 0, nil))
	assert.Equal(t, longval(0x10, "value_dd"), pl.Value(roff, 0, nil))
	assert.Equal(t, longval(0x10, "value_ee"), pl.Value(roff, 1, nil))
}

func testPageKeyCmpLast8(t *testing.T, pl PageLayout) {
	loff, roff, err := pl.Insert(0, 0, []byte("key_aaaa"), []byte("value_aa"))
	assert.NoError(t, err)
	assert.EqualValues(t, -1, roff)

	loff, roff, err = pl.Insert(loff, 1, []byte("key_cccc"), []byte("value_cc"))
	assert.NoError(t, err)
	assert.EqualValues(t, -1, roff)

	loff, roff, err = pl.Insert(loff, 1, []byte("key_bbbb"), []byte("value_bb"))
	assert.NoError(t, err)
	assert.EqualValues(t, -1, roff)

	assert.Equal(t, 3, pl.NKeys(loff))

	assert.Equal(t, 1, bytes.Compare(pl.Key(loff, 0, nil), []byte(nil)))
	assert.Equal(t, -1, bytes.Compare(pl.Key(loff, 0, nil), []byte("key_bbbb")))
	assert.Equal(t, 0, bytes.Compare(pl.Key(loff, 1, nil), []byte("key_bbbb")))
	assert.Equal(t, 1, bytes.Compare(pl.Key(loff, 2, nil), []byte("key_bbbb")))

	assert.EqualValues(t, "key_cccc", pl.LastKey(loff, nil))
}

func testPagePutInt648(t *testing.T, pl PageLayout) {
	loff, roff, err := pl.InsertInt64(0, 0, []byte("key_aaaa"), 1)
	assert.NoError(t, err)
	assert.EqualValues(t, -1, roff)

	loff, roff, err = pl.InsertInt64(loff, 1, []byte("key_cccc"), 3)
	assert.NoError(t, err)
	assert.EqualValues(t, -1, roff)

	loff, roff, err = pl.InsertInt64(loff, 1, []byte("key_bbbb"), 2)
	assert.NoError(t, err)
	assert.EqualValues(t, -1, roff)

	assert.EqualValues(t, "key_aaaa", pl.Key(loff, 0, nil))
	assert.EqualValues(t, "key_bbbb", pl.Key(loff, 1, nil))
	assert.EqualValues(t, "key_cccc", pl.Key(loff, 2, nil))

	assert.EqualValues(t, 1, pl.Int64(loff, 0))
	assert.EqualValues(t, 2, pl.Int64(loff, 1))
	assert.EqualValues(t, 3, pl.Int64(loff, 2))
}

func testPageDelOnePage8(t *testing.T, loff int64, pl PageLayout) int64 {
	if !assert.Equal(t, 3, pl.NKeys(loff)) {
		return NilPage
	}

	old := loff

	loff, err := pl.Delete(loff, 0)
	assert.NoError(t, err)

	log.Printf("Del %d, %x <- %x %+v\n%v", pl.(*FixedLayout).ver, loff, old, pl, dumpFile(pl))

	if !assert.Equal(t, 2, pl.NKeys(loff)) {
		return NilPage
	}

	assert.EqualValues(t, "key_bbbb", pl.Key(loff, 0, nil))
	assert.EqualValues(t, "key_cccc", pl.Key(loff, 1, nil))

	loff, err = pl.Delete(loff, 1)
	assert.NoError(t, err)

	if !assert.Equal(t, 1, pl.NKeys(loff)) {
		return NilPage
	}

	assert.EqualValues(t, "key_bbbb", pl.Key(loff, 0, nil))

	loff, err = pl.Delete(loff, 0)
	assert.NoError(t, err)

	assert.Equal(t, 0, pl.NKeys(loff))

	return loff
}

func testPageRebalance8(t *testing.T, pl PageLayout, ln, rn int, b *MemBack, fl Freelist, ver *int64, alloc bool) {
	loff, err := fl.Alloc(1)
	assert.NoError(t, err)

	roff, err := fl.Alloc(1)
	assert.NoError(t, err)

	{
		l, r := b.Access2(loff, 0x10, roff, 0x10)
		bl := &BaseLayout{}
		bl.setver(l, *ver)
		bl.setver(r, *ver)
		b.Unlock2(l, r)
	}

	v := int64(0)

	for _, tc := range []struct {
		off int64
		n   int
	}{
		{off: loff, n: ln},
		{off: roff, n: rn},
	} {
		for i := 0; i < tc.n; i++ {
			v++
			off, rr, err := pl.InsertInt64(tc.off, i, []byte(fmt.Sprintf("key_%04x", v)), v)
			if !assert.NoError(t, err) {
				return
			}
			if !assert.EqualValues(t, NilPage, rr) {
				return
			}
			if !assert.Equal(t, tc.off, off) {
				log.Printf("dump old\n%v\n%v", hex.Dump(b.d[tc.off:tc.off+0x80]), hex.Dump(b.d[off:off+0x80]))
				return
			}
		}
	}

	if alloc {
		(*ver)++
		pl.SetVer(*ver)
		fl.SetVer(*ver, *ver-1)
	}

	l, r, err := pl.Rebalance(loff, roff)
	assert.NoError(t, err)

	checkPage := func(off int64, z, n int) {
		if !assert.Equal(t, n-z, pl.NKeys(off)) {
			return
		}
		for i := 0; i < n-z; i++ {
			k := pl.Key(off, i, nil)
			assert.EqualValues(t, fmt.Sprintf("key_%04x", i+z+1), k)
		}
	}

	if r == NilPage {
		checkPage(l, 0, ln+rn)
		return
	}

	ls := pl.NKeys(l)
	rs := pl.NKeys(r)

	if ls+rs != ln+rn {
		t.Errorf("%d + %d elements rebalanced into %d + %d", ln, rn, ls, rs)
	}

	if d := ls - rs; d < -1 || d > 1 {
		t.Errorf("too big difference: %d %d", ls, rs)
		return
	}

	checkPage(l, 0, ls)
	checkPage(r, ls, ls+rs)
}

func longval(l int, v string) []byte {
	r := make([]byte, l)
	copy(r, v)
	return r
}

func BenchmarkCRC32(b *testing.B) {
	const Page = 4 << 10 // 4KiB
	b.ReportAllocs()
	b.SetBytes(Page)

	data := make([]byte, Page)
	_, _ = rand.Read(data)

	for i := 0; i < b.N; i++ {
		_ = crc32.ChecksumIEEE(data)
	}
}
