package xrain

import (
	"encoding/hex"
	"log"
	"math/rand"
	"sort"
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

	pl.pageInsert(p, 0, 0, 0, []byte("key2"), []byte("value2"))
	pl.pageInsert(p, 0, 1, 0, []byte("key1"), []byte("value1"))
	pl.pageInsert(p, 2, 2, 0, []byte("key3"), []byte("value3"))

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
	const Page = 0x80

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	testPagePutSplit8(t, pl, 0x20)
}

func TestPageKVPutSplitAlloc8(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))
	pl.SetVer(1)

	testPagePutSplit8(t, pl, 0x20)
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

	off, _, _ = pl.Insert(off, 0, 0, []byte("key_aaaa"), []byte("value_aa"))
	assert.Equal(t, true, pl.NeedRebalance(off))

	off, _, _ = pl.Insert(off, 1, 0, []byte("key_aaaa"), []byte("value_aa"))
	off, _, _ = pl.Insert(off, 2, 0, []byte("key_aaaa"), []byte("value_aa"))
	off, _, _ = pl.Insert(off, 3, 0, []byte("key_aaaa"), []byte("value_aa"))

	assert.Equal(t, false, pl.NeedRebalance(off))
}

func TestPageKVSiblings8(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	off := int64(0)

	off, _, _ = pl.Insert(off, 0, 0, []byte("key_aaaa"), intval(10))
	off, _, _ = pl.Insert(off, 1, 0, []byte("key_bbbb"), intval(20))
	off, _, _ = pl.Insert(off, 2, 0, []byte("key_cccc"), intval(30))

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

func TestPageKVIndexEncoding(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	order := []string{"abqwerty", "abqw11", "abcdef222", "abqwer222", "abcdefgh", "abcd11", "ab0"}
	sorted := make([]string, len(order))
	copy(sorted, order)
	sort.Strings(sorted)

	t.Logf("sorted: %v", sorted)

	var loff, roff int64
	var err error
out1:
	for j, k := range order {
		F := sort.Search(len(order), func(i int) bool {
			return k <= sorted[i]
		})

		i, eq := pl.Search(loff, []byte(k))
		assert.False(t, eq)

		//	t.Logf("insert F %d key %q pos %d", 1+F, k, i)

		loff, roff, err = pl.Insert(loff, i, 1+F, []byte(k), longval(2, "__"))
		assert.NoError(t, err)
		assert.EqualValues(t, -1, roff)

		//	t.Logf("dump:\n%v", hex.Dump(b.d[:Page]))

		subsorted := make([]string, len(order[:j+1]))
		copy(subsorted, order)
		sort.Strings(subsorted)

		for i, k := range subsorted {
			F := sort.Search(len(order), func(i int) bool {
				return k <= sorted[i]
			})
			//	t.Logf("get %d want [%x] %q", i, 1+F, k)
			//	tlog.Printf("get %d want [%x] %q", i, 1+F, k)

			kk, ff := pl.Key(loff, i, nil)
			assert.Equal(t, 1+F, ff)
			assert.Equal(t, []byte(k), kk, "want %q got %q", k, kk)

			if t.Failed() {
				break out1
			}
		}
	}

	if t.Failed() {
		return
	}

out2:
	for j := len(order) - 1; j >= 0; j-- {
		k := order[j]

		i, eq := pl.Search(loff, []byte(k))
		assert.True(t, eq)

		t.Logf("delete key %q pos %d", k, i)
		loff, err = pl.Delete(loff, i)
		assert.NoError(t, err)

		t.Logf("dump:\n%v", hex.Dump(b.d[:Page]))

		subsorted := make([]string, len(order[:j]))
		copy(subsorted, order)
		sort.Strings(subsorted)

		for i, k := range subsorted {
			F := sort.Search(len(order), func(i int) bool {
				return k <= sorted[i]
			})
			t.Logf("get %d want [%x] %q", i, 1+F, k)

			kk, ff := pl.Key(loff, i, nil)
			assert.Equal(t, 1+F, ff)
			assert.Equal(t, []byte(k), kk, "want %q got %q", k, kk)

			vv := pl.Value(loff, i, nil)
			assert.Equal(t, []byte("__"), vv, "value want %q got %q", "__", vv)

			if t.Failed() {
				break out2
			}
		}
	}
}

func TestPageKVIndexEncoding2(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	loff, _, _ := pl.Insert(0, 0, 0, []byte("abcdefgh"), []byte("__"))
	loff, _, _ = pl.Insert(loff, 0, 0, []byte("abcd"), nil)

	loff, _ = pl.Delete(loff, 0)

	k, _ := pl.Key(loff, 0, nil)
	v := pl.Value(loff, 0, nil)

	assert.Equal(t, []byte("abcdefgh"), k)
	assert.Equal(t, []byte("__"), v)

	t.Logf("dump:\n%v", hex.Dump(b.d[:Page]))
}

func TestPageKVIndexEncoding3(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	loff := int64(0)
	loff, _, _ = pl.Insert(loff, 0, 0, []byte("abcdefgh0123456++"), []byte("__"))
	loff, roff, err := pl.Insert(loff, 0, 0, []byte("abcdefgh0123456"), nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(NilPage), roff)

	loff, roff, err = pl.Insert(loff, 2, 0, []byte("bbb"), []byte("__"))
	assert.NoError(t, err)
	assert.Equal(t, int64(NilPage), roff)

	t.Logf("dump:\n%v", hex.Dump(b.d[:Page]))

	loff, _ = pl.Delete(loff, 0)

	k, _ := pl.Key(loff, 0, nil)
	v := pl.Value(loff, 0, nil)

	assert.Equal(t, []byte("abcdefgh0123456++"), k)
	assert.Equal(t, []byte("__"), v)

	t.Logf("dump:\n%v", hex.Dump(b.d[:Page]))
}

func TestPageKVIndexEncoding4(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(2 * Page)
	pl := NewKVLayout(b, Page, NewEverGrowFreelist(b, Page, 0))

	loff, _, _ := pl.Insert(0, 0, 0, []byte("abcdefgh012345678"), []byte("______________"))
	loff, _, _ = pl.Insert(loff, 0, 0, []byte("abcdefgh"), nil)

	t.Logf("dump:\n%v", hex.Dump(b.d[:Page]))

	loff, _ = pl.Delete(loff, 0)

	k, _ := pl.Key(loff, 0, nil)
	v := pl.Value(loff, 0, nil)

	assert.Equal(t, []byte("abcdefgh012345678"), k)
	assert.Equal(t, []byte("______________"), v)

	t.Logf("dump:\n%v", hex.Dump(b.d[:Page]))
}

func BenchmarkPageKVIndexSearch(b *testing.B) {
	const Page = 0x1000
	const M = 160

	back := NewMemBack(2 * Page)
	pl := NewKVLayout(back, Page, NewEverGrowFreelist(back, Page, 0))

	off := int64(0)
	for i := 0; i < M; i++ {
		var k [8]byte
		for j := range k {
			k[j] = byte(rand.Intn(2))
		}

		pos, _ := pl.Search(off, k[:])

		var roff int64
		var err error
		off, roff, err = pl.Insert(off, pos, 0, k[:], []byte("______________"))
		assert.NoError(b, err)
		assert.EqualValues(b, NilPage, roff)
	}

	b.ReportAllocs()
	b.ResetTimer()

	p := back.d[:Page]
	n := pl.nkeys(p)
	b.Logf("page filled: n %d used %x free %x of %x", n, pl.pagedatasize(p, n), pl.pagefree(p, n), Page)

	for i := 0; i < b.N; i++ {
		var k [8]byte
		for j := range k {
			k[j] = byte(rand.Intn(2))
		}

		pl.Search(off, k[:])
	}

	//	b.Logf("max: %d", max)
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
