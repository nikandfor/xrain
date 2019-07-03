package xrain

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTreeSmall(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(Page)
	fl := NewEverGrowFreelist(b, Page, 0)
	pl := NewFixedLayout(b, Page, fl)
	tr := NewTree(pl, 0, Page)

	v, err := tr.Put([]byte("key_aaaa"), []byte("value_aa"))
	assert.NoError(t, err)
	assert.Nil(t, v)

	assert.Equal(t, 1, tr.Size())

	v = tr.Get([]byte("key_aaaa"))
	assert.EqualValues(t, "value_aa", v)

	v, err = tr.Put([]byte("key_aaaa"), []byte("value_22"))
	assert.NoError(t, err)
	assert.EqualValues(t, []byte("value_aa"), v)

	assert.Equal(t, 1, tr.Size())

	v = tr.Get([]byte("key_aaaa"))
	assert.EqualValues(t, "value_22", v)

	v, err = tr.Del([]byte("some_key"))
	assert.NoError(t, err)
	assert.Nil(t, v)

	assert.Equal(t, 1, tr.Size())

	v, err = tr.Del([]byte("key_aaaa"))
	assert.NoError(t, err)
	assert.EqualValues(t, "value_22", v)

	assert.Equal(t, 0, tr.Size())

	v = tr.Get([]byte("key_aaaa"))
	assert.Nil(t, v)

	assert.EqualValues(t, 0, tr.depth)
}

func TestTreeIterator(t *testing.T) {
	const (
		Page  = 0x80
		N     = 20
		Prime = 101
	)

	b := NewMemBack(Page)
	fl := NewEverGrowFreelist(b, Page, 0)
	pl := NewFixedLayout(b, Page, fl)
	tr := NewTree(pl, 0, Page)

	assert.Nil(t, tr.Step(nil, true))
	assert.Nil(t, tr.Step(nil, false))

	for i := 0; i < N; i++ {
		q := ((i + 1) * Prime) % N
		k := fmt.Sprintf("key_%04x", q)
		v := fmt.Sprintf("value_%02x", q)
		_, err := tr.Put([]byte(k), []byte(v))
		assert.NoError(t, err)
	}

	q := 0
	for st := tr.Step(nil, false); st != nil; st = tr.Step(st, false) {
		off, i := st.OffIndex(0x7f)

		ek := fmt.Sprintf("key_%04x", q)
		ev := fmt.Sprintf("value_%02x", q)
		k := pl.Key(off, i, nil)
		v := pl.Value(off, i, nil)

		assert.Equal(t, []byte(ek), k)
		assert.Equal(t, []byte(ev), v)

		q++
	}
	assert.Equal(t, N, q)

	q = N
	for st := tr.Step(nil, true); st != nil; st = tr.Step(st, true) {
		off, i := st.OffIndex(0x7f)

		q--

		ek := fmt.Sprintf("key_%04x", q)
		ev := fmt.Sprintf("value_%02x", q)
		k := pl.Key(off, i, nil)
		v := pl.Value(off, i, nil)

		assert.EqualValues(t, ek, k)
		assert.EqualValues(t, ev, v)
	}
	assert.Equal(t, 0, q)
}

func TestTreeBig(t *testing.T) {
	const (
		Page   = 0x80
		N      = 100
		Prime  = 113
		Prime2 = 117
	)

	b := NewMemBack(Page)
	fl := NewEverGrowFreelist(b, Page, 0)
	pl := NewFixedLayout(b, Page, fl)
	tr := NewTree(pl, 0, Page)

	for i := 0; i < 2*N; i++ {
		q := ((i + 1) * Prime) % N
		k := fmt.Sprintf("key_%04x", q)
		v := fmt.Sprintf("value_%02x", q)

		have := tr.Get([]byte(k))
		n := tr.Size()

		_, err := tr.Put([]byte(k), []byte(v))
		assert.NoError(t, err)

		if have != nil {
			assert.EqualValues(t, n, tr.Size())
		} else {
			assert.EqualValues(t, n+1, tr.Size())
		}
	}

	q := 0
	for st := tr.Step(nil, false); st != nil; st = tr.Step(st, false) {
		off, i := st.OffIndex(0x7f)

		ek := fmt.Sprintf("key_%04x", q)
		ev := fmt.Sprintf("value_%02x", q)
		k := pl.Key(off, i, nil)
		v := tr.Get(k)

		assert.EqualValues(t, ek, k)
		assert.EqualValues(t, ev, v)

		q++
	}
	assert.Equal(t, N, q)

	q = N
	for st := tr.Step(nil, true); st != nil; st = tr.Step(st, true) {
		off, i := st.OffIndex(0x7f)
		q--

		ek := fmt.Sprintf("key_%04x", q)
		ev := fmt.Sprintf("value_%02x", q)
		k := pl.Key(off, i, nil)
		v := tr.Get(k)

		assert.EqualValues(t, ek, k)
		assert.EqualValues(t, ev, v)
	}
	assert.Equal(t, 0, q)

	for i := 0; i < 2*N; i++ {
		q := ((i + 1) * Prime2) % N
		k := fmt.Sprintf("key_%04x", q)
		v := fmt.Sprintf("value_%02x", q)

		have := tr.Get([]byte(k))
		n := tr.Size()

		if (i+1)*Prime%3 == 0 {
			_, err := tr.Put([]byte(k), []byte(v))
			assert.NoError(t, err)

			if have != nil {
				assert.EqualValues(t, n, tr.Size())
			} else {
				assert.EqualValues(t, n+1, tr.Size())
			}
		} else {
			_, err := tr.Del([]byte(k))
			assert.NoError(t, err)

			if have != nil {
				assert.EqualValues(t, n-1, tr.Size())
			} else {
				assert.EqualValues(t, n, tr.Size())
			}
		}
	}

	for st := tr.Step(nil, false); st != nil; st = tr.Step(st, false) {
		off, i := st.OffIndex(0x7f)

		k := pl.Key(off, i, nil)
		_, err := tr.Del(k)

		assert.NoError(t, err)
	}
}

func TestTreeCopy(t *testing.T) {
	tr := NewTree(nil, 0, 0)
	cp := tr.Copy()

	assert.Equal(t, tr, cp)
	assert.False(t, tr == cp)
}

func buildBenchBigTree(b *testing.B) Tree {
	const (
		Page   = 0x80
		N      = 100000
		Prime  = 113
		Prime2 = 117
	)

	bk := NewMemBack(Page)
	fl := NewEverGrowFreelist(bk, Page, 0)
	pl := NewFixedLayout(bk, Page, fl)
	tr := NewTree(pl, 0, Page)

	for i := 0; i < N; i++ {
		q := ((i + 1) * Prime) % N
		k := fmt.Sprintf("key%05x", q)
		v := fmt.Sprintf("val%05x", q)

		tr.Get([]byte(k))

		_, err := tr.Put([]byte(k), []byte(v))
		assert.NoError(b, err)
	}

	//	tlog.Printf("dump:\n%v", dumpFile(pl))

	b.ResetTimer()
	//	tlog.Printf("tree root %x size %d depth %d", tr.Root(), tr.Size(), tr.depth)

	return tr
}

func BenchmarkTreeIterator(b *testing.B) {
	tr := buildBenchBigTree(b)
	pl := tr.PageLayout()

	for j := 0; j < b.N; j++ {
		for st := tr.Step(nil, false); st != nil; st = tr.Step(st, false) {
			off, i := st.OffIndex(0x7f)
			_ = pl.Key(off, i, nil)
			_ = pl.Value(off, i, nil)
		}
	}
}
