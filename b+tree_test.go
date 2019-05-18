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

	for i := 0; i < N; i++ {
		q := ((i + 1) * Prime) % N
		k := fmt.Sprintf("key_%04x", q)
		v := fmt.Sprintf("value_%02x", q)
		_, err := tr.Put([]byte(k), []byte(v))
		assert.NoError(t, err)
	}

	q := 0
	for k := tr.Next(nil); k != nil; k = tr.Next(k) {
		ek := fmt.Sprintf("key_%04x", q)
		ev := fmt.Sprintf("value_%02x", q)
		v := tr.Get(k)

		assert.EqualValues(t, ek, k)
		assert.EqualValues(t, ev, v)

		q++
	}
	assert.Equal(t, N, q)

	q = N
	for k := tr.Prev(nil); k != nil; k = tr.Prev(k) {
		q--

		ek := fmt.Sprintf("key_%04x", q)
		ev := fmt.Sprintf("value_%02x", q)
		v := tr.Get(k)

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
	for k := tr.Next(nil); k != nil; k = tr.Next(k) {
		ek := fmt.Sprintf("key_%04x", q)
		ev := fmt.Sprintf("value_%02x", q)
		v := tr.Get(k)

		assert.EqualValues(t, ek, k)
		assert.EqualValues(t, ev, v)

		q++
	}
	assert.Equal(t, N, q)

	q = N
	for k := tr.Prev(nil); k != nil; k = tr.Prev(k) {
		q--

		ek := fmt.Sprintf("key_%04x", q)
		ev := fmt.Sprintf("value_%02x", q)
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

	for k := tr.Next(nil); k != nil; k = tr.Next(k) {
		_, err := tr.Del(k)
		assert.NoError(t, err)
	}
}
