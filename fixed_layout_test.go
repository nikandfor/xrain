package xrain

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFixedInsertOne(t *testing.T) {
	initLogger(t)

	const Page = 0x40

	b := NewMemBack(0)
	m := &Meta{
		Back: b,
		Page: Page,
		Mask: Page - 1,
	}

	fl := NewEverGrowFreelist(m)
	m.Freelist = fl

	l := NewFixedLayout(m)
	l.SetKVSize(0, 5, 7, 1)

	root, err := l.Alloc()
	require.NoError(t, err)

	st, err := l.Insert(Stack{MakeOffIndex(root, 0)}, 0x1, []byte("key_a"), []byte("value_a"))
	assert.NoError(t, err)
	assert.Equal(t, Stack{MakeOffIndex(root, 0)}, st)

	st, err = l.Insert(Stack{MakeOffIndex(root, 1)}, 0x3, []byte("key_c"), []byte("value_c"))
	assert.NoError(t, err)
	assert.Equal(t, Stack{MakeOffIndex(root, 1)}, st)

	st, err = l.Insert(Stack{MakeOffIndex(root, 1)}, 0x2, []byte("key_b"), []byte("value_b"))
	assert.NoError(t, err)
	assert.Equal(t, Stack{MakeOffIndex(root, 1)}, st)

	t.Logf("dump:\n%v", hex.Dump(b.d))

	tr := NewLayoutShortcut(l, root, Page-1)

	v, _ := tr.Get([]byte("key_0"), nil)
	assert.Nil(t, v)

	v, _ = tr.Get([]byte("key_a"), nil)
	assert.Equal(t, []byte("value_a"), v)

	v, _ = tr.Get([]byte("key_b"), nil)
	assert.Equal(t, []byte("value_b"), v)

	v, _ = tr.Get([]byte("key_c"), nil)
	assert.Equal(t, []byte("value_c"), v)
}

func TestFixedSplitGet(t *testing.T) {
	initLogger(t)

	const Page = 0x40

	b := NewMemBack(0)
	m := &Meta{
		Back: b,
		Page: Page,
		Mask: Page - 1,
	}

	fl := NewEverGrowFreelist(m)
	m.Freelist = fl

	l := NewFixedLayout(m)
	l.SetKVSize(1, 5, 7, 1)

	root, err := l.Alloc()
	require.NoError(t, err)

	st, err := l.Insert(Stack{MakeOffIndex(root, 0)}, 0x1, []byte("key_a"), []byte("value_a"))
	assert.NoError(t, err)
	assert.Equal(t, Stack{MakeOffIndex(root, 0)}, st)

	st, err = l.Insert(Stack{MakeOffIndex(root, 1)}, 0x3, []byte("key_c"), []byte("value_c"))
	assert.NoError(t, err)
	assert.Equal(t, Stack{MakeOffIndex(root, 1)}, st)

	st, err = l.Insert(Stack{MakeOffIndex(root, 1)}, 0x2, []byte("key_b"), []byte("value_b"))
	assert.NoError(t, err)
	assert.Equal(t, Stack{MakeOffIndex(root, 1)}, st)

	st, err = l.Insert(Stack{MakeOffIndex(root, 3)}, 0x4, []byte("key_d"), []byte("value_d"))
	assert.NoError(t, err)
	assert.Equal(t, Stack{MakeOffIndex(3*Page, 1), MakeOffIndex(2*Page, 1)}, st)

	t.Logf("dump:\n%v", hex.Dump(b.d))

	st, eq := l.Seek(nil, 3*Page, []byte("key_d"), nil)
	assert.Equal(t, Stack{MakeOffIndex(3*Page, 1), MakeOffIndex(2*Page, 1)}, st)
	assert.True(t, eq)

	tr := NewLayoutShortcut(l, 3*Page, Page-1)

	v, ff := tr.Get([]byte("key_0"), nil)
	assert.Nil(t, v)
	assert.Equal(t, 0, ff)

	v, ff = tr.Get([]byte("key_a"), nil)
	assert.Equal(t, []byte("value_a"), v)
	assert.Equal(t, 0x1, ff)

	v, ff = tr.Get([]byte("key_b"), nil)
	assert.Equal(t, []byte("value_b"), v)
	assert.Equal(t, 0x2, ff)

	v, ff = tr.Get([]byte("key_c"), nil)
	assert.Equal(t, []byte("value_c"), v)
	assert.Equal(t, 0x3, ff)

	v, ff = tr.Get([]byte("key_d"), nil)
	assert.Equal(t, []byte("value_d"), v)
	assert.Equal(t, 0x4, ff)
}

func TestFixedPutDel(t *testing.T) {
	l := NewFixedLayout(nil)
	l.SetKVSize(1, 5, 7, 1)

	testLayoutPutDel(t, l)
}

func testLayoutPutDel(t *testing.T, l Layout) {
	initLogger(t)

	const Page = 0x40

	b := NewMemBack(0)
	m := &Meta{
		Back: b,
		Page: Page,
		Mask: Page - 1,
	}

	fl := NewEverGrowFreelist(m)
	m.Freelist = fl

	l.SetMeta(m)

	tr := NewLayoutShortcut(l, NilPage, Page-1)

	_, err := tr.Put(0x1, []byte("key_a"), []byte("value_a"), nil)
	assert.NoError(t, err)

	_, err = tr.Put(0x3, []byte("key_c"), []byte("value_c"), nil)
	assert.NoError(t, err)

	_, err = tr.Put(0x2, []byte("key_b"), []byte("value_b"), nil)
	assert.NoError(t, err)

	_, err = tr.Put(0x4, []byte("key_d"), []byte("value_d"), nil)
	assert.NoError(t, err)

	tl.Printf("dump: %x\n%v", tr.Root, hex.Dump(b.d))

	_, err = tr.Del([]byte("key_a"), nil)
	assert.NoError(t, err)

	_, err = tr.Del([]byte("key_d"), nil)
	assert.NoError(t, err)

	tl.Printf("dump: %x\n%v", tr.Root, hex.Dump(b.d))

	v, ff := tr.Get([]byte("key_b"), nil)
	assert.Equal(t, []byte("value_b"), v)
	assert.Equal(t, 0x2, ff)

	v, ff = tr.Get([]byte("key_c"), nil)
	assert.Equal(t, []byte("value_c"), v)
	assert.Equal(t, 0x3, ff)
}

func TestFixedAuto(t *testing.T) {
	l := NewFixedLayout(nil)
	l.SetKVSize(1, 7, 9, 1)

	testLayoutAuto(t, l)
}

func testLayoutAuto(t *testing.T, l Layout) {
	initLogger(t)

	const Page = 0x80
	const N, Prime, Prime2 = 100, 29, 17

	b := NewMemBack(0)
	m := &Meta{
		Back: b,
		Page: Page,
		Mask: Page - 1,
	}

	fl := NewEverGrowFreelist(m)
	m.Freelist = fl

	l.SetMeta(m)

	tr := NewLayoutShortcut(l, NilPage, Page-1)

	mix := func(i int) int {
		return i * Prime % Prime2
	}

	key := func(i int) []byte {
		return []byte(fmt.Sprintf("key_%03x", i))
	}

	value := func(i int) []byte {
		return []byte(fmt.Sprintf("value_%03x", i))
	}

	var err error
	exp := map[string][]byte{}
	cnt := map[string]int{}

	check := func() bool {
		var last []byte

		n := 0
		for st := tr.First(nil); st != nil; st = tr.Next(st) {
			key, ff := tr.Key(st, nil)
			val := tr.Value(st, nil)

			assert.Equal(t, key[4:], val[6:])
			assert.Equal(t, []byte("key_"), key[:4])
			assert.Equal(t, []byte("value_"), val[:6])
			assert.Equal(t, fmt.Sprintf("%02x", ff), string(key[5:]))

			assert.Equal(t, exp[string(key)], val)

			assert.True(t, bytes.Compare(last, key) <= 0, "%q before %q", last, key)

			n++
			last = key
		}

		sum := 0
		for _, c := range cnt {
			sum += c
		}

		if !assert.Equal(t, sum, n) {
			tl.Printf("expected %s %v", exp, cnt)
		}

		return !t.Failed()
	}

	func() {
		for i := 0; i < N; i++ {
			j := mix(i)

			k := key(j)
			v := value(j)
			switch i % 4 {
			case 0, 1:
				tl.V("cmd").Printf("CMD put %q -> %q  %x / %x", k, v, i, N)
				exp[string(k)] = v
				cnt[string(k)]++
				_, err = tr.Put(j, k, v, nil)
			case 2:
				tl.V("cmd").Printf("CMD del %q        %x / %x", k, i, N)
				if c := cnt[string(k)]; c > 0 {
					cnt[string(k)]--
					if c == 1 {
						delete(exp, string(k))
					}
				}
				_, err = tr.Del(k, nil)
			case 3:
				val, ff := tr.Get(k, nil)
				if v, ok := exp[string(k)]; ok {
					assert.Equal(t, j&0xff, ff)
					assert.Equal(t, v, val)
				}
			}
			assert.NoError(t, err)

			if i%4 != 3 {
				tl.V("each_dump").Printf("dump  root %x\n%v", tr.Root, l.(fileDumper).dumpFile())
			}

			if !check() {
				return
			}
		}

		if tl.V("each") == nil {
			tl.Printf("dump: %x\n%v", tr.Root, l.(fileDumper).dumpFile())
		}

		for st := tr.First(nil); st != nil; st = tr.First(st) {

			key, _ := tr.Key(st, nil)

			tl.V("cmd").Printf("cmd del %q", key)

			if c := cnt[string(key)]; c > 0 {
				cnt[string(key)]--
				if c == 1 {
					delete(exp, string(key))
				}
			}
			_, err = tr.Delete(st)
			assert.NoError(t, err)

			tl.V("each").Printf("dump: %x\n%v", tr.Root, l.(fileDumper).dumpFile())

			if !check() {
				return
			}
		}
	}()

	if t.Failed() {
		n := 0
		for st := tr.First(nil); st != nil; st = tr.Next(st) {
			key, ff := tr.Key(st, nil)
			val := tr.Value(st, nil)

			tl.Printf("iter[%3d]  %2x %q -> %q   st %v", n, ff, key, val, st)

			n++
		}

		tl.Printf("dump: %x\n%v", tr.Root, l.(fileDumper).dumpFile())
		tl.Printf("file:\n%v", hex.Dump(b.d))
	} else if tl.V("each") == nil {
		tl.Printf("dump: %x\n%v", tr.Root, l.(fileDumper).dumpFile())
	}
}
