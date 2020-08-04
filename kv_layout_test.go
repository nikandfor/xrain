package xrain

import (
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKV2InsertOne(t *testing.T) {
	initLogger(t)

	const Page = 0x40

	b := NewMemBack(0)

	c := &Common{
		Back: b,
		Page: Page,
		Mask: Page - 1,
	}

	fl := NewEverGrowFreelist(c)
	c.Freelist = fl

	l := NewKVLayout2(c)

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

	tl.Printf("dump:\n%v", hex.Dump(b.d))

	st, eq := l.Seek(nil, root, []byte("key_a"), nil)
	assert.True(t, eq)

	k, ff := l.Key(st, nil)
	v := l.Value(st, nil)

	assert.Equal(t, 0x1, ff)
	assert.Equal(t, []byte("key_a"), k)
	assert.Equal(t, []byte("value_a"), v)
}

func TestKV2InsertSplit(t *testing.T) {
	initLogger(t)

	const Page = 0x40

	b := NewMemBack(0)
	c := &Common{
		Back: b,
		Page: Page,
		Mask: Page - 1,
	}

	fl := NewEverGrowFreelist(c)
	c.Freelist = fl

	l := NewKVLayout2(c)

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

	st, err = l.Insert(Stack{MakeOffIndex(root, 1)}, 0x11, []byte("key_ab"), []byte("value_ab"))
	assert.NoError(t, err)
	assert.Len(t, st, 2)

	t.Logf("dump:\n%v", hex.Dump(b.d))
}

func TestKV2InsertBig(t *testing.T) {
	initLogger(t)

	const Page = 0x100

	b := NewMemBack(0)
	c := &Common{
		Back: b,
		Page: Page,
		Mask: Page - 1,
	}

	fl := NewEverGrowFreelist(c)
	c.Freelist = fl

	l := NewKVLayout2(c)

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

	tl.Printf("dump:\n%v", l.dumpFile())

	// big
	st, err = l.Insert(Stack{MakeOffIndex(root, 2)}, 0x11, []byte("key_b_long"), longval(2*Page, "longlonglong"))
	assert.NoError(t, err)

	k, ff := l.Key(st, nil)
	v := l.Value(st, nil)

	assert.Equal(t, 0x11, ff)
	assert.Equal(t, []byte("key_b_long"), k)
	assert.Equal(t, longval(2*Page, "longlonglong"), v)

	tl.Printf("dump  st %v\n%v", st, l.dumpFile())

	// really big
	st, err = l.Insert(st, 0x12, []byte("key_b_long_long"), longval(5*Page, "longlonglong"))
	assert.NoError(t, err)

	k, ff = l.Key(st, nil)
	v = l.Value(st, nil)

	assert.Equal(t, 0x12, ff)
	assert.Equal(t, []byte("key_b_long_long"), k)
	assert.Equal(t, longval(5*Page, "longlonglong"), v)

	tl.Printf("dump  st %v\n%v", st, l.dumpFile())
}

func TestKV2InsertBigKey(t *testing.T) {
	initLogger(t)

	const Page = 0x100

	b := NewMemBack(0)
	c := &Common{
		Back: b,
		Page: Page,
		Mask: Page - 1,
	}

	fl := NewEverGrowFreelist(c)
	c.Freelist = fl

	l := NewKVLayout2(c)

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

	tl.Printf("dump  %x\n%v", root, l.dumpFile())

	// big
	st, err = l.Insert(Stack{MakeOffIndex(root, 2)}, 0x11, longval(2*Page, "key_b_long"), longval(20, "longlonglong"))
	assert.NoError(t, err)

	root = st[0].Off(Page - 1)

	tl.Printf("dump raw  %x\n%v", root, hex.Dump(b.d))
	tl.Printf("dump all  %x\n%v", root, l.dumpFile())

	//
	st, eq := l.Seek(nil, root, longval(2*Page, "key_b_long"), nil)
	assert.True(t, eq)

	k, ff := l.Key(st, nil)
	v := l.Value(st, nil)

	assert.Equal(t, 0x11, ff)
	assert.Equal(t, longval(2*Page, "key_b_long"), k)
	assert.Equal(t, longval(20, "longlonglong"), v)

	tl.Printf("dump  st %v\n%v", st, l.dumpFile())
}

func TestKV2PutDel(t *testing.T) {
	l := NewKVLayout2(nil)

	testLayoutPutDel(t, l)
}

func TestKV2Auto(t *testing.T) {
	l := NewKVLayout2(nil)

	testLayoutAuto(t, l)
}

func longval(l int, v string) []byte {
	r := make([]byte, l)
	copy(r, v)
	for i := len(v); i < len(r); i += 10 {
		copy(r[i:], "----------")
	}
	return r
}

func intval(v int64) []byte {
	r := make([]byte, 8)
	binary.BigEndian.PutUint64(r, uint64(v))
	return r
}

func nf(k []byte, f int) []byte {
	return k
}
