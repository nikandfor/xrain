package xrain

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKV2InsertOne(t *testing.T) {
	initLogger(t)

	const Page = 0x40

	b := NewMemBack(0)
	fl := NewEverGrowFreelist(b, Page, 0)

	c := &Common{
		Back:     b,
		Page:     Page,
		Mask:     Page - 1,
		Freelist: fl,
	}

	l := NewKVLayout2(c)

	root, err := l.Alloc(1)
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
}

func TestKV2InsertSplit(t *testing.T) {
	initLogger(t)

	const Page = 0x40

	b := NewMemBack(0)
	fl := NewEverGrowFreelist(b, Page, 0)

	c := &Common{
		Back:     b,
		Page:     Page,
		Mask:     Page - 1,
		Freelist: fl,
	}

	l := NewKVLayout2(c)

	root, err := l.Alloc(1)
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
	assert.Equal(t, Stack{MakeOffIndex(Page, 1)}, st)

	t.Logf("dump:\n%v", hex.Dump(b.d))
}

func TestKV2InsertBig(t *testing.T) {
	initLogger(t)

	const Page = 0x40

	b := NewMemBack(0)
	fl := NewEverGrowFreelist(b, Page, 0)

	c := &Common{
		Back:     b,
		Page:     Page,
		Mask:     Page - 1,
		Freelist: fl,
	}

	l := NewKVLayout2(c)

	root, err := l.Alloc(1)
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

	st, err = l.Insert(Stack{MakeOffIndex(root, 1)}, 0x11, []byte("key_long"), longval(Page, "longlonglong"))
	assert.NoError(t, err)
	assert.Equal(t, Stack{MakeOffIndex(Page, 1)}, st)

	t.Logf("dump:\n%v", hex.Dump(b.d))
}