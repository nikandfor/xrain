package xrain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTreeAllocSeq1(t *testing.T) {
	const Page = 0x20
	b := NewMemBack(0 * Page)
	a, _ := NewTreeAlloc(b, Page, 0)

	off, _, err := a.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), off, "%#4x", off)

	off, _, err = a.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, int64(Page), off, "%#4x", off)
}

func TestTreeAllocSeq2(t *testing.T) {
	const Page = 0x20
	b := NewMemBack(2 * Page)
	a, _ := NewTreeAlloc(b, Page, 0)

	off, _, err := a.Write(0, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(2*Page), off, "%#4x", off)

	off, _, err = a.Write(off, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(2*Page), off, "%#4x", off)
}

func TestTreeAllocTree(t *testing.T) {
	const Page = 0x20
	b := NewMemBack(2 * Page)
	a, _ := NewTreeAlloc(b, Page, 2)

	off, _, err := a.Write(0, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(2*Page), off, "%#4x", off)

	off, _, err = a.Write(off, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(2*Page), off, "%#4x", off)

	err = a.Free(off)
	assert.NoError(t, err)

	off, _, err = a.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, int64(2*Page), off, "%#4x", off)

	off, _, err = a.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, int64(4*Page), off, "%#4x", off)

	err = a.Free(2 * Page)
	assert.NoError(t, err)
	err = a.Free(4 * Page)
	assert.NoError(t, err)

	off, _, err = a.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, int64(2*Page), off, "%#4x", off)

	off, _, err = a.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, int64(4*Page), off, "%#4x", off)
}
