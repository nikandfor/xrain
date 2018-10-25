package xrain

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTreeAllocSeq1(t *testing.T) {
	const Page = 0x20
	b := NewMemBack(0 * Page)
	a, _ := NewTreeAlloc(b, Page, 0, 1, 0)

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
	a, _ := NewTreeAlloc(b, Page, 0, 1, 0)

	off, _, err := a.Write(0, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(2*Page), off, "%#4x", off)

	off, _, err = a.Write(off, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(2*Page), off, "%#4x", off)
}

func TestTreeAllocTree(t *testing.T) {
	const Page = 0x40
	b := NewMemBack(2 * Page)
	a, _ := NewTreeAlloc(b, Page, 2, 1, 0)

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
	assert.Equal(t, int64(4*Page), off, "%#4x", off)

	off, _, err = a.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, int64(5*Page), off, "%#4x", off)

	err = a.Free(2 * Page)
	assert.NoError(t, err)
	err = a.Free(4 * Page)
	assert.NoError(t, err)

	off, _, err = a.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, int64(6*Page), off, "%#4x", off)

	off, _, err = a.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, int64(7*Page), off, "%#4x", off)
}

func TestTreeAllocAllocFree(t *testing.T) {
	const Page = 0x40
	b := NewMemBack(1 * Page)

	a, _ := NewTreeAlloc(b, Page, 0, 1, 0)

	off, _, err := a.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, int64(1*Page), off, "%#4x", off)

	t.Logf("off %#4x", off)

	err = a.Free(off)
	assert.NoError(t, err)

	t.Logf("a.t.root %#4x", a.t.root)

	a, _ = NewTreeAlloc(b, Page, a.t.root, 2, 1)

	off, _, err = a.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, int64(3*Page), off, "%#4x", off)

	t.Logf("off %#4x", off)

	err = a.Free(off)
	assert.NoError(t, err)

	t.Logf("a.t.root %#4x", a.t.root)

	a, _ = NewTreeAlloc(b, Page, a.t.root, 3, 2)

	log.Printf("alloc")

	off, _, err = a.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, int64(1*Page), off, "%#4x", off)

	t.Logf("off %#4x", off)

	err = a.Free(off)
	assert.NoError(t, err)

	t.Logf("a.t.root %#4x", a.t.root)

	p0 := b.buf
	for s := 0; s < len(p0); s += Page {
		t.Logf("dump %#4x: %v", s, BytesPage{}.DumpHex(p0[s:s+Page]))
	}
}
