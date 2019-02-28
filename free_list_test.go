package xrain

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFSizeConstants(t *testing.T) {
	assert.Equal(t, 1, B)
	assert.Equal(t, 1024, KiB)
	assert.Equal(t, 1024*1024, MiB)
	assert.Equal(t, 1024*1024*1024, GiB)
}

func TestDumpFreeList(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(0 * Page)
	fl := NewNoRewriteFreeList(Page, b)

	off, err := fl.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), off)

	off, err = fl.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, 1*int64(Page), off)

	off, err = fl.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, 2*int64(Page), off)

	err = fl.Reclaim(off, 0)
	assert.NoError(t, err)

	off, err = fl.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, 3*int64(Page), off)
}

func TestFreeListManual(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(1 * Page)
	pl := &IntLayout{BaseLayout: BaseLayout{b: b, page: Page}}

	log.Printf("First")

	fl := NewFreeList(0, Page, Page, 0, -1, b)

	off1, err := fl.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, 1*int64(Page), off1)
	pl.setver(b.Load(off1, Page), 0)

	off2, err := fl.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, 2*int64(Page), off2)
	pl.setver(b.Load(off2, Page), 0)

	err = fl.Reclaim(off2, 0)
	assert.NoError(t, err)

	err = fl.Reclaim(off1, 0)
	assert.NoError(t, err)

	log.Printf("Second")

	fl = NewFreeList(fl.t.root, fl.next, Page, 1, 0, b)

	off3, err := fl.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, 3*int64(Page), off3)
	pl.setver(b.Load(off3, Page), 1)

	log.Printf("dump free root %x  next %x\n%v", fl.t.root, fl.next, dumpFile(pl))

	log.Printf("Third")

	fl = NewFreeList(fl.t.root, fl.next, Page, 2, 1, b)

	off1, err = fl.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, 1*int64(Page), off1, "%x %x", Page, off1)
	pl.setver(b.Load(off1, Page), 2)

	off2, err = fl.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, 4*int64(Page), off2, "%x %x", 2*Page, off2)
	pl.setver(b.Load(off2, Page), 2)

	log.Printf("dump free root %x  next %x\n%v", fl.t.root, fl.next, dumpFile(pl))
}

func TestFreeListAuto(t *testing.T) {
	const (
		Page = 0x40
		N    = 2
		M    = 1
	)

	b := NewMemBack(1 * Page)
	pl := &IntLayout{BaseLayout: BaseLayout{b: b, page: Page}}

	log.Printf("First")
	fl := NewFreeList(0, Page, Page, 0, -1, b)

	var used []int64

	alloc := func(ver int64) {
		off, err := fl.Alloc()
		assert.NoError(t, err)

		log.Printf("ver %3d alloc %x", ver, off)

		pl.setver(b.Load(off, Page), ver)

		used = append(used, off)
	}
	free := func(cv int64) {
		l := len(used) - 1
		off := used[l]
		used = used[:l]

		ver := pl.getver(b.Load(off, Page))

		log.Printf("ver %3d free  %x", cv, off)

		err := fl.Reclaim(off, ver)
		assert.NoError(t, err)
	}

	basever := int64(0)

	for j := 0; j < M; j++ {
		log.Printf("ver %3d  j %d first", basever, j)

		for i := 0; i < 3*N; i++ {
			ver := basever + int64(i)

			fl = NewFreeList(fl.t.root, fl.next, Page, ver, ver-1, b)

			if (i+1)%3 == 0 {
				free(ver)
			} else {
				alloc(ver)
			}

			assert.False(t, fl.lock)
			assert.Empty(t, fl.deferred)

			log.Printf("f %d/%d ___ root %x  next %x used %x\n%v", j, i, fl.t.root, fl.next, used, dumpFile(pl))
		}

		basever += 3 * N

		log.Printf("dump free root %x  next %x\n%v", fl.t.root, fl.next, dumpFile(pl))

		log.Printf("ver %3d  j %d second - using %x", basever, j, used)

		for i := 0; i < 3*N; i++ {
			ver := basever + int64(i)

			fl = NewFreeList(fl.t.root, fl.next, Page, ver, ver-1, b)

			//	log.Printf("st %d free root %x  next %x used %x\n%v", i, fl.t.root, fl.next, used, dumpFile(pl))

			if (i)%3 != 0 {
				free(ver)
			} else {
				alloc(ver)
			}

			assert.False(t, fl.lock)
			assert.Empty(t, fl.deferred)

			log.Printf("s %d/%d ___ root %x  next %x used %x\n%v", j, i, fl.t.root, fl.next, used, dumpFile(pl))
		}

		basever += 3 * N

		assert.True(t, len(used) == 0, "test is broken")

		log.Printf("dump free root %x  next %x\n%v", fl.t.root, fl.next, dumpFile(pl))
	}
}
