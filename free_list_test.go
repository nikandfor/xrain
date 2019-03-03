package xrain

import (
	"encoding/binary"
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
	t.Skip()

	const Page = 0x40

	b := NewMemBack(2 * Page)
	pl := &IntLayout{BaseLayout: BaseLayout{b: b, page: Page}}

	log.Printf("First")

	fl := NewFreeList(0, Page, 2*Page, Page, 0, -1, b)

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

	fl = NewFreeList(fl.t1.root, fl.t0.root, fl.next, Page, 1, 0, b)

	off3, err := fl.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, 3*int64(Page), off3)
	pl.setver(b.Load(off3, Page), 1)

	log.Printf("dump free root %x %x  next %x\n%v", fl.t0.root, fl.t1.root, fl.next, dumpFile(pl))

	log.Printf("Third")

	fl = NewFreeList(fl.t0.root, fl.t1.root, fl.next, Page, 2, 1, b)

	off1, err = fl.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, 1*int64(Page), off1, "%x %x", Page, off1)
	pl.setver(b.Load(off1, Page), 2)

	off2, err = fl.Alloc()
	assert.NoError(t, err)
	assert.Equal(t, 4*int64(Page), off2, "%x %x", 2*Page, off2)
	pl.setver(b.Load(off2, Page), 2)

	log.Printf("dump free root %x %x  next %x\n%v", fl.t0.root, fl.t1.root, fl.next, dumpFile(pl))
}

func TestFreeListAuto(t *testing.T) {
	defer func() {
		debugChecks = false
	}()
	debugChecks = true

	const (
		Page = 0x100
		N    = 100
	)

	b := NewMemBack(2 * Page)
	pl := &IntLayout{BaseLayout: BaseLayout{b: b, page: Page}}

	fl := NewFreeList(0, Page, 2*Page, Page, 0, -1, b)

	var taken []int64
	var used, recl map[int64]struct{}
	var lastused int64
	var lastgrow int

	alloc := func(ver int64) {
		off, err := fl.Alloc()
		assert.NoError(t, err)

		//	log.Printf("ver %3d alloc %x", ver, off)

		pl.setver(b.Load(off, Page), ver)

		taken = append(taken, off)
	}
	free := func(cv int64) {
		l := len(taken) - 1
		off := taken[l]
		taken = taken[:l]

		ver := pl.getver(b.Load(off, Page))

		//	log.Printf("ver %3d free  %x", cv, off)

		err := fl.Reclaim(off, ver)
		assert.NoError(t, err)
	}
	check := func(n int) bool {
		used = map[int64]struct{}{}
		recl = map[int64]struct{}{}
		lastused = 0

		var add func(int64)
		add = func(off int64) {
			if pl.IsLeaf(off) {
				for i := 0; i < pl.NKeys(off); i++ {
					k := pl.Key(off, i)
					off := int64(binary.BigEndian.Uint64(k))

					recl[off] = struct{}{}
				}
			} else {
				for i := 0; i < pl.NKeys(off); i++ {
					sub := pl.Int64(off, i)
					add(sub)
				}
			}
			used[off] = struct{}{}
			if off > lastused {
				lastused = off
			}
		}

		add(fl.t0.root)
		add(fl.t1.root)

		for p := range used {
			if _, ok := recl[p]; ok {
				t.Errorf("page %x is both free and used", p)
			}
		}
		if len(recl)+len(used)+n != int(fl.next/Page) {
			t.Errorf("%d pages in file, but %d + %d used and %d free", fl.next/Page, n, len(used), len(recl))
		}

		return t.Failed()
	}

	basever := int64(0)
	maxnext := int64(0)

	var j int
	for j = 0; j < 5; j++ {
		//	log.Printf("ver %3d  j %d first", basever, j)

		for ii := 0; ii < 2; ii++ {
			for i := 0; i < 3*N; i++ {
				ver := basever + int64(i)

				fl = NewFreeList(fl.t1.root, fl.t0.root, fl.next, Page, ver, ver-1, b)

				available, available2 := 0, 0
				nextwas := fl.next
				{
					calc := func(t *Tree, c *int) {
						for k := t.Next(nil); k != nil; k = t.Next(k) {
							v := t.Get(k)
							ver := int64(binary.BigEndian.Uint64(v))
							if ver < fl.keep {
								(*c)++
							}
						}
					}
					calc(fl.t0, &available)
					calc(fl.t1, &available2)
				}

				if (i+1-ii)%3 == 0 == (ii == 0) {
					free(ver)
				} else {
					alloc(ver)
				}

				assert.False(t, fl.lock)
				assert.Empty(t, fl.deferred)

				//	log.Printf("%d %d/%d ___ root %x %x  next %x taken %x\n%v", ii, j, i, fl.t0.root, fl.t1.root, fl.next, taken, dumpFile(pl))

				if check(len(taken)) {
					return
				}

				//	log.Printf("out of %d pages: %d taken %d used %d free", fl.next/Page, len(taken), len(used), len(recl))

				{
					cnt := 0
					for off := int64(0); off < fl.next; off += Page {
						p := b.Load(off, Page)
						if pl.getver(p) == fl.ver {
							cnt++
						}
					}
					if fl.next != nextwas {
						if j > 1 {
							log.Fatalf("we changed %3d pages out of %3d (%3d) for update. next %3d <- %3d. is: %d %d %d", cnt, available, available2, fl.next/Page, nextwas/Page, j, ii, i)
						}
						lastgrow = j + 1
					}
				}

				if fl.next > maxnext {
					t := fl.t0
					for k := t.Next(nil); k != nil; k = t.Next(k) {
						v := t.Get(k)
						ver := int64(binary.BigEndian.Uint64(v))
						if ver < fl.keep {
							log.Fatalf("allocated more pages than needed %d -> %d", maxnext/Page, fl.next/Page)
						}
					}
				}

				maxnext = fl.next
			}

			basever += 3 * N
		}

		assert.True(t, len(taken) == 0, "test is broken")

		//	log.Printf("dump free root %x %x  next %x\n%v", fl.t0.root, fl.t1.root, fl.next, dumpFile(pl))
		//	log.Printf("out of %d pages: %d taken %d used %d free. lastused %5x (%3d) ver %4d", fl.next/Page, len(taken), len(used), len(recl), lastused, lastused/Page, fl.ver)
	}
	//	log.Printf("dump free root %x %x  next %x\n%v", fl.t0.root, fl.t1.root, fl.next, dumpFile(pl))
	log.Printf("out of %d pages: %d taken %d used %d free. lastused %5x (%3d) ver %4d", fl.next/Page, len(taken), len(used), len(recl), lastused, lastused/Page, fl.ver)

	log.Printf("for page size 0x%x and %d*3 alloc/free cycles we've made %d iterations, last file grow was at %d", Page, N, j, lastgrow)
}

func BenchmarkFreeListVerInc(t *testing.B) {
	const Page = 0x100

	b := NewMemBack(2 * Page)
	pl := &IntLayout{BaseLayout: BaseLayout{b: b, page: Page}}

	fl := NewFreeList(0, Page, 2*Page, Page, 0, -1, b)

	var taken []int64

	alloc := func(ver int64) {
		off, err := fl.Alloc()
		assert.NoError(t, err)

		//	log.Printf("ver %3d alloc %x", ver, off)

		pl.setver(b.Load(off, Page), ver)

		taken = append(taken, off)
	}
	free := func(cv int64) {
		l := len(taken) - 1
		off := taken[l]
		taken = taken[:l]

		ver := pl.getver(b.Load(off, Page))

		//	log.Printf("ver %3d free  %x", cv, off)

		err := fl.Reclaim(off, ver)
		assert.NoError(t, err)
	}

	for i := 0; i < t.N; i++ {
		ver := int64(i)

		fl = NewFreeList(fl.t1.root, fl.t0.root, fl.next, Page, ver, ver-1, b)

		if (i+1)%3 == 0 {
			free(ver)
		} else {
			alloc(ver)
		}
	}
}

func BenchmarkFreeListVerConst(t *testing.B) {
	const Page = 0x100

	b := NewMemBack(2 * Page)
	pl := &IntLayout{BaseLayout: BaseLayout{b: b, page: Page}}

	fl := NewFreeList(0, Page, 2*Page, Page, 0, -1, b)

	var taken []int64

	alloc := func(ver int64) {
		off, err := fl.Alloc()
		assert.NoError(t, err)

		//	log.Printf("ver %3d alloc %x", ver, off)

		pl.setver(b.Load(off, Page), ver)

		taken = append(taken, off)
	}
	free := func(cv int64) {
		l := len(taken) - 1
		off := taken[l]
		taken = taken[:l]

		ver := pl.getver(b.Load(off, Page))

		//	log.Printf("ver %3d free  %x", cv, off)

		err := fl.Reclaim(off, ver)
		assert.NoError(t, err)
	}

	for i := 0; i < t.N; i++ {
		//	ver := int64(i)

		//	fl = NewFreeList(fl.t1.root, fl.t0.root, fl.next, Page, ver, ver-1, b)

		if (i+1)%3 == 0 {
			free(fl.ver)
		} else {
			alloc(fl.ver)
		}
	}
}
