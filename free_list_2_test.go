package xrain

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFreelist2NSize(t *testing.T) {
	assert.Equal(t, uint(0), nsize(1))
	assert.Equal(t, uint(1), nsize(2))
	assert.Equal(t, uint(2), nsize(3))
	assert.Equal(t, uint(2), nsize(4))
	assert.Equal(t, uint(3), nsize(5))
	assert.Equal(t, uint(3), nsize(6))
	assert.Equal(t, uint(3), nsize(7))
	assert.Equal(t, uint(3), nsize(8))
	assert.Equal(t, uint(4), nsize(9))
	assert.Equal(t, uint(4), nsize(10))
	assert.Equal(t, uint(4), nsize(11))
	assert.Equal(t, uint(4), nsize(15))
	assert.Equal(t, uint(4), nsize(16))
	assert.Equal(t, uint(5), nsize(17))
}

func TestFreelist2Align(t *testing.T) {
	b, _ := align(0x0, 0x100, 0)
	assert.Equal(t, int64(0x0), b)

	b, _ = align(0x12100, 0x100, 0)
	assert.Equal(t, int64(0x0), b)

	b, _ = align(0x13100, 0x100, 4)
	assert.Equal(t, int64(0x0), b)

	b, bs := align(0x13180, 0x100, 4)
	assert.Equal(t, int64(0x80), b)
	assert.Equal(t, 8, bs)

	b, bs = align(0x13140, 0x100, 4)
	assert.Equal(t, int64(0x40), b)
	assert.Equal(t, 4, bs)

	b, bs = align(0x131c0, 0x100, 4)
	assert.Equal(t, int64(0x40), b)
	assert.Equal(t, 4, bs)

	b, bs = align(0x13120, 0x100, 4)
	assert.Equal(t, int64(0x20), b)
	assert.Equal(t, 2, bs)

	b, bs = align(0x13160, 0x100, 4)
	assert.Equal(t, int64(0x20), b)
	assert.Equal(t, 2, bs)

	b, bs = align(0x131a0, 0x100, 4)
	assert.Equal(t, int64(0x20), b)
	assert.Equal(t, 2, bs)

	b, bs = align(0x131e0, 0x100, 4)
	assert.Equal(t, int64(0x20), b)
	assert.Equal(t, 2, bs)
}

func TestFreelist2AllowGrow1(t *testing.T) {
	initLogger(t)

	const Page = 0x40

	b := NewMemBack(1 * Page)
	c := &Common{
		Back:     b,
		Page:     Page,
		Mask:     Page - 1,
		FileNext: Page,
		Ver:      1,
	}

	l := NewFixedLayout(c)
	fl := NewFreelist2(c, l, 0)
	c.Freelist = fl

	//	fl.SetVer(1, 0)

	off, err := fl.Alloc(8)
	assert.NoError(t, err)
	assert.Equal(t, int64(8*Page), off, "%x != %x", 8*Page, off)

	//	tlog.Printf("dump:\n%v", dumpFile(pl))
}

func TestFreelist2AllowGrow2(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(1 * Page)
	c := &Common{
		Back:     b,
		Page:     Page,
		Mask:     Page - 1,
		FileNext: Page,
	}

	l := NewFixedLayout(c)
	fl := NewFreelist2(c, l, 0)
	c.Freelist = fl

	off, err := fl.Alloc(1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1*Page), off, "%x != %x", 1*Page, off)

	off, err = fl.Alloc(2)
	assert.NoError(t, err)
	assert.Equal(t, int64(2*Page), off, "%x != %x", 2*Page, off)

	off, err = fl.Alloc(2)
	assert.NoError(t, err)
	assert.Equal(t, int64(4*Page), off, "%x != %x", 4*Page, off)

	off, err = fl.Alloc(4)
	assert.NoError(t, err)
	assert.Equal(t, int64(8*Page), off, "%x != %x", 8*Page, off)
}

func TestFreelist2AllocPow(t *testing.T) {
	initLogger(t)

	const Page = 0x80

	b := NewMemBack(1 * Page)
	c := &Common{
		Back:     b,
		Page:     Page,
		Mask:     Page - 1,
		FileNext: Page,
		Ver:      1,
	}

	l := NewFixedLayout(c)
	fl := NewFreelist2(c, l, 0)
	c.Freelist = fl

	off, err := fl.Alloc(8)
	assert.NoError(t, err)
	assert.Equal(t, int64(8*Page), off, "%x != %x", 8*Page, off)

	// first page is freed now, but it can't be allocated yet
	st := fl.t.First(nil)
	assert.NotNil(t, st, "non-nil freelist expected")

	//	off, err = fl.Alloc(1)
	//	assert.NoError(t, err)
	//	assert.Equal(t, int64(1*Page), off, "%x != %x", 1*Page, off)

	tl.Printf("dump: root %x next %x (page size %x)\n%v", fl.t.Root, c.FileNext, Page, l.dumpPage(fl.t.Root))

	off, err = fl.Alloc(2)
	assert.NoError(t, err)
	//	assert.Equal(t, int64(2*Page), off, "%x != %x", 2*Page, off)

	c.Ver = 2
	c.Keep = 1
	tl.Printf("version inc")

	tl.Printf("dump: root %x next %x (page size %x)\n%v", fl.t.Root, c.FileNext, Page, l.dumpPage(fl.t.Root))

	off, err = fl.Alloc(2)
	assert.NoError(t, err)
	//	assert.Equal(t, int64(4*Page), off, "%x != %x", 4*Page, off)

	c.Keep = 2
	tl.Printf("dump: root %x next %x (page size %x)\n%v", fl.t.Root, c.FileNext, Page, l.dumpPage(fl.t.Root))

	off, err = fl.Alloc(4)
	assert.NoError(t, err)
	//	assert.Equal(t, int64(20*Page), off, "%x != %x (%x)", 20*Page, off, 4*Page)

	off, err = fl.Alloc(2)
	assert.NoError(t, err)
	//	assert.Equal(t, int64(6*Page), off, "%x != %x", 6*Page, off)

	st = fl.t.First(nil)
	if !assert.NotNil(t, st, "non-nil freelist expected") {
		return
	}

	/*
		next, _ := l.Key(st, nil)
		assert.NotNil(t, next)
		//	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0x08, 0x2}, next)
		st = fl.t.Step(st, NilPage, false)
		assert.Nil(t, st, "nil freelist expected")
	*/

	if t.Failed() {
		tl.Printf("dump: root %x next %x (page size %x)\n%v", fl.t.Root, fl.FileNext, Page, l.dumpPage(fl.t.Root))
	}
}

func TestFreelist2Alloc2(t *testing.T) {
	initLogger(t)

	const Page = 0x80

	b := NewMemBack(1 * Page)
	c := &Common{
		Back:     b,
		Page:     Page,
		Mask:     Page - 1,
		Ver:      1,
		FileNext: Page,
	}

	l := NewFixedLayout(c)
	fl := NewFreelist2(c, l, 0)
	c.Freelist = fl

	off, err := fl.Alloc(5)
	assert.NoError(t, err)
	assert.Equal(t, int64(8*Page), off, "%x != %x", 8*Page, off)

	// first page is freed now, but it can't be allocated yet
	tl.Printf("dump: root %x next %x (page size %x)\n%v", fl.t.Root, fl.FileNext, Page, l.dumpPage(fl.t.Root))

	off, err = fl.Alloc(1)
	assert.NoError(t, err)
	//	assert.Equal(t, int64(1*Page), off, "%x != %x", 0*Page, off)

	off, err = fl.Alloc(2)
	assert.NoError(t, err)
	//	assert.Equal(t, int64(2*Page), off, "%x != %x", 2*Page, off)

	off, err = fl.Alloc(2)
	assert.NoError(t, err)
	//	assert.Equal(t, int64(4*Page), off, "%x != %x", 4*Page, off)

	off, err = fl.Alloc(3)
	assert.NoError(t, err)
	//	assert.Equal(t, int64(16*Page), off, "%x != %x (%x)", 16*Page, off, 4*Page)

	off, err = fl.Alloc(2)
	assert.NoError(t, err)
	//	assert.Equal(t, int64(6*Page), off, "%x != %x", 6*Page, off)

	tl.Printf("dump: root %x next %x (page size %x)\n%v", fl.t.Root, fl.FileNext, Page, l.dumpPage(fl.t.Root))
}

func TestFreelist2Auto(t *testing.T) {
	if testing.Short() {
		return
	}

	initLogger(t)

	const Page = 0x100
	const Mask = Page - 1
	const N, M = 5000, 5

	rnd := rand.New(rand.NewSource(0))

	b := NewMemBack(1 * Page)
	c := &Common{
		Back:     b,
		Page:     Page,
		Mask:     Page - 1,
		Ver:      1,
		FileNext: Page,
	}

	l := NewFixedLayout(c)
	fl := NewFreelist2(c, l, 0)
	c.Freelist = fl

	var alloc Stack

	check := func(pr bool) {
		npages := int(fl.FileNext / Page)
		var tree, free, used int64
		pages := make([]byte, npages)
		sizes := make([]byte, npages)
		index := make([]byte, npages)
		for i := range pages {
			pages[i] = '?'
			sizes[i] = ' '
			index[i] = ' '
		}
		wid := len(fmt.Sprintf("%x", fl.FileNext))
		for i := 0; i < npages; i += 8 {
			a := fmt.Sprintf("%0*x", wid, i*Page)

			if i+len(a) <= len(index) {
				copy(index[i:], a)
			}
		}

		var walk func(int64)
		walk = func(r int64) {
			var isleaf bool
			var n int

			p := b.Access(r, 0x10)
			{
				isleaf = l.isleaf(p)
				n = l.nkeys(p)

				ext := 1 + l.overflow(p)
				tree += 1 << nsize(ext)

				i := 0
				for ; i < ext; i++ {
					pages[int(r/Page)+i] = 't'
				}
				//	for ; i < 1<<nsize(ext); i++ {
				//		pages[int(r/Page)+i] = '-'
				//	}
				copy(sizes[r/Page:], fmt.Sprintf("%x", ext))
			}
			b.Unlock(p)

			if isleaf {
				return
			}

			for i := 0; i < n; i++ {
				walk(l.link(r, i))
			}
		}
		walk(fl.t.Root)

		for st := fl.t.Step(nil, fl.t.Root, false); st != nil; st = fl.t.Step(st, fl.t.Root, false) {
			k, _ := l.Key(st, nil)

			off := int64(binary.BigEndian.Uint64(k))
			size := uint(off & c.Mask)

			free += 1 << size

			func() {
				defer func() {
					perr := recover()
					if perr != nil {
						tl.Printf("PANIC: %v", perr)
					}
				}()
				i := 0
				for ; i < 1<<size; i++ {
					idx := int(off/Page) + i
					pages[idx] = 'f'
				}
				copy(sizes[off/Page:], fmt.Sprintf("%x", 1<<size))
			}()
		}

		for _, m := range alloc {
			off, n := m.OffIndex(Mask)
			used += 1 << nsize(n)

			for i := 0; i < 1<<nsize(n); i++ {
				pages[int(off/Page)+i] = 'u'
			}
			/*
				i := 0
				for ; i < n; i++ {
					pages[int(off/Page)+i] = 'u'
				}
				for ; i < 1<<nsize(n); i++ {
					pages[int(off/Page)+i] = '-'
				}
			*/

			copy(sizes[off/Page:], fmt.Sprintf("%x", 1<<nsize(n)))
		}

		if pr != (tl.V("each").Valid()) {
			tl.Printf("in use: %v", alloc)
			tl.Printf("dump root %x next %x  ver %x %x", fl.t.Root, fl.FileNext, c.Ver, c.Keep)
			tl.Printf("pages %s <- %x = %x * %x", pages, fl.FileNext, npages, Page)
			tl.Printf("sizes %s", sizes)
			tl.Printf("index %s", index)
		}

		tot := float64(npages)
		eq := "=="
		if tree+free+used != fl.FileNext/Page {
			eq = "!="
			t.Fail()
		}

		tl.If(eq == "!=" || pr != (tl.V("each").Valid())).Printf("tree %2x + free %3x (%.2f) + used %4x (%.2f) %s file size %x", tree, free, float64(free)/tot, used, float64(used)/tot, eq, npages)

		if pr != (tl.V("each").Valid()) {
			tl.V("dump,check_dump").Printf("\n%v", l.dumpFile())
		}
	}

	check(false)

	for ver := int64(1); ver <= N; ver++ {
		c.Ver = ver
		c.Keep = ver - 1

		if rnd.Intn(2) == 0 {
			n := rnd.Intn(1<<M) + 1
			tl.V("cmd").Printf("alloc%% %2x        - ver %x", n, ver)

			off, err := fl.Alloc(n)
			if !assert.NoError(t, err) {
				break
			}
			alloc = append(alloc, MakeOffIndex(off, n))

			//	tl.Printf("alloced %d at %x, next: %x", n, off, fl.FileNext)
			p := b.Access(off, Page)
			l.setver(p, ver) //nolint:scopelint
			l.setoverflow(p, n-1)
			l.setnkeys(p, 0)
			l.pageInsert(p, 0, 0, 0xff, []byte("datakey_"), []byte("_value_|"))
			copy(p[0x18:], p[0x8:0x10])
			b.Unlock(p)
		} else if len(alloc) != 0 {
			i := rand.Intn(len(alloc))
			off, n := alloc[i].OffIndex(Mask)
			tl.V("cmd").Printf("free %% %2x %6x - ver %x", n, off, ver)

			var ver int64
			p := b.Access(off, 0x10)
			ver = l.pagever(p)
			b.Unlock(p)

			err := fl.Free(off, ver, n)
			if !assert.NoError(t, err) {
				break
			}

			if i < len(alloc) {
				copy(alloc[i:], alloc[i+1:])
			}
			alloc = alloc[:len(alloc)-1]
		}

		check(false)

		if t.Failed() {
			break
		}
	}

	check(true)
}

func TestFreelistShrinkFile(t *testing.T) {
	initLogger(t)

	const Page = 0x100

	b := NewMemBack(1 * Page)
	c := &Common{
		Back:     b,
		Page:     Page,
		Mask:     Page - 1,
		Ver:      1,
		FileNext: Page,
	}

	l := NewFixedLayout(c)
	fl := NewFreelist2(c, l, 0)
	c.Freelist = fl

	c.Ver, c.Keep = 1, -1
	off1, err := fl.Alloc(1)
	assert.NoError(t, err)

	c.Ver, c.Keep = 2, 1
	off2, err := fl.Alloc(2)
	assert.NoError(t, err)

	c.Ver, c.Keep = 3, 1
	off3, err := fl.Alloc(1)
	assert.NoError(t, err)

	c.Ver, c.Keep = 4, 2
	off4, err := fl.Alloc(4)
	assert.NoError(t, err)

	c.Ver, c.Keep = 5, 1
	fl.Free(off1, 1, 1)

	c.Ver, c.Keep = 6, 5
	fl.Free(off2, 2, 2)

	c.Ver, c.Keep = 7, 5
	fl.Free(off3, 3, 1)

	next := fl.FileNext

	c.Ver, c.Keep = 9, 8
	fl.Free(off4, 4, 3)

	if fl.FileNext >= next {
		t.Errorf("file didn't shrink")

		t.Logf("dump root %x  next %x  ver %x / %x\n%v", fl.t.Root, fl.FileNext, fl.Ver, fl.Keep, fl.l.(fileDumper).dumpFile())

		return
	}

	pver := func(off int64) (ver, size int64) {
		p := b.Access(off, 0x10)
		ver = l.pagever(p)
		size = int64(1+l.overflow(p)) * Page
		b.Unlock(p)
		return
	}

	for off := fl.FileNext; off <= next; {
		v, s := pver(off)
		if v >= c.Keep {
			t.Errorf("page %x with ver %d freed while keep == %d", off, v, c.Keep)
		}
		off += s
	}

	tl.Printf("next %x, pages %x %x %x %x", fl.FileNext, off1, off2, off3, off4)

	//	tlog.Printf("file: %x\n%v", fl.FileNext, dumpFile(pl))
}

func BenchmarkFreelist2(t *testing.B) {
	t.ReportAllocs()

	const Page, M = 0x100, 3
	const Mask = Page - 1

	rnd := rand.New(rand.NewSource(0))

	b := NewMemBack(1 * Page)
	c := &Common{
		Back:     b,
		Page:     Page,
		Mask:     Page - 1,
		Ver:      1,
		FileNext: Page,
	}

	l := NewFixedLayout(c)
	fl := NewFreelist2(c, l, 0)
	c.Freelist = fl

	var alloc Stack

	for ver := int64(1); ver <= int64(t.N); ver++ {
		c.Ver = ver
		c.Keep = ver - 1

		if rnd.Intn(2) == 0 {
			n := rnd.Intn(1<<M) + 1
			if tl.V("cmd") != nil {
				tl.Printf("alloc%% %d       - ver %x", n, ver)
			}

			off, err := fl.Alloc(n)
			if err != nil {
				assert.NoError(t, err)
				break
			}
			alloc = append(alloc, MakeOffIndex(off, n))

			//	tl.Printf("alloced %d at %x, next: %x", n, off, fl.FileNext)
			p := b.Access(off, Page)
			l.setver(p, ver) //nolint:scopelint
			l.setoverflow(p, n-1)
			l.setnkeys(p, 0)
			l.pageInsert(p, 0, 0, 0xff, []byte("datakey_"), []byte("_value_|"))
			copy(p[0x18:], p[0x8:0x10])
			b.Unlock(p)
		} else if len(alloc) != 0 {
			i := rand.Intn(len(alloc))
			off, n := alloc[i].OffIndex(Mask)
			if tl.V("cmd") != nil {
				tl.Printf("free %% %d %5x - ver %x", n, off, ver)
			}

			var ver int64
			p := b.Access(off, 0x10)
			ver = l.pagever(p)
			b.Unlock(p)

			err := fl.Free(off, ver, n)
			if err != nil {
				assert.NoError(t, err)
				break
			}

			if i < len(alloc) {
				copy(alloc[i:], alloc[i+1:])
			}
			alloc = alloc[:len(alloc)-1]
		}

		if t.Failed() {
			break
		}
	}
}
