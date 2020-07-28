package xrain

import (
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"testing"

	"github.com/nikandfor/tlog"
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
	const Page = 0x40

	b := NewMemBack(1 * Page)
	pl := NewFixedLayout(b, Page, nil)
	tr := NewTree(pl, 0, Page)
	fl := NewFreelist2(b, tr, Page, Page)

	//	fl.SetVer(1, 0)

	off, err := fl.Alloc(8)
	assert.NoError(t, err)
	assert.Equal(t, int64(8*Page), off, "%x != %x", 8*Page, off)

	//	tlog.Printf("dump:\n%v", dumpFile(pl))
}

func TestFreelist2AllowGrow2(t *testing.T) {
	const Page = 0x40

	b := NewMemBack(1 * Page)
	pl := NewFixedLayout(b, Page, nil)
	tr := NewTree(pl, 0, Page)
	fl := NewFreelist2(b, tr, Page, Page)

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
	const Page = 0x80
	const Mask = Page - 1

	b := NewMemBack(1 * Page)
	pl := NewFixedLayout(b, Page, nil)
	tr := NewTree(pl, 0, Page)
	fl := NewFreelist2(b, tr, Page, Page)

	pl.SetFreelist(fl)
	fl.SetVer(1, -1)

	off, err := fl.Alloc(8)
	assert.NoError(t, err)
	assert.Equal(t, int64(8*Page), off, "%x != %x", 8*Page, off)

	// first page is freed now, but it can't be allocated yet

	st := tr.Step(nil, false)
	assert.NotNil(t, st, "non-nil freelist expected")

	off, err = fl.Alloc(2)
	assert.NoError(t, err)
	assert.Equal(t, int64(2*Page), off, "%x != %x", 2*Page, off)

	off, err = fl.Alloc(2)
	assert.NoError(t, err)
	assert.Equal(t, int64(4*Page), off, "%x != %x", 4*Page, off)

	dump, psz := dumpPage(pl, tr.root)
	tlog.Printf("dump: root %x (page size %x)\n%v", tr.root, psz, dump)

	off, err = fl.Alloc(4)
	assert.NoError(t, err)
	assert.Equal(t, int64(16*Page), off, "%x != %x (%x)", 8*Page, off, 4*Page)

	off, err = fl.Alloc(2)
	assert.NoError(t, err)
	assert.Equal(t, int64(6*Page), off, "%x != %x", 6*Page, off)

	st = tr.Step(nil, false)
	if !assert.NotNil(t, st, "nil freelist expected") {
		return
	}
	off, i := st.LastOffIndex(Mask)
	next, _ := pl.Key(off, i, nil)
	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 0}, next)
	st = tr.Step(st, false)
	assert.Nil(t, st, "nil freelist expected")

	if t.Failed() {
		dump, psz = dumpPage(pl, tr.root)
		tlog.Printf("dump: root %x (page size %x)\n%v", tr.root, psz, dump)
	}
}

func TestFreelist2Alloc2(t *testing.T) {
	const Page = 0x80

	b := NewMemBack(1 * Page)
	pl := NewFixedLayout(b, Page, nil)
	tr := NewTree(pl, 0, Page)
	fl := NewFreelist2(b, tr, Page, Page)

	pl.SetFreelist(fl)
	fl.SetVer(1, -1)

	off, err := fl.Alloc(5)
	assert.NoError(t, err)
	assert.Equal(t, int64(8*Page), off, "%x != %x", 8*Page, off)

	// first page is freed now, but it can't be allocated yet

	off, err = fl.Alloc(2)
	assert.NoError(t, err)
	assert.Equal(t, int64(2*Page), off, "%x != %x", 2*Page, off)

	off, err = fl.Alloc(2)
	assert.NoError(t, err)
	assert.Equal(t, int64(4*Page), off, "%x != %x", 4*Page, off)

	off, err = fl.Alloc(3)
	assert.NoError(t, err)
	assert.Equal(t, int64(16*Page), off, "%x != %x (%x)", 8*Page, off, 4*Page)

	off, err = fl.Alloc(2)
	assert.NoError(t, err)
	assert.Equal(t, int64(6*Page), off, "%x != %x", 6*Page, off)

	dump, psz := dumpPage(pl, tr.root)
	tlog.Printf("dump: root %x (page size %x)\n%v", tr.root, psz, dump)
}

func TestFreelist2Auto(t *testing.T) {
	const Page = 0x100
	const N, M = 5000, 6
	const prOnce, prEach, prCmd = true, false, false

	rnd := rand.New(rand.NewSource(0))

	b := NewMemBack(1 * Page)
	pl := NewFixedLayout(b, Page, nil)
	tr := NewTree(pl, 0, Page)
	fl := NewFreelist2(b, tr, Page, Page)

	pl.SetFreelist(fl)

	type mem struct {
		off int64
		n   int
	}
	var alloc []mem

	check := func(pr bool) {
		var tree, free, used int64
		pages := make([]byte, fl.next/Page)
		sizes := make([]byte, fl.next/Page)
		for i := range pages {
			pages[i] = '_'
			sizes[i] = ' '
		}

		var walk func(int64)
		walk = func(r int64) {
			p := b.Access(r, 0x10)
			{
				ext := 1 + pl.overflow(p)
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

			if pl.IsLeaf(r) {
				return
			}

			n := pl.NKeys(r)
			for i := 0; i < n; i++ {
				walk(pl.Int64(r, i))
			}
		}
		walk(tr.root)

		for st := tr.Step(nil, false); st != nil; st = tr.Step(st, false) {
			poff, pi := st.LastOffIndex(fl.mask)
			k, _ := pl.Key(poff, pi, nil)

			off := int64(binary.BigEndian.Uint64(k))
			size := uint(off & fl.mask)

			free += 1 << size

			i := 0
			for ; i < 1<<size; i++ {
				idx := int(off/Page) + i
				pages[idx] = 'f'
			}
			copy(sizes[off/Page:], fmt.Sprintf("%x", 1<<size))
		}

		for _, m := range alloc {
			used += 1 << nsize(m.n)

			i := 0
			for ; i < m.n; i++ {
				pages[int(m.off/Page)+i] = 'u'
			}
			for ; i < 1<<nsize(m.n); i++ {
				pages[int(m.off/Page)+i] = '-'
			}

			copy(sizes[m.off/Page:], fmt.Sprintf("%x", 1<<nsize(m.n)))
		}

		if pr {
			tlog.Printf("in use: %x", alloc)
			tlog.Printf("dump root %x next %x  ver %x %x", tr.root, fl.next, fl.ver, fl.keep)
			tlog.Printf("pages %s <- %x = %x * %x", pages, fl.next, fl.next/Page, Page)
			tlog.Printf("sizes %s", sizes)
			//	tlog.Printf("%v", dumpFile(pl))
		}

		frac := float64(free) / float64(fl.next/Page)
		if tree+free+used != fl.next/Page {
			t.Errorf("tree %x + free %x (%.2f) + used %x != file size %x", tree, free, frac, used, fl.next/Page)
		} else if pr {
			log.Printf("tree %x + free %x (%.2f) + used %x == file size %x", tree, free, frac, used, fl.next/Page)
		}
	}

	check(prEach)

	for ver := int64(1); ver <= N; ver++ {
		pl.SetVer(ver)
		fl.SetVer(ver, ver-1)

		if rnd.Intn(3) == 0 {
			n := rnd.Intn(1<<M-1) + 1
			if prCmd {
				log.Printf("alloc%% %d     - ver %d", n, ver)
			}

			off, err := fl.Alloc(n)
			if !assert.NoError(t, err) {
				break
			}
			alloc = append(alloc, mem{off: off, n: n})

			//	log.Printf("alloced %d at %x, next: %x", n, off, fl.next)
			p := b.Access(off, 0x10)
			pl.setver(p, ver) //nolint:scopelint
			pl.setoverflow(p, n-1)
			pl.setnkeys(p, 0)
			b.Unlock(p)
		} else if len(alloc) != 0 {
			i := rand.Intn(len(alloc))
			m := alloc[i]
			if prCmd {
				log.Printf("free %% %d %x  - ver %d", m.n, m.off, ver)
			}

			var ver int64
			p := b.Access(m.off, 0x10)
			ver = pl.getver(p)
			b.Unlock(p)

			err := fl.Free(m.n, m.off, ver)
			if !assert.NoError(t, err) {
				break
			}

			if i < len(alloc) {
				copy(alloc[i:], alloc[i+1:])
			}
			alloc = alloc[:len(alloc)-1]
		}

		check(prEach)

		if t.Failed() {
			break
		}
	}

	check(prOnce && !prEach)
}

func TestFreelistShrinkFile(t *testing.T) {
	const Page = 0x100

	b := NewMemBack(1 * Page)
	pl := NewFixedLayout(b, Page, nil)
	tr := NewTree(pl, 0, Page)
	fl := NewFreelist2(b, tr, Page, Page)

	pl.SetFreelist(fl)

	fl.SetVer(1, -1)
	off1, err := fl.Alloc(1)
	assert.NoError(t, err)

	fl.SetVer(2, 1)
	off2, err := fl.Alloc(2)
	assert.NoError(t, err)

	fl.SetVer(3, 1)
	off3, err := fl.Alloc(1)
	assert.NoError(t, err)

	fl.SetVer(4, 2)
	off4, err := fl.Alloc(4)
	assert.NoError(t, err)

	fl.SetVer(5, 1)
	fl.Free(1, off1, 1)

	fl.SetVer(6, 5)
	fl.Free(2, off2, 2)

	fl.SetVer(7, 5)
	fl.Free(1, off3, 3)

	next := fl.next

	fl.SetVer(8, 6)
	fl.Free(3, off4, 4)

	if fl.next >= next {
		t.Errorf("file didn't shrink")
		return
	}

	pver := func(off int64) (ver, size int64) {
		p := b.Access(off, 0x10)
		ver = pl.getver(p)
		size = int64(1+pl.overflow(p)) * Page
		b.Unlock(p)
		return
	}

	for off := fl.next; off <= next; {
		v, s := pver(off)
		if v >= fl.keep {
			t.Errorf("page %x with ver %d freed while keep == %d", off, v, fl.keep)
		}
		off += s
	}

	tlog.Printf("next %x, pages %x %x %x %x", fl.next, off1, off2, off3, off4)

	//	tlog.Printf("file: %x\n%v", fl.next, dumpFile(pl))
}
