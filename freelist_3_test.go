package xrain

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFreelist3ShrinkFile(t *testing.T) {
	initLogger(t)

	const Page = 0x100

	b := NewMemBack(0)
	m := &Meta{
		Back: b,
		Page: Page,
		Mask: Page - 1,
		Ver:  1,
	}

	fl := NewFreelist3(m, 0)

	m.Ver, m.Keep = 1, -1
	off1, err := fl.Alloc(1)
	assert.NoError(t, err)

	m.Ver, m.Keep = 2, 1
	off2, err := fl.Alloc(2)
	assert.NoError(t, err)

	m.Ver, m.Keep = 3, 1
	off3, err := fl.Alloc(1)
	assert.NoError(t, err)

	m.Ver, m.Keep = 4, 2
	off4, err := fl.Alloc(4)
	assert.NoError(t, err)

	m.Ver, m.Keep = 5, 1
	fl.Free(off1, 1, 1)

	m.Ver, m.Keep = 6, 5
	fl.Free(off2, 2, 2)

	m.Ver, m.Keep = 7, 5
	fl.Free(off3, 3, 1)

	next := fl.next

	m.Ver, m.Keep = 9, 8
	fl.Free(off4, 4, 3)

	if fl.next >= next {
		t.Errorf("file didn't shrink")

		t.Logf("dump next %x  ver %x / %x\n%v", fl.next, fl.Ver, fl.Keep, fl.l.dump())

		return
	}

	tl.Printf("next %x, freed pages %x %x %x %x", fl.next, off1, off2, off3, off4)
}

func BenchmarkFreelist3(t *testing.B) {
	t.ReportAllocs()

	const Page, M = 0x100, 3
	const Mask = Page - 1

	rnd := rand.New(rand.NewSource(0))

	b := NewMemBack(1 * Page)
	m := &Meta{
		Back: b,
		Page: Page,
		Mask: Page - 1,
		Ver:  1,
	}

	l := NewFixedLayout(m)

	fl := NewFreelist3(m, 0)
	m.Freelist = fl

	var alloc Stack

	for ver := int64(1); ver <= int64(t.N); ver++ {
		m.Ver = ver
		m.Keep = ver - 1

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

			//	tl.Printf("alloced %d at %x, next: %x", n, off, fl.next)
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
