package xrain

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.com/nikandfor/tlog"
)

type (
	FixedLayout struct {
		BaseLayout2
		ff, k, v, kv, pm int
		p                int64
	}
)

const fixedIndexStart = 0x10

var _ Layout = &FixedLayout{}

func NewFixedLayout(c *Common) *FixedLayout {
	return &FixedLayout{
		BaseLayout2: BaseLayout2{
			Common: c,
		},
		k:  8,
		v:  8,
		kv: 16,
		pm: 1,
		p:  c.Page,
	}
}

func (l *FixedLayout) SetKVSize(ff, k, v, pm int) {
	l.ff = ff
	l.k = k
	l.v = v
	l.kv = ff + k + v
	l.pm = pm
	l.p = l.Page * int64(pm)
}

func (l *FixedLayout) Free(off int64) error {
	p := l.Access(off, 0x10)
	pages := 1 + l.overflow(p)
	ver := l.pagever(p)
	rec := !l.isleaf(p)
	var sub []int64
	if rec {
		n := l.nkeys(p)
		sub = make([]int64, n)
		for i := range sub {
			v := l.value(off, i, nil)
			sub[i] = int64(binary.BigEndian.Uint64(v))
		}
	}
	l.Unlock(p)

	err := l.Freelist.Free(off, ver, pages)
	if err != nil {
		return err
	}

	for _, off := range sub {
		err = l.Free(off)
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *FixedLayout) Key(st Stack, buf []byte) ([]byte, int) {
	return buf, 0
}

func (l *FixedLayout) Value(st Stack, buf []byte) []byte {
	off, i := st.LastOffIndex(l.Mask)
	return l.value(off, i, nil)
}

func (l *FixedLayout) value(off int64, i int, buf []byte) []byte {
	return nil
}

func (l *FixedLayout) link(st Stack) (off int64) {
	var buf [8]byte

	l.Value(st, buf[:])

	off = int64(binary.BigEndian.Uint64(buf[:]))

	return off
}

func (l *FixedLayout) Seek(st Stack, root int64, k []byte) (_ Stack, eq bool) {
	st = st[:0]

	off := root
	var isleaf bool
	var i, n, d int

	for {
		st = append(st, OffIndex(off))

		i, n, off, eq, isleaf = l.search(off, k)
		//	log.Printf("search %2x %q -> %x %v", off, k, i, eq)

		if isleaf {
			st[d] |= OffIndex(i)
			break
		}

		if i == n {
			i--
		}

		st[d] |= OffIndex(i)
		d++
	}
	//	log.Printf("seek      %q -> %x %v", k, st, eq)

	return st, eq
}

func (l *FixedLayout) search(off int64, k []byte) (i, n int, coff int64, eq, isleaf bool) {
	pages := 1

again:
	p := l.Access(off, l.Page*int64(pages))
	if o := l.overflow(p); o+1 != pages {
		l.Unlock(p)

		pages = 1 + o

		goto again
	}

	keycmp := func(i int) int {
		dst := fixedIndexStart + l.kv*i
		ik := p[dst : dst+l.k]

		return bytes.Compare(ik, k)
	}

	n = l.nkeys(p)

	i = sort.Search(n, func(i int) bool {
		return keycmp(i) >= 0
	})

	eq = i < n && keycmp(i) == 0
	isleaf = l.isleaf(p)

	if !isleaf {
		dst := fixedIndexStart + l.kv*i + l.k
		v := p[dst : dst+l.v]

		coff = int64(binary.BigEndian.Uint64(v))
	}

	l.Unlock(p)

	return
}

func (l *FixedLayout) Step(st Stack, root int64, back bool) Stack {
	if len(st) == 0 {
		return l.firstLast(st, root, back)
	}

	last := len(st) - 1
	off, i := st[last].OffIndex(l.Mask)

	if back {
		if i > 0 {
			i--
			st[last] = MakeOffIndex(off, i)
			return st
		}
	} else {
		n := l.nKeys(off)
		i++
		if i < n {
			st[last] = MakeOffIndex(off, i)
			return st
		}
	}

	if last == 0 {
		return nil
	}

	par := l.Step(st[:last], root, back)
	if par == nil {
		return nil
	}

	off = l.link(par)

	if back {
		i = l.nKeys(off) - 1
	} else {
		i = 0
	}

	st[last] = MakeOffIndex(off, i)

	return st
}

func (l *FixedLayout) firstLast(st Stack, root int64, back bool) Stack {
	off := root
	isleaf := false
	var i int

	for {
		tlog.Printf("here, off %x\n%v", off, l.dumpPage(off))
		p := l.Access(off, l.p)
		func() {
			n := l.nkeys(p)
			if n == 0 {
				st = nil
				return
			}

			if back {
				i = n - 1
			} else {
				i = 0
			}

			st = append(st, MakeOffIndex(off, i))

			isleaf = l.isleaf(p)
			if isleaf {
				return
			}

			dst := fixedIndexStart + i*l.kv
			dst += l.ff + l.k

			v := p[dst : dst+l.v]
			off = int64(binary.BigEndian.Uint64(v))
		}()
		l.Unlock(p)

		if isleaf || st == nil {
			return st
		}
	}
}

func (l *FixedLayout) Insert(st Stack, ff int, k, v []byte) (_ Stack, err error) {
	off, i := st.LastOffIndex(l.Mask)

	pages := l.pm

	var off1 int64 = NilPage
	var ver int64
	var alloc, split bool

again:
	p := l.Access(off, int64(pages)*l.Page)
	func() {
		ver := l.pagever(p)
		alloc = ver != l.Ver

		n := l.nkeys(p)
		split = fixedIndexStart+n*l.kv == int(l.Page)

		if alloc || split && pages == l.pm {
			return
		}

		if !split {
			l.pageInsert(p, i, n, ff, k, v)

			return
		}

		l.pageSplit(p[:l.Page], p[l.Page:], n)

		m := l.nkeys(p[:l.Page])

		if i <= m {
			l.pageInsert(p[:l.Page], i, m, ff, k, v)

			st[len(st)-1] = MakeOffIndex(off, i)
		} else {
			l.pageInsert(p[l.Page:], i-m, n-m, ff, k, v)

			st[len(st)-1] = MakeOffIndex(off+l.p, i-m)
		}

		split = false
	}()
	l.Unlock(p)

	if split {
		pages = l.pm * 2

		off, err = l.realloc(off, ver, l.pm, pages)
		if err != nil {
			return
		}

		off1 = off + l.p

		goto again
	}

	if alloc {
		off, err = l.realloc(off, ver, l.pm, l.pm)
		if err != nil {
			return
		}

		goto again
	}

	return l.out(st, off, off1)
}

func (l *FixedLayout) pageInsert(p []byte, i, n, ff int, k, v []byte) {
	dst := fixedIndexStart + i*l.kv

	if i < n {
		copy(p[dst+l.kv:], p[dst:l.Page])
	}

	p[dst] = byte(ff)
	dst += l.ff

	copy(p[dst:], k)
	copy(p[dst+l.k:], v)

	l.setnkeys(p, n+1)
}

func (l *FixedLayout) pageSplit(p, r []byte, n int) {
	m := (n + 1) / 2

	dst := fixedIndexStart + m*l.kv
	dend := fixedIndexStart + n*l.kv

	copy(r[fixedIndexStart:], p[dst:dend])

	l.setnkeys(p, m)
	l.setnkeys(r, n-m)

	l.setoverflow(p, l.pm-1)
	l.setoverflow(r, l.pm-1)

	l.setver(p, l.Ver)
	l.setver(r, l.Ver)
}

func (l *FixedLayout) Delete(st Stack) (_ Stack, err error) {
	off, i := st.LastOffIndex(l.Mask)

	var rebalance, alloc bool
	var ver int64

again:
	p := l.Access(off, l.p)
	func() {
		defer l.Unlock(p)

		n := l.nkeys(p)

		ver = l.pagever(p)
		alloc = ver != l.Ver

		dend := fixedIndexStart + n*l.kv

		rebalance = dend < int(l.p)*2/5

		if alloc || rebalance {
			return
		}

		dst := fixedIndexStart + i*l.kv

		copy(p[dst:], p[dst+l.kv:])

		n--
		l.setnkeys(p, n)
	}()

	if rebalance {
		return l.rebalance(st)
	}

	if alloc {
		off, err = l.realloc(off, ver, l.pm, l.pm)
		goto again
	}

	return l.out(st, off, NilPage)
}

func (l *FixedLayout) rebalance(st Stack) (_ Stack, err error) {
	return
}

func (l *FixedLayout) out(st Stack, off0, off1 int64) (_ Stack, err error) {
	return
}

func (l *FixedLayout) dumpPage(off int64) string {
	var buf bytes.Buffer

	var isleaf bool
	var n int

	p := l.Access(off, l.Page)
	{
		isleaf = l.isleaf(p)
		tp := 'B'
		if isleaf {
			tp = 'D'
		}
		ver := l.pagever(p)
		over := l.overflow(p)
		n = l.nkeys(p)
		fmt.Fprintf(&buf, "%4x: %c over %2d ver %3d  nkeys %4d  ", off, tp, over, ver, n)
		fmt.Fprintf(&buf, "datasize %3x free space %3x\n", n*16, len(p)-n*16-16)
	}
	l.Unlock(p)

	st := Stack{0}

	for i := 0; i < n; i++ {
		st[0] = MakeOffIndex(off, i)
		k, _ := l.Key(st, nil)

		if isleaf {
			v := l.Value(st, nil)
			fmt.Fprintf(&buf, "    %2x -> %2x  | %q -> %q\n", k, v, k, v)
		} else {
			v := l.link(st)
			fmt.Fprintf(&buf, "    %2x -> %4x  | %q\n", k, v, k)
		}
	}

	return buf.String()
}

func (l *FixedLayout) dumpFile() string {
	var buf strings.Builder

	b := l.Back
	off := int64(0)

	p := b.Access(off, 0x10)
	if bytes.HasPrefix(p, []byte("xrain")) {
		off = 4 * l.Page
	}
	b.Unlock(p)

	for off < b.Size() {
		s := l.dumpPage(off)

		buf.WriteString(s)

		off += l.p
	}

	return buf.String()
}
