package xrain

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"strings"
)

const NilPage = -1

type (
	PageLayout interface {
		AllocRoot() (int64, error)
		Reclaim(p int64) error

		NKeys(p int64) int
		IsLeaf(p int64) bool

		Key(p int64, i int) []byte
		KeyCmp(p int64, i int, k []byte) int
		LastKey(p int64) []byte

		Value(p int64, i int) []byte
		Int64(p int64, i int) int64

		Put(p int64, i int, k, v []byte) (l, r int64, _ error)
		PutInt64(p int64, i int, k []byte, v int64) (l, r int64, _ error)
		Del(p int64, i int) (int64, error)

		NeedRebalance(p int64) bool
		Siblings(p int64, i int, pi int64) (li int, l, r int64)
		Rebalance(l, r int64) (l_, r_ int64, _ error)
	}

	BaseLayout struct { // isbranch bit, size uint15, extended uint24, _ [3]byte, ver int64
		b    Back
		page int64
		ver  int64
		free *FreeList

		meta *treemeta
	}

	KVLayout struct { // base [2]byte, keys [size]int16, data []byte
		BaseLayout
	}

	IntLayout struct { // base [2]byte, _ [14]byte, keyval []{int64,int64}
		BaseLayout
	}
)

func NewPageLayout(b Back, psize, ver int64, free *FreeList) BaseLayout {
	return BaseLayout{
		b:    b,
		page: psize,
		ver:  ver,
		free: free,
	}
}

func (l *BaseLayout) NKeys(off int64) int {
	p := l.b.Load(off, l.page)
	return int(p[0])&0x7f<<8 | int(p[1])
}

func (l *BaseLayout) IsLeaf(off int64) bool {
	p := l.b.Load(off, l.page)
	return p[0]&0x80 == 0
}

func (l *BaseLayout) Write(off int64, p []byte) (_ int64, _ []byte, err error) {
	if off == NilPage {
		return l.alloc()
	}

	if p == nil {
		p = l.b.Load(off, l.page)
	}
	ver := l.getver(p)
	if ver == l.ver {
		return off, p, nil
	}

	err = l.free.Reclaim(off, ver)
	if err != nil {
		return
	}

	var p1 []byte
	off, p1, err = l.alloc()
	if err != nil {
		return
	}

	copy(p1, p)
	l.setver(p1, l.ver)

	return off, p1, nil
}

func (l *BaseLayout) Reclaim(off int64) error {
	if l.free == nil {
		return nil
	}
	p := l.b.Load(off, l.page)
	ver := l.getver(p)

	return l.free.Reclaim(off, ver)
}

func (l *BaseLayout) AllocRoot() (int64, error) {
	off, p, err := l.alloc()
	if err != nil {
		return NilPage, err
	}
	p[0] = 0x80
	l.setsize(p, 0)
	l.setver(p, l.ver)
	return off, nil
}

func (l *BaseLayout) alloc() (off int64, p []byte, err error) {
	off, err = l.free.Alloc()
	if err != nil {
		return
	}

	p = l.b.Load(off, l.page)

	return off, p, nil
}

func (l *BaseLayout) getver(p []byte) int64 {
	return int64(p[8])<<56 | int64(p[9])<<48 | int64(p[10])<<40 | int64(p[11])<<32 | int64(p[12])<<24 | int64(p[13])<<16 | int64(p[14])<<8 | int64(p[15])
}

func (l *BaseLayout) setver(p []byte, v int64) {
	p[8] = byte(v >> 56)
	p[9] = byte(v >> 48)
	p[10] = byte(v >> 40)
	p[11] = byte(v >> 32)
	p[12] = byte(v >> 24)
	p[13] = byte(v >> 16)
	p[14] = byte(v >> 8)
	p[15] = byte(v)
}

func (l *BaseLayout) nkeys(p []byte) int {
	return int(p[0])&0x7f<<8 | int(p[1])
}

func (l *BaseLayout) setsize(p []byte, n int) {
	p[0] = p[0]&0x80 | byte(n>>8&0x7f)
	p[1] = byte(n)
}

func (l *KVLayout) datasize(p []byte) int {
	n := l.nkeys(p)
	if n == 0 {
		return 0
	}
	return n*2 + (l.dataoff(p, -1) - l.dataoff(p, n-1))
}

func (l *KVLayout) dataoff(p []byte, i int) int {
	if i == -1 {
		return len(p)
	}
	s := 16 + i*2
	st := int(p[s])<<8 | int(p[s+1])
	return st
}

func (l *KVLayout) setoff(p []byte, i int, off int) {
	s := 16 + i*2
	p[s] = byte(off >> 8)
	p[s+1] = byte(off)
}

func (l *KVLayout) KeyCmp(off int64, i int, k []byte) int {
	p := l.b.Load(off, l.page)
	st := l.dataoff(p, i)
	kl := int(p[st])
	st++
	return bytes.Compare(p[st:st+kl], k)
}

func (l *KVLayout) Key(off int64, i int) []byte {
	p := l.b.Load(off, l.page)
	st := l.dataoff(p, i)
	kl := int(p[st])
	st++
	return p[st : st+kl]
}

func (l *KVLayout) LastKey(off int64) []byte {
	p := l.b.Load(off, l.page)
	n := l.nkeys(p)
	if n == 0 {
		panic(off)
	}
	st := l.dataoff(p, n-1)
	kl := int(p[st])
	st++
	return p[st : st+kl]
}

func (l *KVLayout) Value(off int64, i int) []byte {
	p := l.b.Load(off, l.page)
	st := l.dataoff(p, i)
	end := l.dataoff(p, i-1)
	kl := int(p[st])
	st++
	st += kl
	return p[st:end]
}

func (l *KVLayout) Int64(off int64, i int) int64 {
	v := l.Value(off, i)
	return int64(binary.BigEndian.Uint64(v))
}

func (l *KVLayout) Del(off int64, i int) (int64, error) {
	off, p, err := l.Write(off, nil)
	if err != nil {
		return 0, err
	}
	n := l.nkeys(p)
	st := l.dataoff(p, i)
	end := l.dataoff(p, i-1)
	size := end - st
	b := l.dataoff(p, n-1)
	copy(p[b+size:], p[b:st])
	for j := i; j < n-1; j++ {
		off := l.dataoff(p, j+1)
		l.setoff(p, j, off+size)
	}
	l.setsize(p, n-1)
	return off, nil
}

func (l *KVLayout) Put(off int64, i int, k, v []byte) (loff, roff int64, err error) {
	p := l.b.Load(off, l.page)
	n := l.nkeys(p)
	b := l.dataoff(p, n-1)
	sp := b - (16 + n*2)
	size := 2 + 1 + len(k) + len(v)
	log.Printf("Put %3x space %x - %x (%x), size %x  (%x i %d set %q -> %q)", off, 16+n*2, b, sp, size, off, i, k, v)

	if size <= sp {
		off, p, err = l.Write(off, p)
		if err != nil {
			return 0, 0, err
		}
		l.putPage(p, i, k, v)
		return off, NilPage, nil
	}

	loff, roff, lp, rp, err := l.split(off, p)
	if err != nil {
		return 0, 0, err
	}
	n = l.nkeys(lp)
	if i < n {
		l.putPage(lp, i, k, v)
	} else {
		l.putPage(rp, i-n, k, v)
	}
	return loff, roff, nil
}

func (l *KVLayout) PutInt64(off int64, i int, k []byte, v int64) (loff, roff int64, err error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	return l.Put(off, i, k, buf[:])
}

func (l *KVLayout) putPage(p []byte, i int, k, v []byte) {
	n := l.nkeys(p)
	size := 1 + len(k) + len(v)
	b := l.dataoff(p, n-1)
	end := l.dataoff(p, i-1)
	//	log.Printf("putPage i %d/%d b %02x size %02x end %02x (%q -> %q)", i, n, b, size, end, k, v)
	copy(p[b-size:], p[b:end])
	for j := n; j > i; j-- {
		off := l.dataoff(p, j-1)
		l.setoff(p, j, off-size)
	}

	st := end - size
	l.setoff(p, i, st)

	p[st] = byte(len(k))
	st++
	st += copy(p[st:], k)
	copy(p[st:], v)

	l.setsize(p, n+1)
}

func (l *KVLayout) split(off int64, p []byte) (loff, roff int64, lp, rp []byte, err error) {
	n := l.nkeys(p)
	m := (n + 1) / 2

	loff, lp, err = l.Write(off, p)
	if err != nil {
		return
	}
	roff, rp, err = l.Write(NilPage, nil)
	if err != nil {
		return
	}

	l.move(rp, p, 0, m, n)

	lp[0] = p[0]
	rp[0] = p[0]
	l.setsize(lp, m)
	l.setsize(rp, n-m)

	log.Printf("split %x -> %x %x  n %d %d", off, loff, roff, l.nkeys(lp), l.nkeys(rp))
	//	log.Printf("split %x -> %x %x\n%v\n%v", off, loff, roff, hex.Dump(lp), hex.Dump(rp))

	return
}

func (l *KVLayout) move(rp, p []byte, ri, i, I int) {
	if i == I {
		return
	}
	st := l.dataoff(p, I-1)
	end := l.dataoff(p, i-1)
	rend := l.dataoff(rp, ri-1)
	size := end - st
	log.Printf("move %d keys of size %x from %x - %x to %x", I-i, size, st, end, rend-size)
	copy(rp[rend-size:], p[st:end])

	diff := rend - end
	for j := 0; j < I-i; j++ {
		off := l.dataoff(p, i+j)
		l.setoff(rp, ri+j, off+diff)
	}
}

func (l *KVLayout) NeedRebalance(off int64) bool {
	p := l.b.Load(off, l.page)
	n := l.nkeys(p)
	b := l.dataoff(p, n-1)
	sp := b - (16 + n*2)
	return sp > len(p)/2
}

func (l *KVLayout) Siblings(off int64, i int, pi int64) (li int, loff, roff int64) {
	p := l.b.Load(off, l.page)
	n := l.nkeys(p)

	var ri int
	if i+1 < n && i&1 == 0 {
		li, ri = i, i+1
		loff = pi
		roff = l.Int64(off, ri)
	} else {
		li, ri = i-1, i
		loff = l.Int64(off, li)
		roff = pi
	}

	return
}

func (l *KVLayout) Rebalance(lpoff, rpoff int64) (loff, roff int64, err error) {
	loff, lp, err := l.Write(lpoff, nil)
	if err != nil {
		return
	}
	rp := l.b.Load(rpoff, l.page)

	ln := l.nkeys(lp)
	lsz := l.datasize(lp)
	rn := l.nkeys(rp)
	rsz := l.datasize(rp)

	log.Printf("rebalace size %x %x n %d %d", lsz, rsz, ln, rn)

	if 16+lsz+rsz <= int(l.page) {
		l.move(lp, rp, ln, 0, rn)
		l.setsize(lp, ln+rn)

		err = l.Reclaim(rpoff)
		if err != nil {
			return
		}

		return loff, NilPage, nil
	}

	roff, rp, err = l.Write(rpoff, nil)
	if err != nil {
		return
	}

	panic("fix moving")
	if lsz > rsz {
		diff := lsz - rsz
		b := l.dataoff(lp, ln-1)
		i := 0
		for {
			bnew := l.dataoff(lp, ln-1-i-1)
			if b-bnew > diff {
				break
			}
			i++
		}
		if i != 0 {
			l.move(rp, lp, rn, ln-i, ln)
			l.setsize(lp, ln-i)
			l.setsize(rp, rn+i)
		}
	} else {
		diff := rsz - lsz
		b := l.dataoff(rp, rn-1)
		i := 0
		for {
			bnew := l.dataoff(rp, rn-1-i-1)
			if b-bnew > diff {
				break
			}
			i++
		}
		if i != 0 {
			l.move(lp, rp, ln, rn-i, rn)
			l.setsize(lp, ln+i)
			l.setsize(rp, rn-i)
		}
	}

	return
}

func (l *IntLayout) KeyCmp(off int64, i int, k []byte) int {
	if k == nil {
		return 1
	}
	if len(k) != 8 {
		panic(len(k))
	}
	p := l.b.Load(off, l.page)
	s := 16 + i*16
	return bytes.Compare(p[s:s+8], k)
}

func (l *IntLayout) Key(off int64, i int) []byte {
	p := l.b.Load(off, l.page)
	s := 16 + i*16
	return p[s : s+8]
}

func (l *IntLayout) LastKey(off int64) []byte {
	p := l.b.Load(off, l.page)
	n := l.nkeys(p)
	st := 16 + (n-1)*16
	return p[st : st+8]
}

func (l *IntLayout) Value(off int64, i int) []byte {
	p := l.b.Load(off, l.page)
	s := 16 + i*16
	return p[s+8 : s+16]
}

func (l *IntLayout) Int64(off int64, i int) int64 {
	v := l.Value(off, i)
	return int64(binary.BigEndian.Uint64(v))
}

func (l *IntLayout) Del(off int64, i int) (int64, error) {
	off, p, err := l.Write(off, nil)
	if err != nil {
		return 0, err
	}
	//	log.Printf("Del %3x i %d\n%v", off, i, dumpPage(l, off))
	n := l.nkeys(p)
	st := 16 + i*16
	end := 16 + n*16
	copy(p[st:], p[st+16:end])
	l.setsize(p, n-1)
	return off, nil
}

func (l *IntLayout) Put(off int64, i int, k, v []byte) (loff, roff int64, err error) {
	if len(k) != 8 {
		panic(k)
	}
	if len(v) != 8 {
		panic(v)
	}
	p := l.b.Load(off, l.page)
	n := l.nkeys(p)
	//	log.Printf("Put  %x i %d/%d  set %2x -> %2x", off, i, n, k, v)
	st := 16 + n*16
	if st < len(p) {
		loff, p, err = l.Write(off, p)
		if err != nil {
			return 0, 0, err
		}
		l.putPage(p, i, n, k, v)
		return loff, NilPage, nil
	}

	// split
	var lp, rp []byte

	loff, lp, err = l.Write(off, p)
	if err != nil {
		return 0, 0, err
	}

	roff, rp, err = l.Write(NilPage, nil)
	if err != nil {
		return 0, 0, err
	}

	m := n / 2

	copy(rp[16:], lp[16+m*16:16+n*16])

	lp[0] = p[0]
	rp[0] = p[0]
	l.setsize(lp, m)
	l.setsize(rp, n-m)

	// add
	if i < m {
		l.putPage(lp, i, m, k, v)
	} else {
		l.putPage(rp, i-m, n-m, k, v)
	}
	return
}

func (l *IntLayout) putPage(p []byte, i, n int, k, v []byte) {
	st := 16 + i*16
	end := 16 + n*16
	copy(p[st+16:], p[st:end])
	copy(p[st:], k)
	copy(p[st+8:], v)
	l.setsize(p, n+1)
}

func (l *IntLayout) PutInt64(off int64, i int, k []byte, v int64) (loff, roff int64, err error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	return l.Put(off, i, k, buf[:])
}

func (l *IntLayout) NeedRebalance(off int64) bool {
	p := l.b.Load(off, l.page)
	n := l.nkeys(p)
	end := 16 + n*16
	if end < len(p)/2 {
		return true
	}
	return false
}

func (l *IntLayout) Siblings(off int64, i int, pi int64) (li int, loff, roff int64) {
	p := l.b.Load(off, l.page)
	n := l.nkeys(p)

	var ri int
	if i+1 < n && i&1 == 0 {
		li, ri = i, i+1
		loff = pi
		roff = l.Int64(off, ri)
	} else {
		li, ri = i-1, i
		loff = l.Int64(off, li)
		roff = pi
	}

	return
}

func (l *IntLayout) Rebalance(lpoff, rpoff int64) (loff, roff int64, err error) {
	//	log.Printf("rebalance pages:\n%v\n%v", dumpPage(l, lpoff), dumpPage(l, rpoff))

	loff, lp, err := l.Write(lpoff, nil)
	if err != nil {
		return
	}
	rp := l.b.Load(rpoff, l.page)

	ln := l.nkeys(lp)
	rn := l.nkeys(rp)
	sum := ln + rn
	rend := 16 + rn*16
	lend := 16 + ln*16

	if 16+sum*16 <= len(lp) {
		//	log.Printf("merge pages %x %x -> %x (%d + %d = %d)", lpoff, rpoff, loff, ln, rn, sum)
		copy(lp[lend:], rp[16:rend])
		l.setsize(lp, sum)

		//	log.Printf("reclain %x %d", rpoff, l.ver)
		err = l.Reclaim(rpoff)
		if err != nil {
			return
		}

		//	log.Printf("rebalanced page %x\n%v", loff, dumpPage(l, loff))

		return loff, NilPage, nil
	}

	roff, rp, err = l.Write(rpoff, nil)
	if err != nil {
		return
	}

	n := (sum + 1) / 2
	end := 16 + n*16

	//	log.Printf("rebalance %x %x n %d %d  n %d  ends %x %x %x", lpoff, rpoff, ln, rn, n, end, lend, rend)

	if ln > rn {
		diff := lend - end
		copy(rp[16+diff:], rp[16:rend])
		copy(rp[16:], lp[end:lend])
		l.setsize(lp, n)
		l.setsize(rp, sum-n)
	} else {
		diff := rend - end
		copy(lp[lend:], rp[16:16+diff])
		copy(rp[16:], rp[16+diff:rend])
		l.setsize(rp, n)
		l.setsize(lp, sum-n)
	}

	return
}

// debugging stuff
type (
	Logger interface {
		Printf(f string, args ...interface{})
	}
	LogLayout struct {
		PageLayout
		Logger
	}
)

func (l LogLayout) NKeys(off int64) int {
	n := l.PageLayout.NKeys(off)
	//	l.Logger.Printf("LayOut %4x NKeys %v", off, n)
	return n
}

func (l LogLayout) IsLeaf(off int64) bool {
	r := l.PageLayout.IsLeaf(off)
	//	l.Logger.Printf("LayOut %4x IsLeaf %v", off, r)
	return r
}

func (l LogLayout) KeyCmp(off int64, i int, k []byte) (c int) {
	c = l.PageLayout.KeyCmp(off, i, k)
	//	l.Logger.Printf("LayOut %4x KeyCmp %v %q -> %v", off, i, k, c)
	return
}

func (l LogLayout) Key(off int64, i int) []byte {
	r := l.PageLayout.Key(off, i)
	l.Logger.Printf("LayOut %4x Key %v -> %q", off, i, r)
	return r
}

func (l LogLayout) Put(off int64, i int, k, v []byte) (loff, roff int64, err error) {
	loff, roff, err = l.PageLayout.Put(off, i, k, v)
	l.Logger.Printf("LayOut %4x Put i %v %q %q -> %x %x %v", off, i, k, v, loff, roff, err)
	return
}

func (l LogLayout) Del(off int64, i int) (loff int64, err error) {
	loff, err = l.PageLayout.Del(off, i)
	l.Logger.Printf("LayOut %4x Del %v -> %x %v", off, i, loff, err)
	return
}

func dumpPage(l PageLayout, off int64) string {
	var b Back
	var base *BaseLayout
	var kvl *KVLayout
	var intl *IntLayout
	var page int64
	switch l := l.(type) {
	case LogLayout:
		return dumpPage(l.PageLayout, off)
	case *KVLayout:
		b = l.b
		base = &l.BaseLayout
		kvl = l
		page = l.page
	case *IntLayout:
		b = l.b
		base = &l.BaseLayout
		intl = l
		page = l.page
	default:
		panic(fmt.Sprintf("layout type %T", l))
	}

	p := b.Load(off, page)
	var buf bytes.Buffer
	tp := 'B'
	if l.IsLeaf(off) {
		tp = 'D'
	}
	ver := base.getver(p)
	n := l.NKeys(off)
	fmt.Fprintf(&buf, "%4x: %c ver %3d  nkeys %4d  ", off, tp, ver, n)
	if kvl != nil {
		fmt.Fprintf(&buf, "datasize %3x free space %3x\n", kvl.datasize(p), len(p)-kvl.datasize(p)-16)
	} else {
		fmt.Fprintf(&buf, "datasize %3x free space %3x\n", n*16, len(p)-n*16-16)
	}
	if intl != nil {
		for i := 0; i < n; i++ {
			k := l.Key(off, i)
			v := l.Int64(off, i)
			fmt.Fprintf(&buf, "    %2x -> %4x\n", k, v)
		}
	} else {
		if l.IsLeaf(off) {
			for i := 0; i < n; i++ {
				k := l.Key(off, i)
				v := l.Value(off, i)
				fmt.Fprintf(&buf, "    %q -> %q\n", k, v)
			}
		} else {
			for i := 0; i < n; i++ {
				k := l.Key(off, i)
				v := l.Int64(off, i)
				fmt.Fprintf(&buf, "    %4x <- % 2x (%q)\n", v, k, k)
			}
		}
	}
	return buf.String()
}

func dumpFile(l PageLayout) string {
	var b Back
	var page int64
	switch l := l.(type) {
	case LogLayout:
		return dumpFile(l.PageLayout)
	case *KVLayout:
		b = l.b
		page = l.page
	case *IntLayout:
		b = l.b
		page = l.page
	default:
		panic(fmt.Sprintf("layout type %T", l))
	}

	var buf strings.Builder
	b.Sync()
	sz := b.Size()
	for off := int64(0); off < sz; off += page {
		buf.WriteString(dumpPage(l, off))
	}
	return buf.String()
}

func checkPage(l PageLayout, off int64) {
	n := l.NKeys(off)
	var prev []byte
	for i := 0; i < n; i++ {
		k := l.Key(off, i)
		if bytes.Compare(prev, k) != -1 {
			log.Fatalf("at page %x of size %d  %2x goes before %2x", off, n, prev, k)
		}
		prev = k
	}
}

func checkFile(l PageLayout) {
	var b Back
	var page int64
	switch l := l.(type) {
	case LogLayout:
		checkFile(l.PageLayout)
		return
	case *KVLayout:
		b = l.b
		page = l.page
	case *IntLayout:
		b = l.b
		page = l.page
	default:
		panic(fmt.Sprintf("layout type %T", l))
	}

	b.Sync()
	sz := b.Size()
	for off := int64(0); off < sz; off += page {
		checkPage(l, off)
	}
}
