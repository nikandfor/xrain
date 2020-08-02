package xrain

import (
	"bytes"
	"sort"
)

type SubpageLayout struct {
	p      []byte
	is, ls int
}

func NewSubpageLayout(p []byte) *SubpageLayout {
	l := &SubpageLayout{
		is: 2,
		ls: 2,
	}

	l.SetBytes(p)

	return l
}

func (l *SubpageLayout) SetBytes(p []byte) {
	l.p = p
}

func (l *SubpageLayout) Bytes() []byte {
	return l.p
}

func (l *SubpageLayout) SetCommon(*Common)     {}
func (l *SubpageLayout) Alloc() (int64, error) { return NilPage, nil }
func (l *SubpageLayout) Free(int64) error      { return nil }

func (l *SubpageLayout) nkeys() int {
	if len(l.p) < l.is {
		return 0
	}
	return int(l.p[0])<<8 | int(l.p[1])
}

func (l *SubpageLayout) setnkeys(n int) {
	l.p[0] = byte(n >> 8)
	l.p[1] = byte(n)
}

func (l *SubpageLayout) dataoff(i, n int) int {
	if i == 0 {
		return l.is + l.ls*n
	}
	i--
	st := l.is + l.ls*i
	return int(l.p[st])<<8 | int(l.p[st+1])
}

func (l *SubpageLayout) dataend(i int) int {
	if i == -1 {
		return l.is
	}
	st := l.is + l.ls*i
	return int(l.p[st])<<8 | int(l.p[st+1])
}

func (l *SubpageLayout) setdataend(i, off int) {
	st := l.is + l.ls*i
	l.p[st] = byte(off >> 8)
	l.p[st+1] = byte(off)
}

func (l *SubpageLayout) Search(k []byte) (i int, eq bool) {
	if len(l.p) <= l.is {
		return 0, false
	}

	n := l.nkeys()

	keycmp := func(i int) int {
		st := l.dataoff(i, n)
		kl := int(l.p[st])
		st++

		ik := l.p[st : st+kl]

		//	tl.Printf("cmp %2d  %q ? %q   dst %2x  kl %d", i, ik, k, st, kl)

		return bytes.Compare(ik, k)
	}

	i = sort.Search(n, func(i int) bool {
		return keycmp(i) >= 0
	})

	eq = i < n && keycmp(i) == 0

	return
}

func (l *SubpageLayout) Seek(st Stack, _ int64, k []byte) (_ Stack, eq bool) {
	if len(l.p) <= l.is {
		return nil, false
	}

	var i int
	i, eq = l.Search(k)

	st = append(st[:0], MakeOffIndex(0, i))

	return st, eq
}

func (l *SubpageLayout) Step(st Stack, _ int64, back bool) Stack {
	if len(l.p) <= l.is {
		return nil
	}

	if len(st) == 0 {
		if back {
			n := l.nkeys()
			st = append(st, OffIndex(n-1))
		} else {
			st = append(st, 0)
		}

		return st
	}

	if back {
		if st[0] == 0 {
			return nil
		}

		st[0]--

		return st[:1]
	} else {
		n := l.nkeys()

		if st[0] == OffIndex(n)-1 {
			return nil
		}

		st[0]++

		return st[:1]
	}
}

func (l *SubpageLayout) Key(st Stack, buf []byte) ([]byte, int) {
	if len(l.p) <= l.is {
		return nil, 0
	}

	i := int(st[0])
	n := l.nkeys()

	dst := l.dataoff(i, n)
	kl := int(l.p[dst])
	dst++

	buf = append(buf, l.p[dst:dst+kl]...)

	return buf, 0
}

func (l *SubpageLayout) Value(st Stack, buf []byte) []byte {
	if len(l.p) <= l.is {
		return nil
	}

	i := int(st[0])
	n := l.nkeys()

	dst := l.dataoff(i, n)
	dend := l.dataend(i)
	kl := int(l.p[dst])
	dst += 1 + kl

	buf = append(buf, l.p[dst:dend]...)

	return buf
}

func (l *SubpageLayout) Insert(st Stack, _ int, k, v []byte) (Stack, error) {
	dsize := 1 + len(k) + len(v)

	var dst, dend int
	var i, n int
	if len(st) != 0 {
		i = int(st[0])
	} else {
		i, _ = l.Search(k)
	}

	n = l.nkeys()

	dst = l.dataoff(i, n)
	dend = l.dataend(n - 1)

	for cap(l.p) < dend+l.ls+dsize {
		l.p = append(l.p[:cap(l.p)], 0, 0, 0, 0)
	}
	l.p = l.p[:dend+l.ls+dsize]

	fst := l.dataoff(0, n)

	copy(l.p[dst+l.ls+dsize:], l.p[dst:])
	copy(l.p[fst+l.ls:], l.p[fst:dst])

	dst += l.ls

	for j := n - 1; j >= i; j-- {
		off := l.dataend(j)
		l.setdataend(j+1, off+l.ls+dsize)
	}

	l.setdataend(i, dst+dsize)

	for j := i - 1; j >= 0; j-- {
		off := l.dataend(j)
		l.setdataend(j, off+l.ls)
	}

	l.p[dst] = byte(len(k))
	dst++
	copy(l.p[dst:], k)
	copy(l.p[dst+len(k):], v)

	l.setnkeys(n + 1)

	st = append(st[:0], OffIndex(i))

	return st, nil
}

func (l *SubpageLayout) Delete(st Stack) (Stack, error) {
	if len(l.p) == 0 {
		return nil, nil
	}

	i := int(st[0])
	n := l.nkeys()

	dst := l.dataoff(i, n)
	dend := l.dataend(i)
	dsize := dend - dst

	for j := 0; j < i; j++ {
		off := l.dataend(j)
		l.setdataend(j, off-l.ls)
	}

	for j := i; j < n-1; j++ {
		off := l.dataend(j + 1)
		l.setdataend(j, off-l.ls-dsize)
	}

	fst := l.dataoff(0, n)

	copy(l.p[fst-l.ls:], l.p[fst:dst])
	copy(l.p[dst-l.ls:], l.p[dend:])

	l.setnkeys(n - 1)

	if n == 1 {
		dend = 0
	} else {
		dend = l.dataend(n - 2)
	}

	l.p = l.p[:dend]

	return st, nil
}
