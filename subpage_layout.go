package xrain

import (
	"bytes"
	"sort"
)

const subpageIndexStart = 2

type SubpageLayout struct {
	p []byte
}

func (l *SubpageLayout) SetCommon(*Common)     {}
func (l *SubpageLayout) Alloc() (int64, error) { return NilPage, nil }
func (l *SubpageLayout) Free(int64) error      { return nil }

func (l *SubpageLayout) nkeys() int {
	return int(l.p[0])<<8 | int(l.p[1])
}

func (l *SubpageLayout) setnkeys(n int) {
	l.p[0] = byte(n >> 8)
	l.p[1] = byte(n)
}

func (l *SubpageLayout) dataoff(i int) int {
	n := l.nkeys()
	if i == 0 {
		return subpageIndexStart + 2*n
	}
	i--
	st := subpageIndexStart + 2*i
	return int(l.p[st])<<8 | int(l.p[st+1])
}

func (l *SubpageLayout) dataend(i int) int {
	st := subpageIndexStart + 2*i
	return int(l.p[st])<<8 | int(l.p[st+1])
}

func (l *SubpageLayout) setdataend(i, off int) {
	st := subpageIndexStart + 2*i
	l.p[st] = byte(off >> 8)
	l.p[st+1] = byte(off)
}

func (l *SubpageLayout) Search(k []byte) (i int, eq bool) {
	keycmp := func(i int) int {
		dst := l.dataoff(i)
		kl := int(l.p[dst])
		dst++

		ik := l.p[dst : dst+kl]

		return bytes.Compare(ik, k)
	}

	n := l.nkeys()

	i = sort.Search(n, func(i int) bool {
		return keycmp(i) >= 0
	})

	eq = i < n && keycmp(i) == 0

	return
}

func (l *SubpageLayout) Seek(st Stack, _ int64, k []byte) (_ Stack, eq bool) {
	var i int

	i, eq = l.Search(k)

	st = append(st[:0], MakeOffIndex(0, i))

	return st, eq
}

func (l *SubpageLayout) Step(st Stack, _ int64, back bool) Stack {
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
	i := int(st[0])

	dst := l.dataoff(i)
	kl := int(l.p[dst])
	dst++

	buf = append(buf[:0], l.p[dst:dst+kl]...)

	return buf, 0
}

func (l *SubpageLayout) Value(st Stack, buf []byte) []byte {
	i := int(st[0])

	dst := l.dataoff(i)
	dend := l.dataoff(i)
	kl := int(l.p[dst])
	dst += 1 + kl

	buf = append(buf[:0], l.p[dst:dend]...)

	return buf
}

func (l *SubpageLayout) Insert(st Stack, i int, k, v []byte) (Stack, error) {
	if len(st) != 0 {
		i = int(st[0])
	} else {
		i, _ = l.Search(k)
	}

	dsize := 1 + len(k) + len(v)

	n := l.nkeys()
	l.setnkeys(n + 1)

	dst := l.dataoff(i)
	dend := l.dataend(n - 1)

	for cap(l.p) < dend+2+dsize {
		l.p = append(l.p[:cap(l.p)], 0, 0, 0, 0)
	}
	l.p = l.p[:dend+2+dsize]

	fst := l.dataoff(0)

	copy(l.p[dst+2+dsize:], l.p[dst:])
	copy(l.p[fst+2:], l.p[fst:dst])

	dst += 2

	for j := n - 1; j >= i; j-- {
		off := l.dataend(j)
		l.setdataend(j+1, off+dsize)
	}

	l.setdataend(i, dst)

	l.p[dst] = byte(len(k))
	dst++
	copy(l.p[dst:], k)
	copy(l.p[dst+len(k):], v)

	st = append(st[:0], OffIndex(i))

	return st, nil
}

func (l *SubpageLayout) Delete(st Stack) (Stack, error) {
	n := l.nkeys()
	i := int(st[0])

	dst := l.dataoff(i)
	dend := l.dataend(i)
	dsize := dend - dst

	for j := i; j < n-1; j++ {
		off := l.dataend(i + 1)
		l.setdataend(i, off-dsize)
	}

	fst := l.dataoff(0)

	copy(l.p[fst-2:], l.p[fst:dst])
	copy(l.p[dst-2:], l.p[dend:])

	dend = l.dataend(n - 1)

	l.p = l.p[:dend]

	l.setnkeys(n - 1)

	return st, nil
}
