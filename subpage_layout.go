package xrain

import (
	"bytes"
	"encoding/binary"
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

func (l *SubpageLayout) Search(k, v []byte) (i int, eq bool) {
	if len(l.p) <= l.is {
		return 0, false
	}

	n := l.nkeys()

	keycmp := func(i int) (c int) {
		st := l.dataoff(i, n)
		kl := int(l.p[st])
		st++

		ik := l.p[st : st+kl]

		//	tl.Printf("cmp %2d  %q ? %q   dst %2x  kl %d", i, ik, k, st, kl)

		c = bytes.Compare(ik, k)
		if c != 0 {
			return
		}

		st += kl
		end := l.dataoff(i+1, n)

		iv := l.p[st:end]

		return bytes.Compare(iv, v)
	}

	i = sort.Search(n, func(i int) bool {
		return keycmp(i) >= 0
	})

	eq = i < n && keycmp(i) == 0

	return
}

func (l *SubpageLayout) Seek(st Stack, _ int64, k, v []byte) (_ Stack, eq bool) {
	if len(l.p) <= l.is {
		return nil, false
	}

	var i int
	i, eq = l.Search(k, v)

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

func (l *SubpageLayout) Int64(s Stack) (v int64) {
	if len(l.p) <= l.is {
		return 0
	}

	i := int(s[0])
	n := l.nkeys()

	st := l.dataoff(i, n)
	end := l.dataend(i)
	kl := int(l.p[st])
	st += 1 + kl
	sz := end - st

	var buf [8]byte
	var b []byte
	if sz >= 8 {
		b = l.p[end-8 : end]
	} else {
		copy(buf[8-sz:], l.p[st:end])
		b = buf[:]
	}

	v = int64(binary.BigEndian.Uint64(b))

	return
}

func (l *SubpageLayout) SetInt64(s Stack, v int64) (old int64, err error) {
	if len(l.p) <= l.is {
		panic("unsupported")
	}

	i := int(s[0])
	n := l.nkeys()

	st := l.dataoff(i, n)
	end := l.dataend(i)
	kl := int(l.p[st])
	st += 1 + kl
	sz := end - st

	var buf [8]byte
	var b []byte
	if sz >= 8 {
		b = l.p[end-8 : end]
	} else {
		copy(buf[8-sz:], l.p[st:end])
		b = buf[:]
	}

	old = int64(binary.BigEndian.Uint64(b))

	binary.BigEndian.PutUint64(b, uint64(v))

	if sz < 8 {
		copy(l.p[st:end], buf[8-sz:])
	}

	return
}

func (l *SubpageLayout) AddInt64(s Stack, v int64) (new int64, err error) {
	new = l.Int64(s) + v
	l.SetInt64(s, new)
	return
}

func (l *SubpageLayout) Insert(st Stack, _ int, k, v []byte) (Stack, error) {
	dsize := 1 + len(k) + len(v)

	var dst, dend int
	var i, n int
	if len(st) != 0 {
		i = int(st[0])
	} else {
		i, _ = l.Search(k, v)
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
