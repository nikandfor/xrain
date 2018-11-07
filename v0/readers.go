package xrain

import (
	"bytes"
	"fmt"
	"unsafe"
)

type (
	rel struct {
		r          *readers
		ver        int64
		cnt        int
		next, prev *rel
	}
	readers struct {
		first, last *rel
	}
)

func (r *readers) Start(v int64) *rel {
	if r.first == nil {
		r.last = &rel{
			r:   r,
			ver: v,
			cnt: 1,
		}
		r.first = r.last
		return r.last
	}

	if r.last.ver > v {
		panic("unexpected ver")
	}

	if r.last.ver == v {
		r.last.cnt++
		return r.last
	}

	a := &rel{
		r:    r,
		ver:  v,
		cnt:  1,
		prev: r.last,
	}

	r.last.next = a
	r.last = a
	return a
}

func (e *rel) Finish() int64 {
	e.cnt--

	if e.cnt != 0 {
		return e.r.first.ver
	}

	r := e.r

	if r.first == e {
		r.first = e.next
		if r.first == nil {
			return e.ver
		} else {
			return r.first.ver
		}
	}

	if r.last == e {
		r.last = e.prev
		if r.last == nil {
			r.first = nil
			return e.ver
		} else {
			return r.first.ver
		}
	}

	e.prev.next = e.next
	e.next.prev = e.prev

	return r.first.ver
}

//*** for debugging ***

func (r *readers) Dump() string {

	var buf bytes.Buffer

	for e := r.first; e != nil; e = e.next {
		fmt.Fprintf(&buf, "rel 0x%4x  ver %5v cnt %3v  next 0x%4x prev 0x%4x\n", link(e), e.ver, e.cnt, link(e.next), link(e.prev))
	}
	fmt.Fprintf(&buf, "last 0x%4x\n", link(r.last))

	return buf.String()
}

func link(e *rel) uintptr {
	return (uintptr)((unsafe.Pointer)(e)) % 0x10000
}

/***/
