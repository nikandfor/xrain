package xrain

import "encoding/binary"

var zeroPage = make([]byte, 1<<20) // 1M

type (
	Allocator interface {
		Alloc() (int64, []byte, error)
		Write(off int64, p []byte) (int64, []byte, error)
		Read(off int64) ([]byte, error)
		Free(off int64) error
		Abort() error
		Commit() (int64, error)
		Page() int64
	}

	TreeAlloc struct {
		b    Back
		t    *tree
		page int64
		free int64
		w    map[int64]struct{}
		buf  [8]byte
	}

	SeqAlloc struct {
		b    Back
		page int64
		free int64
		w    map[int64]struct{}
	}

	NoAlloc struct {
		Allocator
		b    Back
		page int64
	}
)

func NewTreeAlloc(b Back, page, root int64) (*TreeAlloc, error) {
	f, err := b.Len()
	if err != nil {
		return nil, err
	}
	a := &TreeAlloc{
		b:    b,
		page: page,
		free: f,
		w:    make(map[int64]struct{}),
	}
	a.t, err = NewBPTree(root, a, BytesPage{a: a})
	if err != nil {
		return nil, err
	}

	return a, nil
}

func (a *TreeAlloc) Alloc() (off int64, p []byte, err error) {
	for f := a.t.Next(nil); f != nil; f = a.t.Next(f) {
		a.t.Del(f)
		if a.t.err != nil {
			return 0, nil, a.t.err
		}
		off = int64(binary.BigEndian.Uint64(f))
		a.w[off] = struct{}{}
		p, err = a.b.Read(off, a.page)
		return
	}
	off = a.free
	a.free += a.page
	a.w[off] = struct{}{}
	err = a.b.Write(off, zeroPage[:a.page])
	if err != nil {
		return
	}
	p, err = a.b.Read(off, a.page)
	return
}

func (a *TreeAlloc) Write(off int64, p []byte) (int64, []byte, error) {
	if _, ok := a.w[off]; ok {
		var err error
		if p == nil {
			p, err = a.b.Read(off, a.page)
		}
		return off, p, err
	}
	return a.Alloc()
}

func (a *TreeAlloc) Read(off int64) ([]byte, error) {
	return a.b.Read(off, a.page)
}

func (a *TreeAlloc) Free(off int64) error {
	delete(a.w, off)
	buf := a.buf[:]
	binary.BigEndian.PutUint64(buf, uint64(off))
	a.t.Put(buf, nil)
	return a.t.err
}

func (a *TreeAlloc) Page() int64 {
	return a.page
}

func (a *TreeAlloc) Abort() error {
	return nil
}

func (a *TreeAlloc) Commit() (int64, error) {
	return a.t.root, nil
}

func NewSeqAlloc(b Back, page, free int64) *SeqAlloc {
	return &SeqAlloc{
		b:    b,
		page: page,
		free: free,
		w:    make(map[int64]struct{}),
	}
}

func (a *SeqAlloc) Alloc() (int64, []byte, error) {
	off := a.free
	a.free += a.page
	a.w[off] = struct{}{}
	p, err := a.b.Read(off, a.page)
	return off, p, err
}

func (a *SeqAlloc) Write(off int64, p []byte) (int64, []byte, error) {
	if _, ok := a.w[off]; ok {
		var err error
		if p == nil {
			p, err = a.b.Read(off, a.page)
		}
		return off, p, err
	}
	return a.Alloc()
}

func (a *SeqAlloc) Read(off int64) ([]byte, error) {
	return a.b.Read(off, a.page)
}

func (a *SeqAlloc) Free(off int64) error {
	return nil
}

func (a *SeqAlloc) Page() int64 {
	return a.page
}

func (a *SeqAlloc) Abort() error {
	return nil
}

func (a *SeqAlloc) Commit() (int64, error) {
	return a.free, nil
}

func NewNoAlloc(b Back, page int64) NoAlloc { return NoAlloc{b: b, page: page} }

func (a NoAlloc) Read(off int64) ([]byte, error) {
	return a.b.Read(off, a.page)
}

func (a NoAlloc) Page() int64 { return a.page }
