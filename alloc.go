package xrain

type (
	Alloc interface {
		Alloc() (int64, []byte, error)
		Write(off int64, p []byte) (int64, []byte, error)
		Read(off int64) ([]byte, error)
		Free(off int64) error
		Size() int64
	}

	SeqAlloc struct {
		b    Back
		page int64
		free int64
		w    map[int64]struct{}
	}
)

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

func (a *SeqAlloc) Size() int64 {
	return a.page
}
