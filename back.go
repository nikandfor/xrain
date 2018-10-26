package xrain

import "io"

type (
	Back interface {
		Read(off, len int64) ([]byte, error)
		Write(off int64, data []byte) error
		Size() (int64, error)
		Truncate(size int64) error
		Sync() error
	}

	MemBack []byte
)

func (bk *MemBack) Read(off, l int64) ([]byte, error) {
	b := *bk
	if len(b) < int(off+l) {
		if int(off) > len(b) {
			return nil, io.EOF
		}
		return b[off:], io.EOF
	}
	return b[off : off+l], nil
}

func (bk *MemBack) Write(off int64, d []byte) error {
	if len(*bk) < int(off)+len(d) {
		_ = bk.Truncate(off + int64(len(d)))
	}
	copy((*bk)[off:], d)
	return nil
}

func (bk *MemBack) Truncate(s int64) error {
	b := *bk
	if len(b) == int(s) {
		return nil
	}
	if int(s) < cap(b) {
		*bk = b[:s]
		return nil
	}
	l := cap(b)
	for l < int(s) {
		if l < 1024 {
			l *= 2
		}
		l += l / 4
	}
	*bk = make([]byte, l)
	copy(*bk, b)
	return nil
}

func (bk *MemBack) Size() (int64, error) {
	return int64(len(*bk)), nil
}

func Sync() error { return nil }
