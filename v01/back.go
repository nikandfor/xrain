package xrain

import (
	"io"
)

type (
	Back interface {
		Read(off, len int64) ([]byte, error)
		Write(off int64, data []byte) error
		Len() (int64, error)
		Truncate(len int64) error
		Sync() error
	}

	MemBack struct {
		buf []byte
	}
)

func NewMemBack(l int64) *MemBack {
	return &MemBack{buf: make([]byte, l)}
}

func (b *MemBack) Read(off, l int64) ([]byte, error) {
	if off+l > int64(len(b.buf)) {
		return nil, io.EOF
	}
	return b.buf[off : off+l], nil
}

func (b *MemBack) Write(off int64, data []byte) error {
	end := int(off) + len(data)
	if end > len(b.buf) {
		_ = b.Truncate(int64(end))
	}
	copy(b.buf[off:], data)
	return nil
}

func (b *MemBack) Len() (int64, error) {
	return int64(len(b.buf)), nil
}

func (b *MemBack) Truncate(ln int64) error {
	l := int(ln)
	if l == len(b.buf) {
		return nil
	}
	if l > cap(b.buf) {
		cp := make([]byte, l)
		copy(cp, b.buf)
		b.buf = cp
	} else {
		b.buf = b.buf[0:l]
	}
	return nil
}

func (b *MemBack) Sync() error {
	return nil
}
