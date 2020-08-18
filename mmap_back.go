// +build linux darwin

package xrain

import (
	"os"
	"sync"
	"syscall"
	"unsafe"

	"github.com/nikandfor/errors"
)

type (
	MmapBack struct {
		rw bool
		mu sync.RWMutex
		f  *os.File
		d  []byte
	}
)

var _ Back = &MmapBack{}

func Mmap(n string, flags int) (*MmapBack, error) {
	if flags == 0 {
		flags = os.O_CREATE | os.O_RDWR
	}

	f, err := os.OpenFile(n, flags, 0640)
	if err != nil {
		return nil, err
	}

	return MmapFile(f, flags&os.O_WRONLY == os.O_WRONLY)
}

func MmapFile(f *os.File, rw bool) (_ *MmapBack, err error) {
	b := &MmapBack{
		rw: rw,
		f:  f,
	}

	s := b.Size()

	if s == 0 {
		return b, nil
	}

	err = b.mmap(0, s)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (b *MmapBack) Close() error {
	err := b.unmap()
	if err != nil {
		return err
	}

	return b.f.Close()
}

func (b *MmapBack) unmap() error {
	if b.d == nil {
		return nil
	}

	err := syscall.Munmap(b.d)
	if err != nil {
		return err
	}

	return nil
}

func (b *MmapBack) mmap(off, len int64) (err error) {
	flags := syscall.PROT_READ
	if b.rw {
		flags |= syscall.PROT_WRITE
	}

	b.d, err = syscall.Mmap(int(b.f.Fd()), off, int(len), flags, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	return nil
}

func (b *MmapBack) Access(off, l int64) []byte {
	b.mu.RLock()

	if tl.V("back,access") != nil {
		tl.Printf("back access   %5x %5x", off, l)
	}

	if off == NilPage || l == 0 {
		return nil
	}

	return b.d[off : off+l : off+l]
}

func (b *MmapBack) Access2(off, l, off2, l2 int64) (p, p2 []byte) {
	b.mu.RLock()

	if tl.V("back,access") != nil {
		tl.Printf("back access2  %5x %5x   %5x %5x", off, l, off2, l2)
	}

	if off != NilPage && l != 0 {
		p = b.d[off : off+l : off+l]
	}

	if off2 != NilPage && l2 != 0 {
		p2 = b.d[off2 : off2+l2 : off2+l2]
	}

	return
}

func (b *MmapBack) Unlock(p []byte) {
	b.mu.RUnlock()
}

func (b *MmapBack) Unlock2(p, p2 []byte) {
	b.mu.RUnlock()
}

func (b *MmapBack) Truncate(s int64) (err error) {
	defer b.mu.Unlock()
	b.mu.Lock()

	if tl.V("back,truncate") != nil {
		tl.Printf("back truncate %5x <- %5x", s, len(b.d))
	}

	err = b.unmap()
	if err != nil {
		return
	}

	err = b.f.Truncate(s)
	if err != nil {
		return err
	}

	err = b.mmap(0, s)
	if err != nil {
		return err
	}

	return nil
}

func (b *MmapBack) Size() int64 {
	defer b.mu.RUnlock()
	b.mu.RLock()

	if b.d != nil {
		return int64(len(b.d))
	}

	inf, err := b.f.Stat()
	if err != nil {
		panic(err)
	}

	return inf.Size()
}

func (b *MmapBack) Sync() error {
	defer b.mu.RUnlock()
	b.mu.RLock()

	r, _, e := syscall.Syscall(syscall.SYS_MSYNC, uintptr(unsafe.Pointer(&b.d[0])), uintptr(len(b.d)), syscall.MS_SYNC)
	if e != 0 {
		return e
	}

	if r != 0 {
		return errors.New("bad ret code: %x", r)
	}

	return nil
}
