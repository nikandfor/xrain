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
		mu sync.RWMutex
		f  *os.File
		d  []byte
	}
)

var _ Back = &MmapBack{}

func Mmap(n string) (*MmapBack, error) {
	flags := os.O_CREATE | os.O_RDWR

	f, err := os.OpenFile(n, flags, 0640)
	if err != nil {
		return nil, err
	}

	return MmapFile(f), nil
}

func MmapFile(f *os.File) *MmapBack {
	return &MmapBack{
		f: f,
	}
}

func (b *MmapBack) Close() error {
	err := b.close()
	if err != nil {
		return err
	}

	return b.f.Close()
}

func (b *MmapBack) close() error {
	if b.d == nil {
		return nil
	}

	err := syscall.Munmap(b.d)
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

	err = b.close()
	if err != nil {
		return
	}

	err = b.f.Truncate(s)
	if err != nil {
		return err
	}

	b.d, err = syscall.Mmap(int(b.f.Fd()), 0, int(s), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	return nil
}

func (b *MmapBack) Size() int64 {
	defer b.mu.RUnlock()
	b.mu.RLock()

	return int64(len(b.d))
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
