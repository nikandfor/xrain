package xrain

const Mb = 1 << 30

type (
	Back interface {
		Load(off, len int64) []byte
		Size() int64
		Truncate(size int64) error
		Grow(size int64) error
		Sync() error
	}

	MemBack []byte
)

func (bk *MemBack) Load(off, l int64) []byte {
	b := *bk
	if len(b) < int(off+l) {
		panic("out of range")
	}
	return b[off : off+l]
}

func (bk *MemBack) Grow(s int64) error {
	b := *bk
	if len(b) == int(s) {
		return nil
	}
	if int(s) <= cap(b) {
		return nil
	}
	l := cap(b)
	for l < int(s) {
		if l < 1024 {
			l *= 2
		} else if l < 20*Mb {
			l += l / 4
		} else {
			l += 5 * Mb
			if m := l % (5 * Mb); m != 0 {
				l -= m
			}
		}
	}
	*bk = make([]byte, l)
	copy(*bk, b)
	return nil
}

func (bk *MemBack) Truncate(s int64) error {
	if int(s) >= cap(*bk) {
		return bk.Grow(s)
	}
	*bk = (*bk)[:s]
	return nil
}

func (bk *MemBack) Size() (int64, error) {
	return int64(len(*bk)), nil
}

func Sync() error { return nil }
