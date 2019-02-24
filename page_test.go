package xrain

type (
	Logger interface {
		Printf(f string, args ...interface{})
	}
	LogLayout struct {
		PageLayout
		Logger
	}
)

func (l LogLayout) NKeys(off int64) int {
	n := l.PageLayout.NKeys(off)
	//	l.Logger.Printf("LayOut %4x NKeys %v", off, n)
	return n
}

func (l LogLayout) IsLeaf(off int64) bool {
	r := l.PageLayout.IsLeaf(off)
	l.Logger.Printf("LayOut %4x IsLeaf %v", off, r)
	return r
}

func (l LogLayout) KeyCmp(off int64, i int, k []byte) (c int) {
	c = l.PageLayout.KeyCmp(off, i, k)
	//	l.Logger.Printf("LayOut %4x KeyCmp %v %q -> %v", off, i, k, c)
	return
}

func (l LogLayout) Key(off int64, i int) []byte {
	r := l.PageLayout.Key(off, i)
	l.Logger.Printf("LayOut %4x Key %v -> %q", off, i, r)
	return r
}

func (l LogLayout) Put(off int64, i int, k, v []byte) (loff, roff int64, err error) {
	loff, roff, err = l.PageLayout.Put(off, i, k, v)
	l.Logger.Printf("LayOut %4x Put %v %q %q -> %x %x %v", off, i, k, v, loff, roff, err)
	return
}

func (l LogLayout) Del(off int64, i int) (loff int64, err error) {
	loff, err = l.PageLayout.Del(off, i)
	l.Logger.Printf("LayOut %4x Del %v -> %x %v", off, i, loff, err)
	return
}
