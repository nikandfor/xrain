package multilayer

type (
	Tx interface {
		Read(off int64, len int64) []byte
		Write(off int64, b []byte)
		Commit()
		Abort()
		Tx() Tx
	}
	MultiLayer interface {
		Tx() Tx
	}
	File interface {
		Read(off int64, len int64) []byte
		Write(off int64, b []byte)
	}

	seg struct {
		off  int64
		data []byte
	}

	tx struct {
		f   *file
		par *tx
		s   []seg
	}
	file struct {
		l0 File
	}
)

func (f *file) Tx() Tx {
	return &tx{f: f}
}

func (t *tx) Tx() Tx {
	return &tx{f: f, par: t}
}

func (t *tx) Read(off int64, len int64) []byte {

}
