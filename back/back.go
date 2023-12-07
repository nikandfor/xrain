package back

import "io"

type (
	Back interface {
		Open(off, size int64, flags int) (Page, error)

		Delete(off, size int64) error
	}

	Page interface {
		io.Reader
		io.ReaderAt

		io.Writer
		io.WriterAt

		io.Closer
	}
)
