package xrain

type (
	MultiLayer struct {
		f File
	}
)

func (m *MultiLayer) Page(off int64, len int32) []byte {
	return m.f.Read(off, len)
}
