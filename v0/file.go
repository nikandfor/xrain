package xrain

type (
	File interface {
		Read(off int64, len int32) []byte
		Write(p []byte, off int64)
	}
)
