package xrain

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func BenchmarkGetIntFromBytesCast(b *testing.B) {
	page := make([]byte, 100)
	copy(page[4:], []byte{1, 2})
	var n int

	for i := 0; i < b.N; i++ {
		n = (int)(*(*int16)(unsafe.Pointer(&page[4])))
	}

	assert.Equal(b, 0x201, n)
}

func BenchmarkGetIntFromBytesBinary(b *testing.B) {
	page := make([]byte, 100)
	copy(page[4:], []byte{1, 2})
	var n int

	for i := 0; i < b.N; i++ {
		n = (int)(page[4]) | (int)(page[5])<<8
	}

	assert.Equal(b, 0x201, n)
}
