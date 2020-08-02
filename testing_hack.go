package xrain

import (
	"reflect"
	"testing"
	"unsafe"
)

type testingWriter struct {
	t unsafe.Pointer
}

func newTestingWriter(t testing.TB) testingWriter {
	v := reflect.ValueOf(t).Pointer()
	return testingWriter{t: unsafe.Pointer(v)}
}

//go:linkname testingLogDepth testing.(*common).logDepth
func testingLogDepth(t unsafe.Pointer, s string, d int)
