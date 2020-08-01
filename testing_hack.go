package xrain

import (
	"testing"
	_ "unsafe"
)

//go:linkname testingLogDepth testing.(*T).logDepth
func testingLogDepth(t *testing.T, s string, d int)
