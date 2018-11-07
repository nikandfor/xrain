package xrain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReaders(t *testing.T) {
	r := &readers{}

	e1 := r.Start(1)
	e2 := r.Start(1)

	e3 := r.Start(2)
	e4 := r.Start(3)

	//	t.Logf("queue:\n%v", r.Dump())

	var v int64
	v = e2.Finish()
	assert.Equal(t, v, int64(1))

	v = e3.Finish()
	assert.Equal(t, v, int64(1))

	v = e1.Finish()
	assert.Equal(t, v, int64(3))

	v = e4.Finish()
	assert.Equal(t, v, int64(3))

	//	t.Logf("free queue:\n%v", r.Dump())

	e5 := r.Start(3)

	//	t.Logf("queue:\n%v", r.Dump())

	v = e5.Finish()
	assert.Equal(t, v, int64(3))

	//	t.Logf("free queue:\n%v", r.Dump())
}
