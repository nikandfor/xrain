package xrain

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemBack(t *testing.T) {
	b := NewMemBack(0)

	assert.Equal(t, int64(0), b.Size())

	err := b.Truncate(0x200)
	assert.NoError(t, err)

	b.Access(0x100, 0x100, func(p []byte) {
		copy(p, "PAGE2 content")
	})

	b.Access(0x0, 0x10, func(p []byte) {
		copy(p, "PAGE1 content")
	})

	err = b.Sync()
	assert.NoError(t, err)

	b.Access2(0x0, 0x10, 0x100, 0x10, func(lp, rp []byte) {
		r := bytes.HasPrefix(lp, []byte("PAGE1 content"))
		assert.True(t, r)

		r = bytes.HasPrefix(rp, []byte("PAGE2 content"))
		assert.True(t, r)
	})

	err = b.Copy(0x100, 0x0, 0x10)
	assert.NoError(t, err)

	b.Access(0x100, 0x10, func(p []byte) {
		r := bytes.HasPrefix(p, []byte("PAGE1 content"))
		assert.True(t, r)
	})

	assert.Equal(t, int64(0x200), b.Size())

	err = b.Truncate(0x100)
	assert.NoError(t, err)

	b.Access(0x0, 0x10, func(p []byte) {
		r := bytes.HasPrefix(p, []byte("PAGE1 content"))
		assert.True(t, r)
	})

	assert.Panics(t, func() {
		b.Access(0x100, 0x10, nil)
	})
}
