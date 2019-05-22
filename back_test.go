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

	p := b.Access(0x100, 0x100)
	copy(p, "PAGE2 content")
	b.Unlock(p)

	p = b.Access(0x0, 0x10)
	copy(p, "PAGE1 content")
	b.Unlock(p)

	err = b.Sync()
	assert.NoError(t, err)

	lp, rp := b.Access2(0x0, 0x10, 0x100, 0x10)
	r := bytes.HasPrefix(lp, []byte("PAGE1 content"))
	assert.True(t, r)

	r = bytes.HasPrefix(rp, []byte("PAGE2 content"))
	assert.True(t, r)
	b.Unlock2(lp, rp)

	b.Copy(0x100, 0x0, 0x10)

	p = b.Access(0x100, 0x10)
	r = bytes.HasPrefix(p, []byte("PAGE1 content"))
	assert.True(t, r)
	b.Unlock(p)

	assert.Equal(t, int64(0x200), b.Size())

	err = b.Truncate(0x100)
	assert.NoError(t, err)

	p = b.Access(0x0, 0x10)
	r = bytes.HasPrefix(p, []byte("PAGE1 content"))
	assert.True(t, r)
	b.Unlock(p)
}
