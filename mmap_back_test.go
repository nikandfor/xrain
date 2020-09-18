// +build linux darwin

package xrain

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackMmap(t *testing.T) {
	fn := t.TempDir() + "/xrain_mmap_test"
	//	fn := "/tmp/xrain_mmap_test"

	t.Logf("file: %v", fn)

	m, err := Mmap(fn, os.O_CREATE|os.O_RDWR|os.O_TRUNC)
	require.NoError(t, err)

	t.Logf("mmap: %+v  %x %x", m, os.O_RDWR, os.O_WRONLY)

	assert.Equal(t, int64(0), m.Size())

	assert.Panics(t, func() { m.Access(0, 0x10) })

	err = m.Truncate(0x400)
	require.NoError(t, err)

	assert.Equal(t, int64(0x400), m.Size())

	p := m.Access(0, 0x100)
	copy(p, "header")
	m.Unlock(p)

	p, p2 := m.Access2(0, 0x100, 0x100, 0x100)
	copy(p2, p[:0x10])
	m.Unlock2(p, p2)

	p = m.Access(0x100, 0x10)
	assert.Equal(t, []byte("header"), p[:len("header")])
	m.Unlock(p)
}
