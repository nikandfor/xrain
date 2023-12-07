package lsm

import (
	"encoding/hex"
	"testing"

	"github.com/nikandfor/hacked/low"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLSM(t *testing.T) {
	db, err := NewDB("")
	require.NoError(t, err)

	db.Encoder.BlockFlags = PrefixKeyEncoding

	var buf low.Buf

	tab0 := &memTable{}

	n, err := db.writeTable(&buf, tab0.Iter(), 2, 3)
	assert.NoError(t, err)
	assert.Equal(t, len(buf), int(n))

	tab1, err := db.openFileTable(buf, n, nil)
	assert.NoError(t, err)

	_ = tab1

	t.Logf("empty file dump\n%s", hex.Dump(buf))

	buf = buf[:0]

	tab0.PutFlags(s("key0"), s("value0"), 0)
	tab0.PutFlags(s("key1"), s("value1"), 1)
	tab0.PutFlags(s("key2"), s("value2"), 2)

	n, err = db.writeTable(&buf, tab0.Iter(), 2, 3)
	assert.NoError(t, err)
	assert.Equal(t, len(buf), int(n))

	tab1, err = db.openFileTable(buf, n, nil)
	assert.NoError(t, err)

	_ = tab1

	t.Logf("3 keys file dump\n%s", hex.Dump(buf))
	t.Logf("tab1 %+v", tab1)
}

func s(s string) []byte {
	return []byte(s)
}
