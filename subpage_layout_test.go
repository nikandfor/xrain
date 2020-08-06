package xrain

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSubpageLayout(t *testing.T) {
	initLogger(t)

	l := NewSubpageLayout(nil)

	//

	_, eq := l.Search([]byte("qwe"), nil)
	assert.False(t, eq)

	st := l.Step(nil, 0, false)
	assert.Nil(t, st)

	st, eq = l.Seek(st[:0], 0, []byte("key"), nil)
	assert.False(t, eq)
	assert.Equal(t, Stack{0}, st)

	st, err := l.Delete(st)
	assert.NoError(t, err)
	assert.Nil(t, st)

	k, _ := l.Key(st, nil)
	assert.Nil(t, k)

	v := l.Value(st, nil)
	assert.Nil(t, v)

	//

	st, err = l.Insert(Stack{0}, 0, []byte("key"), []byte("value"))
	assert.NoError(t, err)
	assert.Equal(t, Stack{0}, st)

	tl.Printf("dump\n%v", hex.Dump(l.Bytes()))

	st, err = l.Insert(st[:0], 0, []byte("key_b"), []byte("value_b"))
	assert.NoError(t, err)
	assert.Equal(t, Stack{1}, st)

	tl.Printf("dump\n%v", hex.Dump(l.Bytes()))

	st, err = l.Insert(st[:0], 0, []byte("key_a"), []byte("value_a"))
	assert.NoError(t, err)
	assert.Equal(t, Stack{1}, st)

	tl.Printf("dump\n%v", hex.Dump(l.Bytes()))

	st, err = l.Insert(st[:0], 0, []byte("key_aa"), []byte("value_aa"))
	assert.NoError(t, err)
	assert.Equal(t, Stack{2}, st)

	tl.Printf("dump\n%v", hex.Dump(l.Bytes()))

	//

	var last []byte
	cnt := 0
	for st := l.Step(nil, 0, false); st != nil; st = l.Step(st, 0, false) {
		k, _ := l.Key(st, nil)

		assert.True(t, bytes.Compare(last, k) <= 0, "%q before %q", last, k)

		last = k
		cnt++
	}
	assert.Equal(t, 4, cnt)

	cnt = 0
	for st := l.Step(nil, 0, true); st != nil; st = l.Step(st, 0, true) {
		k, _ := l.Key(st, nil)

		if cnt != 0 {
			assert.True(t, bytes.Compare(last, k) >= 0, "%q after %q", last, k)
		}

		last = k
		cnt++
	}
	assert.Equal(t, 4, cnt)

	//

	st, eq = l.Seek(st, 0, []byte("key_a"), nil)
	assert.True(t, eq)
	assert.Equal(t, Stack{1}, st)

	buf := []byte("prefix_")
	k, _ = l.Key(st, buf)
	assert.Equal(t, []byte("prefix_key_a"), k)

	buf = []byte("p__")
	v = l.Value(st, buf)
	assert.Equal(t, []byte("p__value_a"), v)

	//

	st, eq = l.Seek(st, 0, []byte("key_q"), nil)
	assert.False(t, eq)
	assert.Equal(t, Stack{OffIndex(cnt)}, st)

	st, eq = l.Seek(st, 0, []byte("key_aa"), nil)
	assert.True(t, eq)
	assert.Equal(t, Stack{2}, st)
	st, err = l.Delete(st)
	assert.NoError(t, err)

	tl.Printf("dump\n%v", hex.Dump(l.Bytes()))

	st, eq = l.Seek(st, 0, []byte("key"), nil)
	assert.True(t, eq)
	st, err = l.Delete(st)
	assert.NoError(t, err)

	tl.Printf("dump\n%v", hex.Dump(l.Bytes()))

	st, eq = l.Seek(st, 0, []byte("key_b"), nil)
	assert.True(t, eq)
	st, err = l.Delete(st)
	assert.NoError(t, err)

	tl.Printf("dump\n%v", hex.Dump(l.Bytes()))

	st, eq = l.Seek(st, 0, []byte("key_a"), nil)
	assert.True(t, eq)
	st, err = l.Delete(st)
	assert.NoError(t, err)

	tl.Printf("dump\n%v", hex.Dump(l.Bytes()))

	//

	st = l.Step(nil, 0, false)
	assert.Nil(t, st)

	//

	l.Alloc()
	l.Free(NilPage)
	l.SetMeta(nil)
}
