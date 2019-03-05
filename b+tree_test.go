package xrain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTreeSmall(t *testing.T) {
	const Page = 0x80
	b := NewMemBack(Page)
	fl := NewFreeListNoReclaim(b, Page)
	pl := NewFixedLayout(b, Page, 0, fl)
	tr := NewTree(pl, 0, Page)

	err := tr.Put([]byte("key_aaaa"), []byte("value_aa"))
	assert.NoError(t, err)
}
