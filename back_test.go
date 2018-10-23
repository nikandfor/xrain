package xrain

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemBack(t *testing.T) {
	b := NewMemBack(0)

	testBack(t, b)
}

func testBack(t *testing.T, b Back) {
	const Page = int64(0x10)

	_, err := b.Read(0, Page)
	assert.EqualError(t, err, io.EOF.Error())

	l, err := b.Len()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), l)

	err = b.Truncate(Page)
	assert.NoError(t, err)

	l, err = b.Len()
	assert.NoError(t, err)
	assert.Equal(t, Page, l)

	err = b.Truncate(Page)
	assert.NoError(t, err)

	l, err = b.Len()
	assert.NoError(t, err)
	assert.Equal(t, Page, l)

	page1 := make([]byte, Page)
	copy(page1, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6})
	err = b.Write(0, page1)
	assert.NoError(t, err)

	l, err = b.Len()
	assert.NoError(t, err)
	assert.Equal(t, Page, l)

	page3 := make([]byte, Page)
	copy(page3, []byte{11, 12, 13, 14, 15, 16, 17, 18, 19, 10, 11, 12, 13, 14, 15, 16})
	err = b.Write(2*Page, page3)
	assert.NoError(t, err)

	l, err = b.Len()
	assert.NoError(t, err)
	assert.Equal(t, 3*Page, l)

	data, err := b.Read(2*Page, Page)
	assert.NoError(t, err)
	assert.Equal(t, data, page3)

	err = b.Truncate(Page)
	assert.NoError(t, err)

	l, err = b.Len()
	assert.NoError(t, err)
	assert.Equal(t, Page, l)

	data, err = b.Read(0, Page)
	assert.NoError(t, err)
	assert.Equal(t, data, page1)

	err = b.Sync()
	assert.NoError(t, err)
}
