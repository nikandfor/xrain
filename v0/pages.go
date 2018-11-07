package xrain

const (
	// header flags
	FlagIsLeaf = 1 << iota
)

func init() { // type checks
}

type (
	// Page is a common part of Leaf page and Branch page
	//   header  uint8
	//   n       uint8
	//   offsets [n+1]int16 // last element is an offset of next byte after used space
	//   _       [...]byte  // padding to fill page from down to up
	//   ...                // pagetype specific fields
	Page []byte

	// Leaf page
	//   ... // common fields
	//   keyvals [n]KeyValue
	// KeyValue
	//   keylen varint
	//   key    [...]byte // or link
	//   value  [...]byte // or link
	Leaf struct{ Page }

	// Branch page
	//   ... // common fields
	//   keys [n]KeyLink
	// KeyLink
	//   offset int64
	//   key  [...]byte // key
	Branch struct{ page }
)

func (p Page) IsFlag(f int) bool {
	h := p[0]
	return h&f == f
}

func (p Page) SetFlag(f int) {
	p[0] |= f
}

func (p Page) Size() int {
	return p.int8(1)
}

func (p page) Offset(i int) int {
	return p.int16(1 + 1 + 2*i)
}

// int8 decodes LittleEndian int8 number starting from i-th byte
func (p Page) int8(i int) int {
	return (int)(p[i])
}

// int16 decodes LittleEndian int16 number starting from i-th byte
func (p Page) int16(i int) int {
	return (int)(p[i]) + (int)(p[i+1])<<8
}

// int16 decodes LittleEndian int32 number starting from i-th byte
func (p Page) int32(i int) int32 {
	return (int)(p[i]) + (int)(p[i+1])<<8 + (int)(p[i+2])<<16 + (int)(p[i+3])<<24
}

// int64 decodes LittleEndian int64 number starting from i-th byte
func (p Page) int64(i int) int64 {
	return (int64)(p[i]) + (int64)(p[i+1])<<8 + (int64)(p[i+2])<<16 + (int64)(p[i+3])<<24 +
		(int64)(p[i+4])<<32 + (int64)(p[i+5])<<40 + (int64)(p[i+6])<<48 + (int64)(p[i+7])<<56
}
