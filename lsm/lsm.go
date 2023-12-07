package lsm

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	stderrors "errors"
	"io"
	"log"
	"math"
	"sort"

	"github.com/nikandfor/hacked/low"
	"tlog.app/go/errors"
	"tlog.app/go/loc"
)

type (
	DB struct {
		path string

		Encoder Encoder

		BlockSize int64
		TableSize int64

		Level0Size      int64
		LevelMultiplier int64

		levels [][]file
	}

	Batch struct {
		t *memTable
	}

	file struct {
		name string
		ver  int
		key  []byte
		last []byte
	}

	memTable struct {
		kvs []kv
	}

	kv struct {
		k, v  []byte
		flags int
	}

	memTableIter struct {
		*memTable
		i, peek int
	}

	fileTable struct {
		r    io.ReaderAt
		size int64

		flags        int
		level, seq   int
		m1off, m2off int64

		index       []fileTableIndexEl
		first, last []byte

		b []byte
	}

	fileTableIndexEl struct {
		off  int64
		last []byte
	}

	Iter interface {
		Done() bool
		Next() bool
		Peek() bool

		//	Seek(k []byte) bool

		Key() []byte
		KeyValueF() ([]byte, []byte, int)

		Err() error
	}

	Encoder struct {
		BlockFlags int
	}

	Decoder struct{}
)

var zeros = make([]byte, 1024)

/*
Format description

File consists of blocks. Each block starts with tag byte which defines the block type.
If block encoding changes, it's allocated as a new block type.

Typical file structure is:
- Header or Padding if the Header couldn't be written at this space afterwards.
- Data repeated 0 to N times
- Index     // inside-file index: for data blocks
- FileIndex // inter-file index: for multiple files
- Trailer   // the same as Header except the Tag value

Padding can be safely added between any blocks.
Multiple files can be safely concatenated.

Basic types

Int is a basic type for encoding integers. Values up to Int1 (not included) encoded as is.
The next 0x100 values encoded as []byte{Int1, value - Int1}.
The next 0x10000 values encoded as []byte{Int2, value - Int1 - 0x100}.
Substractions are to make it more compact.
So that we encode Int1 as []byte{Int1, 0x00} instead of []byte{Int1, Int1}.
And Int1 + 0xff is encoded as []byte{Int2, 0xff} instead of []byte{Int2, value&0xff, value>>8} (one byte less).

Key is prefix compressed key (optionally). It's encoded as
	prefix Int // omitted and treated as 0 if PrefixKeyEncoding is not set.
	size   Int
	tail   [size]byte

The first key is encoded as {0, len(key), key}.
The all subsequent keys are encoded as {prefix, len(key) - prefix, key[prefix:]} where prefix = common(prevKey, key).

Blocks format

Padding is a sequence of zero bytes until the first non-zero byte.

Header and Trailer have fixed size of 32 bytes.
	tag   byte  // BlockHeader or BlockTrailer
	flags byte  // 8 lower bits of flags, 4 more bits stored in higher 4 bits of level
	level byte  // level this file belongs to
	seq   int40 // little endian 5-byte integer
	            // if two files belong to the same level and key ranges intersect
				// values with higher seq override values with lower seq
	_ [...]byte // padding

Data
	tag   byte
	flags byte
	[...]struct {
		recSize Int // size of the record not including pairSize field
		flags   Int
		pkey    Key       // key is append(lastKey[:pkey.prefix], pkey.tail...)
		value   [...]byte // until end of record
	}

Index
	tag   byte
	flags byte
	[...]struct {
		off  int64
		pkey Key
	}

FileIndex
	tag   byte
	flags byte
	first Key // empty prefix
	last  Key // decoded as append(first[:last.prefix], last.tail...)
*/

// Int lengths.
const (
	_ = 1<<8 - iota
	_ // reserved
	Int8
	Int4
	Int2
	Int1
)

// Block tags.
const (
	BlockPadding = iota
	BlockData
	BlockHeader
	BlockTrailer
	BlockIndex
	BlockFileIndex
)

// Block flags.
const (
	PrefixKeyEncoding = 1 << iota
	TableUnsorted

	blockFlagsSize = iota // bits
)

// Key flags.
const (
	keyFlagsSize = iota // bits
)

const (
	headerSizeV0 = 32
)

var ( // errors
	ErrEndOfBuffer = stderrors.New("unexpected end of buffer")
	ErrOverflow    = stderrors.New("length/offset overflow")
)

func NewDB(path string) (*DB, error) {
	d := &DB{
		path: path,

		BlockSize: 32<<20 - 1<<10,
		TableSize: 2<<30 - 1<<20,
	}

	return d, nil
}

func (d *DB) WriteBatch(b *Batch) error {
	sort.Stable(b.t)
	// TODO: dedup

	err := d.writeTables(b.t.Iter(), 0, 1)

	return err
}

func (d *DB) writeTables(it Iter, level, seq int) error {
	for !it.Done() {
		_, err := d.writeTable(io.Discard, it, level, seq) // TODO
		if err != nil {
			return errors.Wrap(err, "write table")
		}
	}

	return nil
}

func (d *DB) writeTable(w io.Writer, it Iter, level, seq int) (int64, error) {
	defer func() { log.Printf("writeTable  from %v", loc.Caller(1)) }()
	var b, meta []byte
	var key, first, last []byte
	var off int64

	if !it.Peek() {
		return 0, nil
	}

	flags := d.Encoder.BlockFlags

	b = append(b, zeros[:headerSizeV0]...) // padding which we'll try to override with Header in the end

	binary.LittleEndian.PutUint64(b, uint64(seq<<24))

	b[0] = BlockHeader
	b[1] = byte(flags)
	b[2] = byte(level&0x0f) | byte(flags>>4&0xf0)

	n, err := w.Write(b)
	off += int64(n)
	if err != nil {
		return off, errors.Wrap(err, "write header padding")
	}

	first = it.Key() // TODO: slice lifetime

	meta = append(meta, BlockIndex, 0)

	log.Printf("iter %4x  %v  %+v", off, it.Done(), it)

	for !it.Done() && off < d.TableSize {
		b, last = d.appendBlock(b[:0], it, d.BlockSize, last[:0])

		log.Printf("block  off %4x  %q\n%s", off, last, hex.Dump(b))

		//	meta = binary.LittleEndian.AppendUint64(meta, uint64(off))
		meta = d.Encoder.Int64(meta, off)

		pref := common(key, last)
		meta = d.Encoder.Key(meta, last, pref)

		log.Printf("meta elem\n%s", hex.Dump(meta))

		key = append(key[:pref], last[pref:]...)

		n, err := w.Write(b)
		off += int64(n)
		if err != nil {
			return off, errors.Wrap(err, "write block")
		}
	}

	m1off := off
	m2off := off + int64(len(meta))

	meta = append(meta, BlockFileIndex, 0)

	meta = d.Encoder.Key(meta, first, 0)
	meta = d.Encoder.Key(meta, last, common(first, last))

	log.Printf("b before meta\n%s", hex.Dump(*w.(*low.Buf)))
	log.Printf("meta\n%s", hex.Dump(meta))

	n, err = w.Write(meta)
	log.Printf("meta %v/%v  %v", n, len(meta), err)
	log.Printf("b\n%s", hex.Dump(*w.(*low.Buf)))
	off += int64(n)
	if err != nil {
		return off, errors.Wrap(err, "write meta")
	}

	b = b[:headerSizeV0]

	binary.LittleEndian.PutUint64(b, uint64(seq<<24))

	b[0] = BlockTrailer
	b[1] = byte(flags)
	b[2] = byte(level&0x0f) | byte(flags>>4&0xf0)

	binary.LittleEndian.PutUint64(b[8:], uint64(m1off))
	binary.LittleEndian.PutUint64(b[16:], uint64(m2off))
	copy(b[24:], zeros)

	log.Printf("b before trailer\n%s", hex.Dump(*w.(*low.Buf)))

	n, err = w.Write(b)
	log.Printf("trailer %v/%v  %v", n, len(b), err)
	log.Printf("b\n%s", hex.Dump(*w.(*low.Buf)))
	off += int64(n)
	if err != nil {
		return off, errors.Wrap(err, "write trailer")
	}

	if w, ok := w.(io.WriterAt); ok {
		b[0] = BlockHeader

		n, err = w.WriteAt(b, 0)
		log.Printf("header %v/%v  %v", n, len(b), err)
		if err != nil {
			return off, errors.Wrap(err, "write header")
		}
	}

	return off, nil
}

func (d *DB) appendBlock(b []byte, it Iter, lim int64, key []byte) (data, last []byte) {
	b = append(b, BlockData, byte(d.Encoder.BlockFlags))

	for len(b) < int(lim) && it.Next() {
		k, v, f := it.KeyValueF()

		pref := common(key, k)

		size := d.Encoder.IntSize(f) +
			d.Encoder.KeySize(k, pref) +
			len(v)

		b = d.Encoder.Int(b, size)
		b = d.Encoder.Int(b, f)
		b = d.Encoder.Key(b, k, pref)
		b = append(b, v...)

		key = append(key[:pref], k[pref:]...)
	}

	return b, key
}

func (e Encoder) OffKey(b []byte, off int64, key []byte, pref int) []byte {
	b = binary.LittleEndian.AppendUint64(b, uint64(off))
	b = e.Key(b, key, pref)

	return b
}

func (e Encoder) KeySize(key []byte, pref int) int {
	prefix := 0
	tail := len(key)

	if e.BlockFlags&PrefixKeyEncoding != 0 {
		prefix = e.IntSize(pref)
		tail = len(key[pref:])
	}

	return prefix + e.IntSize(tail) + tail
}

func (e Encoder) Key(b, key []byte, pref int) []byte {
	tail := key

	if e.BlockFlags&PrefixKeyEncoding != 0 {
		b = e.Int(b, pref)
		tail = key[pref:]
	}

	b = e.Int(b, len(tail))
	b = append(b, tail...)

	return b
}

func (d *DB) openFileTable(r io.ReaderAt, size int64, t *fileTable) (*fileTable, error) {
	defer func() { log.Printf("openFileTable  from %v", loc.Caller(1)) }()
	if t == nil {
		t = &fileTable{}
	}

	t.reset(r, size)

	var dec Decoder

	off := size - headerSizeV0
	if size == 0 {
		off = 0
	}

	t.b = grow(t.b, headerSizeV0)

	n, err := r.ReadAt(t.b, off)
	if n == 0 && errors.Is(err, io.EOF) {
		return t, nil
	}
	if err != nil {
		return t, err
	}

	// check tag at b[0]
	t.flags = int(t.b[1]) | int(t.b[2])>>4<<8
	t.level = int(t.b[2]) & 0xf
	t.seq = int(binary.LittleEndian.Uint64(t.b) >> 24)

	t.m1off = int64(binary.LittleEndian.Uint64(t.b[8:]))
	t.m2off = int64(binary.LittleEndian.Uint64(t.b[16:]))

	bs := size - t.m1off
	if size == 0 {
		bs = (t.m2off - t.m1off) + 0x100 + headerSizeV0
	}

	t.b = grow(t.b, int(bs))

	n, err = r.ReadAt(t.b, t.m1off)
	if errors.Is(err, io.EOF) {
		err = nil
	}
	if err != nil {
		return t, err
	}

	var last []byte
	i := 2 // tag, flags

	for i < int(t.m2off-t.m1off) {
		off, last, i, err = dec.OffKey(t.b, i, last, nil)
		if err != nil {
			return t, errors.Wrap(err, "decode meta1 index")
		}

		t.index = append(t.index, fileTableIndexEl{
			off:  off,
			last: last,
		})
	}

	log.Printf("m2off %v %v", t.m1off+int64(i), i)

	i += 2 // tag, flags

	t.first, i, err = dec.Key(t.b, i, nil, t.first[:0])
	if err != nil {
		return t, errors.Wrap(err, "decode meta2 index: first")
	}

	t.last, i, err = dec.Key(t.b, i, t.first, t.last[:0])
	if err != nil {
		return t, errors.Wrap(err, "decode meta2 index: first")
	}

	if i+headerSizeV0 != n {
		return t, ErrEndOfBuffer
	}

	return t, nil
}

func (d *Decoder) OffKey(b []byte, st int, last, buf []byte) (off int64, key []byte, i int, err error) {
	off, i, err = d.Int64(b, st)
	if err != nil {
		return off, key, st, err
	}

	key, i, err = d.Key(b, i, last, buf)
	if err != nil {
		return off, key, st, err
	}

	return
}

func (d *Decoder) Key(b []byte, st int, last, buf []byte) (key []byte, i int, err error) {
	key = buf
	i = st

	pref, i, err := d.Int(b, i)
	if err != nil {
		return
	}

	if pref > len(last) {
		return buf, st, ErrOverflow
	}

	l, i, err := d.Int(b, i)
	if err != nil {
		return
	}

	if i+l > len(b) {
		return buf, st, ErrEndOfBuffer
	}

	key = append(key, last[:pref]...)
	key = append(key, b[i:i+l]...)
	i += l

	return key, i, nil
}

func (t *memTable) PutFlags(k, v []byte, flags int) {
	t.kvs = append(t.kvs, kv{
		k: k, v: v, flags: flags,
	})
}

func (t *memTable) Len() int           { return len(t.kvs) }
func (t *memTable) Less(i, j int) bool { return bytes.Compare(t.kvs[i].k, t.kvs[j].k) < 0 }
func (t *memTable) Swap(i, j int)      { t.kvs[i], t.kvs[j] = t.kvs[j], t.kvs[i] }

func (t *memTable) Iter() Iter {
	return &memTableIter{
		memTable: t,
	}
}

func (t *memTableIter) Done() bool {
	return t.i > len(t.kvs)
}

func (t *memTableIter) Peek() bool {
	t.peek = 0

	return t.i < len(t.kvs)
}

func (t *memTableIter) Next() bool {
	t.peek = 1
	t.i++
	return t.i <= len(t.kvs)
}

func (t *memTableIter) Key() []byte {
	k, _, _ := t.KeyValueF()

	return k
}

func (t *memTableIter) KeyValueF() (k, v []byte, f int) {
	e := t.kvs[t.i-t.peek]

	return e.k, e.v, e.flags
}

func (t *memTableIter) Err() error {
	return nil
}

func (t *fileTable) reset(r io.ReaderAt, size int64) {
	t.r = r
	t.size = size

	t.flags, t.level, t.seq = 0, 0, 0
	t.m1off, t.m2off = 0, 0

	t.index = t.index[:0]
	t.first = t.first[:0]
	t.last = t.last[:0]
}

func (e Encoder) IntSize(l int) int {
	q := [...]int{Int1, 0x100, 0x1_0000, 0x1_0000_0000}

	n := 1

	for _, lim := range q {
		if l < lim {
			return n
		}

		l -= lim
		n++
	}

	return n
}

func (e Encoder) Int(b []byte, l int) []byte {
	return e.Int64(b, int64(l))
}

func (e Encoder) Int64(b []byte, l int64) []byte {
	if l < Int1 {
		return append(b, byte(l))
	}

	l -= Int1

	if l < 0x100 {
		return append(b, Int1, byte(l))
	}

	l -= 0x100

	if l < 0x1_0000 {
		return append(b, Int2, byte(l), byte(l>>8))
	}

	l -= 0x1_0000

	if l < 0x1_0000_0000 {
		return append(b, Int4, byte(l), byte(l>>8), byte(l>>16), byte(l>>24))
	}

	panic("too much")
}

func (d Decoder) Int(b []byte, st int) (l, i int, err error) {
	l64, i, err := d.Int64(b, st)
	if err != nil {
		return int(l64), i, err
	}
	if l64 > math.MaxInt {
		return int(l64), i, ErrOverflow
	}

	return int(l64), i, nil
}

func (d Decoder) Int64(b []byte, st int) (l int64, i int, err error) {
	i = st

	if i >= len(b) {
		return 0, st, ErrEndOfBuffer
	}

	l = int64(b[i])
	i++

	switch l {
	default:
	case Int1:
		if i+1 > len(b) {
			return l, st, ErrEndOfBuffer
		}

		l = Int1 + int64(b[i])
		i++
	case Int2:
		if i+2 > len(b) {
			return l, st, ErrEndOfBuffer
		}

		l = Int1 + 0x100
		l += int64(b[i]) + int64(b[i+1])<<8
		i += 2
	case Int4:
		if i+4 > len(b) {
			return l, st, ErrEndOfBuffer
		}

		l = Int1 + 0x100 + 0x1_0000
		l += int64(b[i]) | int64(b[i+1])<<8 | int64(b[i+2])<<16 | int64(b[i+3])<<24
		i += 4
	case Int8:
		if i+8 > len(b) {
			return l, st, ErrEndOfBuffer
		}

		l = Int1 + 0x100 + 0x1_0000 + 0x1_0000_0000
		l += int64(b[i]) | int64(b[i+1])<<8 | int64(b[i+2])<<16 | int64(b[i+3])<<24 |
			int64(b[i+4])<<32 | int64(b[i+5])<<40 | int64(b[i+6])<<48 | int64(b[i+7])<<52
		i += 8
	}

	return
}

func common(x, y []byte) int {
	i := 0

	for i < len(x) && i < len(y) && x[i] == y[i] {
		i++
	}

	return i
}

func grow(b []byte, s int) []byte {
	for cap(b) < s {
		b = append(b[:cap(b)], 0, 0, 0, 0, 0, 0, 0, 0)
	}

	return b[:s]
}
