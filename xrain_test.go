package xrain

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/nikandfor/tlog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type (
	HeavyTester struct {
		DB *DB

		Workers       int
		Keys          int
		Iters         int
		Writes, Reads float64
	}

	TaskType bool

	HeavyTask struct {
		ID         int
		Intent     bool
		TaskType   TaskType
		Key, Value []byte
		Res        []byte
		Err        error
	}
)

const (
	TaskGet TaskType = iota != 0
	TaskPut
)

func TestXRainSmoke(t *testing.T) {
	initLogger(t)

	const Page = 0x100

	b := NewMemBack(0)
	l := NewFixedLayout(nil)
	l.SetKVSize(1, 7, 8, 1)

	db, err := NewDB(b, Page, l)
	assert.NoError(t, err)

	if tl.V("dump") != nil {
		off0, off1 := b.Access2(0, 0x40, Page, 0x40)
		tl.Printf("header pages 0, 1:\n%v%v", hex.Dump(off0), hex.Dump(off1))
		b.Unlock2(off0, off1)

		off0, off1 = b.Access2(2*Page, 0x40, 3*Page, 0x40)
		tl.Printf("header pages 2, 3:\n%v%v", hex.Dump(off0), hex.Dump(off1))
		b.Unlock2(off0, off1)

		tl.Printf("dump ver %x/%x  root %x  fnext %x\n%v\n%v", db.Ver, db.Keep, db.root, db.Freelist.(*Freelist3).next, db.Meta.Meta.Layout.(*SubpageLayout).dump(), db.l.(fileDumper).dumpFile())
	}

	err = db.Update(func(tx *Tx) error {
		return tx.Put([]byte("key_aaa"), []byte("value_aa"))
	})
	assert.NoError(t, err)

	if tl.V("dump") != nil {
		off0, off1 := b.Access2(0, 0x40, Page, 0x40)
		tl.Printf("header pages 0, 1:\n%v%v", hex.Dump(off0), hex.Dump(off1))
		b.Unlock2(off0, off1)

		off0, off1 = b.Access2(2*Page, 0x40, 3*Page, 0x40)
		tl.Printf("header pages 2, 3:\n%v%v", hex.Dump(off0), hex.Dump(off1))
		b.Unlock2(off0, off1)

		tl.Printf("dump ver %x/%x  root %x  fnext %x\n%v\n%v", db.Ver, db.Keep, db.root, db.Freelist.(*Freelist3).next, db.Meta.Meta.Layout.(*SubpageLayout).dump(), db.l.(fileDumper).dumpFile())
	}

	db, err = NewDB(b, 0, l)
	assert.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		v := tx.Get([]byte("key_aaa"))
		assert.Equal(t, []byte("value_aa"), v)
		return nil
	})
	assert.NoError(t, err)

	err = db.Update(func(tx *Tx) error {
		return tx.Del([]byte("key_aaa"))
	})
	assert.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		v := tx.Get([]byte("key_aaa"))
		assert.Equal(t, []byte(nil), v)
		return nil
	})
	assert.NoError(t, err)

	{
		off0, off1 := b.Access2(0, 0x40, Page, 0x40)
		tl.Printf("header pages 0, 1:\n%v%v", hex.Dump(off0), hex.Dump(off1))
		b.Unlock2(off0, off1)

		off0, off1 = b.Access2(2*Page, 0x40, 3*Page, 0x40)
		tl.Printf("header pages 2, 3:\n%v%v", hex.Dump(off0), hex.Dump(off1))
		b.Unlock2(off0, off1)

		//	tl.Printf("dump root %x free %x next %x\n%v", db.root, db.Freelist.(*Freelist2).t.Root, db.Freelist.(*Freelist2).next, db.l.(fileDumper).dumpFile())
	}
}

func TestXRainSmokeConcurrent(t *testing.T) {
	const Page = 0x100
	const N = 10

	b := NewMemBack(0)
	l := NewFixedLayout(nil)

	db, err := NewDB(b, Page, l)
	assert.NoError(t, err)

	//	tl.Printf("dump ver %x/%x  root %x  fnext %x\n%v\n%v", db.Ver, db.Keep, db.root, db.Freelist.(*Freelist3).next, db.Meta.Meta.Layout.(*SubpageLayout).dump(), db.l.(fileDumper).dumpFile())

	var wg sync.WaitGroup
	wg.Add(2 * N)

	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()

			err := db.Update(func(tx *Tx) error {
				val := []byte("value_aa")
				for j := 0; j < i; j++ {
					val[6]++
					val[7]++
				}
				return tx.Put([]byte("key_aaaa"), val)
			})
			assert.NoError(t, err)
		}(i)
	}

	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()

			err := db.View(func(tx *Tx) error {
				v := tx.Get([]byte("key_aaaa"))
				assert.True(t, v == nil || bytes.HasPrefix(v, []byte("value_")) && v[6] == v[7] && v[6] >= 'a' && v[6] < 'a'+N)
				return nil
			})
			assert.NoError(t, err)
		}()
	}

	wg.Wait()

	if t.Failed() {
		l, r := b.Access2(0, 0x40, Page, 0x40)
		tl.Printf("header pages:\n%v%v", hex.Dump(l), hex.Dump(r))
		b.Unlock2(l, r)
		l, r = b.Access2(2*Page, 0x40, 3*Page, 0x40)
		tl.Printf("header pages:\n%v%v", hex.Dump(l), hex.Dump(r))
		b.Unlock2(l, r)
		tl.Printf("dump root %x free %x next %x\n%v", db.root, db.Freelist.(*Freelist2).t.Root, db.Freelist.(*Freelist2).next, db.l.(fileDumper).dumpFile())

		t.Logf("back base addr %p", &b.d[0])
	}
}

func TestXRainHeavy(t *testing.T) {
	const (
		Page  = 0x1000
		Iters = 100
	)

	b := NewMemBack(0)

	db, err := NewDB(b, Page, nil)
	assert.NoError(t, err)

	ht := &HeavyTester{
		DB:      db,
		Workers: 4,
		Keys:    1,
		Iters:   Iters,
		Writes:  1,
		Reads:   6,
	}

	err = ht.Run()
	assert.NoError(t, err)

	t.Logf("db size: 0x%x", b.Size())

	if t.Failed() {
		t.Logf("back base addr %p", &b.d[0])

		l, r := b.Access2(0, 0x40, Page, 0x40)
		tl.Printf("header pages:\n%v%v", hex.Dump(l), hex.Dump(r))
		b.Unlock2(l, r)
		l, r = b.Access2(2*Page, 0x40, 3*Page, 0x40)
		tl.Printf("header pages:\n%v%v", hex.Dump(l), hex.Dump(r))
		b.Unlock2(l, r)
		tl.Printf("dump ver %d root %x (%d) free %x next %x\n%v", db.Ver, db.root, 0, db.Freelist.(*Freelist2).t.Root, db.Freelist.(*Freelist2).next, db.l.(fileDumper).dumpFile())
	}
}

func BenchmarkXRainFixed(b *testing.B) {
	b.ReportAllocs()

	initLogger(b)

	bk := NewMemBack(0)

	defer func() {
		f, err := os.Create("/tmp/xrain_fixed_bench.xrain")
		if err != nil {
			tlog.Fatalf("open: %v", err)
		}

		f.Write(bk.Bytes())
	}()

	l := NewFixedLayout(nil)
	l.SetKVSize(1, 7, 8, 1)

	db, err := NewDB(bk, 0, l)
	require.NoError(b, err)

	bucket := []byte("bucket0")
	k := []byte("key_000")
	v := []byte("value_00")

	for i := 0; i < b.N; i++ {
		err = db.Update(func(tx *Tx) error {
			b, err := tx.PutBucket(bucket)
			if err != nil {
				return err
			}

			tokey(k, i)

			return b.Put(k, v)
		})

		if err != nil {
			b.Errorf("update: %v", err)
			break
		}
	}
}

func BenchmarkXRainKV(b *testing.B) {
	b.ReportAllocs()

	initLogger(b)

	bk := NewMemBack(0)

	defer func() {
		f, err := os.Create("/tmp/xrain_kv_bench.xrain")
		if err != nil {
			tl.Fatalf("open: %v", err)
		}

		n, err := f.Write(bk.Bytes())
		if err != nil {
			tl.Fatalf("write: %v", err)
		}

		tl.Printf("db written: %v   (%x/%d bytes)", f.Name(), n, n)
	}()

	l := NewKVLayout2(nil)

	db, err := NewDB(bk, 0, l)
	require.NoError(b, err)

	bucket := []byte("bucket0")
	k := []byte("key_000")
	v := []byte("value_00")

	var i int
	for i = 0; i < b.N; i++ {
		err = db.Update(func(tx *Tx) error {
			b, err := tx.PutBucket(bucket)
			if err != nil {
				return err
			}

			tokey(k, i)

			return b.Put(k, v)
		})

		if err != nil {
			tl.Printf("update: %v", err)
			b.Fail()
			break
		}
	}

	tl.Printf("%x (%d) keys written", i, i)
	tl.Printf("db %v", DumpDB(db))
}

func tokey(k []byte, i int) {
	l := len(k) - 1
	for i != 0 {
		k[l] = "0123456789abcdef"[i&0xf]
		l--
		i >>= 4
	}
}

func (t *HeavyTester) Run() error {
	c := make(chan HeavyTask, 1)
	r := make(chan HeavyTask, t.Workers)

	logs := make([]HeavyTask, 0, 2*t.Iters)

	for i := 0; i < t.Workers; i++ {
		go t.worker(c, r)
	}

	for i := 0; i < t.Iters; i++ {
		task := HeavyTask{
			ID: i,
		}

		if rand.Float64() < t.Reads/(t.Writes+t.Reads) {
			task.TaskType = TaskGet
		} else {
			task.TaskType = TaskPut
		}

		task.Key = make([]byte, 8)
		binary.BigEndian.PutUint64(task.Key, uint64(rand.Intn(t.Keys)))

		if task.TaskType == TaskPut {
			task.Value = make([]byte, 8)
			binary.BigEndian.PutUint64(task.Value, uint64(i))
		}

	loop:
		select {
		case res := <-r:
			res.Intent = false
			logs = append(logs, res)
			goto loop
		default:
		}

		task.Intent = true
		logs = append(logs, task)
		c <- task
	}
	close(c)

	for len(logs) < 2*t.Iters {
		res := <-r
		res.Intent = false
		logs = append(logs, res)
	}

	return t.analyze(logs)
}

func (t *HeavyTester) analyze(logs []HeavyTask) error {
	keys := map[uint64][]HeavyTask{}

	for _, w := range logs {
		k := binary.BigEndian.Uint64(w.Key)
		keys[k] = append(keys[k], w)
	}

	for _, logs := range keys {
		err := t.analyzeKey(logs)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *HeavyTester) analyzeKey(logs []HeavyTask) error {
	//	vals := map[int64]struct{}{}
	//	var explain []HeavyTask
	for i := len(logs) - 1; i >= 0; i-- {
		//	w := logs[i]
		// for now hope for the race detector :)
	}

	//	t.print(logs)

	return nil
}

func (t *HeavyTester) print(logs []HeavyTask) {
	for i, w := range logs {
		k := binary.BigEndian.Uint64(w.Key)
		var op byte
		if w.Intent {
			op = 'i'
		} else {
			op = 'F'
		}
		var v uint64
		var tp byte
		if w.TaskType == TaskPut {
			tp = 'W'
			v = binary.BigEndian.Uint64(w.Value)
		} else {
			if w.Res != nil {
				tp = 'R'
				v = binary.BigEndian.Uint64(w.Res)
			} else {
				tp = 'r'
			}
		}

		if tp == 'r' {
			log.Printf("log[%5d] %4x %c %c key %4x", i, w.ID, op, tp, k)
		} else {
			log.Printf("log[%5d] %4x %c %c key %4x -> %4x", i, w.ID, op, tp, k, v)
		}
	}
}

func (t *HeavyTester) worker(c, r chan HeavyTask) {
	for w := range c {
		switch w.TaskType {
		case TaskGet:
			w.Err = t.DB.View(func(tx *Tx) error {
				w.Res = tx.Get(w.Key) //nolint:scopelint
				//	log.Printf("get %x -> %x", w.Key, w.Value)
				return nil
			})
		case TaskPut:
			w.Err = t.DB.Update(func(tx *Tx) error {
				//	log.Printf("put %x -> %x", w.Key, w.Value)
				return tx.Put(w.Key, w.Value) //nolint:scopelint
			})
		default:
			panic(w)
		}

		r <- w
	}
}

var (
	flagv    = flag.String("tlog-v", "", "verbocity topics")
	tostderr = flag.Bool("tlog-to-stderr", false, "log to stderr, not in testing.T")

//	det = flag.Bool("detailed", false, "detailed logs")
//	no  = flag.Bool("no-logs", false, "hide logs")
)

func TestMain(m *testing.M) {
	flag.Parse()

	initLogger(nil)

	os.Exit(m.Run())
}

func initLogger(t testing.TB) {
	var w io.Writer
	if *tostderr {
		w = tlog.Stderr
	}

	tl = tlog.NewTestLogger(t, *flagv, w)
}
