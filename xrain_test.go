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

func init() {
	tlog.DefaultLogger = tlog.New(tlog.NewConsoleWriter(os.Stderr, tlog.LdetFlags))
}

const (
	TaskGet TaskType = iota != 0
	TaskPut
)

func TestXRainSmoke(t *testing.T) {
	const Page = 0x100

	b := NewMemBack(0)
	pl := NewFixedLayout(b, Page, nil)

	db, err := NewDB(b, Page, pl)
	assert.NoError(t, err)

	err = db.Update(func(tx *Tx) error {
		return tx.Put([]byte("key_aaaa"), []byte("value_aa"))
	})
	assert.NoError(t, err)

	if false {
		l, r := b.Access2(0, 0x40, Page, 0x40)
		tlog.Printf("header pages:\n%v%v", hex.Dump(l), hex.Dump(r))
		b.Unlock2(l, r)
		tlog.Printf("dump root %x free %x next %x\n%v", db.t.Root(), db.fl.(*Freelist2).t.Root(), db.fl.(*Freelist2).next, dumpFile(pl))
	}

	db, err = NewDB(b, 0, pl)
	assert.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		v := tx.Get([]byte("key_aaaa"))
		assert.Equal(t, []byte("value_aa"), v)
		return nil
	})
	assert.NoError(t, err)

	err = db.Update(func(tx *Tx) error {
		return tx.Del([]byte("key_aaaa"))
	})
	assert.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		v := tx.Get([]byte("key_aaaa"))
		assert.Equal(t, []byte(nil), v)
		return nil
	})
	assert.NoError(t, err)

	l, r := b.Access2(0, 0x40, Page, 0x40)
	tlog.Printf("header pages:\n%v%v", hex.Dump(l), hex.Dump(r))
	b.Unlock2(l, r)
	tlog.Printf("dump root %x free %x next %x\n%v", db.t.Root(), db.fl.(*Freelist2).t.Root(), db.fl.(*Freelist2).next, dumpFile(pl))
}

func TestXRainSmokeConcurrent(t *testing.T) {
	const Page = 0x100
	const N = 10

	b := NewMemBack(0)
	pl := NewFixedLayout(b, Page, nil)

	db, err := NewDB(b, Page, pl)
	assert.NoError(t, err)

	tlog.Printf("dump root %x free %x next %x\n%v", db.t.Root(), db.fl.(*Freelist2).t.Root(), db.fl.(*Freelist2).next, dumpFile(pl))

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
		tlog.Printf("header pages:\n%v%v", hex.Dump(l), hex.Dump(r))
		b.Unlock2(l, r)
		l, r = b.Access2(2*Page, 0x40, 3*Page, 0x40)
		tlog.Printf("header pages:\n%v%v", hex.Dump(l), hex.Dump(r))
		b.Unlock2(l, r)
		tlog.Printf("dump root %x free %x next %x\n%v", db.t.Root(), db.fl.(*Freelist2).t.Root(), db.fl.(*Freelist2).next, dumpFile(pl))

		t.Logf("back base addr %p", &b.d[0])
	}
}

func TestXRainHeavy(t *testing.T) {
	const (
		Page  = 0x100
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
		tlog.Printf("header pages:\n%v%v", hex.Dump(l), hex.Dump(r))
		b.Unlock2(l, r)
		l, r = b.Access2(2*Page, 0x40, 3*Page, 0x40)
		tlog.Printf("header pages:\n%v%v", hex.Dump(l), hex.Dump(r))
		b.Unlock2(l, r)
		pl := NewFixedLayout(b, Page, nil)
		tlog.Printf("dump ver %d root %x (%d) free %x next %x\n%v", db.ver, db.t.Root(), db.t.Size(), db.fl.(*Freelist2).t.Root(), db.fl.(*Freelist2).next, dumpFile(pl))
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
	v = flag.String("tlog-v", "", "verbocity topics")

//	det = flag.Bool("detailed", false, "detailed logs")
//	no  = flag.Bool("no-logs", false, "hide logs")
)

var tl *tlog.Logger

func TestMain(m *testing.M) {
	flag.Parse()

	initLogger(nil)

	os.Exit(m.Run())
}

type testWriter struct {
	t *testing.T
}

func (t testWriter) Write(p []byte) (int, error) {
	t.t.Logf("%s", p)

	return len(p), nil
}

func initLogger(t *testing.T) {
	var w io.Writer = log.Writer()

	if t != nil {
		w = testWriter{t: t}
	}

	tl = tlog.New(tlog.NewConsoleWriter(w, 0))

	if *v != "" {
		tlog.SetFilter(*v)
	}
}
