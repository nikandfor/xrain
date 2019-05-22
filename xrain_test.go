package xrain

import (
	"encoding/binary"
	"encoding/hex"
	"log"
	"math/rand"
	"testing"

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

const (
	TaskGet TaskType = iota != 0
	TaskPut
)

func TestXRainSmoke(t *testing.T) {
	const Page = 0x100

	b := NewMemBack(0)
	pl := NewFixedLayout(b, Page, nil)
	fl := NewFreelist2(b, NewTree(pl, 2*Page, Page), 4*Page, Page)
	pl.SetFreelist(fl)

	db, err := NewDB(b, &Config{
		PageSize: Page,
		Freelist: fl,
		Tree:     NewTree(pl, 3*Page, Page),
	})
	assert.NoError(t, err)

	err = db.Update(func(tx *Tx) error {
		return tx.Put([]byte("key_aaaa"), []byte("value_aa"))
	})
	assert.NoError(t, err)

	//	b.Access2(0, 0x40, Page, 0x40, func(l, r []byte) {
	//		log.Printf("header pages:\n%v%v", hex.Dump(l), hex.Dump(r))
	//	})
	//	log.Printf("dump root %x free %x next %x\n%v", db.last, db.fl.(*Freelist2).t.Root(), db.fl.(*Freelist2).next, dumpFile(pl))

	db, err = NewDB(b, nil)
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
	log.Printf("header pages:\n%v%v", hex.Dump(l), hex.Dump(r))
	b.Unlock2(l, r)
	log.Printf("dump root %x free %x next %x\n%v", db.t.Root(), db.fl.(*Freelist2).t.Root(), db.fl.(*Freelist2).next, dumpFile(pl))
}

func TestXRainHeavy(t *testing.T) {
	const (
		Page  = 0x100
		Iters = 100
	)

	b := NewMemBack(0)

	db, err := NewDB(b, &Config{PageSize: Page})
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

	//	pl := NewFixedLayout(b, Page, nil)
	//	b.Access2(0, 0x40, Page, 0x40, func(l, r []byte) {
	//		log.Printf("header pages:\n%v%v", hex.Dump(l), hex.Dump(r))
	//	})
	//	log.Printf("dump root %x (%d) free %x next %x\n%v", db.last, db.tr.Size(), db.fl.(*Freelist2).t.Root(), db.fl.(*Freelist2).next, dumpFile(pl))

	t.Logf("db size: 0x%x", b.Size())
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
		for {
			select {
			case res := <-r:
				res.Intent = false
				logs = append(logs, res)
			default:
				break loop
			}
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
