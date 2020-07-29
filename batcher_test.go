package xrain

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nikandfor/tlog"
)

type A struct {
	mu     sync.Mutex
	cond   sync.Cond
	batch  int
	flushc chan struct{}
	stopc  chan struct{}

	list []string

	f *os.File
	l *tlog.Logger
}

func NewA() *A {
	f, err := os.Create("batcher.json")
	if err != nil {
		panic(err)
	}

	a := &A{
		flushc: make(chan struct{}, 1),
		stopc:  make(chan struct{}),
		f:      f,
		l:      tlog.New(tlog.NewJSONWriter(f)),
	}
	a.cond.L = &a.mu
	return a
}

func (a *A) stop() {
	close(a.stopc)
}

func (a *A) Close() {
	err := a.f.Close()
	if err != nil {
		panic(err)
	}
}

func (a *A) f1(j, i int) {
	tr := a.l.Start()
	defer tr.Finish()

	a.mu.Lock()
	tr.Printf("locked")

	a.work(j, i)

	tr.Printf("work've done")

	a.sync(-1, []string{fmt.Sprintf("%d %d", j, i)})

	tr.Printf("go out")

	a.mu.Unlock()
}

func (a *A) f2(j, i int) int {
	tr := a.l.Start()
	defer tr.Finish()

	select {
	case a.flushc <- struct{}{}:
	default:
	}

	tr.Printf("notified")

	a.mu.Lock()
	tr.Printf("locked")

	b := a.batch + 1

	a.work(j, i)

	tr.Printf("work've done %v / %d %d", b, j, i)
	for b >= a.batch { // wait for batch to full
		a.cond.Wait()
	}
	tr.Printf("go out")

	a.mu.Unlock()

	return b
}

func (a *A) work(j, i int) {
	//	tlog.Printf("work %2d %2d", j, i)
	a.list = append(a.list, fmt.Sprintf("%d %d", j, i))
	time.Sleep(time.Millisecond)
}
func (a *A) sync(bt int, list []string) error {
	time.Sleep(10 * time.Millisecond)
	//	tlog.Printf("sync %d %v", bt, list)
	return nil
}

func (a *A) flusher() {
	for {
		tr := a.l.Start()

		select {
		case <-a.stopc:
			tr.Finish()
			return
		case <-a.flushc:
		}
		tr.Printf("go")

		a.mu.Lock()
		bt := a.batch
		a.batch++
		list := a.list
		a.list = nil
		a.mu.Unlock()

		tr.Printf("inc1 %d", bt)

		a.sync(bt, list)

		tr.Printf("sync")

		a.mu.Lock()
		a.batch++
		a.cond.Broadcast()
		a.mu.Unlock()

		tr.Printf("inc2")

		tr.Finish()
	}
}

const M = 10

func BenchmarkBatcherNoSync(t *testing.B) {
	a := NewA()

	var wg sync.WaitGroup
	wg.Add(M)
	for j := 0; j < M; j++ {
		go func(j int) {
			for i := 0; i < t.N; i++ {
				a.f1(j, i)
			}
			wg.Done()
		}(j)
	}
	wg.Wait()

	a.Close()
}

func BenchmarkBatcherFastA(t *testing.B) {
	a := NewA()

	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() {
		a.flusher()
		wg2.Done()
	}()

	var wg sync.WaitGroup
	wg.Add(M)
	for j := 0; j < M; j++ {
		go func(j int) {
			for i := 0; i < t.N; i++ {
				bt := a.f2(j, i)
				_ = bt
				//	tlog.Printf("res  %2d %2d  <- %d", j, i, bt)
			}
			wg.Done()
		}(j)
	}
	wg.Wait()

	a.stop()

	wg2.Wait()

	a.Close()
}

func BenchmarkBatcherFastAim(t *testing.B) {
	a := NewA()

	var wg sync.WaitGroup

	for i := 0; i < t.N; i++ {
		wg.Add(1 + 1)

		go func() {
			a.sync(-1, nil)
			wg.Done()
		}()

		go func(i int) {
			for j := 0; j < M; j++ {
				a.work(j, i)
			}
			wg.Done()
		}(i)

		wg.Wait()
	}

	a.Close()
}

func BenchmarkBatcherFastStartFinish(t *testing.B) {
	a := &A{}
	b := NewBatcher(&a.mu, func() error { return a.sync(-1, nil) })

	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() {
		b.Run()
		wg2.Done()
	}()

	var wg sync.WaitGroup
	wg.Add(M)
	for j := 0; j < M; j++ {
		go func(j int) {
			for i := 0; i < t.N; i++ {
				bt := b.Lock()
				a.work(j, i)
				b.Wait(bt)
				b.Unlock()
			}
			wg.Done()
		}(j)
	}
	wg.Wait()

	b.Stop()

	wg2.Wait()
}
