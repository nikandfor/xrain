package xrain

import (
	"sync"
)

type Batcher struct {
	l      sync.Locker
	cond   sync.Cond
	batch  int
	flushc chan struct{}
	stopc  chan struct{}
	sync   func() error
	err    error
}

func NewBatcher(l sync.Locker, s func() error) *Batcher {
	b := &Batcher{
		l:      l,
		flushc: make(chan struct{}, 1),
		sync:   s,
	}
	b.cond.L = l
	return b
}

func (b *Batcher) Run() error {
	b.stopc = make(chan struct{})

loop:
	for {
		select {
		case <-b.stopc:
			break loop
		case <-b.flushc:
		}

		b.l.Lock()
		b.batch++
		b.l.Unlock()

		err := b.sync()

		b.l.Lock()
		b.batch++
		b.err = err
		b.cond.Broadcast()
		b.l.Unlock()

		if err != nil {
			break
		}
	}

	return b.err // it's ok to read it without mutex here. We are the only routine can write it
}

func (b *Batcher) Err() error {
	defer b.l.Unlock()
	b.l.Lock()

	return b.err
}

func (b *Batcher) Lock() int {
	b.l.Lock()

	select {
	case b.flushc <- struct{}{}:
	default:
	}

	return b.batch + 1
}

func (b *Batcher) Wait(bt int) error {
	for bt >= b.batch { // wait for batch to full
		b.cond.Wait()
	}

	return b.err
}

func (b *Batcher) Unlock() {
	b.l.Unlock()
}

func (b *Batcher) Stop() {
	close(b.stopc)
}
