package xrain

import "sync"

type (
	Tx interface {
	}

	tx struct {
		writable bool
		db       *DB

		root int64
		ver  int64

		mem Memory
	}

	Memory interface {
		Read()
		Write()
		Alloc()

		Commit()
		Abort()
	}

	cursor struct {
		depth int
		pages [10]int64
		more  []int64
	}

	DB struct {
		file File

		root int64
		ver  int64

		wmu sync.Mutex
	}
)

func (d *DB) View(cb func(Tx) error) error {
	return nil
}

func (d *DB) Update(cb func(Tx) error) error {
	defer d.wmu.Unlock()
	d.wmu.Lock()

	head := headPage(d.mem.Read(d.root, d.pagesize))

	t := &tx{
		writable: true,
		root:     haed.data,
		ver:      d.ver,
	}

	err := cb(t)

	if err != nil {
		//	_ = t.Abort()
		return err
	}

	//	err = t.Commit()

	return err
}

func (t *tx) Set(k, v []byte) {
	c := &cursor{}
	pg := t.root

	for !pg.IsFlag(FlagIsLeaf) {
		var link int64
		// search key
		pg = Page(mem.Read(link, t.db.pagesize))
	}

	var i int
	// search key

	// if it's equal return

	// if we can update inplace
	np := Page(t.mem.Alloc(t.db.pagesize))
	// copy
	// set value

	for c.depth > 0 {
		// update branch page
	}
}

func (t *tx) Cursor() Cursor {
	return t.c
}

func (t *tx) Get(k []byte) []byte {
	return nil
}

func (t *tx) commit() error {
	return t.mem.Commit()
}
