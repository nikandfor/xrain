package xrain

import "sync"

// db structure
// DB interface -> B+tree -> MultiLayer -> File

type (
	Cursor interface {
	}

	Bucket interface {
		Get(k []byte) []byte
		Put(k, v []byte) error
		Del(k []byte) error
	}

	Tx interface {
	}

	Callback func(Tx) error

	File interface {
	}

	DB struct {
		SyncAtWrite bool

		mu          sync.Mutex
		root        int64
		ver, minVer int64
		readers     readers

		wmu sync.Mutex
	}
)

func (d *DB) View(c Callback) error {
	d.mu.Lock()

	ver := d.ver
	link := d.rootPage()
	reader := d.readers.Start(ver)

	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		d.minVer = reader.Finish()
		d.mu.Unlock()
	}()

	b := bucket{
		db:   d,
		link: link,
	}

	err := c(b)

	return err
}

func (d *DB) Update(c Callback) error {

}

func (d *DB) Close() error {}
