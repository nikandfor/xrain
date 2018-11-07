package xrain

type (
	tx struct {
		db   *DB
		par  *tx
		root int64
		up   []seg
	}
)

func (b bucket) Get(k []byte) []byte {
}

func (b bucket) Put(k, v []byte) {
}

func (b bucket) Del(k []byte) {
}
