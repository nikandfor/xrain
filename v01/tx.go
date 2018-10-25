package xrain

import "errors"

func (tx *Tx) Put(k, v []byte) {
	if !tx.writable {
		tx.err = errors.New("tx is not writable")
		return
	}

	tx.t.Put(k, v)
}

func (tx *Tx) Get(k []byte) []byte {
	return tx.t.Get(k)
}

func (tx *Tx) Next(k []byte) []byte {
	return tx.t.Next(k)
}

func (tx *Tx) Prev(k []byte) []byte {
	return tx.t.Prev(k)
}
