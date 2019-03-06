package xrain

func (tx *Tx) Put(k, v []byte) error {
	if !tx.writable {
		panic("not writable")
	}
	return tx.t.Put(k, v)
}

func (tx *Tx) Get(k []byte) []byte {
	return tx.t.Get(k)
}

func (tx *Tx) Del(k []byte) error {
	return tx.t.Del(k)
}
