package xrain

func (tx *Tx) Put(k, v []byte) error {
	if !tx.writable {
		panic("not writable")
	}
	_, err := tx.t.Put(k, v)
	return err
}

func (tx *Tx) Get(k []byte) []byte {
	return tx.t.Get(k)
}

func (tx *Tx) Del(k []byte) error {
	_, err := tx.t.Del(k)
	return err
}
