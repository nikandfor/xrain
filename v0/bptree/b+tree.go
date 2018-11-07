package bptree

import (
	"bytes"
	"sort"
)

const (
	MaxEls = 128
	MinEls = (MaxEls + 1) / 2
)

type (
	KeyValue struct {
		Key   []byte
		Value []byte
	}
	KeyLink struct {
		Key  []byte
		Link Page
	}

	Tree struct {
		n interface{}
	}

	Leaf struct {
		kv []KeyValue
	}
	Branch struct {
		ks []KeyLink
	}
)

func (n *Leaf) Get(x []byte) []byte {
	i := sort.Search(len(n.kv), func(i int) bool { return n.kv[i].Key >= x })
	if i == len(n.kv) || !bytes.Equal(n.kv[i].Key, x) {
		return nil
	}
	return n.kv[i].Value
}

func (n *Leaf) Put(k, v []byte) {
	i := sort.Search(len(n.kv), func(i int) bool { return n.kv[i].Key >= x })
	if i < len(n.kv) && bytes.Equal(n.kv[i].Key, k) {
		n.kv[i].Value = v
		return
	}
	if len(n.kv) == MaxEls {
	}
}
