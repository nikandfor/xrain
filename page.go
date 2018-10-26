package xrain

const NilPage = -1

type (
	PageLayout interface {
		PageSize() int64

		Size(p int64) int
		IsLeaf(p int64) bool

		KeyCmp(p int64, i int, k []byte) int
		LastKey(p int64) []byte

		Int64(p int64, i int) int64
		Value(p int64, i int) ([]byte, error)

		Del(p int64, i int) (int64, error)
		Put(p int64, i int, k, v []byte) (l, r int64, _ error)
		PutInt64(p int64, i int, k []byte, v int64) (l, r int64, _ error)

		NeedRebalance(p int64) bool
		Siblings(p int64, i int) (li int, l, r int64, _ error)
		Rebalance(l, r int64) (l_, r_ int64, _ error)
	}
)
