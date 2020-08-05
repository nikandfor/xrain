package benchmarks

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/nikandfor/tlog"
	"github.com/nikandfor/xrain"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func BenchmarkXRain(b *testing.B) {
	b.ReportAllocs()

	tl := xrain.InitTestLogger(b, *flagv, *tostderr)

	f, err := ioutil.TempFile("", "bench_xrain_*.xrain")
	require.NoError(b, err)
	defer func() {
		p := recover()
		if p != nil {
			b.Fail()

			panic(p)
		}

		if b.Failed() {
			return
		}

		err = os.Remove(f.Name())
		if err != nil {
			tlog.Printf("remove: %v", err)
		}
	}()

	tl.Printf("tmp file: %v", f.Name())

	bk := xrain.MmapFile(f)
	defer bk.Close()

	l := xrain.NewFixedLayout(nil)
	l.SetKVSize(1, 7, 8, 1)

	db, err := xrain.NewDB(bk, 0, l)
	require.NoError(b, err)

	k := []byte("key_000")

	for i := 0; i < b.N; i++ {
		err = db.Update(func(tx *xrain.Tx) error {
			b, err := tx.PutBucket([]byte("bucket0"))
			if err != nil {
				return err
			}

			kn := fmt.Sprintf("%03x", i)
			copy(k[len(k)-len(kn):], kn)

			return b.Put(k, []byte("value_00"))
		})

		if err != nil {
			b.Errorf("update: %v", err)
			break
		}
	}
}

func BenchmarkBBolt(b *testing.B) {
	b.ReportAllocs()

	tl := xrain.InitTestLogger(b, *flagv, *tostderr)

	f, err := ioutil.TempFile("", "bench_xrain_*.xrain")
	require.NoError(b, err)

	fname := f.Name()

	err = f.Close()
	require.NoError(b, err)

	defer func() {
		p := recover()
		if p != nil {
			b.Fail()

			panic(p)
		}

		if b.Failed() {
			return
		}

		err = os.Remove(fname)
		require.NoError(b, err)
	}()

	tl.Printf("tmp file: %v", fname)

	db, err := bbolt.Open(fname, 0644, nil)
	require.NoError(b, err)

	defer db.Close()

	k := []byte("key_000")

	for i := 0; i < b.N; i++ {
		err = db.Update(func(tx *bbolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("bucket0"))
			if err != nil {
				return err
			}

			kn := fmt.Sprintf("%03x", i)
			copy(k[len(k)-len(kn):], kn)

			return b.Put(k, []byte("value_00"))
		})

		if err != nil {
			b.Errorf("update: %v", err)
			break
		}
	}
}

var (
	flagv    = flag.String("tlog-v", "", "verbocity topics")
	tostderr = flag.Bool("tlog-to-stderr", false, "log to stderr, not in testing.T")
)
