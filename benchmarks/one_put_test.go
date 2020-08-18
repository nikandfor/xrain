// +build linux darwin

package benchmarks

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/nikandfor/tlog"
	"github.com/nikandfor/xrain"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func BenchmarkShortXRain(b *testing.B) {
	benchmarkXRain(b, []byte("value_00"))
}

func BenchmarkShortBBolt(b *testing.B) {
	benchmarkBBolt(b, []byte("value_00"))
}

func BenchmarkMiddleXRain(b *testing.B) {
	benchmarkXRain(b, longval(500, "value_00"))
}

func BenchmarkMiddleBBolt(b *testing.B) {
	benchmarkBBolt(b, longval(500, "value_00"))
}

func BenchmarkLargeXRain(b *testing.B) {
	//	b.Skip()
	benchmarkXRain(b, longval(100*xrain.KB, "value_00"))
}

func BenchmarkLargeBBolt(b *testing.B) {
	benchmarkBBolt(b, longval(100*xrain.KB, "value_00"))
}

func benchmarkXRain(b *testing.B, v []byte) {
	b.ReportAllocs()

	var tl *tlog.Logger
	//	tl = xrain.InitTestLogger(b, *flagv, *tostderr)

	//	f, err := ioutil.TempFile(".", "bench_xrain_*.xrain")
	//	f, err := os.Create(fmt.Sprintf("./bench_xrain_%06x.xrain", len(v)))
	f, err := file("xrain", len(v))
	require.NoError(b, err)
	defer func() {
		if b.Failed() {
			tl.Printf("tmp file: %v", f.Name())
			return
		}

		tl.Printf("tmp file: %v", f.Name())

		//	err = os.Remove(f.Name())
		//	require.NoError(b, err)
	}()

	bk, err := xrain.MmapFile(f, true)
	require.NoError(b, err)
	defer bk.Close()

	l := xrain.NewKVLayout2(nil)

	db, err := xrain.NewDB(bk, 0, l)
	require.NoError(b, err)

	bn := []byte("bucket0")
	k := []byte("key_000")

	for i := 0; i < b.N; i++ {
		err = db.Update(func(tx *xrain.Tx) error {
			b, err := tx.PutBucket(bn)
			if err != nil {
				return err
			}

			tokey(k, i)

			return b.Put(k, v)
		})

		if err != nil {
			b.Errorf("update: %v", err)
			break
		}
	}
}

func benchmarkBBolt(b *testing.B, v []byte) {
	b.ReportAllocs()

	var tl *tlog.Logger
	//	tl = xrain.InitTestLogger(b, *flagv, *tostderr)

	//	f, err := ioutil.TempFile(".", "bench_xrain_*.bbolt")
	//	f, err := os.Create(fmt.Sprintf("./bench_xrain_%06x.bbolt", len(v)))
	f, err := file("bbolt", len(v))
	require.NoError(b, err)

	fname := f.Name()

	err = f.Close()
	require.NoError(b, err)

	defer func() {
		if b.Failed() {
			tl.Printf("tmp file: %v", f.Name())
			return
		}

		//	err = os.Remove(fname)
		//	require.NoError(b, err)
	}()

	db, err := bbolt.Open(fname, 0644, nil)
	require.NoError(b, err)

	defer db.Close()

	bn := []byte("bucket0")
	k := []byte("key_000")

	for i := 0; i < b.N; i++ {
		err = db.Update(func(tx *bbolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists(bn)
			if err != nil {
				return err
			}

			tokey(k, i)

			return b.Put(k, v)
		})

		if err != nil {
			b.Errorf("update: %v", err)
			break
		}
	}
}

func file(n string, d int) (f *os.File, err error) {
	fn := fmt.Sprintf("bench_%06x.%v", d, n)

	f, err = os.Create(fn)

	return
}

func longval(l int, v string) (r []byte) {
	r = make([]byte, l)
	copy(r, v)
	return
}

func tokey(k []byte, i int) {
	l := len(k) - 1
	for i != 0 {
		k[l] = "0123456789abcdef"[i&0xf]
		l--
		i >>= 4
	}
}

var (
	flagv    = flag.String("tlog-v", "", "verbocity topics")
	tostderr = flag.Bool("tlog-to-stderr", false, "log to stderr, not in testing.T")
)
