package xrain

import (
	"fmt"
	"io"
)

func DebugDump(w io.Writer, bk *SimpleBucket) {
	debugDump(w, 0, bk)

	//	tlog.Printf("dump root\n%v", bk.t.Layout.(*SubpageLayout).dump())
	//	tlog.Printf("dump pages\n%v", NewKVLayout2(&bk.tx.d.Meta).dumpFile())
}

func debugDump(w io.Writer, d int, bk *SimpleBucket) {
	const pad = "                                                              "
	var b []byte

	t := bk.Tree()

	for st := t.First(nil); st != nil; st = t.Next(st) {
		b, ff := t.Key(st, b[:0])

		if ff == 1 {
			fmt.Fprintf(w, "%v%16.16x  |  %-40.40q (%4d) st %v (depth %v)--->\n", pad[:d*4], b, b, len(b), st, d)

			debugDump(w, d+1, bk.Bucket(b))
		} else {
			kl := len(b)
			b = t.Value(st, b)

			fmt.Fprintf(w, "%v%16.16x -> %16.16x  |  %-40.40q (%4d) -> %.40q (%4d)  st %v\n", pad[:d*4], b[:kl], b[kl:], b[:kl], kl, b[kl:], len(b)-kl, st)
		}
	}
}
