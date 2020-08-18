// +build linux darwin

package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"

	"github.com/nikandfor/cli"
	"github.com/nikandfor/tlog"
	"github.com/nikandfor/xrain"
)

func main() {
	cli.App = cli.Command{
		Name:   "xrain",
		Before: before,
		Flags: []*cli.Flag{
			cli.NewFlag("verbocity,v", "", "tlog verbocity topics"),
			cli.NewFlag("detailed,vv", false, "detailed log"),
			cli.HelpFlag,
			cli.FlagfileFlag,
		},
		Commands: []*cli.Command{{
			Name:   "dump",
			Action: dump,
			Flags: []*cli.Flag{
				cli.NewFlag("layout,l", "kv", ""),
				cli.NewFlag("file,f", "", ""),
				cli.NewFlag("pad,p", "    ", "pad subbuckets with given string"),
				cli.NewFlag("flags", false, ""),
				cli.NewFlag("key", true, ""),
				cli.NewFlag("value", true, ""),
				cli.NewFlag("stack,st", false, ""),
				cli.NewFlag("quote,q", false, ""),
				cli.NewFlag("quote-key,qk", false, ""),
				cli.NewFlag("quote-value,qk", false, ""),
			},
			Commands: []*cli.Command{{
				Name:   "freelist",
				Action: dumpfreelist,
			}, {
				Name:   "pages",
				Action: dumppages,
			}},
		}, {
			Name:   "stats",
			Action: stats,
			Flags: []*cli.Flag{
				cli.NewFlag("file,f", "", ""),
			},
		}},
	}

	cli.RunAndExit(os.Args)
}

func before(c *cli.Command) error {
	if c.Bool("vv") {
		tlog.DefaultLogger = tlog.New(tlog.NewConsoleWriter(tlog.Stderr, tlog.LdetFlags))
	}

	tlog.SetFilter(c.String("v"))

	return nil
}

func stats(c *cli.Command) (err error) {
	bk, err := xrain.Mmap(c.String("file"), os.O_RDONLY)
	if err != nil {
		return
	}
	defer bk.Close()

	db, err := xrain.NewDB(bk, 0, nil)
	if err != nil {
		return
	}

	m := &db.Meta

	for st, _ := m.Meta.Seek([]byte("stats."), nil, nil); st != nil; st = m.Meta.Step(st, 0, false) {
		k, _ := m.Meta.Layout.Key(st, nil)
		if !bytes.HasPrefix(k, []byte("stats.")) {
			break
		}

		v := m.Meta.Layout.Int64(st)

		fmt.Printf("%-30s  %6d  / %6x (hex)\n", k, v, v)
	}

	return nil
}

func dumppages(c *cli.Command) (err error) {
	//	xrain.InitTestLogger(nil, c.String("v"), true)

	bk, err := xrain.Mmap(c.String("file"), os.O_RDONLY)
	if err != nil {
		return
	}
	defer bk.Close()

	db, err := xrain.NewDB(bk, 0, nil)
	if err != nil {
		return
	}

	m := &db.Meta

	err = db.View(func(tx *xrain.Tx) error {
		return printPages(c, m, tx.SimpleBucket, 0)
	})

	return
}

func printPages(c *cli.Command, m *xrain.Meta, b *xrain.SimpleBucket, d int) (err error) {
	var last []int64
	l := b.Layout()
	t := b.Tree()
	mask := m.Mask

	var k []byte
	var ff int

	for st := t.First(nil); st != nil; st = t.Next(st) {
		k, ff = t.Key(st, k[:0])

		if ff == 1 {
			sub := b.Bucket(k)

			err = printPages(c, m, sub, d+1)
			if err != nil {
				return
			}
		}
	}

	for st := t.Last(nil); st != nil; st = t.Prev(st) {
		k, ff = t.Key(st, k[:0])

		if last == nil {
			last = make([]int64, len(st))
			for j := range st {
				last[j] = xrain.NilPage
			}
		}

		for j := len(st) - 1; j >= 0; j-- {
			off, _ := st[j].OffIndex(mask)

			if last[j] == off {
				break
			}

			//	fmt.Printf("root %6x  page %6x  n %3x\n", t.Root, off, i+1)
			fmt.Printf("%v", xrain.DumpPage(l, off))

			last[j] = off
		}
	}

	return nil
}

func dumpfreelist(c *cli.Command) error {
	bk, err := xrain.Mmap(c.String("file"), os.O_RDONLY)
	if err != nil {
		return err
	}
	defer bk.Close()

	db, err := xrain.NewDB(bk, 0, nil)
	if err != nil {
		return err
	}

	m := &db.Meta

	next := m.Meta.Int64([]byte("freelist3.next"), nil)
	fmt.Printf("next: %x\n", next)

	v, _ := m.Meta.Get([]byte("freelist3.data"), nil)
	if v == nil {
		return errors.New("no freelist data")
	}

	l := xrain.NewSubpageLayout(v)

	for st := l.Step(nil, 0, false); st != nil; st = l.Step(st, 0, false) {
		k, _ := l.Key(st, nil)
		v := l.Value(st, nil)

		fmt.Printf("%x -> %x\n", k, v)
	}

	return nil
}

func dump(c *cli.Command) error {
	bk, err := xrain.Mmap(c.String("file"), os.O_RDONLY)
	if err != nil {
		return err
	}
	defer bk.Close()

	var l xrain.Layout

	switch c.String("layout") {
	case "fixed":
		fl := xrain.NewFixedLayout(nil)
		fl.SetKVSize(1, 7, 8, 1)
		l = fl
	case "kv":
		l = xrain.NewKVLayout2(nil)
	}

	db, err := xrain.NewDB(bk, 0, l)
	if err != nil {
		return err
	}

	//	tlog.Printf("db %v", xrain.DumpDB(db))

	err = db.View(func(tx *xrain.Tx) error {
		b := tx.SimpleBucket

		i := 0
		for i < c.Args.Len() {
			q := b.Bucket([]byte(c.Args[i]))
			if q == nil {
				break
			}

			b = q
			i++
		}

		return printBucket(c, b, c.Args[i:], 0)
	})

	return err
}

func printBucket(c *cli.Command, b *xrain.SimpleBucket, args cli.Args, d int) (err error) {
	if b == nil {
		return nil
	}

	t := b.Tree()

	//	tlog.Printf("bucket: %v", xrain.DumpBucket(b))

	pref := []byte(args.First())

	var ff int
	var k, v []byte

	for st, _ := t.Seek(pref, nil, nil); st != nil; st = t.Next(st) {
		k, ff = t.Key(st, k[:0])
		if !bytes.HasPrefix(k, pref) {
			break
		}

		v = t.Value(st, v[:0])

		space := false

		if p := c.String("pad"); p != "" {
			for i := 0; i < d; i++ {
				fmt.Printf("%v", p)
			}
		}

		if c.Bool("flags") {
			fmt.Printf("%2x", ff)
			space = true
		}

		if c.Bool("key") {
			if space {
				fmt.Printf("  ")
			}

			if c.Bool("quote") || c.Bool("quote-key") {
				fmt.Printf("%q", k)
			} else {
				fmt.Printf("%s", k)
			}

			space = true
		}

		if c.Bool("value") {
			if space {
				fmt.Printf("  ")
			}

			if c.Bool("quote") || c.Bool("quote-value") {
				fmt.Printf("%q", v)
			} else {
				fmt.Printf("%s", v)
			}

			space = true
		}

		if c.Bool("stack") {
			if space {
				fmt.Printf("  ")
			}

			fmt.Printf("  stack %v", st)
		}

		fmt.Printf("\n")

		if ff == 1 {
			sub := b.Bucket(k)

			err = printBucket(c, sub, args.Tail(), d+1)
			if err != nil {
				return
			}
		}
	}

	return nil
}

func IfElse(c bool, t, e interface{}) interface{} {
	if c {
		return t
	} else {
		return e
	}
}
