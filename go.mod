module github.com/nikandfor/xrain

go 1.12

require (
	github.com/nikandfor/cli v0.0.0-20200325075312-052d5b29bac6
	github.com/nikandfor/errors v0.1.0
	github.com/nikandfor/tlog v0.4.2
	github.com/stretchr/testify v1.4.0
	github.com/urfave/cli v1.22.4
	go.etcd.io/bbolt v1.3.5
)

replace github.com/nikandfor/tlog => ../tlog
