module github.com/liquidata-inc/ld/dolt/go

require (
	cloud.google.com/go v0.40.0
	github.com/BurntSushi/toml v0.3.1
	github.com/abiosoft/readline v0.0.0-20180607040430-155bce2042db
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d
	github.com/attic-labs/kingpin v2.2.7-0.20180312050558-442efcfac769+incompatible
	github.com/attic-labs/noms v0.0.0-20190508041645-614eb763b982
	github.com/aws/aws-sdk-go v1.20.6
	github.com/cenkalti/backoff v2.1.1+incompatible
	github.com/codahale/blake2 v0.0.0-20150924215134-8d10d0420cbf
	github.com/dustin/go-humanize v1.0.0
	github.com/edsrzf/mmap-go v1.0.0-20181222142022-904c4ced31cd
	github.com/fatih/color v1.7.0
	github.com/flynn-archive/go-shlex v0.0.0-20150515145356-3f9db97f8568
	github.com/gizak/termui/v3 v3.0.0
	github.com/golang/protobuf v1.3.1
	github.com/golang/snappy v0.0.1
	github.com/google/go-cmp v0.3.0
	github.com/google/uuid v1.1.1
	github.com/jpillora/backoff v0.0.0-20180909062703-3050d21c67d7
	github.com/juju/fslock v0.0.0-20160525022230-4d5c94c67b4b
	github.com/juju/gnuflag v0.0.0-20171113085948-2ce1bb71843d
	github.com/kch42/buzhash v0.0.0-20160816060738-9bdec3dec7c6
	github.com/liquidata-inc/ishell v0.0.0-20190514193646-693241f1f2a0
	github.com/mattn/go-isatty v0.0.8
	github.com/mattn/go-runewidth v0.0.4
	github.com/mgutz/ansi v0.0.0-20170206155736-9520e82c474b
	github.com/nsf/termbox-go v0.0.0-20190121233118-02980233997d
	github.com/pkg/errors v0.8.1
	github.com/pkg/profile v1.3.0
	github.com/rivo/uniseg v0.0.0-20190513083848-b9f5b9457d44
	github.com/skratchdot/open-golang v0.0.0-20190104022628-a2dfa6d0dab6
	github.com/src-d/go-mysql-server v0.4.1-0.20190619104848-eaab1795353a
	github.com/shirou/gopsutil v2.18.12+incompatible
	github.com/stretchr/testify v1.3.0
	github.com/syndtr/goleveldb v1.0.0
	github.com/tealeg/xlsx v1.0.3
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	golang.org/x/crypto v0.0.0-20190506204251-e1dfcc566284
	golang.org/x/net v0.0.0-20190503192946-f4e77d36d62c
	golang.org/x/sys v0.0.0-20190507160741-ecd444e8653b
	google.golang.org/api v0.6.0
	google.golang.org/grpc v1.21.1
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	gopkg.in/square/go-jose.v2 v2.3.1
)

replace github.com/attic-labs/noms => github.com/liquidata-inc/noms v0.0.0-20190531204628-499e9652fee4

replace github.com/xwb1989/sqlparser => ../../../../liquidata-inc/sqlparser
//replace github.com/xwb1989/sqlparser => github.com/liquidata-inc/sqlparser v0.9.8

// The latest release of go-mysql-server has gopkg.in instead of github
replace github.com/src-d/go-mysql-server => ../../../../src-d/go-mysql-server
