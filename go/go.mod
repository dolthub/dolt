module github.com/dolthub/dolt/go

require (
	cloud.google.com/go/storage v1.12.0
	github.com/BurntSushi/toml v0.3.1
	github.com/HdrHistogram/hdrhistogram-go v1.0.0
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/abiosoft/readline v0.0.0-20180607040430-155bce2042db
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d // indirect
	github.com/andreyvit/diff v0.0.0-20170406064948-c7f18ee00883
	github.com/asaskevich/govalidator v0.0.0-20200428143746-21a406dcc535 // indirect
	github.com/attic-labs/kingpin v2.2.7-0.20180312050558-442efcfac769+incompatible
	github.com/aws/aws-sdk-go v1.32.6
	github.com/bcicen/jstream v1.0.0
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/codahale/blake2 v0.0.0-20150924215134-8d10d0420cbf
	github.com/denisbrodbeck/machineid v1.0.1
	github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi v0.0.0-20201005193433-3ee972b1d078
	github.com/dolthub/fslock v0.0.2
	github.com/dolthub/go-mysql-server v0.6.1-0.20201111233201-058dc85fd7d7
	github.com/dolthub/ishell v0.0.0-20201107004254-1592c0036c8d
	github.com/dolthub/mmap-go v1.0.4-0.20201107010347-f9f2a9588a66
	github.com/dolthub/sqllogictest/go v0.0.0-20201105013724-5123fc66e12c
	github.com/dolthub/vitess v0.0.0-20201111232835-de121b254639
	github.com/dustin/go-humanize v1.0.0
	github.com/fatih/color v1.9.0
	github.com/flynn-archive/go-shlex v0.0.0-20150515145356-3f9db97f8568
	github.com/go-kit/kit v0.10.0 // indirect
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/go-openapi/errors v0.19.6 // indirect
	github.com/go-openapi/strfmt v0.19.5 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gocraft/dbr/v2 v2.7.0
	github.com/golang/protobuf v1.4.2
	github.com/golang/snappy v0.0.1
	github.com/google/go-cmp v0.5.2
	github.com/google/uuid v1.1.1
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/jedib0t/go-pretty v4.3.1-0.20191104025401-85fe5d6a7c4d+incompatible
	github.com/jpillora/backoff v1.0.0
	github.com/juju/gnuflag v0.0.0-20171113085948-2ce1bb71843d
	github.com/kch42/buzhash v0.0.0-20160816060738-9bdec3dec7c6
	github.com/lestrrat-go/strftime v1.0.3 // indirect
	github.com/mattn/go-isatty v0.0.12
	github.com/mattn/go-runewidth v0.0.9
	github.com/mgutz/ansi v0.0.0-20170206155736-9520e82c474b
	github.com/mitchellh/mapstructure v1.3.2 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/pkg/profile v1.5.0
	github.com/rivo/uniseg v0.1.0
	github.com/sergi/go-diff v1.1.0 // indirect
	github.com/shirou/gopsutil v2.20.5+incompatible
	github.com/shopspring/decimal v1.2.0
	github.com/sirupsen/logrus v1.6.0
	github.com/skratchdot/open-golang v0.0.0-20200116055534-eef842397966
	github.com/spf13/cast v1.3.1 // indirect
	github.com/spf13/cobra v1.0.0
	github.com/stretchr/testify v1.6.1
	github.com/tealeg/xlsx v1.0.5
	github.com/tidwall/pretty v1.0.1 // indirect
	go.mongodb.org/mongo-driver v1.3.4 // indirect
	go.uber.org/zap v1.15.0
	golang.org/x/crypto v0.0.0-20200622213623-75b288015ac9
	golang.org/x/net v0.0.0-20200904194848-62affa334b73
	golang.org/x/sync v0.0.0-20201008141435-b3e1573b7520
	golang.org/x/sys v0.0.0-20200905004654-be1d3432aa8f
	google.golang.org/api v0.32.0
	google.golang.org/grpc v1.32.0
	google.golang.org/protobuf v1.25.0
	gopkg.in/square/go-jose.v2 v2.5.1
	gopkg.in/src-d/go-errors.v1 v1.0.0
	gopkg.in/yaml.v2 v2.3.0
	gopkg.in/yaml.v3 v3.0.0-20200615113413-eeeca48fe776 // indirect
)

replace github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi => ./gen/proto/dolt/services/eventsapi

//replace github.com/dolthub/go-mysql-server => ../../go-mysql-server

go 1.13
