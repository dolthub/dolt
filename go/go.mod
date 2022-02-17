module github.com/dolthub/dolt/go

require (
	cloud.google.com/go/storage v1.12.0
	github.com/BurntSushi/toml v0.3.1
	github.com/HdrHistogram/hdrhistogram-go v1.0.0
	github.com/abiosoft/readline v0.0.0-20180607040430-155bce2042db
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d
	github.com/andreyvit/diff v0.0.0-20170406064948-c7f18ee00883
	github.com/asaskevich/govalidator v0.0.0-20200428143746-21a406dcc535 // indirect
	github.com/attic-labs/kingpin v2.2.7-0.20180312050558-442efcfac769+incompatible
	github.com/aws/aws-sdk-go v1.32.6
	github.com/bcicen/jstream v1.0.0
	github.com/boltdb/bolt v1.3.1
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/denisbrodbeck/machineid v1.0.1
	github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi v0.0.0-20201005193433-3ee972b1d078
	github.com/dolthub/fslock v0.0.3
	github.com/dolthub/ishell v0.0.0-20220112232610-14e753f0f371
	github.com/dolthub/mmap-go v1.0.4-0.20201107010347-f9f2a9588a66
	github.com/dolthub/sqllogictest/go v0.0.0-20201107003712-816f3ae12d81
	github.com/dolthub/vitess v0.0.0-20220207220721-35d6793fac38
	github.com/dustin/go-humanize v1.0.0
	github.com/fatih/color v1.9.0
	github.com/flynn-archive/go-shlex v0.0.0-20150515145356-3f9db97f8568
	github.com/go-openapi/errors v0.19.6 // indirect
	github.com/go-openapi/strfmt v0.19.5 // indirect
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gocraft/dbr/v2 v2.7.2
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.1
	github.com/google/go-cmp v0.5.7
	github.com/google/uuid v1.2.0
	github.com/jedib0t/go-pretty v4.3.1-0.20191104025401-85fe5d6a7c4d+incompatible
	github.com/jpillora/backoff v1.0.0
	github.com/juju/gnuflag v0.0.0-20171113085948-2ce1bb71843d
	github.com/mattn/go-isatty v0.0.12
	github.com/mattn/go-runewidth v0.0.9
	github.com/mgutz/ansi v0.0.0-20170206155736-9520e82c474b
	github.com/mitchellh/mapstructure v1.3.2 // indirect
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/pkg/profile v1.5.0
	github.com/rivo/uniseg v0.1.0
	github.com/sergi/go-diff v1.1.0 // indirect
	github.com/shopspring/decimal v1.2.0
	github.com/silvasur/buzhash v0.0.0-20160816060738-9bdec3dec7c6
	github.com/sirupsen/logrus v1.8.1
	github.com/skratchdot/open-golang v0.0.0-20200116055534-eef842397966
	github.com/spf13/cobra v1.0.0
	github.com/stretchr/testify v1.7.0
	github.com/tealeg/xlsx v1.0.5
	github.com/tklauser/go-sysconf v0.3.9 // indirect
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	github.com/uber/jaeger-lib v2.4.0+incompatible // indirect
	go.mongodb.org/mongo-driver v1.7.0 // indirect
	go.uber.org/zap v1.15.0
	golang.org/x/crypto v0.0.0-20210513164829-c07d793c2f9a
	golang.org/x/net v0.0.0-20211015210444-4f30a5c0130f
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20220111092808-5a964db01320
	google.golang.org/api v0.32.0
	google.golang.org/grpc v1.37.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/square/go-jose.v2 v2.5.1
	gopkg.in/src-d/go-errors.v1 v1.0.0
	gopkg.in/yaml.v2 v2.3.0
)

require (
	github.com/dolthub/go-mysql-server v0.11.1-0.20220215191257-1f49bce27f46
	github.com/google/flatbuffers v2.0.5+incompatible
	github.com/kch42/buzhash v0.0.0-20160816060738-9bdec3dec7c6
	github.com/prometheus/client_golang v1.11.0
	github.com/shirou/gopsutil/v3 v3.22.1
	github.com/xitongsys/parquet-go v1.6.1
	github.com/xitongsys/parquet-go-source v0.0.0-20211010230925-397910c5e371
)

require (
	cloud.google.com/go v0.66.0 // indirect
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d // indirect
	github.com/apache/thrift v0.13.1-0.20201008052519-daf620915714 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-kit/kit v0.10.0 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/golang/glog v0.0.0-20210429001901-424d2337a529 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/googleapis/gax-go/v2 v2.0.5 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jstemmer/go-junit-report v0.9.1 // indirect
	github.com/klauspost/compress v1.10.10 // indirect
	github.com/lestrrat-go/strftime v1.0.4 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mattn/go-colorable v0.1.7 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mitchellh/hashstructure v1.1.0 // indirect
	github.com/oliveagle/jsonpath v0.0.0-20180606110733-2e52cf6e6852 // indirect
	github.com/pierrec/lz4/v4 v4.1.6 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/src-d/go-oniguruma v1.1.0 // indirect
	github.com/tklauser/numcpus v0.3.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.opencensus.io v0.22.4 // indirect
	go.uber.org/atomic v1.6.0 // indirect
	go.uber.org/multierr v1.5.0 // indirect
	golang.org/x/lint v0.0.0-20201208152925-83fdc39ff7b5 // indirect
	golang.org/x/mod v0.5.1 // indirect
	golang.org/x/oauth2 v0.0.0-20200902213428-5d25da1a8d43 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/tools v0.1.9 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20210506142907-4a47615972c2 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200615113413-eeeca48fe776 // indirect
)

replace (
	github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi => ./gen/proto/dolt/services/eventsapi
	github.com/oliveagle/jsonpath => github.com/dolthub/jsonpath v0.0.0-20210609232853-d49537a30474
)

go 1.17
