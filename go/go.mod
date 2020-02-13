module github.com/liquidata-inc/dolt/go

require (
	cloud.google.com/go/pubsub v1.2.0 // indirect
	cloud.google.com/go/storage v1.5.0
	github.com/BurntSushi/toml v0.3.1
	github.com/DataDog/datadog-go v3.4.0+incompatible // indirect
	github.com/OneOfOne/xxhash v1.2.7 // indirect
	github.com/abiosoft/readline v0.0.0-20180607040430-155bce2042db
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d // indirect
	github.com/andreyvit/diff v0.0.0-20170406064948-c7f18ee00883
	github.com/araddon/dateparse v0.0.0-20190622164848-0fb0a474d195
	github.com/armon/go-metrics v0.3.2 // indirect
	github.com/asaskevich/govalidator v0.0.0-20200108200545-475eaeb16496 // indirect
	github.com/attic-labs/kingpin v2.2.7-0.20180312050558-442efcfac769+incompatible
	github.com/aws/aws-sdk-go v1.29.1
	github.com/bcicen/jstream v0.0.0-20190220045926-16c1f8af81c2
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/codahale/blake2 v0.0.0-20150924215134-8d10d0420cbf
	github.com/denisbrodbeck/machineid v1.0.1
	github.com/dustin/go-humanize v1.0.0
	github.com/fatih/color v1.9.0
	github.com/flynn-archive/go-shlex v0.0.0-20150515145356-3f9db97f8568
	github.com/go-openapi/errors v0.19.3 // indirect
	github.com/go-openapi/strfmt v0.19.4 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gocraft/dbr/v2 v2.7.0
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/protobuf v1.3.3
	github.com/golang/snappy v0.0.1
	github.com/google/go-cmp v0.4.0
	github.com/google/uuid v1.1.1
	github.com/gorilla/handlers v1.4.2 // indirect
	github.com/gorilla/mux v1.7.4 // indirect
	github.com/hashicorp/go-immutable-radix v1.1.0 // indirect
	github.com/hashicorp/go-sockaddr v1.0.2 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/hashicorp/memberlist v0.1.6 // indirect
	github.com/jedib0t/go-pretty v4.3.1-0.20191104025401-85fe5d6a7c4d+incompatible
	github.com/jpillora/backoff v1.0.0
	github.com/juju/fslock v0.0.0-20160525022230-4d5c94c67b4b
	github.com/juju/gnuflag v0.0.0-20171113085948-2ce1bb71843d
	github.com/kch42/buzhash v0.0.0-20160816060738-9bdec3dec7c6
	github.com/kr/pretty v0.2.0 // indirect
	github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi v0.0.0-20200207221941-8e60e239e1fb
	github.com/liquidata-inc/ishell v0.0.0-20190514193646-693241f1f2a0
	github.com/liquidata-inc/mmap-go v1.0.3
	github.com/liquidata-inc/sqllogictest/go v0.0.0-20191127195537-419365e253e7
	github.com/mattn/go-isatty v0.0.12
	github.com/mattn/go-runewidth v0.0.8
	github.com/mgutz/ansi v0.0.0-20170206155736-9520e82c474b
	github.com/miekg/dns v1.1.27 // indirect
	github.com/pelletier/go-toml v1.6.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/pkg/profile v1.4.0
	github.com/prometheus/client_golang v1.4.1 // indirect
	github.com/rivo/uniseg v0.1.0
	github.com/sergi/go-diff v1.1.0 // indirect
	github.com/shirou/gopsutil v2.20.1+incompatible
	github.com/shopspring/decimal v0.0.0-20200105231215-408a2507e114
	github.com/sirupsen/logrus v1.4.2
	github.com/skratchdot/open-golang v0.0.0-20200116055534-eef842397966
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cast v1.3.1 // indirect
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/src-d/go-mysql-server v0.6.0
	github.com/stretchr/testify v1.4.0
	github.com/tealeg/xlsx v1.0.5
	github.com/tidwall/pretty v1.0.1 // indirect
	github.com/uber/jaeger-client-go v2.22.1+incompatible // indirect
	github.com/uber/jaeger-lib v2.2.0+incompatible // indirect
	go.mongodb.org/mongo-driver v1.3.0 // indirect
	go.uber.org/atomic v1.5.1 // indirect
	go.uber.org/multierr v1.4.0 // indirect
	go.uber.org/zap v1.13.0
	golang.org/x/crypto v0.0.0-20200210222208-86ce3cb69678
	golang.org/x/exp v0.0.0-20200207192155-f17229e696bd // indirect
	golang.org/x/net v0.0.0-20200202094626-16171245cfb2
	golang.org/x/sys v0.0.0-20200212091648-12a6c2dcc1e4
	golang.org/x/tools v0.0.0-20200212150539-ea181f53ac56 // indirect
	google.golang.org/api v0.17.0
	google.golang.org/genproto v0.0.0-20200212174721-66ed5ce911ce // indirect
	google.golang.org/grpc v1.27.1
	gopkg.in/square/go-jose.v2 v2.4.1
	gopkg.in/src-d/go-errors.v1 v1.0.0
	gopkg.in/yaml.v2 v2.2.8 // indirect
	modernc.org/mathutil v1.1.0 // indirect
	modernc.org/strutil v1.1.0 // indirect
	vitess.io/vitess v3.0.0-rc.3.0.20190602171040-12bfde34629c+incompatible
)

replace github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi => ./gen/proto/dolt/services/eventsapi

replace github.com/src-d/go-mysql-server => github.com/liquidata-inc/go-mysql-server v0.4.1-0.20200213184529-252b68e6f3a8

replace vitess.io/vitess => github.com/liquidata-inc/vitess v0.0.0-20200102230944-f3410911d61f

go 1.13
