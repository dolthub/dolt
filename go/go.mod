module github.com/liquidata-inc/dolt/go

require (
	cloud.google.com/go v0.51.0 // indirect
	cloud.google.com/go/storage v1.5.0
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.4.0 // indirect
	github.com/DataDog/datadog-go v3.3.1+incompatible // indirect
	github.com/OneOfOne/xxhash v1.2.6 // indirect
	github.com/abiosoft/readline v0.0.0-20180607040430-155bce2042db
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d // indirect
	github.com/araddon/dateparse v0.0.0-20190622164848-0fb0a474d195
	github.com/armon/go-metrics v0.3.0 // indirect
	github.com/asaskevich/govalidator v0.0.0-20200108180119-8dad3749fdf3 // indirect
	github.com/attic-labs/kingpin v2.2.7-0.20180312050558-442efcfac769+incompatible
	github.com/aws/aws-sdk-go v1.27.2
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
	github.com/gocraft/dbr/v2 v2.6.3
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/protobuf v1.3.2
	github.com/golang/snappy v0.0.1
	github.com/google/go-cmp v0.4.0
	github.com/google/uuid v1.1.1
	github.com/gorilla/handlers v1.4.2 // indirect
	github.com/gorilla/mux v1.7.3 // indirect
	github.com/hashicorp/go-immutable-radix v1.1.0 // indirect
	github.com/hashicorp/go-sockaddr v1.0.2 // indirect
	github.com/hashicorp/memberlist v0.1.5 // indirect
	github.com/jedib0t/go-pretty v4.3.0+incompatible
	github.com/jpillora/backoff v1.0.0
	github.com/juju/fslock v0.0.0-20160525022230-4d5c94c67b4b
	github.com/juju/gnuflag v0.0.0-20171113085948-2ce1bb71843d
	github.com/kch42/buzhash v0.0.0-20160816060738-9bdec3dec7c6
	github.com/kr/pretty v0.2.0 // indirect
	github.com/lib/pq v1.3.0 // indirect
	github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi v0.0.0-20200107004437-9e6f9fd56de5
	github.com/liquidata-inc/ishell v0.0.0-20190514193646-693241f1f2a0
	github.com/liquidata-inc/mmap-go v1.0.3
	github.com/liquidata-inc/sqllogictest/go v0.0.0-20191127195537-419365e253e7
	github.com/mattn/go-isatty v0.0.11
	github.com/mattn/go-runewidth v0.0.7
	github.com/mattn/go-sqlite3 v2.0.2+incompatible // indirect
	github.com/mgutz/ansi v0.0.0-20170206155736-9520e82c474b
	github.com/miekg/dns v1.1.27 // indirect
	github.com/pelletier/go-toml v1.6.0 // indirect
	github.com/pkg/errors v0.8.1
	github.com/pkg/profile v1.4.0
	github.com/prometheus/client_golang v1.3.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20190728182440-6a916e37a237 // indirect
	github.com/rivo/uniseg v0.1.0
	github.com/shirou/gopsutil v2.19.12+incompatible
	github.com/shopspring/decimal v0.0.0-20200105231215-408a2507e114 // indirect
	github.com/sirupsen/logrus v1.4.2
	github.com/skratchdot/open-golang v0.0.0-20190402232053-79abb63cd66e
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cast v1.3.1 // indirect
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/src-d/go-mysql-server v0.6.0
	github.com/stretchr/testify v1.4.0
	github.com/tealeg/xlsx v1.0.5
	github.com/uber/jaeger-client-go v2.21.1+incompatible // indirect
	github.com/uber/jaeger-lib v2.2.0+incompatible // indirect
	go.mongodb.org/mongo-driver v1.2.1 // indirect
	go.uber.org/atomic v1.5.1 // indirect
	go.uber.org/multierr v1.4.0 // indirect
	go.uber.org/zap v1.13.0
	golang.org/x/crypto v0.0.0-20191227163750-53104e6ec876
	golang.org/x/net v0.0.0-20191209160850-c0dbc17a3553
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d // indirect
	golang.org/x/sys v0.0.0-20200107162124-548cf772de50
	golang.org/x/tools v0.0.0-20200107184032-11e9d9cc0042 // indirect
	google.golang.org/api v0.15.0
	google.golang.org/grpc v1.26.0
	gopkg.in/square/go-jose.v2 v2.4.1
	gopkg.in/yaml.v2 v2.2.7 // indirect
	modernc.org/strutil v1.1.0 // indirect
	vitess.io/vitess v3.0.0-rc.3.0.20190602171040-12bfde34629c+incompatible
)

replace github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi => ./gen/proto/dolt/services/eventsapi

replace github.com/src-d/go-mysql-server => github.com/liquidata-inc/go-mysql-server v0.4.1-0.20200103221104-851dcd65db36

replace vitess.io/vitess => github.com/liquidata-inc/vitess v0.0.0-20200102230944-f3410911d61f

go 1.13
