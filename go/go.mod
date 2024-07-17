module github.com/dolthub/dolt/go

require (
	cloud.google.com/go/storage v1.31.0
	github.com/BurntSushi/toml v1.1.0
	github.com/HdrHistogram/hdrhistogram-go v1.1.2
	github.com/abiosoft/readline v0.0.0-20180607040430-155bce2042db
	github.com/andreyvit/diff v0.0.0-20170406064948-c7f18ee00883
	github.com/attic-labs/kingpin v2.2.7-0.20180312050558-442efcfac769+incompatible
	github.com/aws/aws-sdk-go v1.34.0
	github.com/bcicen/jstream v1.0.0
	github.com/boltdb/bolt v1.3.1
	github.com/denisbrodbeck/machineid v1.0.1
	github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi v0.0.0-20201005193433-3ee972b1d078
	github.com/dolthub/fslock v0.0.3
	github.com/dolthub/ishell v0.0.0-20240701202509-2b217167d718
	github.com/dolthub/sqllogictest/go v0.0.0-20201107003712-816f3ae12d81
	github.com/dolthub/vitess v0.0.0-20240711213744-4232e1c4edae
	github.com/dustin/go-humanize v1.0.1
	github.com/fatih/color v1.13.0
	github.com/flynn-archive/go-shlex v0.0.0-20150515145356-3f9db97f8568
	github.com/go-sql-driver/mysql v1.7.2-0.20231213112541-0004702b931d
	github.com/gocraft/dbr/v2 v2.7.2
	github.com/golang/snappy v0.0.4
	github.com/google/uuid v1.3.0
	github.com/jpillora/backoff v1.0.0
	github.com/juju/gnuflag v0.0.0-20171113085948-2ce1bb71843d
	github.com/mattn/go-isatty v0.0.17
	github.com/mattn/go-runewidth v0.0.13
	github.com/pkg/errors v0.9.1
	github.com/pkg/profile v1.5.0
	github.com/rivo/uniseg v0.2.0
	github.com/sergi/go-diff v1.1.0
	github.com/shopspring/decimal v1.3.1
	github.com/silvasur/buzhash v0.0.0-20160816060738-9bdec3dec7c6
	github.com/sirupsen/logrus v1.8.1
	github.com/skratchdot/open-golang v0.0.0-20200116055534-eef842397966
	github.com/stretchr/testify v1.8.4
	github.com/tealeg/xlsx v1.0.5
	go.uber.org/zap v1.24.0
	golang.org/x/crypto v0.23.0
	golang.org/x/net v0.25.0
	golang.org/x/sync v0.7.0
	golang.org/x/sys v0.20.0
	google.golang.org/api v0.126.0
	google.golang.org/grpc v1.57.1
	google.golang.org/protobuf v1.31.0
	gopkg.in/src-d/go-errors.v1 v1.0.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/Shopify/toxiproxy/v2 v2.5.0
	github.com/aliyun/aliyun-oss-go-sdk v2.2.5+incompatible
	github.com/cenkalti/backoff/v4 v4.1.3
	github.com/cespare/xxhash v1.1.0
	github.com/cespare/xxhash/v2 v2.2.0
	github.com/creasty/defaults v1.6.0
	github.com/dolthub/flatbuffers/v23 v23.3.3-dh.2
	github.com/dolthub/go-mysql-server v0.18.2-0.20240715180604-3d70c263a451
	github.com/dolthub/gozstd v0.0.0-20240423170813-23a2903bca63
	github.com/dolthub/swiss v0.1.0
	github.com/goccy/go-json v0.10.2
	github.com/google/btree v1.1.2
	github.com/google/go-github/v57 v57.0.0
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/hashicorp/golang-lru/v2 v2.0.2
	github.com/jmoiron/sqlx v1.3.4
	github.com/kch42/buzhash v0.0.0-20160816060738-9bdec3dec7c6
	github.com/kylelemons/godebug v1.1.0
	github.com/lib/pq v1.10.0
	github.com/mohae/uvarint v0.0.0-20160208145430-c3f9e62bf2b0
	github.com/oracle/oci-go-sdk/v65 v65.55.0
	github.com/prometheus/client_golang v1.13.0
	github.com/rs/zerolog v1.28.0
	github.com/shirou/gopsutil/v3 v3.22.1
	github.com/tidwall/gjson v1.14.4
	github.com/tidwall/sjson v1.2.5
	github.com/vbauerster/mpb v3.4.0+incompatible
	github.com/vbauerster/mpb/v8 v8.0.2
	github.com/xitongsys/parquet-go v1.6.1
	github.com/xitongsys/parquet-go-source v0.0.0-20211010230925-397910c5e371
	github.com/zeebo/blake3 v0.2.3
	github.com/zeebo/xxh3 v1.0.2
	go.etcd.io/bbolt v1.3.6
	go.opentelemetry.io/otel v1.7.0
	go.opentelemetry.io/otel/exporters/jaeger v1.7.0
	go.opentelemetry.io/otel/sdk v1.7.0
	go.opentelemetry.io/otel/trace v1.7.0
	golang.org/x/exp v0.0.0-20230522175609-2e198f4a06a1
	golang.org/x/text v0.16.0
	gonum.org/v1/plot v0.11.0
	gopkg.in/errgo.v2 v2.1.0
	gopkg.in/go-jose/go-jose.v2 v2.6.3
	gopkg.in/yaml.v3 v3.0.1
)

require (
	cloud.google.com/go v0.110.7 // indirect
	cloud.google.com/go/compute v1.23.0 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v1.1.1 // indirect
	filippo.io/edwards25519 v1.1.0 // indirect
	git.sr.ht/~sbinet/gg v0.3.1 // indirect
	github.com/ajstarks/svgo v0.0.0-20211024235047-1546f124cd8b // indirect
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d // indirect
	github.com/apache/thrift v0.13.1-0.20201008052519-daf620915714 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dolthub/go-icu-regex v0.0.0-20230524105445-af7e7991c97e // indirect
	github.com/dolthub/jsonpath v0.0.2-0.20240227200619-19675ab05c71 // indirect
	github.com/dolthub/maphash v0.0.0-20221220182448-74e1e1ea1577 // indirect
	github.com/go-fonts/liberation v0.2.0 // indirect
	github.com/go-kit/kit v0.10.0 // indirect
	github.com/go-latex/latex v0.0.0-20210823091927-c0d11ff05a81 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-pdf/fpdf v0.6.0 // indirect
	github.com/gofrs/flock v0.8.1 // indirect
	github.com/golang/freetype v0.0.0-20170609003504-e2365dfdc4a0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/s2a-go v0.1.4 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.3 // indirect
	github.com/googleapis/gax-go/v2 v2.11.0 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.10.5 // indirect
	github.com/klauspost/cpuid/v2 v2.0.12 // indirect
	github.com/lestrrat-go/strftime v1.0.4 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.6 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/rs/xid v1.4.0 // indirect
	github.com/sony/gobreaker v0.5.0 // indirect
	github.com/tetratelabs/wazero v1.1.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tklauser/go-sysconf v0.3.9 // indirect
	github.com/tklauser/numcpus v0.3.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/image v0.18.0 // indirect
	golang.org/x/mod v0.17.0 // indirect
	golang.org/x/oauth2 v0.8.0 // indirect
	golang.org/x/term v0.20.0 // indirect
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0 // indirect
	golang.org/x/tools v0.21.1-0.20240508182429-e35e4ccd0d2d // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20230807174057-1744710a1577 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20230803162519-f966b187b2e5 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230803162519-f966b187b2e5 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
)

replace github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi => ./gen/proto/dolt/services/eventsapi

go 1.22.2
