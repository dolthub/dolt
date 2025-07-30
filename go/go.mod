module github.com/dolthub/dolt/go

require (
	cloud.google.com/go/storage v1.50.0
	github.com/BurntSushi/toml v1.1.0
	github.com/HdrHistogram/hdrhistogram-go v1.1.2
	github.com/abiosoft/readline v0.0.0-20180607040430-155bce2042db
	github.com/andreyvit/diff v0.0.0-20170406064948-c7f18ee00883
	github.com/attic-labs/kingpin v2.2.7-0.20180312050558-442efcfac769+incompatible
	github.com/bcicen/jstream v1.0.0
	github.com/boltdb/bolt v1.3.1
	github.com/denisbrodbeck/machineid v1.0.1
	github.com/dolthub/fslock v0.0.3
	github.com/dolthub/ishell v0.0.0-20240701202509-2b217167d718
	github.com/dolthub/sqllogictest/go v0.0.0-20201107003712-816f3ae12d81
	github.com/dolthub/vitess v0.0.0-20250729231828-87bce34800ce
	github.com/dustin/go-humanize v1.0.1
	github.com/fatih/color v1.13.0
	github.com/flynn-archive/go-shlex v0.0.0-20150515145356-3f9db97f8568
	github.com/go-sql-driver/mysql v1.9.1
	github.com/gocraft/dbr/v2 v2.7.2
	github.com/golang/snappy v0.0.4
	github.com/google/uuid v1.6.0
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
	github.com/stretchr/testify v1.10.0
	github.com/tealeg/xlsx v1.0.5
	go.uber.org/zap v1.24.0
	golang.org/x/crypto v0.40.0
	golang.org/x/net v0.42.0
	golang.org/x/sync v0.16.0
	golang.org/x/sys v0.34.0
	google.golang.org/api v0.241.0
	google.golang.org/grpc v1.74.2
	google.golang.org/protobuf v1.36.6
	gopkg.in/src-d/go-errors.v1 v1.0.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/Shopify/toxiproxy/v2 v2.5.0
	github.com/aliyun/aliyun-oss-go-sdk v2.2.5+incompatible
	github.com/aws/aws-sdk-go-v2 v1.36.3
	github.com/aws/aws-sdk-go-v2/config v1.29.8
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.17.64
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.41.0
	github.com/aws/aws-sdk-go-v2/service/s3 v1.78.0
	github.com/cenkalti/backoff/v4 v4.1.3
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/creasty/defaults v1.6.0
	github.com/dolthub/aws-sdk-go-ini-parser v0.0.0-20250305001723-2821c37f6c12
	github.com/dolthub/eventsapi_schema v0.0.0-20250725194025-a087efa1ee55
	github.com/dolthub/flatbuffers/v23 v23.3.3-dh.2
	github.com/dolthub/go-mysql-server v0.20.1-0.20250730053026-797f25fcfc4f
	github.com/dolthub/gozstd v0.0.0-20240423170813-23a2903bca63
	github.com/esote/minmaxheap v1.0.0
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
	github.com/vbauerster/mpb/v8 v8.0.2
	github.com/xitongsys/parquet-go v1.6.1
	github.com/xitongsys/parquet-go-source v0.0.0-20211010230925-397910c5e371
	github.com/zeebo/blake3 v0.2.3
	github.com/zeebo/xxh3 v1.0.2
	go.etcd.io/bbolt v1.3.6
	go.opentelemetry.io/otel v1.36.0
	go.opentelemetry.io/otel/exporters/jaeger v1.17.0
	go.opentelemetry.io/otel/sdk v1.36.0
	go.opentelemetry.io/otel/trace v1.36.0
	golang.org/x/text v0.27.0
	gonum.org/v1/plot v0.11.0
	gopkg.in/go-jose/go-jose.v2 v2.6.3
	gopkg.in/yaml.v3 v3.0.1
)

require (
	cel.dev/expr v0.24.0 // indirect
	cloud.google.com/go v0.120.0 // indirect
	cloud.google.com/go/auth v0.16.2 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.7.0 // indirect
	cloud.google.com/go/iam v1.5.2 // indirect
	cloud.google.com/go/monitoring v1.24.2 // indirect
	filippo.io/edwards25519 v1.1.0 // indirect
	git.sr.ht/~sbinet/gg v0.3.1 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.27.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.50.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.50.0 // indirect
	github.com/ajstarks/svgo v0.0.0-20211024235047-1546f124cd8b // indirect
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d // indirect
	github.com/apache/thrift v0.13.1-0.20201008052519-daf620915714 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.10 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.17.61 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.30 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.34 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.34 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.34 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.6.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.10.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.18.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.25.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.29.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.33.16 // indirect
	github.com/aws/smithy-go v1.22.2 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cncf/xds/go v0.0.0-20250501225837-2ac532fd4443 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dolthub/go-icu-regex v0.0.0-20250327004329-6799764f2dad // indirect
	github.com/dolthub/jsonpath v0.0.2-0.20240227200619-19675ab05c71 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.32.4 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.2.1 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-fonts/liberation v0.2.0 // indirect
	github.com/go-jose/go-jose/v4 v4.0.5 // indirect
	github.com/go-kit/kit v0.10.0 // indirect
	github.com/go-latex/latex v0.0.0-20210823091927-c0d11ff05a81 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-pdf/fpdf v0.6.0 // indirect
	github.com/gofrs/flock v0.8.1 // indirect
	github.com/golang/freetype v0.0.0-20170609003504-e2365dfdc4a0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.6 // indirect
	github.com/googleapis/gax-go/v2 v2.14.2 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/klauspost/compress v1.10.5 // indirect
	github.com/klauspost/cpuid/v2 v2.0.12 // indirect
	github.com/lestrrat-go/strftime v1.0.4 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.6 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/rs/xid v1.4.0 // indirect
	github.com/sony/gobreaker v0.5.0 // indirect
	github.com/spiffe/go-spiffe/v2 v2.5.0 // indirect
	github.com/tetratelabs/wazero v1.8.2 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tklauser/go-sysconf v0.3.9 // indirect
	github.com/tklauser/numcpus v0.3.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	github.com/zeebo/errs v1.4.0 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.36.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.61.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.61.0 // indirect
	go.opentelemetry.io/otel/metric v1.36.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.36.0 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/exp v0.0.0-20230522175609-2e198f4a06a1 // indirect
	golang.org/x/image v0.18.0 // indirect
	golang.org/x/mod v0.25.0 // indirect
	golang.org/x/oauth2 v0.30.0 // indirect
	golang.org/x/term v0.33.0 // indirect
	golang.org/x/time v0.12.0 // indirect
	golang.org/x/tools v0.34.0 // indirect
	google.golang.org/genproto v0.0.0-20250505200425-f936aa4a68b2 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250528174236-200df99c418a // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250707201910-8d1bb00bc6a7 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
)

go 1.24.0
