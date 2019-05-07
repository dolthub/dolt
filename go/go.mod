module github.com/liquidata-inc/ld/dolt/go

require (
	cloud.google.com/go v0.35.1 // indirect
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d
	github.com/attic-labs/noms v0.0.0-20181127201811-95e8b35cc96f
	github.com/aws/aws-sdk-go v1.16.26 // indirect
	github.com/dustin/go-humanize v1.0.0
	github.com/fatih/color v1.7.0
	github.com/gizak/termui/v3 v3.0.0
	github.com/golang/protobuf v1.2.0
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db
	github.com/google/go-cmp v0.2.0
	github.com/google/uuid v1.1.0
	github.com/jpillora/backoff v0.0.0-20180909062703-3050d21c67d7 // indirect
	github.com/nsf/termbox-go v0.0.0-20190121233118-02980233997d
	github.com/pkg/errors v0.8.1
	github.com/skratchdot/open-golang v0.0.0-20190104022628-a2dfa6d0dab6
	github.com/stretchr/testify v1.3.0
	github.com/tealeg/xlsx v1.0.3
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	go.opencensus.io v0.19.0 // indirect
	golang.org/x/crypto v0.0.0-20190313024323-a1f597ede03a
	golang.org/x/net v0.0.0-20190313220215-9f648a60d977
	golang.org/x/oauth2 v0.0.0-20190115181402-5dab4167f31c // indirect
	google.golang.org/genproto v0.0.0-20190128161407-8ac453e89fca // indirect
	google.golang.org/grpc v1.18.0
	gopkg.in/square/go-jose.v2 v2.2.2
)

replace github.com/attic-labs/noms => github.com/liquidata-inc/noms v0.0.0-20190506171537-518b7edcd73f

replace github.com/xwb1989/sqlparser => github.com/liquidata-inc/sqlparser v0.9.0
