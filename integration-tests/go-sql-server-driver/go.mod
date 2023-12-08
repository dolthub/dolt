module github.com/dolthub/dolt/integration-tests/go-sql-server-driver

go 1.21

require (
	github.com/dolthub/dolt/go v0.40.4
	github.com/google/uuid v1.3.0
	github.com/stretchr/testify v1.8.3
	golang.org/x/sync v0.3.0
	gopkg.in/square/go-jose.v2 v2.5.1
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/creasty/defaults v1.6.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-sql-driver/mysql v1.7.2-0.20230713085235-0b18dac46f7f // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/crypto v0.14.0 // indirect
	golang.org/x/sys v0.13.0 // indirect
)

replace github.com/dolthub/dolt/go => ../../go/
