module github.com/dolthub/dolt/integration-tests/go-sql-server-driver

go 1.24.0

require (
	github.com/dolthub/dolt/go v0.40.4
	github.com/go-sql-driver/mysql v1.9.1
	github.com/google/uuid v1.6.0
	github.com/stretchr/testify v1.9.0
	golang.org/x/sync v0.12.0
	gopkg.in/square/go-jose.v2 v2.5.1
	gopkg.in/yaml.v3 v3.0.1
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/creasty/defaults v1.6.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/crypto v0.36.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
)

replace github.com/dolthub/dolt/go => ../../go/
