module github.com/dolthub/dolt/integration-tests/go-sql-server-driver

go 1.19

require (
	github.com/dolthub/dolt/go v0.40.4
	github.com/stretchr/testify v1.8.1
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/creasty/defaults v1.6.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-sql-driver/mysql v1.6.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
)

replace github.com/dolthub/dolt/go => ../../go/
