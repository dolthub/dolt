# graphql [![Build Status](https://travis-ci.org/graphql-go/graphql.svg)](https://travis-ci.org/graphql-go/graphql) [![GoDoc](https://godoc.org/graphql.co/graphql?status.svg)](https://godoc.org/github.com/attic-labs/graphql) [![Coverage Status](https://coveralls.io/repos/graphql-go/graphql/badge.svg?branch=master&service=github)](https://coveralls.io/github/graphql-go/graphql?branch=master) [![Join the chat at https://gitter.im/chris-ramon/graphql](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/graphql-go/graphql?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A *work-in-progress* implementation of GraphQL for Go.

### Documentation

godoc: https://godoc.org/github.com/attic-labs/graphql

### Getting Started

To install the library, run:
```bash
go get github.com/attic-labs/graphql
```

The following is a simple example which defines a schema with a single `hello` string-type field and a `Resolve` method which returns the string `world`. A GraphQL query is performed against this schema with the resulting output printed in JSON format.

```go
package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/attic-labs/graphql"
)

func main() {
	// Schema
	fields := graphql.Fields{
		"hello": &graphql.Field{
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return "world", nil
			},
		},
	}
	rootQuery := graphql.ObjectConfig{Name: "RootQuery", Fields: fields}
	schemaConfig := graphql.SchemaConfig{Query: graphql.NewObject(rootQuery)}
	schema, err := graphql.NewSchema(schemaConfig)
	if err != nil {
		log.Fatalf("failed to create new schema, error: %v", err)
	}

	// Query
	query := `
		{
			hello
		}
	`
	params := graphql.Params{Schema: schema, RequestString: query}
	r := graphql.Do(params)
	if len(r.Errors) > 0 {
		log.Fatalf("failed to execute graphql operation, errors: %+v", r.Errors)
	}
	rJSON, _ := json.Marshal(r)
	fmt.Printf("%s \n", rJSON) // {“data”:{“hello”:”world”}}
}
```
For more complex examples, refer to the [examples/](https://github.com/attic-labs/graphql/tree/master/examples/) directory and [graphql_test.go](https://github.com/attic-labs/graphql/blob/master/graphql_test.go).

### Origin and Current Direction

This project was originally a port of [v0.4.3](https://github.com/graphql/graphql-js/releases/tag/v0.4.3) of [graphql-js](https://github.com/graphql/graphql-js) (excluding the Validator), which was based on the July 2015 GraphQL specification. `graphql` is currently several versions behind `graphql-js`, however future efforts will be guided directly by the [latest formal GraphQL specification](https://github.com/facebook/graphql/releases) (currently: [October 2015](https://github.com/facebook/graphql/releases/tag/October2015)).

### Third Party Libraries
| Name          | Author        | Description  |
|:-------------:|:-------------:|:------------:|
| [graphql-go-handler](https://github.com/attic-labs/graphql-go-handler) | [Hafiz Ismail](https://github.com/sogko) | Middleware to handle GraphQL queries through HTTP requests. |
| [graphql-relay-go](https://github.com/attic-labs/graphql-relay-go) | [Hafiz Ismail](https://github.com/sogko) | Lib to construct a graphql-go server supporting react-relay. |
| [golang-relay-starter-kit](https://github.com/sogko/golang-relay-starter-kit) | [Hafiz Ismail](https://github.com/sogko) | Barebones starting point for a Relay application with Golang GraphQL server. |

### Blog Posts
- [Golang + GraphQL + Relay](http://wehavefaces.net/)

### Roadmap
- [x] Lexer
- [x] Parser
- [x] Schema Parser
- [x] Printer
- [x] Schema Printer
- [x] Visitor
- [x] Executor
- [x] Validator
- [ ] Examples
  - [ ] Basic Usage (see: [PR-#21](https://github.com/attic-labs/graphql/pull/21)) 
  - [ ] React/Relay
- [ ] Alpha Release (v0.1)

