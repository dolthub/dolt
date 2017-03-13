// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package ngql

import (
	"context"
	"encoding/json"
	"io"

	"github.com/attic-labs/graphql"
	"github.com/attic-labs/graphql/gqlerrors"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
)

const (
	atKey          = "at"
	countKey       = "count"
	elementsKey    = "elements"
	entriesKey     = "entries"
	keyKey         = "key"
	keysKey        = "keys"
	rootKey        = "root"
	rootQueryKey   = "Root"
	scalarValue    = "scalarValue"
	sizeKey        = "size"
	targetHashKey  = "targetHash"
	targetValueKey = "targetValue"
	throughKey     = "through"
	tmKey          = "tm"
	valueKey       = "value"
	valuesKey      = "values"
	vrKey          = "vr"
)

// NewRootQueryObject creates a "root" query object that can be used to
// traverse the value tree of rootValue.
func NewRootQueryObject(rootValue types.Value, tm *TypeMap) *graphql.Object {
	rootNomsType := rootValue.Type()
	rootType := NomsTypeToGraphQLType(rootNomsType, false, tm)

	return graphql.NewObject(graphql.ObjectConfig{
		Name: rootQueryKey,
		Fields: graphql.Fields{
			rootKey: &graphql.Field{
				Type: rootType,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					return MaybeGetScalar(rootValue), nil
				},
			},
		}})
}

// NewContext creates a new context.Context with the extra data added to it
// that is required by ngql.
func NewContext(vr types.ValueReader, tm *TypeMap) context.Context {
	return context.WithValue(context.WithValue(context.Background(), vrKey, vr), tmKey, tm)
}

// Query takes |rootValue|, builds a GraphQL scheme from rootValue.Type() and
// executes |query| against it, encoding the result to |w|.
func Query(rootValue types.Value, query string, vr types.ValueReader, w io.Writer) {
	schemaConfig := graphql.SchemaConfig{}
	tm := NewTypeMap()
	queryWithSchemaConfig(rootValue, query, schemaConfig, vr, tm, w)
}

func queryWithSchemaConfig(rootValue types.Value, query string, schemaConfig graphql.SchemaConfig, vr types.ValueReader, tm *TypeMap, w io.Writer) {
	schemaConfig.Query = NewRootQueryObject(rootValue, tm)
	schema, _ := graphql.NewSchema(schemaConfig)
	ctx := NewContext(vr, tm)

	r := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		Context:       ctx,
	})

	err := json.NewEncoder(w).Encode(r)
	d.PanicIfError(err)
}

// Error writes an error as a GraphQL error to a writer.
func Error(err error, w io.Writer) {
	r := graphql.Result{
		Errors: []gqlerrors.FormattedError{
			{Message: err.Error()},
		},
	}

	jsonErr := json.NewEncoder(w).Encode(r)
	d.PanicIfError(jsonErr)
}
