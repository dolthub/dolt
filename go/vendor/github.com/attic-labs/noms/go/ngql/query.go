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
	valueKey       = "value"
	valuesKey      = "values"
	vrwKey         = "vrw"
)

// NewRootQueryObject creates a "root" query object that can be used to
// traverse the value tree of rootValue.
func NewRootQueryObject(rootValue types.Value, tm *TypeMap) *graphql.Object {
	tc := TypeConverter{*tm, DefaultNameFunc}
	return tc.NewRootQueryObject(rootValue)
}

// NewRootQueryObject creates a "root" query object that can be used to
// traverse the value tree of rootValue.
func (tc *TypeConverter) NewRootQueryObject(rootValue types.Value) *graphql.Object {
	rootNomsType := types.TypeOf(rootValue)
	rootType := tc.NomsTypeToGraphQLType(rootNomsType)

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
func NewContext(vrw types.ValueReader) context.Context {
	return context.WithValue(context.Background(), vrwKey, vrw)
}

// Query takes |rootValue|, builds a GraphQL scheme from rootValue.Type() and
// executes |query| against it, encoding the result to |w|.
func Query(rootValue types.Value, query string, vrw types.ValueReadWriter, w io.Writer) {
	schemaConfig := graphql.SchemaConfig{}
	tc := NewTypeConverter()
	queryWithSchemaConfig(rootValue, query, schemaConfig, vrw, tc, w)
}

func queryWithSchemaConfig(rootValue types.Value, query string, schemaConfig graphql.SchemaConfig, vrw types.ValueReadWriter, tc *TypeConverter, w io.Writer) {
	schemaConfig.Query = tc.NewRootQueryObject(rootValue)
	schema, _ := graphql.NewSchema(schemaConfig)
	ctx := NewContext(vrw)

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
