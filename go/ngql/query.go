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
func NewRootQueryObject(ctx context.Context, rootValue types.Value, tm *TypeMap) *graphql.Object {
	tc := TypeConverter{*tm, DefaultNameFunc}
	return tc.NewRootQueryObject(ctx, rootValue)
}

// NewRootQueryObject creates a "root" query object that can be used to
// traverse the value tree of rootValue.
func (tc *TypeConverter) NewRootQueryObject(ctx context.Context, rootValue types.Value) *graphql.Object {
	rootNomsType := types.TypeOf(rootValue)
	rootType := tc.NomsTypeToGraphQLType(ctx, rootNomsType)

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
func NewContext(ctx context.Context, vrw types.ValueReader) context.Context {
	return context.WithValue(ctx, vrwKey, vrw)
}

// Query takes |rootValue|, builds a GraphQL scheme from rootValue.Type() and
// executes |query| against it, encoding the result to |w|.
func Query(ctx context.Context, rootValue types.Value, query string, vrw types.ValueReadWriter, w io.Writer) {
	schemaConfig := graphql.SchemaConfig{}
	tc := NewTypeConverter()
	queryWithSchemaConfig(ctx, rootValue, query, schemaConfig, vrw, tc, w)
}

func queryWithSchemaConfig(ctx context.Context, rootValue types.Value, query string, schemaConfig graphql.SchemaConfig, vrw types.ValueReadWriter, tc *TypeConverter, w io.Writer) {
	schemaConfig.Query = tc.NewRootQueryObject(ctx, rootValue)
	schema, _ := graphql.NewSchema(schemaConfig)
	qlCtx := NewContext(ctx, vrw)

	r := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		Context:       qlCtx,
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
