// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package ngql

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
	"github.com/graphql-go/graphql"
)

const (
	atKey          = "at"
	countKey       = "count"
	keyKey         = "key"
	rootQueryKey   = "Root"
	sizeKey        = "size"
	targetHashKey  = "targetHash"
	targetValueKey = "targetValue"
	tmKey          = "tm"
	valueKey       = "value"
	rootKey        = "root"
	elementsKey    = "elements"
	vrKey          = "vr"
)

func constructQueryType(rootValue types.Value, tm typeMap) *graphql.Object {
	rootNomsType := rootValue.Type()
	rootType := nomsTypeToGraphQLType(rootNomsType, tm)

	return graphql.NewObject(graphql.ObjectConfig{
		Name: rootQueryKey,
		Fields: graphql.Fields{
			rootKey: &graphql.Field{
				Type: rootType,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					return maybeGetScalar(rootValue), nil
				},
			},
		}})
}

// Query takes |rootValue|, builds a GraphQL scheme from rootValue.Type() and executes |query| against it, encoding the result to |w|.
func Query(rootValue types.Value, query string, vr types.ValueReader, w io.Writer) error {
	tm := newTypeMap()

	queryObj := constructQueryType(rootValue, tm)
	schemaConfig := graphql.SchemaConfig{Query: queryObj}
	schema, _ := graphql.NewSchema(schemaConfig)
	ctx := context.WithValue(context.WithValue(context.Background(), vrKey, vr), tmKey, tm)

	r := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		Context:       ctx,
	})

	rJSON, err := json.Marshal(r)
	d.Chk.NoError(err)
	io.Copy(w, bytes.NewBuffer([]byte(rJSON)))
	return nil
}
