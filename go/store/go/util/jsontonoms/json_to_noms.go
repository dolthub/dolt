// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package jsontonoms

import (
	"context"
	"reflect"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
)

func nomsValueFromDecodedJSONBase(ctx context.Context, vrw types.ValueReadWriter, o interface{}, useStruct bool, namedStructs bool) types.Value {
	switch o := o.(type) {
	case string:
		return types.String(o)
	case bool:
		return types.Bool(o)
	case float64:
		return types.Float(o)
	case nil:
		return nil
	case []interface{}:
		items := make([]types.Value, 0, len(o))
		for _, v := range o {
			nv := nomsValueFromDecodedJSONBase(ctx, vrw, v, useStruct, namedStructs)
			if nv != nil {
				items = append(items, nv)
			}
		}
		return types.NewList(ctx, vrw, items...)
	case map[string]interface{}:
		var v types.Value
		if useStruct {
			structName := ""
			fields := make(types.StructData, len(o))
			for k, v := range o {
				if namedStructs && k == "_name" {
					if s1, isString := v.(string); isString {
						structName = s1
						continue
					}
				}
				nv := nomsValueFromDecodedJSONBase(ctx, vrw, v, useStruct, namedStructs)
				if nv != nil {
					k := types.EscapeStructField(k)
					fields[k] = nv
				}
			}
			v = types.NewStruct(structName, fields)
		} else {
			kv := make([]types.Value, 0, len(o)*2)
			for k, v := range o {
				nv := nomsValueFromDecodedJSONBase(ctx, vrw, v, useStruct, namedStructs)
				if nv != nil {
					kv = append(kv, types.String(k), nv)
				}
			}
			v = types.NewMap(ctx, vrw, kv...)
		}
		return v

	default:
		d.Chk.Fail("Nomsification failed.", "I don't understand %+v, which is of type %s!\n", o, reflect.TypeOf(o).String())
	}
	return nil
}

// NomsValueFromDecodedJSON takes a generic Go interface{} and recursively
// tries to resolve the types within so that it can build up and return
// a Noms Value with the same structure.
//
// Currently, the only types supported are the Go versions of legal JSON types:
// Primitives:
//  - float64
//  - bool
//  - string
//  - nil
//
// Composites:
//  - []interface{}
//  - map[string]interface{}
func NomsValueFromDecodedJSON(ctx context.Context, vrw types.ValueReadWriter, o interface{}, useStruct bool) types.Value {
	return nomsValueFromDecodedJSONBase(ctx, vrw, o, useStruct, false)
}

// NomsValueUsingNamedStructsFromDecodedJSON performs the same function as
// NomsValueFromDecodedJson except that it always decodes JSON objects into
// structs. If the JSON object has a string field name '_name' it uses the
// value of that field as the name of the Noms struct.
func NomsValueUsingNamedStructsFromDecodedJSON(ctx context.Context, vrw types.ValueReadWriter, o interface{}) types.Value {
	return nomsValueFromDecodedJSONBase(ctx, vrw, o, true, true)
}
