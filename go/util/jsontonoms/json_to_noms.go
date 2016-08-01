// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package jsontonoms

import (
	"reflect"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
)

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
func NomsValueFromDecodedJSON(o interface{}, useStruct bool) types.Value {
	switch o := o.(type) {
	case string:
		return types.String(o)
	case bool:
		return types.Bool(o)
	case float64:
		return types.Number(o)
	case nil:
		return nil
	case []interface{}:
		items := make([]types.Value, 0, len(o))
		for _, v := range o {
			nv := NomsValueFromDecodedJSON(v, useStruct)
			if nv != nil {
				items = append(items, nv)
			}
		}
		return types.NewList(items...)
	case map[string]interface{}:
		var v types.Value
		if useStruct {
			fields := make(types.StructData, len(o))
			for k, v := range o {
				nv := NomsValueFromDecodedJSON(v, useStruct)
				if nv != nil {
					k := types.EscapeStructField(k)
					fields[k] = nv
				}
			}
			v = types.NewStruct("", fields)
		} else {
			kv := make([]types.Value, 0, len(o)*2)
			for k, v := range o {
				nv := NomsValueFromDecodedJSON(v, useStruct)
				if nv != nil {
					kv = append(kv, types.String(k), nv)
				}
			}
			v = types.NewMap(kv...)
		}
		return v

	default:
		d.Chk.Fail("Nomsification failed.", "I don't understand %+v, which is of type %s!\n", o, reflect.TypeOf(o).String())
	}
	return nil
}
