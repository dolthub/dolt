package util

import (
	"fmt"

	"github.com/attic-labs/noms/types"
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
func NomsValueFromDecodedJSON(o interface{}) types.Value {
	switch o := o.(type) {
	case string:
		return types.NewString(o)
	case bool:
		return types.Bool(o)
	case float64:
		return types.Float64(o)
	case nil:
		return nil
	case []interface{}:
		out := types.NewList()
		for _, v := range o {
			nv := NomsValueFromDecodedJSON(v)
			if nv != nil {
				out = out.Append(nv)
			}
		}
		return out
	case map[string]interface{}:
		out := types.NewMap()
		for k, v := range o {
			nv := NomsValueFromDecodedJSON(v)
			if nv != nil {
				out = out.Set(types.NewString(k), nv)
			}
		}
		return out
	default:
		panic(fmt.Sprintln(o, "is of a type I don't know how to handle!"))
	}
}
