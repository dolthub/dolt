package util

import (
	"reflect"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
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
func NomsValueFromDecodedJSON(cs chunks.ChunkStore, o interface{}) types.Value {
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
		items := make([]types.Value, 0, len(o))
		for _, v := range o {
			nv := NomsValueFromDecodedJSON(cs, v)
			if nv != nil {
				items = append(items, nv)
			}
		}
		return types.NewList(cs, items...)
	case map[string]interface{}:
		outDef := MapOfStringToValueDef{}

		for k, v := range o {
			nv := NomsValueFromDecodedJSON(cs, v)
			if nv != nil {
				outDef[k] = nv
			}
		}
		return outDef.New(cs)
	default:
		d.Chk.Fail("Nomsification failed.", "I don't understand %+v, which is of type %s!\n", o, reflect.TypeOf(o).String())
	}
	return nil
}
