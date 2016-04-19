package util

import (
	"github.com/attic-labs/noms/types"
)

func NewMapOfStringToValue(kv ...types.Value) types.Map {
	return types.NewTypedMap(
		types.MakeCompoundType(types.MapKind, types.MakePrimitiveType(types.StringKind), types.MakePrimitiveType(types.ValueKind)),
		kv...)
}