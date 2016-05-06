package util

import (
	"github.com/attic-labs/noms/types"
)

func NewMapOfStringToValue(kv ...types.Value) types.Map {
	return types.NewTypedMap(
		types.MakeMapType(types.StringType, types.ValueType),
		kv...)
}