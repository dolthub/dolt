package doltdb

import "github.com/attic-labs/noms/go/types"

type Conflict struct {
	Base       types.Value
	Value      types.Value
	MergeValue types.Value
}

func NewConflict(base, value, mergeValue types.Value) Conflict {
	if base == nil {
		base = types.NullValue
	}
	if value == nil {
		value = types.NullValue
	}
	if mergeValue == nil {
		mergeValue = types.NullValue
	}
	return Conflict{base, value, mergeValue}
}

func ConflictFromNomsList(lst types.List) Conflict {
	return Conflict{lst.Get(0), lst.Get(1), lst.Get(2)}
}

func (c Conflict) ToNomsList(vrw types.ValueReadWriter) types.List {
	return types.NewList(vrw, c.Base, c.Value, c.MergeValue)
}
