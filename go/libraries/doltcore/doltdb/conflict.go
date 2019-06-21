package doltdb

import "github.com/liquidata-inc/ld/dolt/go/store/go/types"

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

func ConflictFromTuple(tpl types.Tuple) Conflict {
	return Conflict{tpl.Get(0), tpl.Get(1), tpl.Get(2)}
}

func (c Conflict) ToNomsList(vrw types.ValueReadWriter) types.Tuple {
	return types.NewTuple(c.Base, c.Value, c.MergeValue)
}
