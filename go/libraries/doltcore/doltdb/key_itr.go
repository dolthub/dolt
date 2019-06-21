package doltdb

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

// PKItr defines a function that iterates over a collection of noms values.  The PKItr will return a valid value
// and true until all the values in the collection are exhausted.  At that time nil and false will be returned.
type PKItr func() (val types.Tuple, ok bool)

func SingleColPKItr(pkTag uint64, vals []types.Value) func() (types.Tuple, bool) {
	next := 0
	size := len(vals)
	return func() (types.Tuple, bool) {
		current := next
		next++

		if current < size {
			tpl := types.NewTuple(types.Uint(pkTag), vals[current])
			return tpl, true
		}

		return types.EmptyTuple, false
	}
}

func TaggedValueSliceItr(sch schema.Schema, vals []row.TaggedValues) func() (types.Tuple, bool) {
	pkTags := sch.GetPKCols().Tags
	next := 0
	size := len(vals)
	return func() (types.Tuple, bool) {
		current := next
		next++

		if current < size {
			tpl := vals[current].NomsTupleForTags(pkTags, true)
			return tpl.Value(context.TODO()).(types.Tuple), true
		}

		return types.EmptyTuple, false
	}
}

// TupleSliceItr returns a closure that has the signature of a PKItr and can be used to iterate over a slice of values
func TupleSliceItr(vals []types.Tuple) func() (types.Tuple, bool) {
	next := 0
	size := len(vals)
	return func() (types.Tuple, bool) {
		current := next
		next++

		if current < size {
			return vals[current], true
		}

		return types.EmptyTuple, false
	}
}

// SetItr returns a closure that has the signature of a PKItr and can be used to iterate over a noms Set of vaules
func SetItr(ctx context.Context, valSet types.Set) func() (types.Tuple, bool) {
	itr := valSet.Iterator(ctx)
	return func() (types.Tuple, bool) {
		// TODO: Should this be a `ctx` from the iter call?
		v := itr.Next(ctx)
		return v.(types.Tuple), v != nil
	}
}
