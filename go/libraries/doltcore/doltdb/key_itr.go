// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package doltdb

import (
	"context"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// PKItr defines a function that iterates over a collection of noms values.  The PKItr will return a valid value
// and true until all the values in the collection are exhausted.  At that time nil and false will be returned.
type PKItr func() (val types.Tuple, ok bool, err error)

func SingleColPKItr(nbf *types.NomsBinFormat, pkTag uint64, vals []types.Value) func() (types.Tuple, bool, error) {
	next := 0
	size := len(vals)
	return func() (types.Tuple, bool, error) {
		current := next
		next++

		if current < size {
			tpl, err := types.NewTuple(nbf, types.Uint(pkTag), vals[current])

			if err != nil {
				return types.EmptyTuple(nbf), false, err
			}

			return tpl, true, nil
		}

		return types.EmptyTuple(nbf), false, nil
	}
}

func TaggedValueSliceItr(nbf *types.NomsBinFormat, sch schema.Schema, vals []row.TaggedValues) func() (types.Tuple, bool, error) {
	next := 0
	size := len(vals)
	return func() (types.Tuple, bool, error) {
		current := next
		next++

		if current < size {
			tpl := vals[current].NomsTupleForPKCols(nbf, sch.GetPKCols())
			v, err := tpl.Value(context.TODO())

			if err != nil {
				return types.EmptyTuple(nbf), false, err
			}

			return v.(types.Tuple), true, nil
		}

		return types.EmptyTuple(nbf), false, nil
	}
}

// TupleSliceItr returns a closure that has the signature of a PKItr and can be used to iterate over a slice of values
func TupleSliceItr(nbf *types.NomsBinFormat, vals []types.Tuple) func() (types.Tuple, bool, error) {
	next := 0
	size := len(vals)
	return func() (types.Tuple, bool, error) {
		current := next
		next++

		if current < size {
			return vals[current], true, nil
		}

		return types.EmptyTuple(nbf), false, nil
	}
}

// SetItr returns a closure that has the signature of a PKItr and can be used to iterate over a noms Set of vaules
func SetItr(ctx context.Context, valSet types.Set) (func() (types.Tuple, bool, error), error) {
	itr, err := valSet.Iterator(ctx)

	if err != nil {
		return nil, err
	}

	return func() (types.Tuple, bool, error) {
		// TODO: Should this be a `ctx` from the iter call?
		v, err := itr.Next(ctx)

		if err != nil {
			return types.EmptyTuple(valSet.Format()), false, err
		}

		return v.(types.Tuple), v != nil, nil
	}, nil
}
