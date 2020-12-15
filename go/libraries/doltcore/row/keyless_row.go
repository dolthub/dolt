// Copyright 2020 Dolthub, Inc.
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

package row

import (
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrZeroCardinality = fmt.Errorf("read row with zero cardinality")

// keylessRow is a Row without PRIMARY_KEY fields
//
// key: Tuple(
//          UUID(hash.Of(tag1, val1, ..., tagN, valN))
//      )
// val: Tuple(
//          Uint(count),
//          Uint(tag1), Value(val1),
//          ...,
//          Uint(tagN), Value(valN)
//      )
type keylessRow struct {
	key types.Tuple
	val types.Tuple
}

var _ Row = keylessRow{}

func KeylessRow(nbf *types.NomsBinFormat, vals ...types.Value) (Row, error) {
	return keylessRowWithCardinality(nbf, 1, vals...)
}

func KeylessRowsFromTuples(key, val types.Tuple) (Row, uint64, error) {
	c, err := val.Get(0)
	if err != nil {
		return nil, 0, err
	}

	card := uint64(c.(types.Uint))
	r := keylessRow{
		key: key,
		val: val,
	}

	return r, card, err
}

func keylessRowWithCardinality(nbf *types.NomsBinFormat, card uint64, vals ...types.Value) (Row, error) {
	id, err := types.UUIDHashedFromValues(nbf, vals...) // don't hash cardinality
	if err != nil {
		return nil, err
	}

	kt, err := types.NewTuple(nbf, id)
	if err != nil {
		return nil, err
	}

	vals = append([]types.Value{types.Uint(card)}, vals...)

	vt, err := types.NewTuple(nbf, vals...)
	if err != nil {
		return nil, err
	}

	return keylessRow{
		key: kt,
		val: vt,
	}, nil
}

func (r keylessRow) NomsMapKey(sch schema.Schema) types.LesserValuable {
	return r.key
}

func (r keylessRow) NomsMapValue(sch schema.Schema) types.Valuable {
	return r.val
}

func (r keylessRow) IterCols(cb func(tag uint64, val types.Value) (stop bool, err error)) (bool, error) {
	iter, err := r.val.Iterator()
	if err != nil {
		return false, err
	}
	_, card, err := iter.Next()
	if err != nil {
		return false, err
	}
	if card.(types.Uint) < 1 {
		return false, ErrZeroCardinality
	}

	for {
		_, v, err := iter.Next()
		if err != nil {
			return false, err
		}
		if v == nil {
			break
		}

		tag, ok := v.(types.Uint)
		if !ok {
			return false, fmt.Errorf("expected tag types.Uint, got %v", v)
		}

		_, v, err = iter.Next()
		if err != nil {
			return false, err
		}

		stop, err := cb(uint64(tag), v)
		if err != nil {
			return false, nil
		}
		if stop {
			return stop, nil
		}
	}

	return true, nil
}

func (r keylessRow) IterSchema(sch schema.Schema, cb func(tag uint64, val types.Value) (stop bool, err error)) (bool, error) {
	iter, err := r.val.Iterator()
	if err != nil {
		return false, err
	}
	_, card, err := iter.Next()
	if err != nil {
		return false, err
	}
	if card.(types.Uint) < 1 {
		return false, ErrZeroCardinality
	}

	tags := sch.GetAllCols().Tags
	vals := make([]types.Value, len(tags))

	for {
		_, v, err := iter.Next()
		if err != nil {
			return false, err
		}
		if v == nil {
			break
		}

		tag, ok := v.(types.Uint)
		if !ok {
			return false, fmt.Errorf("expected tag types.Uint, got %v", v)
		}

		idx := sch.GetAllCols().TagToIdx[uint64(tag)]
		_, vals[idx], err = iter.Next()
		if err != nil {
			return false, err
		}
	}

	for idx, tag := range tags {
		stop, err := cb(tag, vals[idx])
		if err != nil {
			return false, err
		}
		if stop {
			return stop, nil
		}
	}

	return true, nil
}

func (r keylessRow) GetColVal(tag uint64) (val types.Value, ok bool) {
	_, _ = r.IterCols(func(t uint64, v types.Value) (stop bool, err error) {
		if tag == t {
			val = v
			ok, stop = true, true
		}
		return
	})
	return val, ok
}

func (r keylessRow) SetColVal(updateTag uint64, updateVal types.Value, sch schema.Schema) (Row, error) {
	iter, err := r.val.Iterator()
	if err != nil {
		return nil, err
	}
	_, c, err := iter.Next()
	if err != nil {
		return nil, err
	}

	card := uint64(c.(types.Uint))
	if card < 1 {
		return nil, ErrZeroCardinality
	}

	i := 0
	vals := make([]types.Value, sch.GetAllCols().Size())

	_, err = r.IterSchema(sch, func(tag uint64, val types.Value) (stop bool, err error) {
		if tag == updateTag {
			val = updateVal
		}

		if val != nil {
			vals[i] = types.Uint(tag)
			vals[i+1] = val
			i += 2
		}

		return
	})

	if err != nil {
		return nil, err
	}

	return keylessRowWithCardinality(r.val.Format(), card, vals[:i]...)
}

func (r keylessRow) Format() *types.NomsBinFormat {
	return r.val.Format()
}
