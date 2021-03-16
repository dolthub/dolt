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

const (
	KeylessCardinalityTagIdx = uint64(0)
	KeylessCardinalityValIdx = uint64(1)
	KeylessFirstValIdx       = uint64(2)
)

var ErrZeroCardinality = fmt.Errorf("read row with zero cardinality")

// keylessRow is a Row without PRIMARY_KEY fields
//
// key: Tuple(
// 			Uint(schema.KeylessRowIdTag),
//          UUID(hash.Of(tag1, val1, ..., tagN, valN))
//      )
// val: Tuple(
// 			Uint(schema.KeylessRowCardinalityTag),
//          Uint(cardinality),
//          Uint(tag1), Value(val1),
//            ...
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
	c, err := val.Get(1)
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

func keylessRowFromTaggedValued(nbf *types.NomsBinFormat, sch schema.Schema, tv TaggedValues) (Row, error) {
	vals := make([]types.Value, len(tv)*2)
	i := 0

	err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		v, ok := tv[tag]
		if ok && v.Kind() != types.NullKind {
			vals[i] = types.Uint(tag)
			vals[i+1] = v
			i += 2
		}
		return
	})
	if err != nil {
		return nil, err
	}

	return keylessRowWithCardinality(nbf, 1, vals[:i]...)
}

func keylessRowWithCardinality(nbf *types.NomsBinFormat, card uint64, vals ...types.Value) (Row, error) {
	id, err := types.UUIDHashedFromValues(nbf, vals...) // don't hash cardinality
	if err != nil {
		return nil, err
	}
	idTag := types.Uint(schema.KeylessRowIdTag)

	kt, err := types.NewTuple(nbf, idTag, id)
	if err != nil {
		return nil, err
	}

	prefix := []types.Value{
		types.Uint(schema.KeylessRowCardinalityTag),
		types.Uint(card),
	}
	vals = append(prefix, vals...)

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
	iter, err := r.val.IteratorAt(KeylessFirstValIdx) // skip cardinality tag & val
	if err != nil {
		return false, err
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
	iter, err := r.val.IteratorAt(KeylessFirstValIdx) // skip cardinality tag & val
	if err != nil {
		return false, err
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
	iter, err := r.val.IteratorAt(KeylessCardinalityValIdx) // skip cardinality tag
	if err != nil {
		return nil, err
	}
	_, c, err := iter.Next()
	if err != nil {
		return nil, err
	}
	card := uint64(c.(types.Uint))

	i := 0
	vals := make([]types.Value, sch.GetAllCols().Size()*2)

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

// TaggedValues implements the Row interface.
func (r keylessRow) TaggedValues() (TaggedValues, error) {
	tv := make(TaggedValues)
	_, err := r.IterCols(func(tag uint64, val types.Value) (stop bool, err error) {
		tv[tag] = val
		return false, nil
	})
	return tv, err
}

func (r keylessRow) Format() *types.NomsBinFormat {
	return r.val.Format()
}
