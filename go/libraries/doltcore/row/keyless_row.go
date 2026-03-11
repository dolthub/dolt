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
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// keylessRow is a Row without PRIMARY_KEY fields.
// Stores column values as TaggedValues for the deprecated Row interface.
type keylessRow struct {
	value TaggedValues
}

var _ Row = keylessRow{}

func keylessRowFromTaggedValued(nbf *types.NomsBinFormat, sch schema.Schema, tv TaggedValues) (Row, error) {
	vals := make(TaggedValues)

	err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		v, ok := tv[tag]
		if ok && v.Kind() != types.NullKind {
			vals[tag] = v
		}
		return
	})
	if err != nil {
		return nil, err
	}

	return keylessRow{value: vals}, nil
}

func (r keylessRow) IterSchema(sch schema.Schema, cb func(tag uint64, val types.Value) (stop bool, err error)) (bool, error) {
	err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (bool, error) {
		value, _ := r.GetColVal(tag)
		return cb(tag, value)
	})

	return false, err
}

func (r keylessRow) GetColVal(tag uint64) (val types.Value, ok bool) {
	val, ok = r.value[tag]
	return val, ok
}
