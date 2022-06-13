// Copyright 2022 Dolthub, Inc.
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

package merge

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

// MutableSecondaryIdx wraps a prolly.MutableMap of a secondary table index. It
// provides the InsertEntry, UpdateEntry, and DeleteEntry functions which can be
// used to modify the index based on a modification to corresponding primary row.
type MutableSecondaryIdx struct {
	Name     string
	mut      prolly.MutableMap
	keyMap   val.OrdinalMapping
	pkLen    int
	keyBld   *val.TupleBuilder
	syncPool pool.BuffPool
}

// NewMutableSecondaryIdx returns a MutableSecondaryIdx. |m| is the secondary idx data.
func NewMutableSecondaryIdx(m prolly.Map, sch schema.Schema, index schema.Index, syncPool pool.BuffPool) MutableSecondaryIdx {
	kD, _ := m.Descriptors()
	return MutableSecondaryIdx{
		index.Name(),
		m.Mutate(),
		creation.GetIndexKeyMapping(sch, index),
		sch.GetPKCols().Size(),
		val.NewTupleBuilder(kD),
		syncPool,
	}
}

// InsertEntry inserts a secondary index entry given the key and new value
// of the primary row.
func (m MutableSecondaryIdx) InsertEntry(ctx context.Context, key, newValue val.Tuple) error {
	newKey := m.mapKeyValue(key, newValue)
	err := m.mut.Put(ctx, newKey, val.EmptyTuple)
	if err != nil {
		return nil
	}
	return nil
}

// UpdateEntry modifies the corresponding secondary index entry given the key
// and curr/new values of the primary row.
func (m MutableSecondaryIdx) UpdateEntry(ctx context.Context, key, currValue, newValue val.Tuple) error {
	currKey := m.mapKeyValue(key, currValue)
	newKey := m.mapKeyValue(key, newValue)

	err := m.mut.Delete(ctx, currKey)
	if err != nil {
		return nil
	}
	err = m.mut.Put(ctx, newKey, val.EmptyTuple)
	if err != nil {
		return nil
	}

	return nil
}

// DeleteEntry deletes a secondary index entry given they key and value of the primary row.
func (m MutableSecondaryIdx) DeleteEntry(ctx context.Context, key val.Tuple, value val.Tuple) error {
	currKey := m.mapKeyValue(key, value)
	err := m.mut.Delete(ctx, currKey)
	if err != nil {
		return nil
	}
	return nil
}

// Map returns the finalized prolly.Map of the underlying prolly.MutableMap.
func (m MutableSecondaryIdx) Map(ctx context.Context) (prolly.Map, error) {
	return m.mut.Map(ctx)
}

// mapKeyValue returns the secondary index entry key given the key and value of
// the corresponding primary row.
func (m MutableSecondaryIdx) mapKeyValue(k, v val.Tuple) val.Tuple {
	for to := range m.keyMap {
		from := m.keyMap.MapOrdinal(to)
		if from < m.pkLen {
			m.keyBld.PutRaw(to, k.GetField(from))
		} else {
			from -= m.pkLen
			m.keyBld.PutRaw(to, v.GetField(from))
		}
	}
	return m.keyBld.Build(m.syncPool)
}
