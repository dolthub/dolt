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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

// GetMutableSecondaryIdxs returns a MutableSecondaryIdx for each secondary index in |indexes|.
func GetMutableSecondaryIdxs(ctx *sql.Context, ourSch, sch schema.Schema, tableName string, indexes durable.IndexSet) ([]MutableSecondaryIdx, error) {
	mods := make([]MutableSecondaryIdx, sch.Indexes().Count())
	for i, index := range sch.Indexes().AllIndexes() {
		idx, err := indexes.GetIndex(ctx, sch, nil, index.Name())
		if err != nil {
			return nil, err
		}
		m := durable.ProllyMapFromIndex(idx)
		if schema.IsKeyless(sch) {
			m = prolly.ConvertToSecondaryKeylessIndex(m)
		}
		mods[i], err = NewMutableSecondaryIdx(ctx, m, ourSch, sch, tableName, index)
		if err != nil {
			return nil, err
		}
	}
	return mods, nil
}

// GetMutableSecondaryIdxsWithPending returns a MutableSecondaryIdx for each secondary index in |indexes|. If an index
// is listed in the given |sch|, but does not exist in the given |indexes|, then it is skipped. This is useful when
// merging a schema that has a new index, but the index does not exist on the index set being modified.
func GetMutableSecondaryIdxsWithPending(ctx *sql.Context, ourSch, sch schema.Schema, tableName string, indexes durable.IndexSet, pendingSize int) ([]MutableSecondaryIdx, error) {
	mods := make([]MutableSecondaryIdx, 0, sch.Indexes().Count())
	for _, index := range sch.Indexes().AllIndexes() {

		// If an index isn't found on the left side, we know it must be a new index added on the right side,
		// so just skip it, and we'll rebuild the full index at the end of merging when we notice it's missing.
		// TODO: GetMutableSecondaryIdxs should get this same treatment, or we should have a flag that
		//       allows skipping over missing indexes. Seems like we could refactor this code to remove
		//       the duplication.
		hasIndex, err := indexes.HasIndex(ctx, index.Name())
		if err != nil {
			return nil, err
		}
		if !hasIndex {
			continue
		}

		idx, err := indexes.GetIndex(ctx, sch, nil, index.Name())
		if err != nil {
			return nil, err
		}
		m := durable.ProllyMapFromIndex(idx)

		// If the schema has changed, don't reuse the index.
		// TODO: This isn't technically required, but correctly handling updating secondary indexes when only some
		// of the table's rows have been updated is difficult to get right.
		// Dropping the index is potentially slower but guarenteed to be correct.
		if !m.KeyDesc().Equals(index.Schema().GetKeyDescriptorWithNoConversion()) {
			continue
		}

		if !m.ValDesc().Equals(index.Schema().GetValueDescriptor()) {
			continue
		}

		if schema.IsKeyless(sch) {
			m = prolly.ConvertToSecondaryKeylessIndex(m)
		}
		newMutableSecondaryIdx, err := NewMutableSecondaryIdx(ctx, m, ourSch, sch, tableName, index)
		if err != nil {
			return nil, err
		}

		newMutableSecondaryIdx.mut = newMutableSecondaryIdx.mut.WithMaxPending(pendingSize)
		mods = append(mods, newMutableSecondaryIdx)
	}
	return mods, nil
}

// MutableSecondaryIdx wraps a prolly.MutableMap of a secondary table index. It
// provides the InsertEntry, UpdateEntry, and DeleteEntry functions which can be
// used to modify the index based on a modification to corresponding primary row.
type MutableSecondaryIdx struct {
	Name                       string
	mut                        *prolly.MutableMap
	leftBuilder, mergedBuilder index.SecondaryKeyBuilder
}

// NewMutableSecondaryIdx returns a MutableSecondaryIdx. |m| is the secondary idx data.
func NewMutableSecondaryIdx(ctx *sql.Context, idx prolly.Map, ourSch, mergedSch schema.Schema, tableName string, def schema.Index) (MutableSecondaryIdx, error) {
	leftBuilder, err := index.NewSecondaryKeyBuilder(ctx, tableName, ourSch, def, idx.KeyDesc(), idx.Pool(), idx.NodeStore())
	mergedBuilder, err := index.NewSecondaryKeyBuilder(ctx, tableName, mergedSch, def, idx.KeyDesc(), idx.Pool(), idx.NodeStore())
	if err != nil {
		return MutableSecondaryIdx{}, err
	}

	return MutableSecondaryIdx{
		Name:          def.Name(),
		mut:           idx.Mutate(),
		leftBuilder:   leftBuilder,
		mergedBuilder: mergedBuilder,
	}, nil
}

// InsertEntry inserts a secondary index entry given the key and new value
// of the primary row.
func (m MutableSecondaryIdx) InsertEntry(ctx context.Context, key, newValue val.Tuple) error {
	newKey, err := m.mergedBuilder.SecondaryKeyFromRow(ctx, key, newValue)
	if err != nil {
		return err
	}

	// secondary indexes only use their key tuple
	err = m.mut.Put(ctx, newKey, val.EmptyTuple)
	if err != nil {
		return err
	}
	return nil
}

// UpdateEntry modifies the corresponding secondary index entry given the key
// and curr/new values of the primary row.
func (m MutableSecondaryIdx) UpdateEntry(ctx context.Context, key, currValue, newValue val.Tuple) error {
	currKey, err := m.leftBuilder.SecondaryKeyFromRow(ctx, key, currValue)
	if err != nil {
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			currKey, err = m.leftBuilder.SecondaryKeyFromRow(ctx, key, currValue)
			_ = currKey
		}
	}()
	newKey, err := m.mergedBuilder.SecondaryKeyFromRow(ctx, key, newValue)
	if err != nil {
		return err
	}

	err = m.mut.Delete(ctx, currKey)
	if err != nil {
		return err
	}
	return m.mut.Put(ctx, newKey, val.EmptyTuple)
}

// DeleteEntry deletes a secondary index entry given they key and value of the primary row.
func (m MutableSecondaryIdx) DeleteEntry(ctx context.Context, key val.Tuple, value val.Tuple) error {
	currKey, err := m.leftBuilder.SecondaryKeyFromRow(ctx, key, value)
	if err != nil {
		return err
	}

	return m.mut.Delete(ctx, currKey)
}

// Map returns the finalized prolly.Map of the underlying prolly.MutableMap.
func (m MutableSecondaryIdx) Map(ctx context.Context) (prolly.Map, error) {
	return m.mut.Map(ctx)
}
