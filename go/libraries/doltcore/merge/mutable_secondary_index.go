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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

// GetMutableSecondaryIdxs returns a MutableSecondaryIdx for each secondary index in |indexes|.
func GetMutableSecondaryIdxs(ctx context.Context, sch schema.Schema, indexes durable.IndexSet) ([]MutableSecondaryIdx, error) {
	mods := make([]MutableSecondaryIdx, sch.Indexes().Count())
	for i, index := range sch.Indexes().AllIndexes() {
		idx, err := indexes.GetIndex(ctx, sch, index.Name())
		if err != nil {
			return nil, err
		}
		m := durable.ProllyMapFromIndex(idx)
		if schema.IsKeyless(sch) {
			m = prolly.ConvertToSecondaryKeylessIndex(m)
		}
		mods[i] = NewMutableSecondaryIdx(m, sch, index)
	}
	return mods, nil
}

// GetMutableSecondaryIdxsWithPending returns a MutableSecondaryIdx for each secondary index in |indexes|. If an index
// is listed in the given |sch|, but does not exist in the given |indexes|, then it is skipped. This is useful when
// merging a schema that has a new index, but the index does not exist on the index set being modified.
func GetMutableSecondaryIdxsWithPending(ctx context.Context, sch schema.Schema, indexes durable.IndexSet, pendingSize int) ([]MutableSecondaryIdx, error) {
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

		idx, err := indexes.GetIndex(ctx, sch, index.Name())
		if err != nil {
			return nil, err
		}
		m := durable.ProllyMapFromIndex(idx)
		if schema.IsKeyless(sch) {
			m = prolly.ConvertToSecondaryKeylessIndex(m)
		}
		newMutableSecondaryIdx := NewMutableSecondaryIdx(m, sch, index)
		newMutableSecondaryIdx.mut = newMutableSecondaryIdx.mut.WithMaxPending(pendingSize)
		mods = append(mods, newMutableSecondaryIdx)
	}
	return mods, nil
}

// MutableSecondaryIdx wraps a prolly.MutableMap of a secondary table index. It
// provides the InsertEntry, UpdateEntry, and DeleteEntry functions which can be
// used to modify the index based on a modification to corresponding primary row.
type MutableSecondaryIdx struct {
	Name    string
	mut     *prolly.MutableMap
	builder index.SecondaryKeyBuilder
}

// NewMutableSecondaryIdx returns a MutableSecondaryIdx. |m| is the secondary idx data.
func NewMutableSecondaryIdx(idx prolly.Map, sch schema.Schema, def schema.Index) MutableSecondaryIdx {
	b := index.NewSecondaryKeyBuilder(sch, def, idx.KeyDesc(), idx.Pool())
	return MutableSecondaryIdx{
		Name:    def.Name(),
		mut:     idx.Mutate(),
		builder: b,
	}
}

// InsertEntry inserts a secondary index entry given the key and new value
// of the primary row.
func (m MutableSecondaryIdx) InsertEntry(ctx context.Context, key, newValue val.Tuple) error {
	newKey := m.builder.SecondaryKeyFromRow(key, newValue)
	err := m.mut.Put(ctx, newKey, val.EmptyTuple)
	if err != nil {
		return nil
	}
	return nil
}

// UpdateEntry modifies the corresponding secondary index entry given the key
// and curr/new values of the primary row.
func (m MutableSecondaryIdx) UpdateEntry(ctx context.Context, key, currValue, newValue val.Tuple) error {
	currKey := m.builder.SecondaryKeyFromRow(key, currValue)
	newKey := m.builder.SecondaryKeyFromRow(key, newValue)

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
	currKey := m.builder.SecondaryKeyFromRow(key, value)
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
