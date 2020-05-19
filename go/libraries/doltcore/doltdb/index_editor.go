// Copyright 2020 Liquidata, Inc.
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
	"fmt"
	"io"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// IndexEditor takes in changes to an index map and returns the updated map if changes have been made.
// This type is not thread-safe, and is intended for use in a single-threaded environment.
type IndexEditor struct {
	keyCount map[hash.Hash]int64
	ed       *types.MapEditor
	data     types.Map
	idx      schema.Index
	idxSch   schema.Schema // idx.Schema() builds the schema every call, so we cache it here
}

func NewIndexEditor(index schema.Index, indexData types.Map) *IndexEditor {
	return &IndexEditor{
		keyCount: make(map[hash.Hash]int64),
		data:     indexData,
		idx:      index,
		idxSch:   index.Schema(),
	}
}

// HasChanges returns whether the editor has any changes that need to be applied.
func (indexEd *IndexEditor) HasChanges() bool {
	return indexEd.ed != nil
}

// Index returns the index used for this editor.
func (indexEd *IndexEditor) Index() schema.Index {
	return indexEd.idx
}

// Map applies all edits and returns a newly updated Map.
func (indexEd *IndexEditor) Map(ctx context.Context) (types.Map, error) {
	if !indexEd.HasChanges() {
		return indexEd.data, nil
	}
	if indexEd.Index().IsUnique() {
		for _, numOfKeys := range indexEd.keyCount {
			if numOfKeys > 1 {
				return types.EmptyMap, fmt.Errorf("UNIQUE constraint violation on index: %s", indexEd.idx.Name())
			}
		}
	}
	return indexEd.ed.Map(ctx)
}

// UpdateIndex updates the index map according to the given reduced index rows.
func (indexEd *IndexEditor) UpdateIndex(ctx context.Context, originalIndexRow row.Row, updatedIndexRow row.Row) error {
	if row.AreEqual(originalIndexRow, updatedIndexRow, indexEd.idxSch) {
		return nil
	}

	if originalIndexRow != nil {
		indexKey, err := originalIndexRow.NomsMapKey(indexEd.idxSch).Value(ctx)
		if err != nil {
			return err
		}
		if indexEd.ed == nil {
			indexEd.ed = indexEd.data.Edit()
		}
		if indexEd.idx.IsUnique() {
			partialKey, err := originalIndexRow.ReduceToIndexPartialKey(indexEd.idx)
			if err != nil {
				return err
			}
			partialKeyHash, err := partialKey.Hash(originalIndexRow.Format())
			if err != nil {
				return err
			}
			indexEd.keyCount[partialKeyHash]--
		}
		indexEd.ed.Remove(indexKey)
	}
	if updatedIndexRow != nil {
		indexKey, err := updatedIndexRow.NomsMapKey(indexEd.idxSch).Value(ctx)
		if err != nil {
			return err
		}
		if indexEd.ed == nil {
			indexEd.ed = indexEd.data.Edit()
		}
		if indexEd.idx.IsUnique() {
			partialKey, err := updatedIndexRow.ReduceToIndexPartialKey(indexEd.idx)
			if err != nil {
				return err
			}
			partialKeyHash, err := partialKey.Hash(updatedIndexRow.Format())
			if err != nil {
				return err
			}
			var mapIter table.TableReadCloser = noms.NewNomsRangeReader(indexEd.idxSch, indexEd.data,
				[]*noms.ReadRange{{Start: partialKey, Inclusive: true, Reverse: false, Check: func(tuple types.Tuple) (bool, error) {
					return tuple.StartsWith(partialKey), nil
				}}})
			_, err = mapIter.ReadRow(ctx)
			if err == nil { // row exists
				indexEd.keyCount[partialKeyHash]++
			} else if err != io.EOF {
				return err
			}
			indexEd.keyCount[partialKeyHash]++
		}
		indexEd.ed.Set(indexKey, updatedIndexRow.NomsMapValue(indexEd.idxSch))
	}

	return nil
}
