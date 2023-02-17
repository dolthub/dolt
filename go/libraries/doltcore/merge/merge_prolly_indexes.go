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
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

// indexEdit is an edit to a secondary index based on the primary row's key and value.
// The members of tree.Diff have the following meanings:
// - |Key| = the key to apply this edit on
// - |From| = the previous value of this key (If nil, an insert is performed.)
// - |To| = the new value of this key (If nil, a delete is performed.)
//
// It is invalid for |From| and |To| to be both be nil.
type indexEdit interface {
	leftEdit() tree.Diff
	rightEdit() tree.Diff
}

// cellWiseMergeEdit implements indexEdit. It resets the left and updates the
// right to the merged value.
type cellWiseMergeEdit struct {
	left   tree.Diff
	right  tree.Diff
	merged tree.Diff
}

var _ indexEdit = cellWiseMergeEdit{}

func (m cellWiseMergeEdit) leftEdit() tree.Diff {
	// Reset left
	return tree.Diff{
		Key:  m.left.Key,
		From: m.left.To,
		To:   m.left.From,
	}
}

func (m cellWiseMergeEdit) rightEdit() tree.Diff {
	// Update right to merged val
	return tree.Diff{
		Key:  m.merged.Key,
		From: m.right.To,
		To:   m.merged.To,
	}
}

// conflictEdit implements indexEdit and it resets the right value.
type conflictEdit struct {
	right tree.Diff
}

var _ indexEdit = conflictEdit{}

func (c conflictEdit) leftEdit() tree.Diff {
	// Noop left
	return tree.Diff{}
}

func (c conflictEdit) rightEdit() tree.Diff {
	// Reset right
	return tree.Diff{
		Key:  c.right.Key,
		From: c.right.To,
		To:   c.right.From,
	}
}

type confVals struct {
	key      val.Tuple
	ourVal   val.Tuple
	theirVal val.Tuple
	baseVal  val.Tuple
}

// mergeProllySecondaryIndexes merges the secondary indexes of the given |tbl|,
// |mergeTbl|, and |ancTbl|. It stores the merged indexes into |tableToUpdate|
// and returns its updated value.
func mergeProllySecondaryIndexes(
	ctx context.Context,
	tm TableMerger,
	leftSet, rightSet durable.IndexSet,
	finalSch schema.Schema,
	finalRows durable.Index,
	artifacts *prolly.ArtifactsEditor,
) (durable.IndexSet, error) {

	ancSet, err := tm.ancTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}
	mergedIndexSet, err := durable.NewIndexSet(ctx, tm.vrw, tm.ns)
	if err != nil {
		return nil, err
	}

	mergedM := durable.ProllyMapFromIndex(finalRows)

	tryGetIdx := func(sch schema.Schema, iS durable.IndexSet, indexName string) (prolly.Map, bool, error) {
		ok := sch.Indexes().Contains(indexName)
		if ok {
			idx, err := iS.GetIndex(ctx, sch, indexName)
			if err != nil {
				return prolly.Map{}, false, err
			}
			m := durable.ProllyMapFromIndex(idx)
			if schema.IsKeyless(sch) {
				m = prolly.ConvertToSecondaryKeylessIndex(m)
			}
			return m, true, nil
		}
		return prolly.Map{}, false, nil
	}

	// Based on the indexes in the post merge schema, merge the root, merge,
	// and ancestor indexes.
	for _, index := range finalSch.Indexes().AllIndexes() {

		left, rootOK, err := tryGetIdx(tm.leftSch, leftSet, index.Name())
		if err != nil {
			return nil, err
		}
		right, mergeOK, err := tryGetIdx(tm.rightSch, rightSet, index.Name())
		if err != nil {
			return nil, err
		}
		anc, ancOK, err := tryGetIdx(tm.ancSch, ancSet, index.Name())
		if err != nil {
			return nil, err
		}

		mergedIndex, err := func() (durable.Index, error) {
			if !rootOK || !mergeOK || !ancOK {
				return buildIndex(ctx, tm.vrw, tm.ns, finalSch, index, mergedM, artifacts, tm.rightSrc, tm.name)
			}

			if index.IsUnique() {
				err = addUniqIdxViols(ctx, finalSch, index, left, right, anc, mergedM, artifacts, tm.rightSrc, tm.name)
				if err != nil {
					return nil, err
				}
			}

			var collision = false
			merged, _, err := prolly.MergeMaps(ctx, left, right, anc, func(left, right tree.Diff) (tree.Diff, bool) {
				if left.Type == right.Type && bytes.Equal(left.To, right.To) {
					// convergent edit
					return left, true
				}

				collision = true
				return tree.Diff{}, true
			})
			if err != nil {
				return nil, err
			}
			if collision {
				return nil, errors.New("collisions not implemented")
			}
			return durable.IndexFromProllyMap(merged), nil
		}()
		if err != nil {
			return nil, err
		}

		mergedIndexSet, err = mergedIndexSet.PutIndex(ctx, index.Name(), mergedIndex)
		if err != nil {
			return nil, err
		}
	}

	return mergedIndexSet, nil
}

func buildIndex(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, postMergeSchema schema.Schema, index schema.Index, m prolly.Map, artEditor *prolly.ArtifactsEditor, theirRootIsh doltdb.Rootish, tblName string) (durable.Index, error) {
	if index.IsUnique() {
		meta, err := makeUniqViolMeta(postMergeSchema, index)
		if err != nil {
			return nil, err
		}
		vInfo, err := json.Marshal(meta)
		if err != nil {
			return nil, err
		}
		kd := postMergeSchema.GetKeyDescriptor()
		kb := val.NewTupleBuilder(kd)
		p := m.Pool()

		pkMapping := ordinalMappingFromIndex(index)

		mergedMap, err := creation.BuildUniqueProllyIndex(
			ctx,
			vrw,
			ns,
			postMergeSchema,
			index,
			m,
			func(ctx context.Context, existingKey, newKey val.Tuple) (err error) {
				eK := getPKFromSecondaryKey(kb, p, pkMapping, existingKey)
				nK := getPKFromSecondaryKey(kb, p, pkMapping, newKey)
				err = replaceUniqueKeyViolation(ctx, artEditor, m, eK, kd, theirRootIsh, vInfo, tblName)
				if err != nil {
					return err
				}
				err = replaceUniqueKeyViolation(ctx, artEditor, m, nK, kd, theirRootIsh, vInfo, tblName)
				if err != nil {
					return err
				}
				return nil
			})
		if err != nil {
			return nil, err
		}
		return mergedMap, nil
	}

	mergedIndex, err := creation.BuildSecondaryProllyIndex(ctx, vrw, ns, postMergeSchema, index, m)
	if err != nil {
		return nil, err
	}
	return mergedIndex, nil
}

// Given cellWiseMergeEdit's sent on |cellWiseChan|, update the secondary indexes in
// |rootIndexSet| and |mergeIndexSet| such that when the index sets are merged,
// they produce entries consistent with the cell-wise merges. The updated
// |rootIndexSet| and |mergeIndexSet| are returned.
func updateProllySecondaryIndexes(ctx context.Context, tm TableMerger, cellWiseEdits chan indexEdit) (durable.IndexSet, durable.IndexSet, error) {
	ls, err := tm.leftTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, nil, err
	}
	lm, err := GetMutableSecondaryIdxs(ctx, tm.leftSch, ls)
	if err != nil {
		return nil, nil, err
	}

	rs, err := tm.rightTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, nil, err
	}
	rm, err := GetMutableSecondaryIdxs(ctx, tm.rightSch, rs)
	if err != nil {
		return nil, nil, err
	}

	err = applyCellwiseEdits(ctx, lm, rm, cellWiseEdits)
	if err != nil {
		return nil, nil, err
	}

	ls, err = persistIndexMuts(ctx, ls, lm)
	if err != nil {
		return nil, nil, err
	}

	rs, err = persistIndexMuts(ctx, rs, rm)
	if err != nil {
		return nil, nil, err
	}

	return ls, rs, nil
}

func persistIndexMuts(ctx context.Context, indexSet durable.IndexSet, idxs []MutableSecondaryIdx) (durable.IndexSet, error) {
	for _, idx := range idxs {
		m, err := idx.Map(ctx)
		if err != nil {
			return nil, err
		}
		indexSet, err = indexSet.PutIndex(ctx, idx.Name, durable.IndexFromProllyMap(m))
		if err != nil {
			return nil, err
		}
	}
	return indexSet, nil
}

func applyCellwiseEdits(ctx context.Context, rootIdxs, mergeIdxs []MutableSecondaryIdx, edits chan indexEdit) error {
	for {
		select {
		case e, ok := <-edits:
			if !ok {
				return nil
			}
			// See cellWiseMergeEdit and conflictEdit for implementations of leftEdit and rightEdit
			if edit := e.leftEdit(); !emptyDiff(edit) {
				for _, idx := range rootIdxs {
					if err := applyEdit(ctx, idx, edit); err != nil {
						return err
					}
				}
			}
			if edit := e.rightEdit(); !emptyDiff(edit) {
				for _, idx := range mergeIdxs {
					if err := applyEdit(ctx, idx, edit); err != nil {
						return err
					}
				}
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// applyEdit applies |edit| to |idx|. If |len(edit.To)| == 0, then action is
// a delete, if |len(edit.From)| == 0 then it is an insert, otherwise it is an
// update.
func applyEdit(ctx context.Context, idx MutableSecondaryIdx, edit tree.Diff) (err error) {
	if len(edit.From) == 0 {
		err := idx.InsertEntry(ctx, val.Tuple(edit.Key), val.Tuple(edit.To))
		if err != nil {
			return err
		}
	} else if len(edit.To) == 0 {
		err := idx.DeleteEntry(ctx, val.Tuple(edit.Key), val.Tuple(edit.From))
		if err != nil {
			return err
		}
	} else {
		err := idx.UpdateEntry(ctx, val.Tuple(edit.Key), val.Tuple(edit.From), val.Tuple(edit.To))
		if err != nil {
			return err
		}
	}
	return nil
}

func emptyDiff(d tree.Diff) bool {
	return d.Key == nil
}
