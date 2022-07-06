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
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/shim"
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
	leftEdit() *tree.Diff
	rightEdit() *tree.Diff
}

// cellWiseMergeEdit implements indexEdit. It resets the left and updates the
// right to the merged value.
type cellWiseMergeEdit struct {
	left   tree.Diff
	right  tree.Diff
	merged tree.Diff
}

var _ indexEdit = cellWiseMergeEdit{}

func (m cellWiseMergeEdit) leftEdit() *tree.Diff {
	// Reset left
	return &tree.Diff{
		Key:  m.left.Key,
		From: m.left.To,
		To:   m.left.From,
	}
}

func (m cellWiseMergeEdit) rightEdit() *tree.Diff {
	// Update right to merged val
	return &tree.Diff{
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

func (c conflictEdit) leftEdit() *tree.Diff {
	// Noop left
	return nil
}

func (c conflictEdit) rightEdit() *tree.Diff {
	// Reset right
	return &tree.Diff{
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
	vrw types.ValueReadWriter,
	ns tree.NodeStore,
	postMergeSchema, rootSch, mergeSch, ancSch schema.Schema,
	mergedData durable.Index,
	tbl, mergeTbl, tableToUpdate *doltdb.Table,
	ancSet durable.IndexSet,
	artEditor prolly.ArtifactsEditor,
	theirRootIsh hash.Hash,
	tblName string) (*doltdb.Table, error) {

	rootSet, err := tbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}
	mergeSet, err := mergeTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}
	mergedSet, err := mergeProllyIndexSets(
		ctx,
		vrw,
		ns,
		postMergeSchema, rootSch, mergeSch, ancSch,
		mergedData,
		rootSet, mergeSet, ancSet,
		artEditor,
		theirRootIsh,
		tblName)
	if err != nil {
		return nil, err
	}
	updatedTbl, err := tableToUpdate.SetIndexSet(ctx, mergedSet)
	if err != nil {
		return nil, err
	}
	return updatedTbl, nil
}

// mergeProllyIndexSets merges the |root|, |merge|, and |anc| index sets based
// on the provided |postMergeSchema|. It returns the merged index set.
func mergeProllyIndexSets(
	ctx context.Context,
	vrw types.ValueReadWriter,
	ns tree.NodeStore,
	postMergeSchema, rootSch, mergeSch, ancSch schema.Schema,
	mergedData durable.Index,
	root, merge, anc durable.IndexSet,
	artEditor prolly.ArtifactsEditor,
	theirRootIsh hash.Hash,
	tblName string) (durable.IndexSet, error) {
	mergedIndexSet := durable.NewIndexSet(ctx, vrw, ns)

	mergedM := durable.ProllyMapFromIndex(mergedData)

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
	for _, index := range postMergeSchema.Indexes().AllIndexes() {

		rootI, rootOK, err := tryGetIdx(rootSch, root, index.Name())
		if err != nil {
			return nil, err
		}
		mergeI, mergeOK, err := tryGetIdx(mergeSch, merge, index.Name())
		if err != nil {
			return nil, err
		}
		ancI, ancOK, err := tryGetIdx(ancSch, anc, index.Name())
		if err != nil {
			return nil, err
		}

		mergedIndex, err := func() (durable.Index, error) {
			if !rootOK || !mergeOK || !ancOK {
				return buildIndex(ctx, vrw, ns, postMergeSchema, index, mergedM, artEditor, theirRootIsh, tblName)
			}

			if index.IsUnique() {
				err = addUniqIdxViols(ctx, postMergeSchema, index, rootI, mergeI, ancI, mergedM, artEditor, theirRootIsh, tblName)
				if err != nil {
					return nil, err
				}
			}

			var collision = false
			merged, err := prolly.MergeMaps(ctx, rootI, mergeI, ancI, func(left, right tree.Diff) (tree.Diff, bool) {
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

func buildIndex(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, postMergeSchema schema.Schema, index schema.Index, m prolly.Map, artEditor prolly.ArtifactsEditor, theirRootIsh hash.Hash, tblName string) (durable.Index, error) {
	if index.IsUnique() {
		meta, err := makeUniqViolMeta(postMergeSchema, index)
		if err != nil {
			return nil, err
		}
		vInfo, err := json.Marshal(meta)
		if err != nil {
			return nil, err
		}
		kd := shim.KeyDescriptorFromSchema(postMergeSchema)
		kb := val.NewTupleBuilder(kd)
		p := m.Pool()

		mergedMap, err := creation.BuildUniqueProllyIndex(
			ctx,
			vrw,
			ns,
			postMergeSchema,
			index,
			m,
			func(ctx context.Context, existingKey, newKey val.Tuple) (err error) {
				eK := getSuffix(kb, p, existingKey)
				nK := getSuffix(kb, p, newKey)
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
func updateProllySecondaryIndexes(
	ctx context.Context,
	cellWiseChan chan indexEdit,
	rootSchema, mergeSchema schema.Schema,
	tbl, mergeTbl *doltdb.Table,
	rootIndexSet, mergeIndexSet durable.IndexSet) (durable.IndexSet, durable.IndexSet, error) {

	rootIdxs, err := getMutableSecondaryIdxs(ctx, rootSchema, tbl)
	if err != nil {
		return nil, nil, err
	}
	mergeIdxs, err := getMutableSecondaryIdxs(ctx, mergeSchema, mergeTbl)
	if err != nil {
		return nil, nil, err
	}

OUTER:
	for {
		select {
		case e, ok := <-cellWiseChan:
			if !ok {
				break OUTER
			}
			// See cellWiseMergeEdit and conflictEdit for implementations of leftEdit and rightEdit
			if edit := e.leftEdit(); edit != nil {
				for _, idx := range rootIdxs {
					err := applyEdit(ctx, idx, edit)
					if err != nil {
						return nil, nil, err
					}
				}
			}
			if edit := e.rightEdit(); edit != nil {
				for _, idx := range mergeIdxs {
					err := applyEdit(ctx, idx, edit)
					if err != nil {
						return nil, nil, err
					}
				}
			}
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}

	persistIndexMuts := func(indexSet durable.IndexSet, idxs []MutableSecondaryIdx) (durable.IndexSet, error) {
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

	updatedRootIndexSet, err := persistIndexMuts(rootIndexSet, rootIdxs)
	if err != nil {
		return nil, nil, err
	}

	updatedMergeIndexSet, err := persistIndexMuts(mergeIndexSet, mergeIdxs)
	if err != nil {
		return nil, nil, err
	}

	return updatedRootIndexSet, updatedMergeIndexSet, nil
}

// getMutableSecondaryIdxs returns a MutableSecondaryIdx for each secondary index
// defined in |schema| and |tbl|.
func getMutableSecondaryIdxs(ctx context.Context, sch schema.Schema, tbl *doltdb.Table) ([]MutableSecondaryIdx, error) {
	indexSet, err := tbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	mods := make([]MutableSecondaryIdx, sch.Indexes().Count())
	for i, index := range sch.Indexes().AllIndexes() {
		idx, err := indexSet.GetIndex(ctx, sch, index.Name())
		if err != nil {
			return nil, err
		}
		m := durable.ProllyMapFromIndex(idx)
		if schema.IsKeyless(sch) {
			m = prolly.ConvertToSecondaryKeylessIndex(m)
		}

		mods[i] = NewMutableSecondaryIdx(m, sch, index, m.Pool())
	}

	return mods, nil
}

// applyEdit applies |edit| to |idx|. If |len(edit.To)| == 0, then action is
// a delete, if |len(edit.From)| == 0 then it is an insert, otherwise it is an
// update.
func applyEdit(ctx context.Context, idx MutableSecondaryIdx, edit *tree.Diff) error {
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
