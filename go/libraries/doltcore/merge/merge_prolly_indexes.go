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
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
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
	postMergeSchema, rootSch, mergeSch, ancSch schema.Schema,
	mergedData durable.Index,
	tbl, mergeTbl, tblToUpdate *doltdb.Table,
	ancSet durable.IndexSet,
	artEditor prolly.ArtifactsEditor,
	cmHash hash.Hash) (*doltdb.Table, error) {
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
		postMergeSchema, rootSch, mergeSch, ancSch,
		mergedData,
		rootSet, mergeSet, ancSet,
		artEditor,
		cmHash)
	if err != nil {
		return nil, err
	}
	updatedTbl, err := tblToUpdate.SetIndexSet(ctx, mergedSet)
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
	postMergeSchema, rootSch, mergeSch, ancSch schema.Schema,
	mergedData durable.Index,
	root, merge, anc durable.IndexSet,
	artEditor prolly.ArtifactsEditor,
	cmHash hash.Hash) (durable.IndexSet, error) {
	mergedIndexSet := durable.NewIndexSet(ctx, vrw)

	tryGetIdx := func(sch schema.Schema, iS durable.IndexSet, indexName string) (idx durable.Index, ok bool, err error) {
		ok = sch.Indexes().Contains(indexName)
		if ok {
			idx, err = iS.GetIndex(ctx, sch, indexName)
			if err != nil {
				return nil, false, err
			}
			return idx, true, nil
		}
		return nil, false, nil
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
				mergedIndex, err := creation.BuildSecondaryProllyIndex(ctx, vrw, postMergeSchema, index, durable.ProllyMapFromIndex(mergedData))
				if err != nil {
					return nil, err
				}
				return mergedIndex, nil
			}

			left := durable.ProllyMapFromIndex(rootI)
			right := durable.ProllyMapFromIndex(mergeI)
			base := durable.ProllyMapFromIndex(ancI)
			primary := durable.ProllyMapFromIndex(mergedData)

			m, err := mergeProllySecondaryIdx(ctx, left, right, base, primary, mergeSch, index, artEditor, cmHash)
			if err != nil {
				return nil, err
			}
			return durable.IndexFromProllyMap(m), nil
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

func mergeProllySecondaryIdx(ctx context.Context, left, right, base, primary prolly.Map, sch schema.Schema, index schema.Index, artEditor prolly.ArtifactsEditor, cmHash hash.Hash) (prolly.Map, error) {
	if index.IsUnique() {
		return mergeProllyUniqueIdx(ctx, left, right, base, primary, sch, index, artEditor, cmHash)
	}

	var collision = false
	merged, err := prolly.MergeMaps(ctx, left, right, base, func(left, right tree.Diff) (tree.Diff, bool) {
		collision = true
		return tree.Diff{}, true
	})
	if err != nil {
		return prolly.Map{}, err
	}
	if collision {
		return prolly.Map{}, errors.New("collisions not implemented")
	}
	return merged, nil
}

func mergeProllyUniqueIdx(ctx context.Context, left, right, base, primary prolly.Map, sch schema.Schema, index schema.Index, artEditor prolly.ArtifactsEditor, cmHash hash.Hash) (prolly.Map, error) {
	idxDesc := shim.KeyDescriptorFromSchema(index.Schema())
	partialDesc := makePartialDescriptor(idxDesc, index.Count())
	partialKB := val.NewTupleBuilder(partialDesc)
	pool := left.Pool()

	suffixDesc := makeSuffixDescriptor(idxDesc, idxDesc.Count()-index.Count())
	suffixKB := val.NewTupleBuilder(suffixDesc)

	m, err := makeUniqViolMeta(sch, index)
	if err != nil {
		return prolly.Map{}, err
	}
	vInfo, err := json.Marshal(m)
	if err != nil {
		return prolly.Map{}, err
	}

	var collision = false
	merged, err := prolly.FilterMergeMaps(ctx, left, right, base,
		func(left, right tree.Diff) (tree.Diff, bool) {
			collision = true
			return tree.Diff{}, true
		},
		func(ctx context.Context, diff tree.Diff) (bool, error) {
			if diff.Type != tree.AddedDiff {
				return true, nil
			}

			// partial scan the left to see if we have this key already

			// TODO (dhruv):
			// 	I want an iter that can update its range. When the
			// 	new range's start is greater than the current cursor, it seeks
			// 	to the start of the updated range.

			prefix := getPrefix(partialKB, pool, val.Tuple(diff.Key))
			rng := prolly.ClosedRange(prefix, prefix, partialDesc)
			itr, err := left.IterRange(ctx, rng)
			if err != nil {
				return false, err
			}

			// We only need to itr once. Left either doesn't have the partial key, has the partial key and it is a
			// convergent edit, or has the partial key and it is a unique key conflict.
			k, _, err := itr.Next(ctx)
			if err != nil && err != io.EOF {
				return false, err
			}
			if err == io.EOF {
				// no partial key
				return true, nil
			}

			left := getSuffix(suffixKB, pool, k)
			right := getSuffix(suffixKB, pool, val.Tuple(diff.Key))
			if bytes.Compare(left, right) == 0 {
				// convergent edit
				return true, nil
			}

			// TODO: throw a unique key violation for the right's pk

			err = addUniqueKeyViolation(ctx, artEditor, primary, right, cmHash, vInfo)
			if err != nil {
				return false, err
			}

			// Eventually we want to filter the edit, but we can't do it when
			// merging secondary indexes. It needs to be done when we merge
			// primary data because we need to update the other indexes too.
			return true, nil
		})
	if collision {
		return prolly.Map{}, errors.New("collisions not implemented")
	}

	return merged, nil
}

func addUniqueKeyViolation(ctx context.Context, edt prolly.ArtifactsEditor, m prolly.Map, k val.Tuple, cmHash hash.Hash, vInfo []byte) error {
	var value val.Tuple
	err := m.Get(ctx, k, func(_, v val.Tuple) error {
		value = v
		return nil
	})
	if err != nil {
		return err
	}

	meta := prolly.ConstraintViolationMeta{
		VInfo: vInfo,
		Value: value,
	}
	d, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	err = edt.Add(ctx, k, cmHash[:], prolly.ArtifactTypeUniqueKeyViol, d)
	if err != nil {
		return err
	}

	return nil
}

func getPrefix(pKB *val.TupleBuilder, pool pool.BuffPool, k val.Tuple) val.Tuple {
	n := pKB.Desc.Count()
	for i := 0; i < n; i++ {
		pKB.PutRaw(i, k.GetField(i))
	}
	return pKB.Build(pool)
}

func makeSuffixDescriptor(d val.TupleDesc, n int) val.TupleDesc {
	return val.NewTupleDescriptor(d.Types[len(d.Types)-n:]...)
}

func getSuffix(sKB *val.TupleBuilder, pool pool.BuffPool, k val.Tuple) val.Tuple {
	n := sKB.Desc.Count()
	m := k.Count()
	for i, j := 0, m-n; j < k.Count(); i, j = i+1, j+1 {
		sKB.PutRaw(i, k.GetField(j))
	}
	return sKB.Build(pool)
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
func getMutableSecondaryIdxs(ctx context.Context, schema schema.Schema, tbl *doltdb.Table) ([]MutableSecondaryIdx, error) {
	indexSet, err := tbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	mods := make([]MutableSecondaryIdx, schema.Indexes().Count())
	for i, index := range schema.Indexes().AllIndexes() {
		idx, err := indexSet.GetIndex(ctx, schema, index.Name())
		if err != nil {
			return nil, err
		}
		m := durable.ProllyMapFromIndex(idx)

		mods[i] = NewMutableSecondaryIdx(m, schema, index, m.Pool())
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
