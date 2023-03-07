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
	"encoding/json"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

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

	// Schema merge can introduce new constraints/uniqueness checks.
	for _, index := range finalSch.Indexes().AllIndexes() {

		left, rootOK, err := tryGetIdx(tm.leftSch, leftSet, index.Name())
		if err != nil {
			return nil, err
		}
		_, mergeOK, err := tryGetIdx(tm.rightSch, rightSet, index.Name())
		if err != nil {
			return nil, err
		}
		_, ancOK, err := tryGetIdx(tm.ancSch, ancSet, index.Name())
		if err != nil {
			return nil, err
		}

		mergedIndex, err := func() (durable.Index, error) {
			if !rootOK || !mergeOK || !ancOK {
				return buildIndex(ctx, tm.vrw, tm.ns, finalSch, index, mergedM, artifacts, tm.rightSrc, tm.name)
			}
			return durable.IndexFromProllyMap(left), nil
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

// applyEdit applies |edit| to |idx|. If |len(edit.To)| == 0, then action is
// a delete, if |len(edit.From)| == 0 then it is an insert, otherwise it is an
// update.
func applyEdit(ctx context.Context, idx MutableSecondaryIdx, key, from, to val.Tuple) (err error) {
	if len(from) == 0 {
		err := idx.InsertEntry(ctx, key, to)
		if err != nil {
			return err
		}
	} else if len(to) == 0 {
		err := idx.DeleteEntry(ctx, key, from)
		if err != nil {
			return err
		}
	} else {
		err := idx.UpdateEntry(ctx, key, from, to)
		if err != nil {
			return err
		}
	}
	return nil
}

func emptyDiff(d tree.Diff) bool {
	return d.Key == nil
}
