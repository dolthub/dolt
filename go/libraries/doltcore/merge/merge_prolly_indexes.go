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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

// mergeProllySecondaryIndexes merges the secondary indexes of the given |tbl|,
// |mergeTbl|, and |ancTbl|. It stores the merged indexes into |tableToUpdate|
// and returns its updated value. If |forceIndexRebuild| is true, then all indexes
// will be rebuilt from the table's data, instead of relying on incremental
// changes from the other side of the merge to have been merged in before this
// function was called. This is safer, but less efficient.
func mergeProllySecondaryIndexes(
	ctx *sql.Context,
	tm *TableMerger,
	leftSet, rightSet durable.IndexSet,
	finalSch schema.Schema,
	finalRows durable.Index,
	artifacts *prolly.ArtifactsEditor,
	forceIndexRebuild bool,
) (durable.IndexSet, error) {
	mergedIndexSet, err := durable.NewIndexSet(ctx, tm.vrw, tm.ns)
	if err != nil {
		return nil, err
	}

	mergedM, err := durable.ProllyMapFromIndex(finalRows)
	if err != nil {
		return nil, err
	}

	tryGetIdx := func(sch schema.Schema, iS durable.IndexSet, indexName string) (prolly.Map, bool, error) {
		ok := sch.Indexes().Contains(indexName)
		if ok {
			idx, err := iS.GetIndex(ctx, sch, nil, indexName)
			if err != nil {
				return prolly.Map{}, false, err
			}
			m, err := durable.ProllyMapFromIndex(idx)
			if err != nil {
				return prolly.Map{}, false, err
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

		// If the left (destination) side of the merge doesn't have an index it is supposed to have,
		// then a full rebuild for this index is required.
		rebuildRequired := !rootOK

		// If the index existed on the left (destination) side, before this merge, and differs
		// from the final version we need for the merged schema, then it needs to be rebuilt.
		leftIndexDefinition := tm.leftSch.Indexes().GetByName(index.Name())
		if leftIndexDefinition != nil && leftIndexDefinition.Equals(index) == false {
			rebuildRequired = true
		}

		mergedIndex, err := func() (durable.Index, error) {
			if forceIndexRebuild || rebuildRequired {
				return buildIndex(ctx, tm.vrw, tm.ns, finalSch, index, mergedM, artifacts, tm.rightSrc, tm.name.Name)
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

func buildIndex(
	ctx *sql.Context,
	vrw types.ValueReadWriter,
	ns tree.NodeStore,
	postMergeSchema schema.Schema,
	index schema.Index,
	m prolly.Map,
	artEditor *prolly.ArtifactsEditor,
	theirRootIsh doltdb.Rootish,
	tblName string,
) (durable.Index, error) {
	if index.IsUnique() {
		meta, err := makeUniqViolMeta(postMergeSchema, index)
		if err != nil {
			return nil, err
		}
		vInfo, err := json.Marshal(meta)
		if err != nil {
			return nil, err
		}
		kd := postMergeSchema.GetKeyDescriptor(ns)
		kb := val.NewTupleBuilder(kd)
		p := m.Pool()

		pkMapping := ordinalMappingFromIndex(index)

		mergedMap, err := creation.BuildUniqueProllyIndex(ctx, vrw, ns, postMergeSchema, tblName, index, m, func(ctx context.Context, existingKey, newKey val.Tuple) (err error) {
			eK := getPKFromSecondaryKey(kb, p, pkMapping, existingKey)
			nK := getPKFromSecondaryKey(kb, p, pkMapping, newKey)
			err = replaceUniqueKeyViolation(ctx, artEditor, m, eK, theirRootIsh, vInfo)
			if err != nil {
				return err
			}
			err = replaceUniqueKeyViolation(ctx, artEditor, m, nK, theirRootIsh, vInfo)
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

	mergedIndex, err := creation.BuildSecondaryProllyIndex(ctx, vrw, ns, postMergeSchema, tblName, index, m)
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
