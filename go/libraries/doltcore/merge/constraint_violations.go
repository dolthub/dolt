// Copyright 2021 Dolthub, Inc.
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
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	json2 "github.com/dolthub/dolt/go/libraries/doltcore/sqle/json"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	diff2 "github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/types"
)

// constraintViolationsLoadedTable is a collection of items needed to process constraint violations for a single table.
type constraintViolationsLoadedTable struct {
	TableName   string
	Table       *doltdb.Table
	Schema      schema.Schema
	RowData     types.Map
	Index       schema.Index
	IndexSchema schema.Schema
	IndexData   types.Map
}

// cvType is an enum for a constraint violation type.
type cvType uint64

const (
	cvType_ForeignKey cvType = iota + 1
	cvType_UniqueIndex
	cvType_CheckConstraint
)

// AddConstraintViolations adds all constraint violations to each table.
func AddConstraintViolations(ctx context.Context, newRoot, baseRoot *doltdb.RootValue, tables *set.StrSet) (*doltdb.RootValue, *set.StrSet, error) {
	fkColl, err := newRoot.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, nil, err
	}
	foundViolationsSet := set.NewStrSet(nil)
	for _, foreignKey := range fkColl.AllKeys() {
		if !foreignKey.IsResolved() || (tables.Size() != 0 && !tables.Contains(foreignKey.TableName)) {
			continue
		}

		postParent, ok, err := newConstraintViolationsLoadedTable(ctx, foreignKey.ReferencedTableName, foreignKey.ReferencedTableIndex, newRoot)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			return nil, nil, fmt.Errorf("foreign key %s should have index %s on table %s but it cannot be found",
				foreignKey.Name, foreignKey.ReferencedTableIndex, foreignKey.ReferencedTableName)
		}

		postChild, ok, err := newConstraintViolationsLoadedTable(ctx, foreignKey.TableName, foreignKey.TableIndex, newRoot)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			return nil, nil, fmt.Errorf("foreign key %s should have index %s on table %s but it cannot be found",
				foreignKey.Name, foreignKey.TableIndex, foreignKey.TableName)
		}

		foundViolations := false
		preParent, _, err := newConstraintViolationsLoadedTable(ctx, foreignKey.ReferencedTableName, "", baseRoot)
		if err != nil {
			if err != doltdb.ErrTableNotFound {
				return nil, nil, err
			}
			// Parent does not exist in the ancestor so we use an empty map
			emptyMap, err := types.NewMap(ctx, postParent.Table.ValueReadWriter())
			if err != nil {
				return nil, nil, err
			}
			postChild.Table, foundViolations, err = parentFkConstraintViolations(ctx, foreignKey, postParent, postChild, postParent.Schema, emptyMap)
			if err != nil {
				return nil, nil, err
			}
		} else {
			// Parent exists in the ancestor
			postChild.Table, foundViolations, err = parentFkConstraintViolations(ctx, foreignKey, postParent, postChild, preParent.Schema, preParent.RowData)
			if err != nil {
				return nil, nil, err
			}
		}

		preChild, _, err := newConstraintViolationsLoadedTable(ctx, foreignKey.TableName, "", baseRoot)
		if err != nil {
			if err != doltdb.ErrTableNotFound {
				return nil, nil, err
			}
			innerFoundViolations := false
			// Child does not exist in the ancestor so we use an empty map
			emptyMap, err := types.NewMap(ctx, postChild.Table.ValueReadWriter())
			if err != nil {
				return nil, nil, err
			}
			postChild.Table, innerFoundViolations, err = childFkConstraintViolations(ctx, foreignKey, postParent, postChild, postChild.Schema, emptyMap)
			if err != nil {
				return nil, nil, err
			}
			foundViolations = foundViolations || innerFoundViolations
		} else {
			// Child exists in the ancestor
			innerFoundViolations := false
			postChild.Table, innerFoundViolations, err = childFkConstraintViolations(ctx, foreignKey, postParent, postChild, preChild.Schema, preChild.RowData)
			if err != nil {
				return nil, nil, err
			}
			foundViolations = foundViolations || innerFoundViolations
		}

		newRoot, err = newRoot.PutTable(ctx, postChild.TableName, postChild.Table)
		if err != nil {
			return nil, nil, err
		}
		if foundViolations {
			foundViolationsSet.Add(postChild.TableName)
		}
	}
	return newRoot, foundViolationsSet, nil
}

// parentFkConstraintViolations processes foreign key constraint violations for the parent in a foreign key.
func parentFkConstraintViolations(
	ctx context.Context,
	foreignKey doltdb.ForeignKey,
	postParent, postChild *constraintViolationsLoadedTable,
	preParentSch schema.Schema,
	preParentRowData types.Map,
) (*doltdb.Table, bool, error) {
	foundViolations := false
	postParentIndexTags := postParent.Index.IndexedColumnTags()
	postChildIndexTags := postChild.Index.IndexedColumnTags()
	postChildCVMap, err := postChild.Table.GetConstraintViolations(ctx)
	if err != nil {
		return nil, false, err
	}
	postChildCVMapEditor := postChildCVMap.Edit()

	differ := diff.NewRowDiffer(ctx, preParentSch, postParent.Schema, 1024)
	defer differ.Close()
	differ.Start(ctx, preParentRowData, postParent.RowData)
	for {
		diffSlice, hasMore, err := differ.GetDiffs(1, 10*time.Second)
		if err != nil {
			return nil, false, err
		}
		if len(diffSlice) != 1 {
			if hasMore {
				return nil, false, fmt.Errorf("no diff returned but should have errored earlier")
			}
			break
		}
		rowDiff := diffSlice[0]
		switch rowDiff.ChangeType {
		case types.DiffChangeRemoved, types.DiffChangeModified:
			postParentRow, err := row.FromNoms(postParent.Schema, rowDiff.KeyValue.(types.Tuple), rowDiff.OldValue.(types.Tuple))
			if err != nil {
				return nil, false, err
			}
			hasNulls := false
			for _, tag := range postParentIndexTags {
				if postParentRowEntry, ok := postParentRow.GetColVal(tag); !ok || types.IsNull(postParentRowEntry) {
					hasNulls = true
					break
				}
			}
			if hasNulls {
				continue
			}

			postParentIndexPartialKey, err := row.ReduceToIndexPartialKey(postParent.Index, postParentRow)
			if err != nil {
				return nil, false, err
			}

			shouldContinue, err := func() (bool, error) {
				var mapIter table.TableReadCloser = noms.NewNomsRangeReader(postParent.IndexSchema, postParent.IndexData,
					[]*noms.ReadRange{{Start: postParentIndexPartialKey, Inclusive: true, Reverse: false, Check: noms.InRangeCheckPartial(postParentIndexPartialKey)}})
				defer mapIter.Close(ctx)
				if _, err := mapIter.ReadRow(ctx); err == nil {
					// If the parent has more rows that satisfy the partial key then we choose to do nothing
					return true, nil
				} else if err != io.EOF {
					return false, err
				}
				return false, nil
			}()
			if err != nil {
				return nil, false, err
			}
			if shouldContinue {
				continue
			}

			postParentIndexPartialKeySlice, err := postParentIndexPartialKey.AsSlice()
			if err != nil {
				return nil, false, err
			}
			for i := 0; i < len(postChildIndexTags); i++ {
				postParentIndexPartialKeySlice[2*i] = types.Uint(postChildIndexTags[i])
			}
			postChildIndexPartialKey, err := types.NewTuple(postChild.Table.Format(), postParentIndexPartialKeySlice...)
			if err != nil {
				return nil, false, err
			}
			changeViolates, err := parentFkConstraintViolationsProcess(ctx, foreignKey, postParent, postChild, postChildIndexPartialKey, postChildCVMapEditor)
			if err != nil {
				return nil, false, err
			}
			foundViolations = foundViolations || changeViolates
		case types.DiffChangeAdded:
			// We don't do anything if a parent row was added
		default:
			return nil, false, fmt.Errorf("unknown diff change type")
		}
		if !hasMore {
			break
		}
	}

	postChildCVMap, err = postChildCVMapEditor.Map(ctx)
	if err != nil {
		return nil, false, err
	}
	updatedTbl, err := postChild.Table.SetConstraintViolations(ctx, postChildCVMap)
	return updatedTbl, foundViolations, err
}

// parentFkConstraintViolationsProcess handles processing the reference options on a child, or creating a violation if
// necessary.
func parentFkConstraintViolationsProcess(
	ctx context.Context,
	foreignKey doltdb.ForeignKey,
	postParent, postChild *constraintViolationsLoadedTable,
	postChildIndexPartialKey types.Tuple,
	postChildCVMapEditor *types.MapEditor,
) (bool, error) {
	foundViolation := false
	mapIter := noms.NewNomsRangeReader(postChild.IndexSchema, postChild.IndexData,
		[]*noms.ReadRange{{Start: postChildIndexPartialKey, Inclusive: true, Reverse: false, Check: noms.InRangeCheckPartial(postChildIndexPartialKey)}})
	defer mapIter.Close(ctx)
	var postChildIndexRow row.Row
	var err error
	for postChildIndexRow, err = mapIter.ReadRow(ctx); err == nil; postChildIndexRow, err = mapIter.ReadRow(ctx) {
		postChildIndexKey, err := postChildIndexRow.NomsMapKey(postChild.IndexSchema).Value(ctx)
		if err != nil {
			return false, err
		}
		postChildRowKey, err := postChild.Index.ToTableTuple(ctx, postChildIndexKey.(types.Tuple), postChild.Table.Format())
		if err != nil {
			return false, err
		}
		postChildRowVal, ok, err := postChild.RowData.MaybeGetTuple(ctx, postChildRowKey)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, fmt.Errorf("index %s on %s contains data that table does not", foreignKey.TableIndex, foreignKey.TableName)
		}
		vInfo, err := foreignKeyCVJson(ctx, foreignKey, postChild.Table.ValueReadWriter(), postChild.Schema, postParent.Schema)
		if err != nil {
			return false, err
		}
		cvKey, cvVal, err := toConstraintViolationRow(ctx, cvType_ForeignKey, vInfo, postChildRowKey, postChildRowVal)
		if err != nil {
			return false, err
		}
		postChildCVMapEditor.Set(cvKey, cvVal)
		foundViolation = true
	}
	if err != io.EOF {
		return false, err
	}
	return foundViolation, nil
}

// childFkConstraintViolations processes foreign key constraint violations for the child in a foreign key.
func childFkConstraintViolations(
	ctx context.Context,
	foreignKey doltdb.ForeignKey,
	postParent, postChild *constraintViolationsLoadedTable,
	preChildSch schema.Schema,
	preChildRowData types.Map,
) (*doltdb.Table, bool, error) {
	foundViolations := false
	postParentIndexTags := postParent.Index.IndexedColumnTags()
	postChildIndexTags := postChild.Index.IndexedColumnTags()
	postChildCVMap, err := postChild.Table.GetConstraintViolations(ctx)
	if err != nil {
		return nil, false, err
	}
	postChildCVMapEditor := postChildCVMap.Edit()

	differ := diff.NewRowDiffer(ctx, preChildSch, postChild.Schema, 1024)
	defer differ.Close()
	differ.Start(ctx, preChildRowData, postChild.RowData)
	for {
		diffSlice, hasMore, err := differ.GetDiffs(1, 10*time.Second)
		if err != nil {
			return nil, false, err
		}
		if len(diffSlice) != 1 {
			if hasMore {
				return nil, false, fmt.Errorf("no diff returned but should have errored earlier")
			}
			break
		}
		rowDiff := diffSlice[0]
		switch rowDiff.ChangeType {
		case types.DiffChangeAdded, types.DiffChangeModified:
			postChildRow, err := row.FromNoms(postChild.Schema, rowDiff.KeyValue.(types.Tuple), rowDiff.NewValue.(types.Tuple))
			if err != nil {
				return nil, false, err
			}
			hasNulls := false
			for _, tag := range postChildIndexTags {
				if postChildRowEntry, ok := postChildRow.GetColVal(tag); !ok || types.IsNull(postChildRowEntry) {
					hasNulls = true
					break
				}
			}
			if hasNulls {
				continue
			}

			postChildIndexPartialKey, err := row.ReduceToIndexPartialKey(postChild.Index, postChildRow)
			if err != nil {
				return nil, false, err
			}
			postChildIndexPartialKeySlice, err := postChildIndexPartialKey.AsSlice()
			if err != nil {
				return nil, false, err
			}
			for i := 0; i < len(postParentIndexTags); i++ {
				postChildIndexPartialKeySlice[2*i] = types.Uint(postParentIndexTags[i])
			}
			parentPartialKey, err := types.NewTuple(postChild.Table.Format(), postChildIndexPartialKeySlice...)
			if err != nil {
				return nil, false, err
			}
			diffViolates, err := childFkConstraintViolationsProcess(ctx, foreignKey, postParent, postChild, rowDiff, parentPartialKey, postChildCVMapEditor)
			if err != nil {
				return nil, false, err
			}
			foundViolations = foundViolations || diffViolates
		case types.DiffChangeRemoved:
			// We don't do anything if a child row was removed
		default:
			return nil, false, fmt.Errorf("unknown diff change type")
		}
		if !hasMore {
			break
		}
	}
	postChildCVMap, err = postChildCVMapEditor.Map(ctx)
	if err != nil {
		return nil, false, err
	}
	updatedTbl, err := postChild.Table.SetConstraintViolations(ctx, postChildCVMap)
	return updatedTbl, foundViolations, err
}

// childFkConstraintViolationsProcess handles processing the constraint violations for the child of a foreign key.
func childFkConstraintViolationsProcess(
	ctx context.Context,
	foreignKey doltdb.ForeignKey,
	postParent, postChild *constraintViolationsLoadedTable,
	rowDiff *diff2.Difference,
	parentPartialKey types.Tuple,
	postChildCVMapEditor *types.MapEditor,
) (bool, error) {
	var mapIter table.TableReadCloser = noms.NewNomsRangeReader(postParent.IndexSchema, postParent.IndexData,
		[]*noms.ReadRange{{Start: parentPartialKey, Inclusive: true, Reverse: false, Check: noms.InRangeCheckPartial(parentPartialKey)}})
	defer mapIter.Close(ctx)
	// If the row exists in the parent, then we don't need to do anything
	if _, err := mapIter.ReadRow(ctx); err != nil {
		if err != io.EOF {
			return false, err
		}
		vInfo, err := foreignKeyCVJson(ctx, foreignKey, postChild.Table.ValueReadWriter(), postChild.Schema, postParent.Schema)
		if err != nil {
			return false, err
		}
		cvKey, cvVal, err := toConstraintViolationRow(ctx, cvType_ForeignKey, vInfo, rowDiff.KeyValue.(types.Tuple), rowDiff.NewValue.(types.Tuple))
		if err != nil {
			return false, err
		}
		postChildCVMapEditor.Set(cvKey, cvVal)
		return true, nil
	}
	return false, nil
}

// newConstraintViolationsLoadedTable returns a *constraintViolationsLoadedTable. Returns false if the table was loaded
// but the index could not be found. If the table could not be found, then an error is returned.
func newConstraintViolationsLoadedTable(ctx context.Context, tblName, idxName string, root *doltdb.RootValue) (*constraintViolationsLoadedTable, bool, error) {
	tbl, trueTblName, ok, err := root.GetTableInsensitive(ctx, tblName)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, doltdb.ErrTableNotFound
	}
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, false, err
	}
	rowData, err := tbl.GetNomsRowData(ctx)
	if err != nil {
		return nil, false, err
	}
	idx, ok := sch.Indexes().GetByNameCaseInsensitive(idxName)
	if !ok {
		return &constraintViolationsLoadedTable{
			TableName: trueTblName,
			Table:     tbl,
			Schema:    sch,
			RowData:   rowData,
		}, false, nil
	}
	indexData, err := tbl.GetNomsIndexRowData(ctx, idx.Name())
	if err != nil {
		return nil, false, err
	}
	return &constraintViolationsLoadedTable{
		TableName:   trueTblName,
		Table:       tbl,
		Schema:      sch,
		RowData:     rowData,
		Index:       idx,
		IndexSchema: idx.Schema(),
		IndexData:   indexData,
	}, true, nil
}

// toConstraintViolationRow converts the given key and value tuples into ones suitable to add to a constraint violation map.
func toConstraintViolationRow(ctx context.Context, vType cvType, vInfo types.JSON, k, v types.Tuple) (types.Tuple, types.Tuple, error) {
	constraintViolationKeyVals := []types.Value{types.Uint(schema.DoltConstraintViolationsTypeTag), types.Uint(vType)}
	keySlice, err := k.AsSlice()
	if err != nil {
		emptyTuple := types.EmptyTuple(k.Format())
		return emptyTuple, emptyTuple, err
	}
	constraintViolationKeyVals = append(constraintViolationKeyVals, keySlice...)
	constraintViolationKey, err := types.NewTuple(k.Format(), constraintViolationKeyVals...)
	if err != nil {
		emptyTuple := types.EmptyTuple(k.Format())
		return emptyTuple, emptyTuple, err
	}

	constraintViolationValVals, err := v.AsSlice()
	if err != nil {
		emptyTuple := types.EmptyTuple(k.Format())
		return emptyTuple, emptyTuple, err
	}
	constraintViolationValVals = append(constraintViolationValVals, types.Uint(schema.DoltConstraintViolationsInfoTag), vInfo)
	constraintViolationVal, err := types.NewTuple(v.Format(), constraintViolationValVals...)
	if err != nil {
		emptyTuple := types.EmptyTuple(k.Format())
		return emptyTuple, emptyTuple, err
	}

	return constraintViolationKey, constraintViolationVal, nil
}

// foreignKeyCVJson converts a foreign key to a JSON document for use as the info field in a constraint violations map.
func foreignKeyCVJson(ctx context.Context, foreignKey doltdb.ForeignKey, vrw types.ValueReadWriter, sch, refSch schema.Schema) (types.JSON, error) {
	schCols := sch.GetAllCols()
	refSchCols := refSch.GetAllCols()
	fkCols := make([]string, len(foreignKey.TableColumns))
	refFkCols := make([]string, len(foreignKey.ReferencedTableColumns))
	for i, tag := range foreignKey.TableColumns {
		if col, ok := schCols.TagToCol[tag]; !ok {
			return types.JSON{}, fmt.Errorf("foreign key '%s' references tag '%d' on table '%s' but it cannot be found",
				foreignKey.Name, tag, foreignKey.TableName)
		} else {
			fkCols[i] = col.Name
		}
	}
	for i, tag := range foreignKey.ReferencedTableColumns {
		if col, ok := refSchCols.TagToCol[tag]; !ok {
			return types.JSON{}, fmt.Errorf("foreign key '%s' references tag '%d' on table '%s' but it cannot be found",
				foreignKey.Name, tag, foreignKey.ReferencedTableName)
		} else {
			refFkCols[i] = col.Name
		}
	}
	jsonStr := fmt.Sprintf(`{`+
		`"ForeignKey":"%s",`+
		`"Table":"%s",`+
		`"Index":"%s",`+
		`"Columns":["%s"],`+
		`"ReferencedTable":"%s",`+
		`"ReferencedIndex":"%s",`+
		`"ReferencedColumns":["%s"],`+
		`"OnUpdate":"%s",`+
		`"OnDelete":"%s"}`,
		foreignKey.Name,
		foreignKey.TableName,
		foreignKey.TableIndex,
		strings.Join(fkCols, `','`),
		foreignKey.ReferencedTableName,
		foreignKey.ReferencedTableIndex,
		strings.Join(refFkCols, `','`),
		foreignKey.OnUpdate.ReducedString(),
		foreignKey.OnDelete.ReducedString())

	var doc interface{}
	if err := json.Unmarshal([]byte(jsonStr), &doc); err != nil {
		return types.JSON{}, err
	}
	sqlDoc := sql.JSONDocument{Val: doc}
	nomsJson, err := json2.NomsJSONFromJSONValue(ctx, vrw, sqlDoc)
	if err != nil {
		return types.JSON{}, err
	}
	return types.JSON(nomsJson), nil
}
