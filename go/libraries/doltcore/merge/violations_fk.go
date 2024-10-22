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
	"time"

	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	json2 "github.com/dolthub/dolt/go/libraries/doltcore/sqle/json"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	diff2 "github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

// constraintViolationsLoadedTable is a collection of items needed to process constraint violations for a single table.
type constraintViolationsLoadedTable struct {
	TableName   string
	Table       *doltdb.Table
	Schema      schema.Schema
	RowData     durable.Index
	Index       schema.Index
	IndexSchema schema.Schema
	IndexData   durable.Index
}

// cvType is an enum for a constraint violation type.
type CvType uint64

const (
	CvType_ForeignKey CvType = iota + 1
	CvType_UniqueIndex
	CvType_CheckConstraint
	CvType_NotNull
)

type FKViolationReceiver interface {
	StartFK(ctx context.Context, fk doltdb.ForeignKey) error
	EndCurrFK(ctx context.Context) error
	NomsFKViolationFound(ctx context.Context, rowKey, rowValue types.Tuple) error
	ProllyFKViolationFound(ctx context.Context, rowKey, rowValue val.Tuple) error
}

// GetForeignKeyViolations returns the violations that have been created as a
// result of the diff between |baseRoot| and |newRoot|. It sends the violations to |receiver|.
func GetForeignKeyViolations(ctx context.Context, newRoot, baseRoot doltdb.RootValue, tables *doltdb.TableNameSet, receiver FKViolationReceiver) error {
	fkColl, err := newRoot.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	for _, foreignKey := range fkColl.AllKeys() {
		// TODO: schema names
		if !foreignKey.IsResolved() || (tables.Size() != 0 && !tables.Contains(doltdb.TableName{Name: foreignKey.TableName})) {
			continue
		}

		err = receiver.StartFK(ctx, foreignKey)
		if err != nil {
			return err
		}

		postParent, ok, err := newConstraintViolationsLoadedTable(ctx, foreignKey.ReferencedTableName, foreignKey.ReferencedTableIndex, newRoot)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("foreign key %s should have index %s on table %s but it cannot be found",
				foreignKey.Name, foreignKey.ReferencedTableIndex, foreignKey.ReferencedTableName)
		}

		postChild, ok, err := newConstraintViolationsLoadedTable(ctx, foreignKey.TableName, foreignKey.TableIndex, newRoot)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("foreign key %s should have index %s on table %s but it cannot be found",
				foreignKey.Name, foreignKey.TableIndex, foreignKey.TableName)
		}

		preParent, _, err := newConstraintViolationsLoadedTable(ctx, foreignKey.ReferencedTableName, foreignKey.ReferencedTableIndex, baseRoot)
		if err != nil {
			if err != doltdb.ErrTableNotFound {
				return err
			}
			// Parent does not exist in the ancestor so we use an empty map
			emptyIdx, err := durable.NewEmptyIndex(ctx, postParent.Table.ValueReadWriter(), postParent.Table.NodeStore(), postParent.Schema, false)
			if err != nil {
				return err
			}
			err = parentFkConstraintViolations(ctx, baseRoot.VRW(), foreignKey, postParent, postParent, postChild, emptyIdx, receiver)
			if err != nil {
				return err
			}
		} else {
			// Parent exists in the ancestor
			err = parentFkConstraintViolations(ctx, baseRoot.VRW(), foreignKey, preParent, postParent, postChild, preParent.RowData, receiver)
			if err != nil {
				return err
			}
		}

		preChild, _, err := newConstraintViolationsLoadedTable(ctx, foreignKey.TableName, foreignKey.TableIndex, baseRoot)
		if err != nil {
			if err != doltdb.ErrTableNotFound {
				return err
			}
			// Child does not exist in the ancestor so we use an empty map
			emptyIdx, err := durable.NewEmptyIndex(ctx, postChild.Table.ValueReadWriter(), postChild.Table.NodeStore(), postChild.Schema, false)
			if err != nil {
				return err
			}

			err = childFkConstraintViolations(ctx, baseRoot.VRW(), foreignKey, postParent, postChild, postChild, emptyIdx, receiver)
			if err != nil {
				return err
			}
		} else {
			err = childFkConstraintViolations(ctx, baseRoot.VRW(), foreignKey, postParent, postChild, preChild, preChild.RowData, receiver)
			if err != nil {
				return err
			}
		}

		err = receiver.EndCurrFK(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// AddForeignKeyViolations adds foreign key constraint violations to each table.
// todo(andy): pass doltdb.Rootish
func AddForeignKeyViolations(ctx context.Context, newRoot, baseRoot doltdb.RootValue, tables *doltdb.TableNameSet, theirRootIsh hash.Hash) (doltdb.RootValue, *set.StrSet, error) {
	violationWriter := &foreignKeyViolationWriter{rootValue: newRoot, theirRootIsh: theirRootIsh, violatedTables: set.NewStrSet(nil)}
	err := GetForeignKeyViolations(ctx, newRoot, baseRoot, tables, violationWriter)
	if err != nil {
		return nil, nil, err
	}
	return violationWriter.rootValue, violationWriter.violatedTables, nil
}

// GetForeignKeyViolatedTables returns a list of tables that have foreign key
// violations based on the diff between |newRoot| and |baseRoot|.
func GetForeignKeyViolatedTables(ctx context.Context, newRoot, baseRoot doltdb.RootValue, tables *doltdb.TableNameSet) (*set.StrSet, error) {
	handler := &foreignKeyViolationTracker{tableSet: set.NewStrSet(nil)}
	err := GetForeignKeyViolations(ctx, newRoot, baseRoot, tables, handler)
	if err != nil {
		return nil, err
	}
	return handler.tableSet, nil
}

// foreignKeyViolationTracker tracks which tables have foreign key violations
type foreignKeyViolationTracker struct {
	tableSet *set.StrSet
	currFk   doltdb.ForeignKey
}

func (f *foreignKeyViolationTracker) StartFK(ctx context.Context, fk doltdb.ForeignKey) error {
	f.currFk = fk
	return nil
}

func (f *foreignKeyViolationTracker) EndCurrFK(ctx context.Context) error {
	return nil
}

func (f *foreignKeyViolationTracker) NomsFKViolationFound(ctx context.Context, rowKey, rowValue types.Tuple) error {
	f.tableSet.Add(f.currFk.TableName)
	return nil
}

func (f *foreignKeyViolationTracker) ProllyFKViolationFound(ctx context.Context, rowKey, rowValue val.Tuple) error {
	f.tableSet.Add(f.currFk.TableName)
	return nil
}

var _ FKViolationReceiver = (*foreignKeyViolationTracker)(nil)

// foreignKeyViolationWriter updates rootValue with the foreign key constraint violations.
type foreignKeyViolationWriter struct {
	rootValue      doltdb.RootValue
	theirRootIsh   hash.Hash
	violatedTables *set.StrSet

	currFk  doltdb.ForeignKey
	currTbl *doltdb.Table

	// prolly
	artEditor     *prolly.ArtifactsEditor
	kd            val.TupleDesc
	cInfoJsonData []byte

	// noms
	violMapEditor *types.MapEditor
	nomsVInfo     types.JSON
}

var _ FKViolationReceiver = (*foreignKeyViolationWriter)(nil)

func (f *foreignKeyViolationWriter) StartFK(ctx context.Context, fk doltdb.ForeignKey) error {
	f.currFk = fk

	tbl, ok, err := f.rootValue.GetTable(ctx, doltdb.TableName{Name: fk.TableName})
	if err != nil {
		return err
	}
	if !ok {
		return doltdb.ErrTableNotFound
	}

	f.currTbl = tbl

	refTbl, ok, err := f.rootValue.GetTable(ctx, doltdb.TableName{Name: fk.ReferencedTableName})
	if err != nil {
		return err
	}
	if !ok {
		return doltdb.ErrTableNotFound
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return err
	}

	refSch, err := refTbl.GetSchema(ctx)
	if err != nil {
		return err
	}

	jsonData, err := foreignKeyCVJson(fk, sch, refSch)
	if err != nil {
		return err
	}

	if types.IsFormat_DOLT(tbl.Format()) {
		arts, err := tbl.GetArtifacts(ctx)
		if err != nil {
			return err
		}
		artMap := durable.ProllyMapFromArtifactIndex(arts)
		f.artEditor = artMap.Editor()
		f.cInfoJsonData = jsonData
		f.kd = sch.GetKeyDescriptor()
	} else {
		violMap, err := tbl.GetConstraintViolations(ctx)
		if err != nil {
			return err
		}
		f.violMapEditor = violMap.Edit()

		f.nomsVInfo, err = jsonDataToNomsValue(ctx, tbl.ValueReadWriter(), jsonData)
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *foreignKeyViolationWriter) EndCurrFK(ctx context.Context) error {
	if types.IsFormat_DOLT(f.currTbl.Format()) {
		artMap, err := f.artEditor.Flush(ctx)
		if err != nil {
			return err
		}
		artIdx := durable.ArtifactIndexFromProllyMap(artMap)
		tbl, err := f.currTbl.SetArtifacts(ctx, artIdx)
		if err != nil {
			return err
		}
		f.rootValue, err = f.rootValue.PutTable(ctx, doltdb.TableName{Name: f.currFk.TableName}, tbl)
		if err != nil {
			return err
		}
		return nil
	}

	violMap, err := f.violMapEditor.Map(ctx)
	if err != nil {
		return err
	}
	tbl, err := f.currTbl.SetConstraintViolations(ctx, violMap)
	if err != nil {
		return err
	}
	f.rootValue, err = f.rootValue.PutTable(ctx, doltdb.TableName{Name: f.currFk.TableName}, tbl)
	if err != nil {
		return err
	}
	return nil
}

func (f *foreignKeyViolationWriter) NomsFKViolationFound(ctx context.Context, rowKey, rowValue types.Tuple) error {

	cvKey, cvVal, err := toConstraintViolationRow(ctx, CvType_ForeignKey, f.nomsVInfo, rowKey, rowValue)
	if err != nil {
		return err
	}

	f.violMapEditor.Set(cvKey, cvVal)

	f.violatedTables.Add(f.currFk.TableName)

	return nil
}

func (f *foreignKeyViolationWriter) ProllyFKViolationFound(ctx context.Context, rowKey, rowValue val.Tuple) error {

	meta := prolly.ConstraintViolationMeta{VInfo: f.cInfoJsonData, Value: rowValue}

	err := f.artEditor.ReplaceConstraintViolation(ctx, rowKey, f.theirRootIsh, prolly.ArtifactTypeForeignKeyViol, meta)
	if err != nil {
		return err
	}

	f.violatedTables.Add(f.currFk.TableName)

	return nil
}

var _ FKViolationReceiver = (*foreignKeyViolationWriter)(nil)

// parentFkConstraintViolations processes foreign key constraint violations for the parent in a foreign key.
func parentFkConstraintViolations(
	ctx context.Context,
	vr types.ValueReader,
	foreignKey doltdb.ForeignKey,
	preParent, postParent, postChild *constraintViolationsLoadedTable,
	preParentRowData durable.Index,
	receiver FKViolationReceiver,
) error {
	if preParentRowData.Format() != types.Format_DOLT {
		m := durable.NomsMapFromIndex(preParentRowData)
		return nomsParentFkConstraintViolations(ctx, vr, foreignKey, postParent, postChild, preParent.Schema, m, receiver)
	}
	if preParent.IndexData == nil || postParent.Schema.GetPKCols().Size() == 0 || preParent.Schema.GetPKCols().Size() == 0 {
		m := durable.ProllyMapFromIndex(preParentRowData)
		return prollyParentPriDiffFkConstraintViolations(ctx, foreignKey, postParent, postChild, m, receiver)
	}
	empty, err := preParentRowData.Empty()
	if err != nil {
		return err
	}
	var idx durable.Index
	if empty {
		idx, err = durable.NewEmptyIndex(ctx, postChild.Table.ValueReadWriter(), postParent.Table.NodeStore(), postParent.Schema, false)
		if err != nil {
			return err
		}
	} else {
		idx = preParent.IndexData
	}
	m := durable.ProllyMapFromIndex(idx)
	return prollyParentSecDiffFkConstraintViolations(ctx, foreignKey, postParent, postChild, m, receiver)
}

// childFkConstraintViolations handles processing the reference options on a child, or creating a violation if
// necessary.
func childFkConstraintViolations(
	ctx context.Context,
	vr types.ValueReader,
	foreignKey doltdb.ForeignKey,
	postParent, postChild, preChild *constraintViolationsLoadedTable,
	preChildRowData durable.Index,
	receiver FKViolationReceiver,
) error {
	if preChildRowData.Format() != types.Format_DOLT {
		m := durable.NomsMapFromIndex(preChildRowData)
		return nomsChildFkConstraintViolations(ctx, vr, foreignKey, postParent, postChild, preChild.Schema, m, receiver)
	}
	if preChild.IndexData == nil || postChild.Schema.GetPKCols().Size() == 0 || preChild.Schema.GetPKCols().Size() == 0 {
		m := durable.ProllyMapFromIndex(preChildRowData)
		return prollyChildPriDiffFkConstraintViolations(ctx, foreignKey, postParent, postChild, m, receiver)
	}
	empty, err := preChildRowData.Empty()
	if err != nil {
		return err
	}
	var idx durable.Index
	if empty {
		idx, err = durable.NewEmptyIndex(ctx, postChild.Table.ValueReadWriter(), postChild.Table.NodeStore(), postChild.Schema, false)
		if err != nil {
			return err
		}
	} else {
		idx = preChild.IndexData
	}
	m := durable.ProllyMapFromIndex(idx)
	return prollyChildSecDiffFkConstraintViolations(ctx, foreignKey, postParent, postChild, m, receiver)
}

func nomsParentFkConstraintViolations(
	ctx context.Context,
	vr types.ValueReader,
	foreignKey doltdb.ForeignKey,
	postParent, postChild *constraintViolationsLoadedTable,
	preParentSch schema.Schema,
	preParentRowData types.Map,
	receiver FKViolationReceiver) error {

	postParentIndexTags := postParent.Index.IndexedColumnTags()
	postChildIndexTags := postChild.Index.IndexedColumnTags()

	differ := diff.NewRowDiffer(ctx, preParentRowData.Format(), preParentSch, postParent.Schema, 1024)
	defer differ.Close()
	differ.Start(ctx, preParentRowData, durable.NomsMapFromIndex(postParent.RowData))
	for {
		diffSlice, hasMore, err := differ.GetDiffs(1, 10*time.Second)
		if err != nil {
			return err
		}
		if len(diffSlice) != 1 {
			if hasMore {
				return fmt.Errorf("no diff returned but should have errored earlier")
			}
			break
		}
		rowDiff := diffSlice[0]
		switch rowDiff.ChangeType {
		case types.DiffChangeRemoved, types.DiffChangeModified:
			postParentRow, err := row.FromNoms(postParent.Schema, rowDiff.KeyValue.(types.Tuple), rowDiff.OldValue.(types.Tuple))
			if err != nil {
				return err
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

			postParentIndexPartialKey, err := row.ReduceToIndexPartialKey(foreignKey.TableColumns, postParent.Index, postParentRow)
			if err != nil {
				return err
			}

			shouldContinue, err := func() (bool, error) {
				var mapIter table.ReadCloser = noms.NewNomsRangeReader(
					vr,
					postParent.IndexSchema,
					durable.NomsMapFromIndex(postParent.IndexData),
					[]*noms.ReadRange{{Start: postParentIndexPartialKey, Inclusive: true, Reverse: false, Check: noms.InRangeCheckPartial(postParentIndexPartialKey)}})
				defer mapIter.Close(ctx)
				if _, err := mapIter.ReadRow(ctx); err == nil {
					// If the parent table has other rows that satisfy the partial key then we choose to do nothing
					return true, nil
				} else if err != io.EOF {
					return false, err
				}
				return false, nil
			}()
			if err != nil {
				return err
			}
			if shouldContinue {
				continue
			}

			postParentIndexPartialKeySlice, err := postParentIndexPartialKey.AsSlice()
			if err != nil {
				return err
			}
			for i := 0; i < len(postChildIndexTags); i++ {
				postParentIndexPartialKeySlice[2*i] = types.Uint(postChildIndexTags[i])
			}
			postChildIndexPartialKey, err := types.NewTuple(postChild.Table.Format(), postParentIndexPartialKeySlice...)
			if err != nil {
				return err
			}
			err = nomsParentFkConstraintViolationsProcess(ctx, vr, foreignKey, postChild, postChildIndexPartialKey, receiver)
			if err != nil {
				return err
			}
		case types.DiffChangeAdded:
			// We don't do anything if a parent row was added
		default:
			return fmt.Errorf("unknown diff change type")
		}
		if !hasMore {
			break
		}
	}

	return nil
}

func nomsParentFkConstraintViolationsProcess(
	ctx context.Context,
	vr types.ValueReader,
	foreignKey doltdb.ForeignKey,
	postChild *constraintViolationsLoadedTable,
	postChildIndexPartialKey types.Tuple,
	receiver FKViolationReceiver,
) error {
	indexData := durable.NomsMapFromIndex(postChild.IndexData)
	rowData := durable.NomsMapFromIndex(postChild.RowData)

	mapIter := noms.NewNomsRangeReader(
		vr,
		postChild.IndexSchema,
		indexData,
		[]*noms.ReadRange{{Start: postChildIndexPartialKey, Inclusive: true, Reverse: false, Check: noms.InRangeCheckPartial(postChildIndexPartialKey)}})
	defer mapIter.Close(ctx)
	var postChildIndexRow row.Row
	var err error
	for postChildIndexRow, err = mapIter.ReadRow(ctx); err == nil; postChildIndexRow, err = mapIter.ReadRow(ctx) {
		postChildIndexKey, err := postChildIndexRow.NomsMapKey(postChild.IndexSchema).Value(ctx)
		if err != nil {
			return err
		}
		postChildRowKey, err := postChild.Index.ToTableTuple(ctx, postChildIndexKey.(types.Tuple), postChild.Table.Format())
		if err != nil {
			return err
		}
		postChildRowVal, ok, err := rowData.MaybeGetTuple(ctx, postChildRowKey)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("index %s on %s contains data that table does not", foreignKey.TableIndex, foreignKey.TableName)
		}

		err = receiver.NomsFKViolationFound(ctx, postChildRowKey, postChildRowVal)
		if err != nil {
			return err
		}
	}
	if err != io.EOF {
		return err
	}
	return nil
}

// nomsChildFkConstraintViolations processes foreign key constraint violations for the child in a foreign key.
func nomsChildFkConstraintViolations(
	ctx context.Context,
	vr types.ValueReader,
	foreignKey doltdb.ForeignKey,
	postParent, postChild *constraintViolationsLoadedTable,
	preChildSch schema.Schema,
	preChildRowData types.Map,
	receiver FKViolationReceiver,
) error {
	var postParentIndexTags, postChildIndexTags []uint64
	if postParent.Index.Name() == "" {
		postParentIndexTags = foreignKey.ReferencedTableColumns
		postChildIndexTags = foreignKey.TableColumns
	} else {
		postParentIndexTags = postParent.Index.IndexedColumnTags()
		postChildIndexTags = postChild.Index.IndexedColumnTags()
	}

	differ := diff.NewRowDiffer(ctx, preChildRowData.Format(), preChildSch, postChild.Schema, 1024)
	defer differ.Close()
	differ.Start(ctx, preChildRowData, durable.NomsMapFromIndex(postChild.RowData))
	for {
		diffSlice, hasMore, err := differ.GetDiffs(1, 10*time.Second)
		if err != nil {
			return err
		}
		if len(diffSlice) != 1 {
			if hasMore {
				return fmt.Errorf("no diff returned but should have errored earlier")
			}
			break
		}
		rowDiff := diffSlice[0]
		switch rowDiff.ChangeType {
		case types.DiffChangeAdded, types.DiffChangeModified:
			postChildRow, err := row.FromNoms(postChild.Schema, rowDiff.KeyValue.(types.Tuple), rowDiff.NewValue.(types.Tuple))
			if err != nil {
				return err
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

			postChildIndexPartialKey, err := row.ReduceToIndexPartialKey(postChildIndexTags, postChild.Index, postChildRow)
			if err != nil {
				return err
			}
			postChildIndexPartialKeySlice, err := postChildIndexPartialKey.AsSlice()
			if err != nil {
				return err
			}
			for i := 0; i < len(postParentIndexTags); i++ {
				postChildIndexPartialKeySlice[2*i] = types.Uint(postParentIndexTags[i])
			}
			parentPartialKey, err := types.NewTuple(postChild.Table.Format(), postChildIndexPartialKeySlice...)
			if err != nil {
				return err
			}
			err = childFkConstraintViolationsProcess(ctx, vr, postParent, rowDiff, parentPartialKey, receiver)
			if err != nil {
				return err
			}
		case types.DiffChangeRemoved:
			// We don't do anything if a child row was removed
		default:
			return fmt.Errorf("unknown diff change type")
		}
		if !hasMore {
			break
		}
	}

	return nil
}

// childFkConstraintViolationsProcess handles processing the constraint violations for the child of a foreign key.
func childFkConstraintViolationsProcess(
	ctx context.Context,
	vr types.ValueReader,
	postParent *constraintViolationsLoadedTable,
	rowDiff *diff2.Difference,
	parentPartialKey types.Tuple,
	receiver FKViolationReceiver,
) error {
	var mapIter table.ReadCloser = noms.NewNomsRangeReader(
		vr,
		postParent.IndexSchema,
		durable.NomsMapFromIndex(postParent.IndexData),
		[]*noms.ReadRange{{Start: parentPartialKey, Inclusive: true, Reverse: false, Check: noms.InRangeCheckPartial(parentPartialKey)}})
	defer mapIter.Close(ctx)
	// If the row exists in the parent, then we don't need to do anything
	if _, err := mapIter.ReadRow(ctx); err != nil {
		if err != io.EOF {
			return err
		}
		err = receiver.NomsFKViolationFound(ctx, rowDiff.KeyValue.(types.Tuple), rowDiff.NewValue.(types.Tuple))
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

// newConstraintViolationsLoadedTable returns a *constraintViolationsLoadedTable. Returns false if the table was loaded
// but the index could not be found. If the table could not be found, then an error is returned.
func newConstraintViolationsLoadedTable(ctx context.Context, tblName, idxName string, root doltdb.RootValue) (*constraintViolationsLoadedTable, bool, error) {
	// TODO: schema name
	tbl, trueTblName, ok, err := doltdb.GetTableInsensitive(ctx, root, doltdb.TableName{Name: tblName})
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
	rowData, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, false, err
	}

	// Create Primary Key Index
	if idxName == "" {
		pkCols := sch.GetPKCols()
		pkIdxColl := schema.NewIndexCollection(pkCols, pkCols)
		pkIdxProps := schema.IndexProperties{
			IsUnique:      true,
			IsUserDefined: false,
			Comment:       "",
		}
		pkIdx := schema.NewIndex("", pkCols.Tags, pkCols.Tags, pkIdxColl, pkIdxProps)
		return &constraintViolationsLoadedTable{
			TableName:   trueTblName,
			Table:       tbl,
			Schema:      sch,
			RowData:     rowData,
			Index:       pkIdx,
			IndexSchema: pkIdx.Schema(),
			IndexData:   rowData,
		}, true, nil
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
	indexData, err := tbl.GetIndexRowData(ctx, idx.Name())
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
func toConstraintViolationRow(ctx context.Context, vType CvType, vInfo types.JSON, k, v types.Tuple) (types.Tuple, types.Tuple, error) {
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

// foreignKeyCVJson converts a foreign key to JSON data for use as the info field in a constraint violations map.
func foreignKeyCVJson(foreignKey doltdb.ForeignKey, sch, refSch schema.Schema) ([]byte, error) {
	schCols := sch.GetAllCols()
	refSchCols := refSch.GetAllCols()
	fkCols := make([]string, len(foreignKey.TableColumns))
	refFkCols := make([]string, len(foreignKey.ReferencedTableColumns))
	for i, tag := range foreignKey.TableColumns {
		if col, ok := schCols.TagToCol[tag]; !ok {
			return nil, fmt.Errorf("foreign key '%s' references tag '%d' on table '%s' but it cannot be found",
				foreignKey.Name, tag, foreignKey.TableName)
		} else {
			fkCols[i] = col.Name
		}
	}
	for i, tag := range foreignKey.ReferencedTableColumns {
		if col, ok := refSchCols.TagToCol[tag]; !ok {
			return nil, fmt.Errorf("foreign key '%s' references tag '%d' on table '%s' but it cannot be found",
				foreignKey.Name, tag, foreignKey.ReferencedTableName)
		} else {
			refFkCols[i] = col.Name
		}
	}

	m := FkCVMeta{
		Columns:           fkCols,
		ForeignKey:        foreignKey.Name,
		Index:             foreignKey.TableIndex,
		OnDelete:          foreignKey.OnDelete.ReducedString(),
		OnUpdate:          foreignKey.OnUpdate.ReducedString(),
		ReferencedColumns: refFkCols,
		ReferencedIndex:   foreignKey.ReferencedTableIndex,
		ReferencedTable:   foreignKey.ReferencedTableName,
		Table:             foreignKey.TableName,
	}
	d, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	return d, nil
}

func jsonDataToNomsValue(ctx context.Context, vrw types.ValueReadWriter, data []byte) (types.JSON, error) {
	var doc interface{}
	if err := json.Unmarshal(data, &doc); err != nil {
		return types.JSON{}, err
	}
	sqlDoc := gmstypes.JSONDocument{Val: doc}
	nomsJson, err := json2.NomsJSONFromJSONValue(ctx, vrw, sqlDoc)
	if err != nil {
		return types.JSON{}, err
	}
	return types.JSON(nomsJson), nil
}
