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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
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
	StartFK(ctx *sql.Context, fk doltdb.ForeignKey) error
	EndCurrFK(ctx context.Context) error
	NomsFKViolationFound(ctx context.Context, rowKey, rowValue types.Tuple) error
	ProllyFKViolationFound(ctx context.Context, rowKey, rowValue val.Tuple) error
}

// RegisterForeignKeyViolations emits constraint violations that have been created as a
// result of the diff between |baseRoot| and |newRoot|. It sends violations to |receiver|.
func RegisterForeignKeyViolations(
		ctx *sql.Context,
		tableResolver doltdb.TableResolver,
		newRoot, baseRoot doltdb.RootValue,
		tables *doltdb.TableNameSet,
		receiver FKViolationReceiver,
) error {
	fkColl, err := newRoot.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	for _, foreignKey := range fkColl.AllKeys() {
		if !foreignKey.IsResolved() || (tables.Size() != 0 && !tables.Contains(foreignKey.TableName)) {
			continue
		}

		err = receiver.StartFK(ctx, foreignKey)
		if err != nil {
			return err
		}

		postParent, ok, err := newConstraintViolationsLoadedTable(ctx, tableResolver, foreignKey.ReferencedTableName, foreignKey.ReferencedTableIndex, newRoot)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("foreign key %s should have index %s on table %s but it cannot be found",
				foreignKey.Name, foreignKey.ReferencedTableIndex, foreignKey.ReferencedTableName)
		}

		postChild, ok, err := newConstraintViolationsLoadedTable(ctx, tableResolver, foreignKey.TableName, foreignKey.TableIndex, newRoot)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("foreign key %s should have index %s on table %s but it cannot be found",
				foreignKey.Name, foreignKey.TableIndex, foreignKey.TableName)
		}

		preParent, _, err := newConstraintViolationsLoadedTable(ctx, tableResolver, foreignKey.ReferencedTableName, foreignKey.ReferencedTableIndex, baseRoot)
		if err != nil {
			if err != doltdb.ErrTableNotFound {
				return err
			}
			// Parent does not exist in the ancestor so we use an empty map
			emptyIdx, err := durable.NewEmptyPrimaryIndex(ctx, postParent.Table.ValueReadWriter(), postParent.Table.NodeStore(), postParent.Schema)
			if err != nil {
				return err
			}
			err = parentFkConstraintViolations(ctx, foreignKey, postParent, postParent, postChild, emptyIdx, receiver)
			if err != nil {
				return err
			}
		} else {
			// Parent exists in the ancestor
			err = parentFkConstraintViolations(ctx, foreignKey, preParent, postParent, postChild, preParent.RowData, receiver)
			if err != nil {
				return err
			}
		}

		preChild, _, err := newConstraintViolationsLoadedTable(ctx, tableResolver, foreignKey.TableName, foreignKey.TableIndex, baseRoot)
		if err != nil {
			if err != doltdb.ErrTableNotFound {
				return err
			}
			// Child does not exist in the ancestor so we use an empty map
			emptyIdx, err := durable.NewEmptyPrimaryIndex(ctx, postChild.Table.ValueReadWriter(), postChild.Table.NodeStore(), postChild.Schema)
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
func AddForeignKeyViolations(ctx *sql.Context, tableResolver doltdb.TableResolver, newRoot, baseRoot doltdb.RootValue, tables *doltdb.TableNameSet, theirRootIsh hash.Hash) (doltdb.RootValue, *doltdb.TableNameSet, error) {
	violationWriter := &foreignKeyViolationWriter{tableResolver: tableResolver, rootValue: newRoot, theirRootIsh: theirRootIsh, violatedTables: doltdb.NewTableNameSet(nil)}
	err := RegisterForeignKeyViolations(ctx, tableResolver, newRoot, baseRoot, tables, violationWriter)
	if err != nil {
		return nil, nil, err
	}
	return violationWriter.rootValue, violationWriter.violatedTables, nil
}

// GetForeignKeyViolatedTables returns a list of tables that have foreign key
// violations based on the diff between |newRoot| and |baseRoot|.
func GetForeignKeyViolatedTables(ctx *sql.Context, tableResolver doltdb.TableResolver, newRoot, baseRoot doltdb.RootValue, tables *doltdb.TableNameSet) (*doltdb.TableNameSet, error) {
	handler := &foreignKeyViolationTracker{tableSet: doltdb.NewTableNameSet(nil)}
	err := RegisterForeignKeyViolations(ctx, tableResolver, newRoot, baseRoot, tables, handler)
	if err != nil {
		return nil, err
	}
	return handler.tableSet, nil
}

// foreignKeyViolationTracker tracks which tables have foreign key violations
type foreignKeyViolationTracker struct {
	tableSet *doltdb.TableNameSet
	currFk   doltdb.ForeignKey
}

func (f *foreignKeyViolationTracker) StartFK(ctx *sql.Context, fk doltdb.ForeignKey) error {
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
	tableResolver  doltdb.TableResolver
	rootValue      doltdb.RootValue
	theirRootIsh   hash.Hash
	violatedTables *doltdb.TableNameSet

	currFk  doltdb.ForeignKey
	currTbl *doltdb.Table

	// prolly
	artEditor     *prolly.ArtifactsEditor
	kd            *val.TupleDesc
	cInfoJsonData []byte

	// noms
	violMapEditor *types.MapEditor
	nomsVInfo     types.JSON
}

var _ FKViolationReceiver = (*foreignKeyViolationWriter)(nil)

func (f *foreignKeyViolationWriter) StartFK(ctx *sql.Context, fk doltdb.ForeignKey) error {
	f.currFk = fk

	tbl, ok, err := f.tableResolver.ResolveTable(ctx, f.rootValue, fk.TableName)
	if err != nil {
		return err
	}
	if !ok {
		return doltdb.ErrTableNotFound
	}

	f.currTbl = tbl
	refTbl, ok, err := f.tableResolver.ResolveTable(ctx, f.rootValue, fk.ReferencedTableName)
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

	types.AssertFormat_DOLT(tbl.Format())
	arts, err := tbl.GetArtifacts(ctx)
	if err != nil {
		return err
	}
	artMap := durable.ProllyMapFromArtifactIndex(arts)
	f.artEditor = artMap.Editor()
	f.cInfoJsonData = jsonData
	f.kd = sch.GetKeyDescriptor(tbl.NodeStore())

	return nil
}

func (f *foreignKeyViolationWriter) EndCurrFK(ctx context.Context) error {
	types.AssertFormat_DOLT(f.currTbl.Format())

	artMap, err := f.artEditor.Flush(ctx)
	if err != nil {
		return err
	}
	artIdx := durable.ArtifactIndexFromProllyMap(artMap)
	tbl, err := f.currTbl.SetArtifacts(ctx, artIdx)
	if err != nil {
		return err
	}
	f.rootValue, err = f.rootValue.PutTable(ctx, f.currFk.TableName, tbl)
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
		foreignKey doltdb.ForeignKey,
		preParent, postParent, postChild *constraintViolationsLoadedTable,
		preParentRowData durable.Index,
		receiver FKViolationReceiver,
) error {
	if preParentRowData.Format() != types.Format_DOLT {
		panic("unsupported format: " + preParentRowData.Format().VersionString())
	}

	if preParent.IndexData == nil || postParent.Schema.GetPKCols().Size() == 0 || preParent.Schema.GetPKCols().Size() == 0 {
		m, err := durable.ProllyMapFromIndex(preParentRowData)
		if err != nil {
			return err
		}
		return prollyParentPriDiffFkConstraintViolations(ctx, foreignKey, postParent, postChild, m, receiver)
	}
	empty, err := preParentRowData.Empty()
	if err != nil {
		return err
	}
	var idx durable.Index
	if empty {
		idx, err = durable.NewEmptyForeignKeyIndex(ctx, postChild.Table.ValueReadWriter(), postParent.Table.NodeStore(), postParent.Index.Schema())
		if err != nil {
			return err
		}
	} else {
		idx = preParent.IndexData
	}
	m, err := durable.ProllyMapFromIndex(idx)
	if err != nil {
		return err
	}
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
		panic("unsupported format: " + preChildRowData.Format().VersionString())
	}

	if preChild.IndexData == nil || postChild.Schema.GetPKCols().Size() == 0 || preChild.Schema.GetPKCols().Size() == 0 {
		m, err := durable.ProllyMapFromIndex(preChildRowData)
		if err != nil {
			return err
		}
		return prollyChildPriDiffFkConstraintViolations(ctx, foreignKey, postParent, postChild, m, receiver)
	}
	empty, err := preChildRowData.Empty()
	if err != nil {
		return err
	}
	var idx durable.Index
	if empty {
		idx, err = durable.NewEmptyForeignKeyIndex(ctx, postChild.Table.ValueReadWriter(), postChild.Table.NodeStore(), postChild.Index.Schema())
		if err != nil {
			return err
		}
	} else {
		idx = preChild.IndexData
	}
	m, err := durable.ProllyMapFromIndex(idx)
	if err != nil {
		return err
	}

	return prollyChildSecDiffFkConstraintViolations(ctx, foreignKey, postParent, postChild, m, receiver)
}

// newConstraintViolationsLoadedTable returns a *constraintViolationsLoadedTable. Returns false if the table was loaded
// but the index could not be found. If the table could not be found, then an error is returned.
func newConstraintViolationsLoadedTable(
		ctx *sql.Context,
		tableResolver doltdb.TableResolver,
		tblName doltdb.TableName,
		idxName string,
		root doltdb.RootValue,
) (*constraintViolationsLoadedTable, bool, error) {
	trueTblName, tbl, ok, err := tableResolver.ResolveTableInsensitive(ctx, root, tblName)
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
			TableName:   trueTblName.Name,
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
			TableName: trueTblName.Name,
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
		TableName:   trueTblName.Name,
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

	// TODO: we need to change our serialization strategy for table names with schemas in order to properly store these.
	//  You can't encode a null byte in a JSON blob like we do elsewhere. Probably the right move is to swap this JSON
	//  blob out for an actual flatbuffer.
	m := FkCVMeta{
		Columns:           fkCols,
		ForeignKey:        foreignKey.Name,
		Index:             foreignKey.TableIndex,
		OnDelete:          foreignKey.OnDelete.ReducedString(),
		OnUpdate:          foreignKey.OnUpdate.ReducedString(),
		ReferencedColumns: refFkCols,
		ReferencedIndex:   foreignKey.ReferencedTableIndex,
		ReferencedTable:   foreignKey.ReferencedTableName.Name,
		Table:             foreignKey.TableName.Name,
	}
	d, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	return d, nil
}
