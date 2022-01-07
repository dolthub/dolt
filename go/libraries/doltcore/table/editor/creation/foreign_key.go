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

package creation

import (
	"context"
	"fmt"

	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

// ResolveForeignKey resolves the given foreign key. Errors if the foreign key is already resolved.
func ResolveForeignKey(
	ctx context.Context,
	root *doltdb.RootValue,
	table *doltdb.Table,
	foreignKey doltdb.ForeignKey,
	opts editor.Options,
) (*doltdb.RootValue, doltdb.ForeignKey, error) {
	// There's a logic error if we attempt to resolve an already-resolved foreign key at this point. This should only
	// be called on unresolved foreign keys.
	if foreignKey.IsResolved() {
		return nil, doltdb.ForeignKey{}, fmt.Errorf("cannot resolve foreign key `%s` as it has already been resolved", foreignKey.Name)
	}
	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, doltdb.ForeignKey{}, err
	}

	tblCols := make([]schema.Column, len(foreignKey.UnresolvedFKDetails.TableColumns))
	colTags := make([]uint64, len(foreignKey.UnresolvedFKDetails.TableColumns))
	actualColNames := make([]string, len(foreignKey.UnresolvedFKDetails.TableColumns))
	for i, col := range foreignKey.UnresolvedFKDetails.TableColumns {
		tableCol, ok := sch.GetAllCols().GetByNameCaseInsensitive(col)
		if !ok {
			//TODO: fix go-mysql-server equivalent check, needs two vals
			return nil, doltdb.ForeignKey{}, fmt.Errorf("table `%s` does not have column `%s`", foreignKey.TableName, col)
		}
		if (foreignKey.OnUpdate == doltdb.ForeignKeyReferenceOption_SetNull || foreignKey.OnDelete == doltdb.ForeignKeyReferenceOption_SetNull) &&
			!tableCol.IsNullable() {
			return nil, doltdb.ForeignKey{}, fmt.Errorf("cannot use SET NULL as column `%s` is non-nullable", tableCol.Name)
		}
		tblCols[i] = tableCol
		colTags[i] = tableCol.Tag
		actualColNames[i] = tableCol.Name
	}

	var refTbl *doltdb.Table
	var ok bool
	var refSch schema.Schema
	if foreignKey.IsSelfReferential() {
		refTbl = table
		refSch = sch
	} else {
		refTbl, _, ok, err = root.GetTableInsensitive(ctx, foreignKey.ReferencedTableName)
		if err != nil {
			return nil, doltdb.ForeignKey{}, err
		}
		if !ok {
			return nil, doltdb.ForeignKey{}, fmt.Errorf("referenced table `%s` does not exist", foreignKey.ReferencedTableName)
		}
		refSch, err = refTbl.GetSchema(ctx)
		if err != nil {
			return nil, doltdb.ForeignKey{}, err
		}
	}

	refColTags := make([]uint64, len(foreignKey.UnresolvedFKDetails.ReferencedTableColumns))
	for i, name := range foreignKey.UnresolvedFKDetails.ReferencedTableColumns {
		refCol, ok := refSch.GetAllCols().GetByNameCaseInsensitive(name)
		if !ok {
			return nil, doltdb.ForeignKey{}, fmt.Errorf("table `%s` does not have column `%s`", foreignKey.ReferencedTableName, name)
		}
		if !tblCols[i].TypeInfo.Equals(refCol.TypeInfo) {
			return nil, doltdb.ForeignKey{}, fmt.Errorf("column type mismatch on `%s` and `%s`", foreignKey.UnresolvedFKDetails.TableColumns[i], refCol.Name)
		}
		sqlParserType := refCol.TypeInfo.ToSqlType().Type()
		if sqlParserType == sqltypes.Blob || sqlParserType == sqltypes.Text {
			return nil, doltdb.ForeignKey{}, fmt.Errorf("TEXT/BLOB are not valid types for foreign keys")
		}
		refColTags[i] = refCol.Tag
	}

	if foreignKey.IsSelfReferential() {
		for i := range colTags {
			if colTags[i] == refColTags[i] {
				return nil, doltdb.ForeignKey{}, fmt.Errorf("the same column `%s` cannot be used in self referential foreign keys", tblCols[i].Name)
			}
		}
	}

	tableIndex, ok := sch.Indexes().GetIndexByTags(colTags...)
	if !ok {
		// if child index doesn't exist, create it
		ret, err := CreateIndex(ctx, table, "", actualColNames, false, false, "", opts)
		if err != nil {
			return nil, doltdb.ForeignKey{}, err
		}

		table = ret.NewTable
		tableIndex = ret.NewIndex
		root, err = root.PutTable(ctx, foreignKey.TableName, table)
		if err != nil {
			return nil, doltdb.ForeignKey{}, err
		}
		if foreignKey.IsSelfReferential() {
			refTbl = table
		}
	}

	refTableIndex, ok := refSch.Indexes().GetIndexByTags(refColTags...)
	if !ok {
		parentPKs := set.NewUint64Set(refSch.GetPKCols().Tags)
		if parentPKs.ContainsAll(refColTags) {
			// special exception for parent table primary keys
			// todo: make clustered PK index usable as parent table FK index
			var colNames []string
			for _, t := range refColTags {
				c, _ := refSch.GetAllCols().GetByTag(t)
				colNames = append(colNames, c.Name)
			}
			ret, err := CreateIndex(ctx, refTbl, "", colNames, true, false, "", opts)
			if err != nil {
				return nil, doltdb.ForeignKey{}, err
			}
			refTbl = ret.NewTable
			refTableIndex = ret.NewIndex
			root, err = root.PutTable(ctx, foreignKey.ReferencedTableName, refTbl)
			if err != nil {
				return nil, doltdb.ForeignKey{}, err
			}
		} else {
			// parent index must exist
			return nil, doltdb.ForeignKey{}, fmt.Errorf("missing index for constraint '%s' in the referenced table '%s'", foreignKey.Name, foreignKey.ReferencedTableName)
		}
	}

	foreignKey = doltdb.ForeignKey{
		Name:                   foreignKey.Name,
		TableName:              foreignKey.TableName,
		TableIndex:             tableIndex.Name(),
		TableColumns:           colTags,
		ReferencedTableName:    foreignKey.ReferencedTableName,
		ReferencedTableIndex:   refTableIndex.Name(),
		ReferencedTableColumns: refColTags,
		OnUpdate:               foreignKey.OnUpdate,
		OnDelete:               foreignKey.OnDelete,
		UnresolvedFKDetails:    doltdb.UnresolvedFKDetails{},
	}

	tableData, err := table.GetNomsRowData(ctx)
	if err != nil {
		return nil, doltdb.ForeignKey{}, err
	}
	tableIndexData, err := table.GetNomsIndexRowData(ctx, tableIndex.Name())
	if err != nil {
		return nil, doltdb.ForeignKey{}, err
	}
	refTableIndexData, err := refTbl.GetNomsIndexRowData(ctx, refTableIndex.Name())
	if err != nil {
		return nil, doltdb.ForeignKey{}, err
	}
	err = foreignKey.ValidateData(ctx, sch, tableData, tableIndexData, refTableIndexData, tableIndex, refTableIndex, table.ValueReadWriter())
	if err != nil {
		return nil, doltdb.ForeignKey{}, err
	}

	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, doltdb.ForeignKey{}, err
	}
	_ = fkc.RemoveUnresolvedKeyByName(foreignKey.Name)
	err = fkc.AddKeys(foreignKey)
	if err != nil {
		return nil, doltdb.ForeignKey{}, err
	}
	root, err = root.PutForeignKeyCollection(ctx, fkc)
	if err != nil {
		return nil, doltdb.ForeignKey{}, err
	}

	return root, foreignKey, nil
}
