// Copyright 2024 Dolthub, Inc.
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

package sqle

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/go-mysql-server/sql"
	"sort"
	"strings"
)

func CreateDoltCITableForeignKey(
	ctx context.Context,
	root doltdb.RootValue,
	tbl *doltdb.Table,
	sch schema.Schema,
	sqlFk sql.ForeignKeyConstraint,
	onUpdateRefAction, onDeleteRefAction doltdb.ForeignKeyReferentialAction,
	schemaName string) (doltdb.ForeignKey, error) {
	if !sqlFk.IsResolved {
		return doltdb.ForeignKey{
			Name:                   sqlFk.Name,
			TableName:              doltdb.TableName{Name: sqlFk.Table},
			TableIndex:             "",
			TableColumns:           nil,
			ReferencedTableName:    doltdb.TableName{Name: sqlFk.ParentTable},
			ReferencedTableIndex:   "",
			ReferencedTableColumns: nil,
			OnUpdate:               onUpdateRefAction,
			OnDelete:               onDeleteRefAction,
			UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
				TableColumns:           sqlFk.Columns,
				ReferencedTableColumns: sqlFk.ParentColumns,
			},
		}, nil
	}
	colTags := make([]uint64, len(sqlFk.Columns))
	for i, col := range sqlFk.Columns {
		tableCol, ok := sch.GetAllCols().GetByNameCaseInsensitive(col)
		if !ok {
			return doltdb.ForeignKey{}, fmt.Errorf("table `%s` does not have column `%s`", sqlFk.Table, col)
		}
		colTags[i] = tableCol.Tag
	}

	var refTbl *doltdb.Table
	var refSch schema.Schema
	if sqlFk.IsSelfReferential() {
		refTbl = tbl
		refSch = sch
	} else {
		var ok bool
		var err error
		// TODO: the parent table can be in another schema

		refTbl, _, ok, err = doltdb.GetTableInsensitive(ctx, root, doltdb.TableName{Name: sqlFk.ParentTable, Schema: schemaName})
		if err != nil {
			return doltdb.ForeignKey{}, err
		}
		if !ok {
			return doltdb.ForeignKey{}, fmt.Errorf("referenced table `%s` does not exist", sqlFk.ParentTable)
		}
		refSch, err = refTbl.GetSchema(ctx)
		if err != nil {
			return doltdb.ForeignKey{}, err
		}
	}

	refColTags := make([]uint64, len(sqlFk.ParentColumns))
	for i, name := range sqlFk.ParentColumns {
		refCol, ok := refSch.GetAllCols().GetByNameCaseInsensitive(name)
		if !ok {
			return doltdb.ForeignKey{}, fmt.Errorf("table `%s` does not have column `%s`", sqlFk.ParentTable, name)
		}
		refColTags[i] = refCol.Tag
	}

	var tableIndexName, refTableIndexName string
	tableIndex, ok, err := FindIndexWithPrefix(sch, sqlFk.Columns)
	if err != nil {
		return doltdb.ForeignKey{}, err
	}
	// Use secondary index if found; otherwise it will use empty string, indicating primary key
	if ok {
		tableIndexName = tableIndex.Name()
	}
	refTableIndex, ok, err := FindIndexWithPrefix(refSch, sqlFk.ParentColumns)
	if err != nil {
		return doltdb.ForeignKey{}, err
	}
	// Use secondary index if found; otherwise it will use  empty string, indicating primary key
	if ok {
		refTableIndexName = refTableIndex.Name()
	}
	return doltdb.ForeignKey{
		Name:                   sqlFk.Name,
		TableName:              doltdb.TableName{Name: sqlFk.Table},
		TableIndex:             tableIndexName,
		TableColumns:           colTags,
		ReferencedTableName:    doltdb.TableName{Name: sqlFk.ParentTable},
		ReferencedTableIndex:   refTableIndexName,
		ReferencedTableColumns: refColTags,
		OnUpdate:               onUpdateRefAction,
		OnDelete:               onDeleteRefAction,
		UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
			TableColumns:           sqlFk.Columns,
			ReferencedTableColumns: sqlFk.ParentColumns,
		},
	}, nil
}

func FindIndexWithPrefix(sch schema.Schema, prefixCols []string) (schema.Index, bool, error) {
	type idxWithLen struct {
		schema.Index
		colLen int
	}

	prefixCols = lowercaseSlice(prefixCols)
	indexes := sch.Indexes().AllIndexes()
	colLen := len(prefixCols)
	var indexesWithLen []idxWithLen
	for _, idx := range indexes {
		idxCols := lowercaseSlice(idx.ColumnNames())
		if ok, prefixCount := colsAreIndexSubset(prefixCols, idxCols); ok && prefixCount == colLen {
			indexesWithLen = append(indexesWithLen, idxWithLen{idx, len(idxCols)})
		}
	}
	if len(indexesWithLen) == 0 {
		return nil, false, nil
	}

	sort.Slice(indexesWithLen, func(i, j int) bool {
		idxI := indexesWithLen[i]
		idxJ := indexesWithLen[j]
		if idxI.colLen == colLen && idxJ.colLen != colLen {
			return true
		} else if idxI.colLen != colLen && idxJ.colLen == colLen {
			return false
		} else if idxI.colLen != idxJ.colLen {
			return idxI.colLen > idxJ.colLen
		} else if idxI.IsUnique() != idxJ.IsUnique() {
			// prefer unique indexes
			return idxI.IsUnique() && !idxJ.IsUnique()
		} else {
			return idxI.Index.Name() < idxJ.Index.Name()
		}
	})
	sortedIndexes := make([]schema.Index, len(indexesWithLen))
	for i := 0; i < len(sortedIndexes); i++ {
		sortedIndexes[i] = indexesWithLen[i].Index
	}
	return sortedIndexes[0], true, nil
}

func colsAreIndexSubset(cols, indexCols []string) (ok bool, prefixCount int) {
	if len(cols) > len(indexCols) {
		return false, 0
	}

	visitedIndexCols := make([]bool, len(indexCols))
	for _, expr := range cols {
		found := false
		for j, indexExpr := range indexCols {
			if visitedIndexCols[j] {
				continue
			}
			if expr == indexExpr {
				visitedIndexCols[j] = true
				found = true
				break
			}
		}
		if !found {
			return false, 0
		}
	}

	// This checks the length of the prefix by checking how many true booleans are encountered before the first false
	for i, visitedCol := range visitedIndexCols {
		if visitedCol {
			continue
		}
		return true, i
	}

	return true, len(cols)
}

func lowercaseSlice(strs []string) []string {
	newStrs := make([]string, len(strs))
	for i, str := range strs {
		newStrs[i] = strings.ToLower(str)
	}
	return newStrs
}
