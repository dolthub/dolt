// Copyright 2023 Dolthub, Inc.
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
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/fulltext"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

// rebuildableFulltextTable contains a table and schema that should have its Full-Text indexes rebuilt.
type rebuildableFulltextTable struct {
	Name   string
	Table  *doltdb.Table
	Schema schema.Schema
}

// rebuildFullTextIndexes scans the mergedRoot and rebuilds all of the
// pseudo-index tables that were modified by both roots (ours and theirs), or
// had parents that were modified by both roots.
func rebuildFullTextIndexes(ctx *sql.Context, mergedRoot, ourRoot, theirRoot doltdb.RootValue, visitedTables map[string]struct{}) (doltdb.RootValue, error) {
	// Grab a list of all tables on the root
	allTableNames, err := mergedRoot.GetTableNames(ctx, doltdb.DefaultSchemaName)
	if err != nil {
		return nil, err
	}

	// Contains all of the tables for which we need to rebuild full-text indexes.
	var tablesToRebuild []rebuildableFulltextTable

	// This loop will create a set of tables and psuedo-index tables which
	// will not be deleted at the end of this loop. Orphaned psuedo-index
	// tables, which no longer have a parent table, will be deleted, for
	// example, because they will not appear in this set.
	doNotDeleteTables := make(map[string]struct{})

	// The following loop will populate |doNotDeleteTables| and
	// |tablesToRebuild|.
	//
	// For |doNotDeleteTables|, its logic is as follows:
	// 1) Every existing real table in |mergedRoot| should be in it.
	// 2) The psuedo-table for every existing full-text index in every
	// existing table in |mergedRoot| should be in it.
	//
	// For |tablesToRebuild|, its logic is as follows:
	//
	// 1) If the table or any of its full-text index pseudo-tables were
	// visited by the merge--i.e., merger.MergeTable() reported an
	// operation result other than |TableUnmodified|.
	// 2) *And* if the table or any of its full-text index pseudo-tables
	// are different between the merge base and ours.
	// 3) *And* if the table or any of its full-text index pseudo-tables
	// are different between the merge base and theirs.
	//
	// Then the table or its full-text index pseudo-tables were potentially
	// involved in an actual three-way merge and the full-text index
	// pseudo-tables could be out of date.
	for _, tblName := range allTableNames {
		if doltdb.IsFullTextTable(tblName, doltdb.HasDoltPrefix(tblName)) {
			continue
		}
		// Add this table to the non-deletion set tables, since it's not a pseudo-index table.
		doNotDeleteTables[tblName] = struct{}{}

		tbl, ok, err := mergedRoot.GetTable(ctx, doltdb.TableName{Name: tblName})
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("attempted to load `%s` during Full-Text merge but it could not be found", tblName)
		}
		sch, err := tbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		if !sch.Indexes().ContainsFullTextIndex() {
			continue
		}

		// Also adds items to |doNotDeleteTables|.
		needsRebuild, err := tableNeedsFullTextIndexRebuild(ctx, tblName, tbl, sch, mergedRoot, ourRoot, theirRoot, visitedTables, doNotDeleteTables)
		if err != nil {
			return nil, err
		}
		if needsRebuild {
			tablesToRebuild = append(tablesToRebuild, rebuildableFulltextTable{
				Name:   tblName,
				Table:  tbl,
				Schema: sch,
			})
		}

	}

	// Now loop over the tables that we were visited and rebuild only if they were modified in both roots
	for _, tableToRebuild := range tablesToRebuild {
		mergedRoot, err = rebuildFullTextIndexesForTable(ctx, tableToRebuild, mergedRoot)
		if err != nil {
			return nil, err
		}
	}

	// Our last loop removes any orphaned pseudo-index tables
	for _, tblName := range allTableNames {
		if _, doNotDelete := doNotDeleteTables[tblName]; doNotDelete || !doltdb.IsFullTextTable(tblName, doltdb.HasDoltPrefix(tblName)) {
			continue
		}
		// TODO: schema name
		mergedRoot, err = mergedRoot.RemoveTables(ctx, true, true, doltdb.TableName{Name: tblName})
		if err != nil {
			return nil, err
		}
	}

	return mergedRoot, nil
}

func tableNeedsFullTextIndexRebuild(ctx *sql.Context, tblName string, tbl *doltdb.Table, sch schema.Schema,
	mergedRoot, ourRoot, theirRoot doltdb.RootValue,
	visitedTables map[string]struct{}, doNotDeleteTables map[string]struct{}) (bool, error) {
	// Even if the parent table was not visited, we still need to check every pseudo-index table due to potential
	// name overlapping between roots. This also applies to checking whether both ours and theirs have changes.
	_, wasVisited := visitedTables[tblName]
	oursChanged, err := tableChangedFromRoot(ctx, tblName, tbl, ourRoot)
	if err != nil {
		return false, err
	}
	theirsChanged, err := tableChangedFromRoot(ctx, tblName, tbl, theirRoot)
	if err != nil {
		return false, err
	}
	for _, idx := range sch.Indexes().AllIndexes() {
		if !idx.IsFullText() {
			continue
		}
		props := idx.FullTextProperties()
		for _, ftTable := range props.TableNameSlice() {
			// Add all of the pseudo-index tables to the non-deletion set
			doNotDeleteTables[ftTable] = struct{}{}

			// Check if the pseudo-index table was visited
			if !wasVisited {
				_, wasVisited = visitedTables[ftTable]
			}

			// Check if the pseudo-index table changed in both our root and their root
			if !oursChanged {
				oursChanged, err = tableChangedBetweenRoots(ctx, tblName, ourRoot, mergedRoot)
				if err != nil {
					return false, err
				}
			}

			if !theirsChanged {
				theirsChanged, err = tableChangedBetweenRoots(ctx, tblName, theirRoot, mergedRoot)
				if err != nil {
					return false, err
				}
			}
		}
	}

	// If least one table was visited and something was different in all three roots, we rebuild all the indexes.
	return wasVisited && oursChanged && theirsChanged, nil
}

func rebuildFullTextIndexesForTable(ctx *sql.Context, tableToRebuild rebuildableFulltextTable, mergedRoot doltdb.RootValue) (doltdb.RootValue, error) {
	parentTable, err := createFulltextTable(ctx, tableToRebuild.Name, mergedRoot)
	if err != nil {
		return nil, err
	}

	var configTable *fulltextTable
	var tableSet []fulltext.TableSet
	allFTDoltTables := make(map[string]*fulltextTable)
	for _, idx := range tableToRebuild.Schema.Indexes().AllIndexes() {
		if !idx.IsFullText() {
			continue
		}
		props := idx.FullTextProperties()
		// Purge the existing data in each table
		mergedRoot, err = purgeFulltextTableData(ctx, mergedRoot, props.TableNameSlice()...)
		if err != nil {
			return nil, err
		}
		// The config table is shared, and it's not written to during this process
		if configTable == nil {
			configTable, err = createFulltextTable(ctx, props.ConfigTable, mergedRoot)
			if err != nil {
				return nil, err
			}
			allFTDoltTables[props.ConfigTable] = configTable
		}
		positionTable, err := createFulltextTable(ctx, props.PositionTable, mergedRoot)
		if err != nil {
			return nil, err
		}
		docCountTable, err := createFulltextTable(ctx, props.DocCountTable, mergedRoot)
		if err != nil {
			return nil, err
		}
		globalCountTable, err := createFulltextTable(ctx, props.GlobalCountTable, mergedRoot)
		if err != nil {
			return nil, err
		}
		rowCountTable, err := createFulltextTable(ctx, props.RowCountTable, mergedRoot)
		if err != nil {
			return nil, err
		}
		allFTDoltTables[props.PositionTable] = positionTable
		allFTDoltTables[props.DocCountTable] = docCountTable
		allFTDoltTables[props.GlobalCountTable] = globalCountTable
		allFTDoltTables[props.RowCountTable] = rowCountTable
		ftIndex, err := index.ConvertFullTextToSql(ctx, "", tableToRebuild.Name, tableToRebuild.Schema, idx)
		if err != nil {
			return nil, err
		}
		tableSet = append(tableSet, fulltext.TableSet{
			Index:       ftIndex.(fulltext.Index),
			Position:    positionTable,
			DocCount:    docCountTable,
			GlobalCount: globalCountTable,
			RowCount:    rowCountTable,
		})
	}

	// We'll write the entire contents of our table into the Full-Text editor
	ftEditor, err := fulltext.CreateEditor(ctx, parentTable, configTable, tableSet...)
	if err != nil {
		return nil, err
	}
	err = func() error {
		defer ftEditor.Close(ctx)
		ftEditor.StatementBegin(ctx)
		defer ftEditor.StatementComplete(ctx)

		rowIter, err := createRowIterForTable(ctx, tableToRebuild.Table, tableToRebuild.Schema)
		if err != nil {
			return err
		}
		defer rowIter.Close(ctx)

		row, err := rowIter.Next(ctx)
		for ; err == nil; row, err = rowIter.Next(ctx) {
			if err = ftEditor.Insert(ctx, row); err != nil {
				return err
			}
		}
		if err != nil && err != io.EOF {
			return err
		}
		return nil
	}()
	if err != nil {
		return nil, err
	}

	// Update the root with all of the new tables' contents
	for _, ftTable := range allFTDoltTables {
		newTbl, err := ftTable.ApplyToTable(ctx)
		if err != nil {
			return nil, err
		}
		mergedRoot, err = mergedRoot.PutTable(ctx, doltdb.TableName{Name: ftTable.Name()}, newTbl)
		if err != nil {
			return nil, err
		}
	}

	return mergedRoot, nil
}

// createRowIterForTable creates a sql.RowIter for the given table.
func createRowIterForTable(ctx *sql.Context, t *doltdb.Table, sch schema.Schema) (sql.RowIter, error) {
	rowData, err := t.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	rows := durable.ProllyMapFromIndex(rowData)
	rowCount, err := rows.Count()
	if err != nil {
		return nil, err
	}

	iter, err := rows.FetchOrdinalRange(ctx, 0, uint64(rowCount))
	if err != nil {
		return nil, err
	}

	return index.NewProllyRowIterForMap(sch, rows, iter, nil), nil
}

// purgeFulltextTableData purges all Full-Text tables with the names given. Ignores any tables that are not Full-Text.
// Also ignores Full-Text config tables. Returns the updated root with the tables purged.
func purgeFulltextTableData(ctx *sql.Context, root doltdb.RootValue, tableNames ...string) (doltdb.RootValue, error) {
	for _, tableName := range tableNames {
		if !doltdb.IsFullTextTable(tableName, doltdb.HasDoltPrefix(tableName)) {
			continue
		} else if strings.HasSuffix(tableName, "config") {
			// We don't want to purge the config table, we'll just roll with whatever is there for now
			continue
		}
		tbl, ok, err := root.GetTable(ctx, doltdb.TableName{Name: tableName})
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("attempted to purge `%s` during Full-Text merge but it could not be found", tableName)
		}
		sch, err := tbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		rows, err := durable.NewEmptyIndex(ctx, tbl.ValueReadWriter(), tbl.NodeStore(), sch, false)
		if err != nil {
			return nil, err
		}
		tbl, err = tbl.UpdateRows(ctx, rows)
		if err != nil {
			return nil, err
		}
		root, err = root.PutTable(ctx, doltdb.TableName{Name: tableName}, tbl)
		if err != nil {
			return nil, err
		}
	}
	return root, nil
}

// tableChangedBetweenRoots returns whether the given table changed between roots.
func tableChangedBetweenRoots(ctx *sql.Context, tblName string, fromRoot, toRoot doltdb.RootValue) (bool, error) {
	tbl, ok, err := toRoot.GetTable(ctx, doltdb.TableName{Name: tblName})
	if err != nil {
		return false, err
	}
	if !ok {
		return tableChangedFromRoot(ctx, tblName, nil, fromRoot)
	}
	return tableChangedFromRoot(ctx, tblName, tbl, fromRoot)
}

// tableChangedFromRoot returns whether the given table has changed compared to the one found in the given root. If the
// table does not exist in the root, then that counts as a change. A nil `tbl` is valid, which then checks if the table
// exists in the root.
func tableChangedFromRoot(ctx *sql.Context, tblName string, tbl *doltdb.Table, root doltdb.RootValue) (bool, error) {
	// If `tbl` is nil, then we simply check if the table exists in the root
	if tbl == nil {
		return root.HasTable(ctx, doltdb.TableName{Name: tblName})
	}
	fromTbl, ok, err := root.GetTable(ctx, doltdb.TableName{Name: tblName})
	if err != nil {
		return false, err
	}
	if !ok {
		return true, nil
	}
	// If the tables have different hashes, then something has changed. We don't know exactly what has changed, but
	// we'll be conservative and accept any change.
	tblHash, err := tbl.HashOf()
	if err != nil {
		return false, err
	}
	fromHash, err := fromTbl.HashOf()
	if err != nil {
		return false, err
	}
	return !tblHash.Equal(fromHash), nil
}
