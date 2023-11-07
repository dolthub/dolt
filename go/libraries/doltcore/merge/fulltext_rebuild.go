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

// rebuildFullTextIndexes scans the entire root and rebuilds all of the pseudo-index tables. This is not the most
// efficient way to go about it, but it at least produces the correct result.
func rebuildFullTextIndexes(ctx *sql.Context, root *doltdb.RootValue) (*doltdb.RootValue, error) {
	// Grab a list of all tables on the root
	allTableNames, err := root.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}
	// Create a set that we'll check later to remove any orphaned pseudo-index tables.
	// These may appear when a table is renamed on another branch and the index was recreated before merging.
	foundTables := make(map[string]struct{})
	// We'll purge the data from every Full-Text table so that we may rewrite their contents
	for _, tblName := range allTableNames {
		if !doltdb.IsFullTextTable(tblName) {
			continue
		} else if strings.HasSuffix(tblName, "config") {
			// We don't want to purge the config table, we'll just roll with whatever is there for now
			continue
		}
		tbl, ok, err := root.GetTable(ctx, tblName)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("attempted to purge `%s` during Full-Text merge but it could not be found", tblName)
		}
		sch, err := tbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		rows, err := durable.NewEmptyIndex(ctx, tbl.ValueReadWriter(), tbl.NodeStore(), sch)
		if err != nil {
			return nil, err
		}
		tbl, err = tbl.UpdateRows(ctx, rows)
		if err != nil {
			return nil, err
		}
		root, err = root.PutTable(ctx, tblName, tbl)
		if err != nil {
			return nil, err
		}
	}
	// Loop again, this time only looking at tables that declare a Full-Text index
	for _, tblName := range allTableNames {
		if doltdb.IsFullTextTable(tblName) {
			continue
		}
		// Add this table to the found tables, since it's not a pseudo-index table.
		foundTables[tblName] = struct{}{}
		tbl, ok, err := root.GetTable(ctx, tblName)
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
		parentTable, err := createFulltextTable(ctx, tblName, root)
		if err != nil {
			return nil, err
		}

		var configTable *fulltextTable
		var tableSet []fulltext.TableSet
		allFTDoltTables := make(map[string]*fulltextTable)
		for _, idx := range sch.Indexes().AllIndexes() {
			if !idx.IsFullText() {
				continue
			}
			props := idx.FullTextProperties()
			// Add all of the pseudo-index tables as found tables
			foundTables[props.ConfigTable] = struct{}{}
			foundTables[props.PositionTable] = struct{}{}
			foundTables[props.DocCountTable] = struct{}{}
			foundTables[props.GlobalCountTable] = struct{}{}
			foundTables[props.RowCountTable] = struct{}{}
			// The config table is shared, and it's not written to during this process
			if configTable == nil {
				configTable, err = createFulltextTable(ctx, props.ConfigTable, root)
				if err != nil {
					return nil, err
				}
				allFTDoltTables[props.ConfigTable] = configTable
			}
			positionTable, err := createFulltextTable(ctx, props.PositionTable, root)
			if err != nil {
				return nil, err
			}
			docCountTable, err := createFulltextTable(ctx, props.DocCountTable, root)
			if err != nil {
				return nil, err
			}
			globalCountTable, err := createFulltextTable(ctx, props.GlobalCountTable, root)
			if err != nil {
				return nil, err
			}
			rowCountTable, err := createFulltextTable(ctx, props.RowCountTable, root)
			if err != nil {
				return nil, err
			}
			allFTDoltTables[props.PositionTable] = positionTable
			allFTDoltTables[props.DocCountTable] = docCountTable
			allFTDoltTables[props.GlobalCountTable] = globalCountTable
			allFTDoltTables[props.RowCountTable] = rowCountTable
			ftIndex, err := index.ConvertFullTextToSql(ctx, "", tblName, sch, idx)
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

			rowIter, err := createRowIterForTable(ctx, tbl, sch)
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
			root, err = root.PutTable(ctx, ftTable.Name(), newTbl)
			if err != nil {
				return nil, err
			}
		}
	}
	// Our last loop removes any orphaned pseudo-index tables
	for _, tblName := range allTableNames {
		if _, found := foundTables[tblName]; found || !doltdb.IsFullTextTable(tblName) {
			continue
		}
		root, err = root.RemoveTables(ctx, true, true, tblName)
		if err != nil {
			return nil, err
		}
	}
	return root, nil
}

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
