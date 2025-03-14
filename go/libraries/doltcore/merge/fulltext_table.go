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

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/fulltext"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

var sharePool = pool.NewBuffPool()

type fulltextTable struct {
	GMSTable *memory.Table
	Table    *doltdb.Table
	Sch      schema.Schema
	SqlSch   sql.Schema
}

var _ fulltext.EditableTable = (*fulltextTable)(nil)

// createFulltextTable creates an in-memory Full-Text table from the given table name on the given root. This table will
// be used to read/write data from/to the underlying Dolt table.
func createFulltextTable(ctx *sql.Context, name string, root doltdb.RootValue) (*fulltextTable, error) {
	tbl, ok, err := root.GetTable(ctx, doltdb.TableName{Name: name})
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("attempted to load Full-Text table `%s` during Full-Text merge but it could not be found", name)
	}
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	sqlSch, err := sqlutil.FromDoltSchema("", name, sch)
	if err != nil {
		return nil, err
	}

	gmsDb := memory.NewDatabase("gms_db")
	gmsTable := memory.NewLocalTable(gmsDb, name, sqlSch, nil)
	gmsTable.EnablePrimaryKeyIndexes()
	return &fulltextTable{
		GMSTable: gmsTable,
		Table:    tbl,
		Sch:      sch,
		SqlSch:   sqlSch.Schema,
	}, nil
}

// Name implements the interface fulltext.EditableTable.
func (table *fulltextTable) Name() string {
	return table.GMSTable.Name()
}

// String implements the interface fulltext.EditableTable.
func (table *fulltextTable) String() string {
	return table.GMSTable.String()
}

// Schema implements the interface fulltext.EditableTable.
func (table *fulltextTable) Schema() sql.Schema {
	return table.GMSTable.Schema()
}

// Collation implements the interface fulltext.EditableTable.
func (table *fulltextTable) Collation() sql.CollationID {
	return table.GMSTable.Collation()
}

// Partitions implements the interface fulltext.EditableTable.
func (table *fulltextTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return table.GMSTable.Partitions(ctx)
}

// PartitionRows implements the interface fulltext.EditableTable.
func (table *fulltextTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return table.GMSTable.PartitionRows(ctx, partition)
}

// Inserter implements the interface fulltext.EditableTable.
func (table *fulltextTable) Inserter(ctx *sql.Context) sql.RowInserter {
	return table.GMSTable.Inserter(ctx)
}

// Updater implements the interface fulltext.EditableTable.
func (table *fulltextTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return table.GMSTable.Updater(ctx)
}

// Deleter implements the interface fulltext.EditableTable.
func (table *fulltextTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	return table.GMSTable.Deleter(ctx)
}

// IndexedAccess implements the interface fulltext.EditableTable.
func (table *fulltextTable) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	return table.GMSTable.IndexedAccess(lookup)
}

// GetIndexes implements the interface fulltext.EditableTable.
func (table *fulltextTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return table.GMSTable.GetIndexes(ctx)
}

// PreciseMatch implements the interface fulltext.EditableTable.
func (table *fulltextTable) PreciseMatch() bool {
	return false
}

// ApplyToTable writes the data from the internal GMS table to the internal Dolt table, then returns the updated Dolt
// table. The updated Dolt table is not stored.
func (table *fulltextTable) ApplyToTable(ctx *sql.Context) (*doltdb.Table, error) {
	partIter, err := table.GMSTable.Partitions(ctx)
	if err != nil {
		return nil, err
	}
	rowIter := sql.NewTableRowIter(ctx, table.GMSTable, partIter)

	idx, err := table.Table.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	m := durable.ProllyMapFromIndex(idx)
	keyDesc, valDesc := m.Descriptors()
	keyMap, valMap := ordinalMappingsFromSchema(table.SqlSch, table.Sch)
	mut := m.Mutate()
	keyBld := val.NewTupleBuilder(keyDesc, m.NodeStore())
	valBld := val.NewTupleBuilder(valDesc, m.NodeStore())

	sqlRow, err := rowIter.Next(ctx)
	for ; err == nil; sqlRow, err = rowIter.Next(ctx) {
		for to := range keyMap {
			from := keyMap.MapOrdinal(to)
			if err = tree.PutField(ctx, mut.NodeStore(), keyBld, to, sqlRow[from]); err != nil {
				return nil, err
			}
		}
		k := keyBld.Build(sharePool)

		for to := range valMap {
			from := valMap.MapOrdinal(to)
			if err = tree.PutField(ctx, mut.NodeStore(), valBld, to, sqlRow[from]); err != nil {
				return nil, err
			}
		}
		v := valBld.Build(sharePool)

		if err = mut.Put(ctx, k, v); err != nil {
			return nil, err
		}
	}
	if err != nil && err != io.EOF {
		return nil, err
	}

	mapped, err := mut.Map(ctx)
	if err != nil {
		return nil, err
	}
	return table.Table.UpdateRows(ctx, durable.IndexFromProllyMap(mapped))
}

func ordinalMappingsFromSchema(from sql.Schema, to schema.Schema) (km, vm val.OrdinalMapping) {
	km = makeOrdinalMapping(from, to.GetPKCols())
	vm = makeOrdinalMapping(from, to.GetNonPKCols())
	return
}

func makeOrdinalMapping(from sql.Schema, to *schema.ColCollection) (m val.OrdinalMapping) {
	m = make(val.OrdinalMapping, len(to.GetColumns()))
	for i := range m {
		name := to.GetByIndex(i).Name
		for j, col := range from {
			if col.Name == name {
				m[i] = j
			}
		}
	}
	return
}
