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

package writer

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

type writeDependency interface {
	sql.RowReplacer
	sql.RowUpdater
	sql.RowInserter
	sql.RowDeleter
}

// columnMapping defines a childMap from a table schema to an uniqueIndex schema.
// The ith entry in a columnMapping corresponds to the ith column of the
// uniqueIndex schema, and contains the array uniqueIndex of the corresponding
// table schema column.
type columnMapping []int

// foreignKeyChild enforces the child side of a Foreign Key
// constraint. It does not maintain the Foreign Key uniqueIndex.
type foreignKeyChild struct {
	fk          doltdb.ForeignKey
	parentIndex index.DoltIndex
	expr        []sql.ColumnExpressionType

	// mapping from child table to parent FK uniqueIndex.
	childMap columnMapping

	parent *sqlTableWriter
}

var _ writeDependency = foreignKeyChild{}

func makeFkChildConstraint(ctx context.Context, db string, root *doltdb.RootValue, fk doltdb.ForeignKey, parent *sqlTableWriter) (writeDependency, error) {
	parentTbl, ok, err := root.GetTable(ctx, fk.ReferencedTableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, doltdb.ErrTableNotFound
	}

	parentSch, err := parentTbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	pi := parentSch.Indexes().GetByName(fk.ReferencedTableIndex)

	parentIndex, err := index.GetSecondaryIndex(ctx, db, fk.ReferencedTableName, parentTbl, parentSch, pi)
	if err != nil {
		return nil, err
	}

	childTbl, ok, err := root.GetTable(ctx, fk.TableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, doltdb.ErrTableNotFound
	}

	childSch, err := childTbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	childMap := indexMapForIndex(childSch, fk.TableIndex)
	expr := parentIndex.ColumnExpressionTypes(nil) // todo(andy)

	return foreignKeyChild{
		fk:          fk,
		parentIndex: parentIndex,
		expr:        expr,
		childMap:    childMap,
		parent:      parent,
	}, nil
}

func (c foreignKeyChild) Insert(ctx *sql.Context, row sql.Row) error {
	if containsNulls(c.childMap, row) {
		return nil
	}

	lookup, err := c.parentIndexLookup(ctx, row)
	if err != nil {
		return err
	}

	ok, err := c.parent.Contains(ctx, lookup)
	if err != nil {
		return err
	}
	if !ok {
		return c.violationErr(row)
	}

	return nil
}

func (c foreignKeyChild) Update(ctx *sql.Context, old, new sql.Row) error {
	ok, err := c.childColumnsUnchanged(old, new)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	return c.Insert(ctx, new)
}

func (c foreignKeyChild) ValidateDelete(ctx *sql.Context, row sql.Row) error {
	return nil
}

func (c foreignKeyChild) Delete(ctx *sql.Context, row sql.Row) error {
	return nil
}

func (c foreignKeyChild) StatementBegin(ctx *sql.Context) {
	return
}

func (c foreignKeyChild) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return nil
}

func (c foreignKeyChild) StatementComplete(ctx *sql.Context) error {
	return nil
}

func (c foreignKeyChild) Close(ctx *sql.Context) error {
	return nil
}

// childColumnsUnchanged returns true if the fields indexed by the foreign key are unchanged.
func (c foreignKeyChild) childColumnsUnchanged(old, new sql.Row) (bool, error) {
	return indexColumnsUnchanged(c.expr, c.childMap, old, new)
}

func (c foreignKeyChild) parentIndexLookup(ctx *sql.Context, row sql.Row) (sql.IndexLookup, error) {
	builder := sql.NewIndexBuilder(ctx, c.parentIndex)

	for i, j := range c.childMap {
		builder.Equals(ctx, c.expr[i].Expression, row[j])
	}

	return builder.Build(ctx)
}

func (c foreignKeyChild) violationErr(row sql.Row) error {
	// todo(andy): incorrect string for key
	s := sql.FormatRow(row)
	return sql.ErrForeignKeyChildViolation.New(c.fk.Name, c.fk.TableName, c.fk.ReferencedTableName, s)
}

func indexMapForIndex(sch schema.Schema, idxName string) (mapping columnMapping) {
	idx := sch.Indexes().GetByName(idxName)
	indexTags := idx.IndexedColumnTags()
	mapping = make(columnMapping, len(indexTags))
	cols := sch.GetAllCols().GetColumns()

	for i, col := range cols {
		for j, tag := range indexTags {
			if col.Tag == tag {
				mapping[j] = i
				break
			}
		}
	}
	return
}

func indexColumnsUnchanged(expr []sql.ColumnExpressionType, indexMap columnMapping, old, new sql.Row) (bool, error) {
	for i, j := range indexMap {
		cmp, err := expr[i].Type.Compare(old[j], new[j])
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
}

func containsNulls(mapping columnMapping, row sql.Row) bool {
	for _, j := range mapping {
		if row[j] == nil {
			return true
		}
	}
	return false
}
