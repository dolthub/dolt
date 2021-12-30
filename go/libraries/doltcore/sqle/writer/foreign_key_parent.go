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
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

// foreignKeyParent enforces the parent side of a Foreign Key
// constraint, and executes reference option logic.
// It does not maintain the Foreign Key uniqueIndex.
type foreignKeyParent struct {
	fk         doltdb.ForeignKey
	childIndex index.DoltIndex
	expr       []sql.ColumnExpressionType

	// mapping from parent table to child FK uniqueIndex.
	parentMap columnMapping
	// mapping from child table to child FK uniqueIndex.
	childMap columnMapping

	child *sqlTableWriter
}

func makeFkParentConstraint(ctx context.Context, db string, root *doltdb.RootValue, fk doltdb.ForeignKey, child *sqlTableWriter) (writeDependency, error) {
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
	ci := childSch.Indexes().GetByName(fk.TableIndex)

	childIndex, err := index.GetSecondaryIndex(ctx, db, fk.TableName, childTbl, childSch, ci)
	if err != nil {
		return nil, err
	}

	parentMap := indexMapForIndex(parentSch, fk.ReferencedTableIndex)
	childMap := indexMapForIndex(childSch, fk.TableIndex)
	expr := childIndex.ColumnExpressionTypes(nil) // todo(andy)

	return foreignKeyParent{
		fk:         fk,
		childIndex: childIndex,
		expr:       expr,
		parentMap:  parentMap,
		childMap:   childMap,
		child:      child,
	}, nil
}

var _ writeDependency = foreignKeyParent{}

func (p foreignKeyParent) Insert(ctx *sql.Context, row sql.Row) error {
	return nil
}

func (p foreignKeyParent) Update(ctx *sql.Context, old, new sql.Row) error {
	ok, err := p.parentColumnsUnchanged(old, new)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	if containsNulls(p.parentMap, new) {
		return nil
	}

	lookup, err := p.childIndexLookup(ctx, old)
	if err != nil {
		return err
	}

	iter, err := index.RowIterForIndexLookup(ctx, lookup)
	if err != nil {
		return err
	}

	if isRestrict(p.fk.OnUpdate) {
		rows, err := sql.RowIterToRows(ctx, iter)
		if err != nil {
			return err
		}
		if len(rows) > 0 {
			return p.violationErr(new)
		}
	}

	return p.executeUpdateReferenceOption(ctx, new, iter)
}

func (p foreignKeyParent) executeUpdateReferenceOption(ctx *sql.Context, parent sql.Row, iter sql.RowIter) error {
	for {
		before, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch p.fk.OnUpdate {
		case doltdb.ForeignKeyReferenceOption_SetNull:
			after := childRowSetNull(p.childMap, before)
			if err = p.child.update(ctx, before, after); err != nil {
				return err
			}

		case doltdb.ForeignKeyReferenceOption_Cascade:
			after := childRowCascade(p.parentMap, p.childMap, parent, before)
			if err = p.child.update(ctx, before, after); err != nil {
				return err
			}

		default:
			panic("unexpected reference option")
		}
	}
}

func (p foreignKeyParent) Delete(ctx *sql.Context, row sql.Row) error {
	if containsNulls(p.parentMap, row) {
		return nil
	}

	lookup, err := p.childIndexLookup(ctx, row)
	if err != nil {
		return err
	}

	iter, err := index.RowIterForIndexLookup(ctx, lookup)
	if err != nil {
		return err
	}

	if isRestrict(p.fk.OnDelete) {
		rows, err := sql.RowIterToRows(ctx, iter)
		if err != nil {
			return err
		}
		if len(rows) > 0 {
			return p.violationErr(row)
		}
	}

	return p.executeDeleteReferenceOption(ctx, row, iter)
}

func (p foreignKeyParent) executeDeleteReferenceOption(ctx *sql.Context, parent sql.Row, iter sql.RowIter) error {
	for {
		before, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch p.fk.OnUpdate {
		case doltdb.ForeignKeyReferenceOption_SetNull:
			after := childRowSetNull(p.childMap, before)
			if err = p.child.update(ctx, before, after); err != nil {
				return err
			}

		case doltdb.ForeignKeyReferenceOption_Cascade:
			if err = p.child.delete(ctx, before); err != nil {
				return err
			}

		default:
			panic("unexpected reference option")
		}
	}
}

func (p foreignKeyParent) Close(ctx *sql.Context) error {
	return nil
}

func (p foreignKeyParent) StatementBegin(ctx *sql.Context) {
	p.child.StatementBegin(ctx)
}

func (p foreignKeyParent) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return p.child.DiscardChanges(ctx, errorEncountered)
}

func (p foreignKeyParent) StatementComplete(ctx *sql.Context) error {
	return p.child.StatementComplete(ctx)
}

func (p foreignKeyParent) parentColumnsUnchanged(old, new sql.Row) (bool, error) {
	return indexColumnsUnchanged(p.expr, p.parentMap, old, new)
}

func (p foreignKeyParent) childIndexLookup(ctx *sql.Context, row sql.Row) (sql.IndexLookup, error) {
	builder := sql.NewIndexBuilder(ctx, p.childIndex)

	for i, j := range p.parentMap {
		builder.Equals(ctx, p.expr[i].Expression, row[j])
	}

	return builder.Build(ctx)
}

func (p foreignKeyParent) violationErr(row sql.Row) error {
	// todo(andy): incorrect string for key
	s := sql.FormatRow(row)
	return sql.ErrForeignKeyParentViolation.New(p.fk.Name, p.fk.TableName, p.fk.ReferencedTableName, s)
}

func isRestrict(referenceOption doltdb.ForeignKeyReferenceOption) bool {
	return referenceOption == doltdb.ForeignKeyReferenceOption_DefaultAction ||
		referenceOption == doltdb.ForeignKeyReferenceOption_NoAction ||
		referenceOption == doltdb.ForeignKeyReferenceOption_Restrict
}

func childRowSetNull(childMap columnMapping, child sql.Row) (updated sql.Row) {
	updated = child.Copy()
	for _, j := range childMap {
		updated[j] = nil
	}
	return
}

func childRowCascade(parentMap, childMap columnMapping, parent, child sql.Row) (updated sql.Row) {
	updated = child.Copy()
	for i := range parentMap {
		pi, ci := parentMap[i], childMap[i]
		updated[ci] = parent[pi]
	}
	return
}
