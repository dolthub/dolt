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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

// foreignKeyRecursive enforces the parent side of a Foreign Key
// constraint, and executes reference option logic.
// It does not maintain the Foreign Key uniqueIndex.
type foreignKeyRecursive struct {
	fk doltdb.ForeignKey

	childIdx  index.DoltIndex
	childExpr []sql.ColumnExpressionType

	parentIdx  index.DoltIndex
	parentExpr []sql.ColumnExpressionType

	// mapping from child columns to fk parent index.
	childMap indexMapping
	// mapping from parent columns to fk child index.
	parentMap indexMapping

	self *sqlTableWriter
	sch  sql.Schema

	memo *set.StrSet
}

func makeFkRecursiveConstraint(ctx context.Context, db string, root *doltdb.RootValue, fk doltdb.ForeignKey, self *sqlTableWriter) (writeDependency, error) {
	tbl, ok, err := root.GetTable(ctx, fk.ReferencedTableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, doltdb.ErrTableNotFound
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	pi := sch.Indexes().GetByName(fk.ReferencedTableIndex)
	parentIndex, err := index.GetSecondaryIndex(ctx, db, fk.TableName, tbl, sch, pi)
	if err != nil {
		return nil, err
	}
	parentExpr := parentIndex.ColumnExpressionTypes(nil) // todo(andy)

	ci := sch.Indexes().GetByName(fk.TableIndex)
	childIndex, err := index.GetSecondaryIndex(ctx, db, fk.TableName, tbl, sch, ci)
	if err != nil {
		return nil, err
	}
	childExpr := childIndex.ColumnExpressionTypes(nil) // todo(andy)

	parentMap := indexMapForIndex(sch, fk.ReferencedTableIndex)
	childMap := indexMapForIndex(sch, fk.TableIndex)

	s, err := sqlutil.FromDoltSchema(self.tableName, sch)
	if err != nil {
		return nil, err
	}

	return &foreignKeyRecursive{
		fk:         fk,
		childIdx:   childIndex,
		childExpr:  childExpr,
		parentIdx:  parentIndex,
		parentExpr: parentExpr,
		parentMap:  parentMap,
		childMap:   childMap,
		self:       self,
		sch:        s.Schema,
	}, nil
}

var _ writeDependency = &foreignKeyRecursive{}

func (r *foreignKeyRecursive) Insert(ctx *sql.Context, row sql.Row) error {
	if containsNulls(r.childMap, row) {
		return nil
	}
	ok, err := r.selfReferentialRow(row)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	lookup, err := r.childLookupParent(ctx, row)
	if err != nil {
		return err
	}

	ok, err = r.self.Contains(ctx, lookup)
	if err != nil {
		return err
	}
	if !ok {
		return r.childErr(row)
	}

	return nil
}

func (r *foreignKeyRecursive) Update(ctx *sql.Context, old, new sql.Row) error {
	ok, err := r.childColumnsUnchanged(old, new)
	if err != nil {
		return err
	}
	if !ok {
		if err = r.Insert(ctx, new); err != nil {
			return err
		}
	}

	ok, err = r.parentColumnsUnchanged(old, new)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	if containsNulls(r.parentMap, new) {
		return nil
	}

	lookup, err := r.parentLookupChild(ctx, old)
	if err != nil {
		return err
	}

	iter, err := index.RowIterForIndexLookup(ctx, lookup)
	if err != nil {
		return err
	}

	// always restrict on update
	rows, err := sql.RowIterToRows(ctx, iter)
	if err != nil {
		return err
	}
	if len(rows) > 0 {
		return r.parentErr(new)
	}

	return nil
}

func (r *foreignKeyRecursive) Delete(ctx *sql.Context, row sql.Row) error {
	if containsNulls(r.parentMap, row) {
		return nil
	}

	lookup, err := r.parentLookupChild(ctx, row)
	if err != nil {
		return err
	}

	iter, err := index.RowIterForIndexLookup(ctx, lookup)
	if err != nil {
		return err
	}
	r.memo.Add(sql.FormatRow(row))

	for {
		before, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch r.fk.OnUpdate {
		case doltdb.ForeignKeyReferenceOption_SetNull:
			after := childRowSetNull(r.childMap, before)
			if err = r.Update(ctx, before, after); err != nil {
				return err
			}

		case doltdb.ForeignKeyReferenceOption_Cascade:
			if r.memo.Contains(sql.FormatRow(before)) {
				continue // break loops
			}
			if err = r.Delete(ctx, before); err != nil {
				return err
			}

		case doltdb.ForeignKeyReferenceOption_DefaultAction,
			doltdb.ForeignKeyReferenceOption_NoAction,
			doltdb.ForeignKeyReferenceOption_Restrict:
			return r.parentErr(row)

		default:
			panic("unexpected reference option")
		}
	}
	return nil
}

func (r *foreignKeyRecursive) Close(ctx *sql.Context) error {
	return nil
}

func (r *foreignKeyRecursive) StatementBegin(ctx *sql.Context) {
	r.memo = set.NewStrSet(nil)
	return
}

func (r *foreignKeyRecursive) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	r.memo = nil
	return nil
}

func (r *foreignKeyRecursive) StatementComplete(ctx *sql.Context) error {
	r.memo = nil
	return nil
}

func (r *foreignKeyRecursive) childColumnsUnchanged(old, new sql.Row) (bool, error) {
	return indexColumnsUnchanged(r.childExpr, r.childMap, old, new)
}

func (r *foreignKeyRecursive) parentColumnsUnchanged(old, new sql.Row) (bool, error) {
	return indexColumnsUnchanged(r.parentExpr, r.parentMap, old, new)
}

func (r *foreignKeyRecursive) selfReferentialRow(row sql.Row) (bool, error) {
	for i := range r.childMap {
		childVal := row[r.childMap[i]]
		parentVal := row[r.parentMap[i]]

		cmp, err := r.childExpr[i].Type.Compare(childVal, parentVal)
		if err != nil || cmp != 0 {
			return false, err
		}
	}
	return true, nil
}

func (r *foreignKeyRecursive) childLookupParent(ctx *sql.Context, row sql.Row) (sql.IndexLookup, error) {
	builder := sql.NewIndexBuilder(ctx, r.parentIdx)
	for i, j := range r.childMap {
		builder.Equals(ctx, r.parentExpr[i].Expression, row[j])
	}
	return builder.Build(ctx)
}

func (r *foreignKeyRecursive) parentLookupChild(ctx *sql.Context, row sql.Row) (sql.IndexLookup, error) {
	builder := sql.NewIndexBuilder(ctx, r.childIdx)
	for i, j := range r.parentMap {
		builder.Equals(ctx, r.childExpr[i].Expression, row[j])
	}
	return builder.Build(ctx)
}

func (r *foreignKeyRecursive) parentErr(row sql.Row) error {
	// todo(andy): incorrect string for key
	s := sql.FormatRow(row)
	return sql.ErrForeignKeyParentViolation.New(r.fk.Name, r.fk.TableName, r.fk.ReferencedTableName, s)
}

func (r *foreignKeyRecursive) childErr(row sql.Row) error {
	// todo(andy): incorrect string for key
	s := sql.FormatRow(row)
	return sql.ErrForeignKeyChildViolation.New(r.fk.Name, r.fk.TableName, r.fk.ReferencedTableName, s)
}
