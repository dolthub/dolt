// Copyright 2019 Liquidata, Inc.
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

package sql

import (
	"context"
	"errors"
	"fmt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/store/types"
	"io"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/resultset"
)

type DeleteResult struct {
	Root           *doltdb.RootValue
	NumRowsDeleted int
}

func ExecuteDelete(ctx context.Context, db *doltdb.DoltDB, root *doltdb.RootValue, s *sqlparser.Delete, query string) (*DeleteResult, error) {
	tableExprs := s.TableExprs
	if len(tableExprs) != 1 {
		return errDelete("Exactly one table to delete from must be specified")
	}

	var tableName string
	tableExpr := tableExprs[0]
	switch t := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch e := t.Expr.(type) {
		case sqlparser.TableName:
			tableName = e.Name.String()
		case *sqlparser.Subquery:
			return errDelete("Subqueries are not supported: %v.", query)
		default:
			return errDelete("Unrecognized expression: %v", nodeToString(e))
		}
	case *sqlparser.ParenTableExpr:
		return errDelete("Only simple table expression are supported")
	case *sqlparser.JoinTableExpr:
		return errDelete("Joins are not supported")
	default:
		return errDelete("Unsupported update statement %v", query)
	}

	if has, err := root.HasTable(ctx, tableName); err != nil {
		return nil, err
	} else if !has {
		return errDelete("Unknown table '%s'", tableName)
	}
	table, _, err := root.GetTable(ctx, tableName)

	if err != nil {
		return nil, err
	}

	tableSch, err := table.GetSchema(ctx)

	if err != nil {
		return nil, err
	}

	rss := resultset.Identity(tableName, tableSch)

	// TODO: support aliases
	filter, err := createFilterForWhere(s.Where, map[string]schema.Schema{tableName: tableSch}, NewAliases())
	if err != nil {
		return errDelete(err.Error())
	}
	if err = filter.Init(rss); err != nil {
		return errDelete(err.Error())
	}

	// Perform the delete
	var result DeleteResult
	rowData, err := table.GetRowData(ctx)

	if err != nil {
		return nil, err
	}

	me := rowData.Edit()
	rowReader, err := noms.NewNomsMapReader(ctx, rowData, tableSch)

	if err != nil {
		return nil, err
	}

	for {
		r, err := rowReader.ReadRow(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if !filter.filter(r) {
			continue
		}

		result.NumRowsDeleted++
		me.Remove(r.NomsMapKey(tableSch))
	}

	m, err := me.Map(ctx)

	if err != nil {
		return nil, err
	}

	table, err = table.UpdateRows(ctx, m)

	if err != nil {
		return nil, err
	}

	result.Root, err = root.PutTable(ctx, tableName, table)

	if err != nil {
		return nil, err
	}

	return &result, nil
}

func RowAsDeleteStmt(r row.Row, tableName string, tableSch schema.Schema) (string, error) {
	var b strings.Builder
	b.WriteString("DELETE FROM ")
	b.WriteString(QuoteIdentifier(tableName))

	b.WriteString(" WHERE (")
	seenOne := false
	_, err := r.IterSchema(tableSch, func(tag uint64, val types.Value) (stop bool, err error) {
		col := tableSch.GetAllCols().TagToCol[tag]
		if col.IsPartOfPK {
			if seenOne {
				b.WriteString(" AND ")
			}
			sqlString, err := ValueAsSqlString(val)
			if err != nil {
				return true, err
			}
			b.WriteString(QuoteIdentifier(col.Name))
			b.WriteRune('=')
			b.WriteString(sqlString)
			seenOne = true
		}
		return false, nil
	})

	if err != nil {
		return "", err
	}

	b.WriteString(");")
	return b.String(), nil
}

func errDelete(errorFmt string, args ...interface{}) (*DeleteResult, error) {
	return nil, errors.New(fmt.Sprintf(errorFmt, args...))
}
