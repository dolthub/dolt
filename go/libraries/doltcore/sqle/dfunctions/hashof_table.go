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

package dfunctions

import (
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const HashOfTableFuncName = "dolt_hashof_table"

type HashOfTable struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*HashOfTable)(nil)

// NewHashOfTable creates a new HashOfTable expression.
func NewHashOfTable(e sql.Expression) sql.Expression {
	return &HashOfTable{expression.UnaryExpressionStub{Child: e}}
}

// Eval implements the Expression interface.
func (t *HashOfTable) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := t.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	tableName, ok := val.(string)

	if !ok {
		return nil, errors.New("table name is not a string")
	}

	dbName := ctx.GetCurrentDatabase()
	ds := dsess.DSessFromSess(ctx.Session)
	roots, ok := ds.GetRoots(ctx, dbName)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	tbl, ok, err := roots.Working.GetTable(ctx, doltdb.TableName{Name: tableName})

	if err != nil {
		return nil, fmt.Errorf("error getting table %s: %w", tableName, err)
	} else if !ok {
		return nil, sql.ErrTableNotFound.New(tableName)
	}

	h, err := tbl.HashOf()
	if err != nil {
		return nil, fmt.Errorf("error getting hash of table %s: %w", tableName, err)
	}

	return h.String(), nil
}

// String implements the Stringer interface.
func (t *HashOfTable) String() string {
	return fmt.Sprintf("%s(%s)", HashOfTableFuncName, t.Child.String())
}

// FunctionName implements the FunctionExpression interface
func (t *HashOfTable) FunctionName() string {
	return HashOfTableFuncName
}

// Description implements the FunctionExpression interface
func (t *HashOfTable) Description() string {
	return "returns a hash of the contents of a table, typically used for detecting if a table's data has changed"
}

// IsNullable implements the Expression interface.
func (t *HashOfTable) IsNullable() bool {
	return t.Child.IsNullable()
}

// WithChildren implements the Expression interface.
func (t *HashOfTable) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 1)
	}
	return NewHashOfTable(children[0]), nil
}

// Type implements the Expression interface.
func (t *HashOfTable) Type() sql.Type {
	return types.Text
}
