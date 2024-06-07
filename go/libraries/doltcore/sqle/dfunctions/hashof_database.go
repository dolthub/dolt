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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const HashOfDatabaseFuncName = "dolt_hashof_db"

type HashOfDatabase struct{}

// NewHashOfDatabase creates a new HashOfDatabase expression.
func NewHashOfDatabase() sql.Expression {
	return &HashOfDatabase{}
}

// Children implements the Expression interface.
func (t *HashOfDatabase) Children() []sql.Expression {
	return nil
}

// Eval implements the Expression interface.
func (t *HashOfDatabase) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	dbName := ctx.GetCurrentDatabase()
	ds := dsess.DSessFromSess(ctx.Session)
	roots, ok := ds.GetRoots(ctx, dbName)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	h, err := roots.Working.HashOf()
	if err != nil {
		return nil, fmt.Errorf("error getting hash of the database '%s': %w", dbName, err)
	}

	return h.String(), nil
}

// String implements the Stringer interface.
func (t *HashOfDatabase) String() string {
	return fmt.Sprintf("%s()", HashOfDatabaseFuncName)
}

// Description implements the FunctionExpression interface
func (t *HashOfDatabase) Description() string {
	return "returns a hash of the contents of the current database, typically used for detecting if a database has changed"
}

// IsNullable implements the Expression interface.
func (t *HashOfDatabase) IsNullable() bool {
	return false
}

// Resolved implements the Expression interface.
func (*HashOfDatabase) Resolved() bool {
	return true
}

// WithChildren implements the Expression interface.
func (t *HashOfDatabase) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 1)
	}
	return NewHashOfDatabase(), nil
}

// Type implements the Expression interface.
func (t *HashOfDatabase) Type() sql.Type {
	return types.Text
}
