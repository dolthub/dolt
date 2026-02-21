// Copyright 2026 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const BaseDatabaseFuncName = "base_database"

type BaseDatabaseFunc struct{}

// NewBaseDatabaseFunc creates a new BaseDatabaseFunc expression.
func NewBaseDatabaseFunc() sql.Expression {
	return &BaseDatabaseFunc{}
}

// Eval implements the Expression interface.
func (*BaseDatabaseFunc) Eval(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	baseDatabase, _, err := resolveSessionDatabaseIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if baseDatabase == "" {
		return nil, nil
	}
	return baseDatabase, nil
}

// String implements the Stringer interface.
func (*BaseDatabaseFunc) String() string {
	return "BASE_DATABASE()"
}

// IsNullable implements the Expression interface.
func (*BaseDatabaseFunc) IsNullable() bool {
	return true
}

// Resolved implements the Expression interface.
func (*BaseDatabaseFunc) Resolved() bool {
	return true
}

// Type implements the Expression interface.
func (*BaseDatabaseFunc) Type() sql.Type {
	return types.Text
}

// Children implements the Expression interface.
func (*BaseDatabaseFunc) Children() []sql.Expression {
	return nil
}

// WithChildren implements the Expression interface.
func (f *BaseDatabaseFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 0)
	}
	return NewBaseDatabaseFunc(), nil
}

// resolveSessionDatabaseIdentity resolves the base database and active revision for the current session database.
func resolveSessionDatabaseIdentity(ctx *sql.Context) (baseDatabase string, activeRevision string, err error) {
	dbName := ctx.GetCurrentDatabase()
	if dbName == "" {
		return "", "", nil
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	sessionDb, ok, err := dSess.Provider().SessionDatabase(ctx, dbName)
	if err != nil {
		return "", "", err
	}
	if !ok {
		// Non-Dolt databases can still be current.
		return dbName, "", nil
	}

	baseDatabase = sessionDb.AliasedName()
	activeRevision = sessionDb.Revision()
	return baseDatabase, activeRevision, nil
}
