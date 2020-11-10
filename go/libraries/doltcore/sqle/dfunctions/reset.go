// Copyright 2020 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"

	"github.com/dolthub/go-mysql-server/sql"
)

const ResetHardFuncName = "reset_hard"

type ResetHardFunc struct{}

// NewResetHardFunc creates a new ResetHardFunc expression.
func NewResetHardFunc() sql.Expression {
	return ResetHardFunc{}
}

// Eval implements the Expression interface.
func (rf ResetHardFunc) Eval(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	dbName := ctx.GetCurrentDatabase()
	dSess := sqle.DSessFromSess(ctx.Session)
	parent, _, err := dSess.GetParentCommit(ctx, dbName)
	if err != nil {
		return nil, err
	}

	h, err := parent.HashOf()
	if err != nil {
		return nil, err
	}

	return h.String(), nil
}

// Resolved implements the Expression interface.
func (rf ResetHardFunc) Resolved() bool {
	return true
}

// String implements the Stringer interface.
func (rf ResetHardFunc) String() string {
	return "RESET_HARD()"
}

// IsNullable implements the Expression interface.
func (rf ResetHardFunc) IsNullable() bool {
	return false
}

// Children implements the Expression interface.
func (rf ResetHardFunc) Children() []sql.Expression {
	return nil
}

// WithChildren implements the Expression interface.
func (rf ResetHardFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(rf, len(children), 0)
	}
	return rf, nil
}

// Type implements the Expression interface.
func (rf ResetHardFunc) Type() sql.Type {
	return sql.Text
}
