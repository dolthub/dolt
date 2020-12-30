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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	resetFuncName = "reset"

	resetHardParameter = "hard"
)

type ResetFunc struct {
	expression.UnaryExpression
}

// NewDoltResetFunc creates a new ResetFunc expression.
func NewResetFunc(e sql.Expression) sql.Expression {
	return ResetFunc{expression.UnaryExpression{Child: e}}
}

// Eval implements the Expression interface.
func (rf ResetFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := rf.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	arg, ok := val.(string)
	if !ok {
		return nil, sql.ErrInvalidChildType.New(rf.Child.String(), rf.Child.Type(), sql.Text.Type())
	}

	dbName := ctx.GetCurrentDatabase()
	dSess := sqle.DSessFromSess(ctx.Session)

	var h hash.Hash
	if strings.ToLower(arg) != resetHardParameter {
		return nil, fmt.Errorf("invalid arugument to %s(): %s", resetFuncName, arg)
	}

	parent, _, err := dSess.GetParentCommit(ctx, dbName)
	if err != nil {
		return nil, err
	}

	h, err = parent.HashOf()
	if err != nil {
		return nil, err
	}

	return h.String(), nil
}

// Resolved implements the Expression interface.
func (rf ResetFunc) Resolved() bool {
	return rf.Child.Resolved()
}

// String implements the Stringer interface.
func (rf ResetFunc) String() string {
	return fmt.Sprintf("RESET_HARD(%s)", rf.Child.String())
}

// IsNullable implements the Expression interface.
func (rf ResetFunc) IsNullable() bool {
	return false
}

// Children implements the Expression interface.
func (rf ResetFunc) Children() []sql.Expression {
	return []sql.Expression{rf.Child}
}

// WithChildren implements the Expression interface.
func (rf ResetFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(rf, len(children), 1)
	}
	return NewResetFunc(children[0]), nil
}

// Type implements the Expression interface.
func (rf ResetFunc) Type() sql.Type {
	return sql.Text
}
