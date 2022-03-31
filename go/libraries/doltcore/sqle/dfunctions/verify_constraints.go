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

package dfunctions

import (
	"fmt"
	"reflect"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

const (
	ConstraintsVerifyFuncName    = "constraints_verify"
	ConstraintsVerifyAllFuncName = "constraints_verify_all"
)

// ConstraintsVerifyFunc represents the sql functions "verify_constraints" and "verify_constraints_all".
type ConstraintsVerifyFunc struct {
	expression.NaryExpression
	isAll bool
}

var _ sql.Expression = (*ConstraintsVerifyFunc)(nil)

// NewConstraintsVerifyFunc creates a new ConstraintsVerifyFunc expression that verifies the diff.
func NewConstraintsVerifyFunc(args ...sql.Expression) (sql.Expression, error) {
	return &ConstraintsVerifyFunc{expression.NaryExpression{ChildExpressions: args}, false}, nil
}

// NewConstraintsVerifyAllFunc creates a new ConstraintsVerifyFunc expression that verifies all rows.
func NewConstraintsVerifyAllFunc(args ...sql.Expression) (sql.Expression, error) {
	return &ConstraintsVerifyFunc{expression.NaryExpression{ChildExpressions: args}, true}, nil
}

// Eval implements the Expression interface.
func (vc *ConstraintsVerifyFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)
	workingSet, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return nil, err
	}
	workingRoot := workingSet.WorkingRoot()
	var comparingRoot *doltdb.RootValue
	if vc.isAll {
		comparingRoot, err = doltdb.EmptyRootValue(ctx, workingRoot.VRW())
		if err != nil {
			return nil, err
		}
	} else {
		headCommit, err := dSess.GetHeadCommit(ctx, dbName)
		if err != nil {
			return nil, err
		}
		comparingRoot, err = headCommit.GetRootValue(ctx)
		if err != nil {
			return nil, err
		}
	}

	tableSet := set.NewStrSet(nil)
	for i, expr := range vc.ChildExpressions {
		evaluatedVal, err := expr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		val, ok := evaluatedVal.(string)
		if !ok {
			return nil, sql.ErrUnexpectedType.New(i, reflect.TypeOf(evaluatedVal))
		}
		_, tableName, ok, err := workingRoot.GetTableInsensitive(ctx, val)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, sql.ErrTableNotFound.New(tableName)
		}
		tableSet.Add(tableName)
	}

	newRoot, tablesWithViolations, err := merge.AddConstraintViolations(ctx, workingRoot, comparingRoot, tableSet)
	if err != nil {
		return nil, err
	}
	if tablesWithViolations.Size() == 0 {
		return 1, nil
	} else {
		err = dSess.SetRoot(ctx, dbName, newRoot)
		if err != nil {
			return nil, err
		}
		return 0, nil
	}
}

// String implements the Stringer interface.
func (vc *ConstraintsVerifyFunc) String() string {
	if vc.isAll {
		return fmt.Sprint("CONSTRAINTS_VERIFY_ALL()")
	} else {
		return fmt.Sprint("CONSTRAINTS_VERIFY()")
	}
}

// IsNullable implements the Expression interface.
func (vc *ConstraintsVerifyFunc) IsNullable() bool {
	return false
}

// Resolved implements the Expression interface.
func (vc *ConstraintsVerifyFunc) Resolved() bool {
	for _, expr := range vc.ChildExpressions {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

func (vc *ConstraintsVerifyFunc) Type() sql.Type {
	return sql.Int8
}

// Children implements the Expression interface.
func (vc *ConstraintsVerifyFunc) Children() []sql.Expression {
	exprs := make([]sql.Expression, len(vc.ChildExpressions))
	for i := range exprs {
		exprs[i] = vc.ChildExpressions[i]
	}
	return exprs
}

// WithChildren implements the Expression interface.
func (vc *ConstraintsVerifyFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if vc.isAll {
		return NewConstraintsVerifyAllFunc(children...)
	} else {
		return NewConstraintsVerifyFunc(children...)
	}
}
