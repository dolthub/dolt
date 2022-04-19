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
// Deprecated: please use the version in the dprocedures package
type ConstraintsVerifyFunc struct {
	expression.NaryExpression
	isAll bool
}

var _ sql.Expression = (*ConstraintsVerifyFunc)(nil)

// NewConstraintsVerifyFunc creates a new ConstraintsVerifyFunc expression that verifies the diff.
// Deprecated: please use the version in the dprocedures package
func NewConstraintsVerifyFunc(args ...sql.Expression) (sql.Expression, error) {
	return &ConstraintsVerifyFunc{expression.NaryExpression{ChildExpressions: args}, false}, nil
}

// NewConstraintsVerifyAllFunc creates a new ConstraintsVerifyFunc expression that verifies all rows.
// Deprecated: please use the version in the dprocedures package
func NewConstraintsVerifyAllFunc(args ...sql.Expression) (sql.Expression, error) {
	return &ConstraintsVerifyFunc{expression.NaryExpression{ChildExpressions: args}, true}, nil
}

// Eval implements the Expression interface.
func (vc *ConstraintsVerifyFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	vals := make([]string, len(vc.ChildExpressions))
	for i, expr := range vc.ChildExpressions {
		evaluatedVal, err := expr.Eval(ctx, row)
		if err != nil {
			return 1, err
		}
		val, ok := evaluatedVal.(string)
		if !ok {
			return 1, sql.ErrUnexpectedType.New(i, reflect.TypeOf(evaluatedVal))
		}
		vals[i] = val
	}
	return DoDoltConstraintsVerify(ctx, vc.isAll, vals)
}

func DoDoltConstraintsVerify(ctx *sql.Context, isAll bool, vals []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)
	workingSet, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return 1, err
	}
	workingRoot := workingSet.WorkingRoot()
	var comparingRoot *doltdb.RootValue
	if isAll {
		comparingRoot, err = doltdb.EmptyRootValue(ctx, workingRoot.VRW())
		if err != nil {
			return 1, err
		}
	} else {
		headCommit, err := dSess.GetHeadCommit(ctx, dbName)
		if err != nil {
			return 1, err
		}
		comparingRoot, err = headCommit.GetRootValue(ctx)
		if err != nil {
			return 1, err
		}
	}

	tableSet := set.NewStrSet(nil)
	for _, val := range vals {
		_, tableName, ok, err := workingRoot.GetTableInsensitive(ctx, val)
		if err != nil {
			return 1, err
		}
		if !ok {
			return 1, sql.ErrTableNotFound.New(tableName)
		}
		tableSet.Add(tableName)
	}

	newRoot, tablesWithViolations, err := merge.AddConstraintViolations(ctx, workingRoot, comparingRoot, tableSet)
	if err != nil {
		return 1, err
	}
	if tablesWithViolations.Size() == 0 {
		return 1, nil
	} else {
		err = dSess.SetRoot(ctx, dbName, newRoot)
		if err != nil {
			return 1, err
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
