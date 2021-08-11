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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const (
	RevertFuncName = "dolt_revert"
)

// RevertFunc represents the dolt function "dolt revert".
type RevertFunc struct {
	expression.NaryExpression
}

var _ sql.Expression = (*RevertFunc)(nil)

// NewRevertFunc creates a new RevertFunc expression that reverts commits.
func NewRevertFunc(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	return &RevertFunc{expression.NaryExpression{ChildExpressions: args}}, nil
}

// Eval implements the Expression interface.
func (r *RevertFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)
	ddb, ok := dSess.GetDoltDB(ctx, dbName)
	if !ok {
		return nil, fmt.Errorf("dolt database could not be found")
	}
	workingSet, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return nil, err
	}
	workingRoot := workingSet.WorkingRoot()
	headCommit, err := dSess.GetHeadCommit(ctx, dbName)
	if err != nil {
		return nil, err
	}
	headRoot, err := headCommit.GetRootValue()
	if err != nil {
		return nil, err
	}
	headHash, err := headRoot.HashOf()
	if err != nil {
		return nil, err
	}
	workingHash, err := workingRoot.HashOf()
	if err != nil {
		return nil, err
	}
	if !headHash.Equal(workingHash) {
		return nil, fmt.Errorf("you must commit any changes before using revert")
	}

	headRef, err := dSess.CWBHeadRef(ctx, dbName)
	if err != nil {
		return nil, err
	}
	commits := make([]*doltdb.Commit, len(r.ChildExpressions))
	for i, expr := range r.ChildExpressions {
		res, err := expr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		revisionStr, ok := res.(string)
		if !ok {
			return nil, sql.ErrUnexpectedType.New(i, fmt.Sprintf("%T", res))
		}
		commitSpec, err := doltdb.NewCommitSpec(revisionStr)
		if err != nil {
			return nil, err
		}
		commit, err := ddb.Resolve(ctx, commitSpec, headRef)
		if err != nil {
			return nil, err
		}
		commits[i] = commit
	}

	workingRoot, revertMessage, err := merge.Revert(ctx, ddb, workingRoot, commits)
	if err != nil {
		return nil, err
	}
	workingHash, err = workingRoot.HashOf()
	if err != nil {
		return nil, err
	}
	if !headHash.Equal(workingHash) {
		err = dSess.SetRoot(ctx, dbName, workingRoot)
		if err != nil {
			return nil, err
		}
		stringType := typeinfo.StringDefaultType.ToSqlType()
		commitFunc, err := NewDoltCommitFunc(ctx,
			expression.NewLiteral("-a", stringType), expression.NewLiteral("-m", stringType), expression.NewLiteral(revertMessage, stringType))
		if err != nil {
			return nil, err
		}
		_, err = commitFunc.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
	}
	return 0, nil
}

// String implements the Stringer interface.
func (r *RevertFunc) String() string {
	return fmt.Sprint("DOLT_REVERT()")
}

// IsNullable implements the Expression interface.
func (r *RevertFunc) IsNullable() bool {
	return false
}

// Resolved implements the Expression interface.
func (r *RevertFunc) Resolved() bool {
	for _, expr := range r.ChildExpressions {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

func (r *RevertFunc) Type() sql.Type {
	return sql.Int8
}

// Children implements the Expression interface.
func (r *RevertFunc) Children() []sql.Expression {
	exprs := make([]sql.Expression, len(r.ChildExpressions))
	for i := range exprs {
		exprs[i] = r.ChildExpressions[i]
	}
	return exprs
}

// WithChildren implements the Expression interface.
func (r *RevertFunc) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewRevertFunc(ctx, children...)
}
