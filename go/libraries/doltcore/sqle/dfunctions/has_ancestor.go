// Copyright 2023 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const HasAncestorFuncName = "has_ancestor"

type HasAncestor struct {
	reference sql.Expression
	ancestor  sql.Expression
}

var _ sql.FunctionExpression = (*HasAncestor)(nil)

// NewHasAncestor creates a new HasAncestor expression.
func NewHasAncestor(head, anc sql.Expression) sql.Expression {
	return &HasAncestor{reference: head, ancestor: anc}
}

// Eval implements the Expression interface.
func (a *HasAncestor) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if !types.IsText(a.reference.Type()) {
		return nil, sql.ErrInvalidArgumentDetails.New(a, a.reference)
	}
	if !types.IsText(a.ancestor.Type()) {
		return nil, sql.ErrInvalidArgumentDetails.New(a, a.ancestor)
	}

	// TODO analysis should embed a database the same way as table functions
	sess := dsess.DSessFromSess(ctx.Session)
	db := sess.GetCurrentDatabase()
	dbd, ok := sess.GetDbData(ctx, db)
	if !ok {
		return nil, fmt.Errorf("error during has_ancestor check: database not found '%s'", db)
	}
	ddb := dbd.Ddb

	// this errors for non-branch refs
	// ddb.Resolve will error if combination of head and commit are invalid
	headRef, _ := sess.CWBHeadRef(ctx, db)
	var headCommit *doltdb.Commit
	{
		headIf, err := a.reference.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		headStr, _, err := types.Text.Convert(ctx, headIf)
		if err != nil {
			return nil, err
		}
		headCommit, err = resolveRefSpec(ctx, headRef, ddb, headStr.(string))
		if err != nil {
			return nil, err
		}
	}

	var ancCommit *doltdb.Commit
	{
		ancIf, err := a.ancestor.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		ancStr, _, err := types.Text.Convert(ctx, ancIf)
		if err != nil {
			return nil, err
		}
		ancCommit, err = resolveRefSpec(ctx, headRef, ddb, ancStr.(string))
		if err != nil {
			return nil, err
		}
	}

	headHash, err := headCommit.HashOf()
	if err != nil {
		return nil, fmt.Errorf("error during has_ancestor check: %s", err.Error())
	}

	ancHash, err := ancCommit.HashOf()
	if err != nil {
		return nil, fmt.Errorf("error during has_ancestor check: %s", err.Error())
	}
	if headHash == ancHash {
		return true, nil
	}

	cc, err := headCommit.GetCommitClosure(ctx)
	if err != nil {
		return nil, fmt.Errorf("error during has_ancestor check: %s", err.Error())
	}
	ancHeight, err := ancCommit.Height()
	if err != nil {
		return nil, fmt.Errorf("error during has_ancestor check: %s", err.Error())
	}

	isAncestor, err := cc.ContainsKey(ctx, ancHash, ancHeight)
	if err != nil {
		return nil, fmt.Errorf("error during has_ancestor check: %s", err.Error())
	}

	return isAncestor, nil
}

func (a *HasAncestor) Resolved() bool {
	return a.reference.Resolved() && a.ancestor.Resolved()
}

func (a *HasAncestor) Children() []sql.Expression {
	return []sql.Expression{a.reference, a.ancestor}
}

// String implements the Stringer interface.
func (a *HasAncestor) String() string {
	return fmt.Sprintf("HAS_ANCESTOR(%s, %s)", a.reference, a.ancestor)
}

// FunctionName implements the FunctionExpression interface
func (a *HasAncestor) FunctionName() string {
	return HasAncestorFuncName
}

// Description implements the FunctionExpression interface
func (a *HasAncestor) Description() string {
	return "returns whether a reference commit's ancestor graph contains a target commit"
}

// IsNullable implements the Expression interface.
func (a *HasAncestor) IsNullable() bool {
	return a.reference.IsNullable() || a.ancestor.IsNullable()
}

// WithChildren implements the Expression interface.
func (a *HasAncestor) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 2)
	}
	return NewHasAncestor(children[0], children[1]), nil
}

// Type implements the Expression interface.
func (a *HasAncestor) Type() sql.Type {
	return types.Boolean
}
