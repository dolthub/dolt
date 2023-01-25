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
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const ActiveBranchFuncName = "active_branch"

type ActiveBranchFunc struct {
}

// NewActiveBranchFunc creates a new ActiveBranchFunc expression.
func NewActiveBranchFunc() sql.Expression {
	return &ActiveBranchFunc{}
}

// Eval implements the Expression interface.
func (ab *ActiveBranchFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)

	ddb, ok := dSess.GetDoltDB(ctx, dbName)

	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	currentBranchRef, err := dSess.CWBHeadRef(ctx, dbName)
	if err != nil {
		return nil, err
	}

	branches, err := ddb.GetBranches(ctx)
	if err != nil {
		return nil, err
	}

	for _, br := range branches {
		if ref.Equals(br, currentBranchRef) {
			return br.GetPath(), nil
		}
	}

	return nil, nil
}

// String implements the Stringer interface.
func (ab *ActiveBranchFunc) String() string {
	return fmt.Sprint("ACTIVE_BRANCH()")
}

// IsNullable implements the Expression interface.
func (ab *ActiveBranchFunc) IsNullable() bool {
	return false
}

// Resolved implements the Expression interface.
func (*ActiveBranchFunc) Resolved() bool {
	return true
}

func (ab *ActiveBranchFunc) Type() sql.Type {
	return types.Text
}

// Children implements the Expression interface.
func (*ActiveBranchFunc) Children() []sql.Expression {
	return nil
}

// WithChildren implements the Expression interface.
func (ab *ActiveBranchFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(ab, len(children), 0)
	}
	return NewActiveBranchFunc(), nil
}
