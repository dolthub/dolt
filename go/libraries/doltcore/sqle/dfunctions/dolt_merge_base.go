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
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const DoltMergeBaseFuncName = "dolt_merge_base"

type MergeBase struct {
	expression.BinaryExpression
}

// NewMergeBase returns a MergeBase sql function.
func NewMergeBase(left, right sql.Expression) sql.Expression {
	return &MergeBase{expression.BinaryExpression{Left: left, Right: right}}
}

// Eval implements the sql.Expression interface.
func (d MergeBase) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if _, ok := d.Left.Type().(types.StringType); !ok {
		return nil, sql.ErrInvalidType.New(d.Left.Type())
	}
	if _, ok := d.Right.Type().(types.StringType); !ok {
		return nil, sql.ErrInvalidType.New(d.Right.Type())
	}

	leftSpec, err := d.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	rightSpec, err := d.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if leftSpec == nil || rightSpec == nil {
		return nil, nil
	}

	left, right, err := resolveRefSpecs(ctx, leftSpec.(string), rightSpec.(string))
	if err != nil {
		return nil, err
	}

	mergeBase, err := merge.MergeBase(ctx, left, right)
	if err != nil {
		return nil, err
	}

	return mergeBase.String(), nil
}

func resolveRefSpecs(ctx *sql.Context, leftSpec, rightSpec string) (left, right *doltdb.Commit, err error) {
	lcs, err := doltdb.NewCommitSpec(leftSpec)
	if err != nil {
		return nil, nil, err
	}
	rcs, err := doltdb.NewCommitSpec(rightSpec)
	if err != nil {
		return nil, nil, err
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbName := ctx.GetCurrentDatabase()

	dbData, ok := sess.GetDbData(ctx, dbName)
	if !ok {
		return nil, nil, sql.ErrDatabaseNotFound.New(dbName)
	}
	doltDB, ok := sess.GetDoltDB(ctx, dbName)
	if !ok {
		return nil, nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	left, err = doltDB.Resolve(ctx, lcs, dbData.Rsr.CWBHeadRef())
	if err != nil {
		return nil, nil, err
	}
	right, err = doltDB.Resolve(ctx, rcs, dbData.Rsr.CWBHeadRef())
	if err != nil {
		return nil, nil, err
	}

	return
}

// String implements the sql.Expression interface.
func (d MergeBase) String() string {
	return fmt.Sprintf("DOLT_MERGE_BASE(%s,%s)", d.Left.String(), d.Right.String())
}

// Type implements the sql.Expression interface.
func (d MergeBase) Type() sql.Type {
	return types.Text
}

// WithChildren implements the sql.Expression interface.
func (d MergeBase) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 2)
	}
	return NewMergeBase(children[0], children[1]), nil
}
