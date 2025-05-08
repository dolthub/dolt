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
	"errors"
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
	expression.BinaryExpressionStub
}

// NewMergeBase returns a MergeBase sql function.
func NewMergeBase(left, right sql.Expression) sql.Expression {
	return &MergeBase{expression.BinaryExpressionStub{LeftChild: left, RightChild: right}}
}

// Eval implements the sql.Expression interface.
func (d MergeBase) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	leftSpec, err := d.Left().Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	rightSpec, err := d.Right().Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if leftSpec == nil || rightSpec == nil {
		return nil, nil
	}

	leftStr, ok := leftSpec.(string)
	if !ok {
		return nil, errors.New("left value is not a string")
	}

	rightStr, ok := rightSpec.(string)
	if !ok {
		return nil, errors.New("right value is not a string")
	}

	left, right, err := resolveRefSpecs(ctx, leftStr, rightStr)
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

	headRef, err := dbData.Rsr.CWBHeadRef()
	if err != nil {
		return nil, nil, err
	}

	optCmt, err := doltDB.Resolve(ctx, lcs, headRef)
	if err != nil {
		return nil, nil, err
	}
	left, ok = optCmt.ToCommit()
	if !ok {
		return nil, nil, doltdb.ErrGhostCommitEncountered
	}

	optCmt, err = doltDB.Resolve(ctx, rcs, headRef)
	if err != nil {
		return nil, nil, err
	}
	right, ok = optCmt.ToCommit()
	if !ok {
		return nil, nil, doltdb.ErrGhostCommitEncountered
	}

	return
}

// String implements the sql.Expression interface.
func (d MergeBase) String() string {
	return fmt.Sprintf("DOLT_MERGE_BASE(%s,%s)", d.Left().String(), d.Right().String())
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
