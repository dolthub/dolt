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
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const DoltCommitFuncName = "dolt_commit"

var hashType = sql.MustCreateString(query.Type_TEXT, 32, sql.Collation_ascii_bin)

// DoltCommitFunc runs a `dolt commit` in the SQL context, committing staged changes to head.
type DoltCommitFunc struct {
	children []sql.Expression
}

// NewDoltCommitFunc creates a new DoltCommitFunc expression whose children represents the args passed in DOLT_COMMIT.
func NewDoltCommitFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltCommitFunc{children: args}, nil
}

func (d DoltCommitFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return doDoltCommit(ctx, row, d.Children())
}

func doDoltCommit(ctx *sql.Context, row sql.Row, exprs []sql.Expression) (interface{}, error) {
	// Get the information for the sql context.
	dbName := ctx.GetCurrentDatabase()
	ap := cli.CreateCommitArgParser()

	// Get the args for DOLT_COMMIT.
	args, err := getDoltArgs(ctx, row, exprs)
	if err != nil {
		return nil, err
	}

	apr, err := ap.Parse(args)
	if err != nil {
		return nil, err
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return nil, fmt.Errorf("Could not load database %s", dbName)
	}

	if apr.Contains(cli.AllFlag) {
		roots, err = actions.StageAllTablesNoDocs(ctx, roots)
		if err != nil {
			return nil, fmt.Errorf(err.Error())
		}
	}

	var name, email string
	if authorStr, ok := apr.GetValue(cli.AuthorParam); ok {
		name, email, err = cli.ParseAuthor(authorStr)
		if err != nil {
			return nil, err
		}
	} else {
		name = dSess.Username()
		email = dSess.Email()
	}

	msg, msgOk := apr.GetValue(cli.CommitMessageArg)
	if !msgOk {
		return nil, fmt.Errorf("Must provide commit message.")
	}

	t := ctx.QueryTime()
	if commitTimeStr, ok := apr.GetValue(cli.DateParam); ok {
		var err error
		t, err = cli.ParseDate(commitTimeStr)

		if err != nil {
			return nil, fmt.Errorf(err.Error())
		}
	}

	pendingCommit, err := dSess.NewPendingCommit(ctx, dbName, roots, actions.CommitStagedProps{
		Message:    msg,
		Date:       t,
		AllowEmpty: apr.Contains(cli.AllowEmptyFlag),
		Force:      apr.Contains(cli.ForceFlag),
		Name:       name,
		Email:      email,
	})
	if err != nil {
		return nil, err
	}

	// Nothing to commit, and we didn't pass --allowEmpty
	if pendingCommit == nil {
		return nil, errors.New("nothing to commit")
	}

	newCommit, err := dSess.DoltCommit(ctx, dbName, dSess.GetTransaction(), pendingCommit)
	if err != nil {
		return nil, err
	}

	h, err := newCommit.HashOf()
	if err != nil {
		return nil, err
	}

	return h.String(), nil
}

func getDoltArgs(ctx *sql.Context, row sql.Row, children []sql.Expression) ([]string, error) {
	args := make([]string, len(children))
	for i := range children {
		childVal, err := children[i].Eval(ctx, row)

		if err != nil {
			return nil, err
		}

		text, err := sql.Text.Convert(childVal)

		if err != nil {
			return nil, err
		}

		args[i] = text.(string)
	}

	return args, nil
}

func (d DoltCommitFunc) String() string {
	childrenStrings := make([]string, len(d.children))

	for _, child := range d.children {
		childrenStrings = append(childrenStrings, child.String())
	}
	return fmt.Sprintf("commit_hash")
}

func (d DoltCommitFunc) Type() sql.Type {
	return sql.Text
}

func (d DoltCommitFunc) IsNullable() bool {
	return false
}

func (d DoltCommitFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltCommitFunc(children...)
}

func (d DoltCommitFunc) Resolved() bool {
	for _, child := range d.Children() {
		if !child.Resolved() {
			return false
		}
	}
	return true
}

func (d DoltCommitFunc) Children() []sql.Expression {
	return d.children
}
