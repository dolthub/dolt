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

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas"
)

const CommitFuncName = "commit"

type CommitFunc struct {
	children []sql.Expression
}

// NewCommitFunc creates a new CommitFunc expression.
func NewCommitFunc(args ...sql.Expression) (sql.Expression, error) {
	return &CommitFunc{children: args}, nil
}

// Eval implements the Expression interface.
func (cf *CommitFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)

	//  Get the params associated with COMMIT.
	ap := cli.CreateCommitArgParser()
	args, err := getDoltArgs(ctx, row, cf.Children())
	if err != nil {
		return nil, err
	}

	apr, err := ap.Parse(args)
	if err != nil {
		return nil, err
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

	// Get the commit message.
	commitMessage, msgOk := apr.GetValue(cli.CommitMessageArg)
	if !msgOk {
		return nil, fmt.Errorf("Must provide commit message.")
	}

	parent, err := dSess.GetHeadCommit(ctx, dbName)
	if err != nil {
		return nil, err
	}

	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return nil, fmt.Errorf("unknown database '%s'", dbName)
	}
	root := roots.Working

	// Update the superschema to with any new information from the table map.
	tblNames, err := root.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}

	root, err = root.UpdateSuperSchemasFromOther(ctx, tblNames, root)
	if err != nil {
		return nil, err
	}

	ddb, ok := dSess.GetDoltDB(ctx, dbName)

	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	h, err := ddb.WriteRootValue(ctx, root)
	if err != nil {
		return nil, err
	}

	meta, err := datas.NewCommitMeta(name, email, commitMessage)
	if err != nil {
		return nil, err
	}

	cm, err := ddb.CommitDanglingWithParentCommits(ctx, h, []*doltdb.Commit{parent}, meta)
	if err != nil {
		return nil, err
	}

	h, err = cm.HashOf()
	if err != nil {
		return nil, err
	}

	return h.String(), nil
}

// String implements the Stringer interface.
func (cf *CommitFunc) String() string {
	childrenStrings := make([]string, len(cf.children))

	for i, child := range cf.children {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("COMMIT(%s)", strings.Join(childrenStrings, ","))
}

// IsNullable implements the Expression interface.
func (cf *CommitFunc) IsNullable() bool {
	return false
}

func (cf *CommitFunc) Resolved() bool {
	for _, child := range cf.Children() {
		if !child.Resolved() {
			return false
		}
	}
	return true
}

func (cf *CommitFunc) Children() []sql.Expression {
	return cf.children
}

// WithChildren implements the Expression interface.
func (cf *CommitFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewCommitFunc(children...)
}

func (cf *CommitFunc) Type() sql.Type {
	return sql.Text
}
