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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

const DoltCommitFuncName = "dolt_commit"

type DoltCommitFunc struct {
	children []sql.Expression
}

// NewDoltCommitFunc creates a new DoltCommitFunc expression whose children represents the args passed in DOLT_COMMIT.
func NewDoltCommitFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltCommitFunc{children: args}, nil
}

// Runs DOLT_COMMIT in the sql engine which models the behavior of `dolt commit`. Commits staged staged changes to head.
func (d DoltCommitFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Get the information for the sql context.
	dbName := ctx.GetCurrentDatabase()
	dSess := sqle.DSessFromSess(ctx.Session)
	dbData, ok := dSess.GetDbData(dbName)

	if !ok {
		return nil, fmt.Errorf("Could not load %s", dbName)
	}

	ddb := dbData.Ddb
	rsr := dbData.Rsr

	ap := cli.CreateCommitArgParser()

	// Get the args for DOLT_COMMIT.
	args, err := getDoltArgs(ctx, row, d.Children())

	if err != nil {
		return nil, err
	}

	apr := cli.ParseArgs(ap, args, nil)

	allFlag := apr.Contains(cli.AllFlag)
	allowEmpty := apr.Contains(cli.AllowEmptyFlag)

	// Check if there are no changes in the working set but the -a flag is true
	if allFlag && !hasWorkingSetChanges(rsr) && !allowEmpty {
		return nil, fmt.Errorf("Cannot commit an empty commit. See the --allow-empty if you want to.")
	}

	// Check if there are no changes in the staged set but the -a flag is false
	hasStagedChanges, err := hasStagedSetChanges(ctx, ddb, rsr)
	if err != nil {
		return nil, err
	}

	if !allFlag && !hasStagedChanges && !allowEmpty {
		return nil, fmt.Errorf("Cannot commit an empty commit. See the --allow-empty if you want to.")
	}

	if allFlag {
		err = actions.StageAllTables(ctx, dbData)
	}

	if err != nil {
		return nil, fmt.Errorf(err.Error())
	}

	// Parse the author flag. Return an error if not.
	var name, email string
	if authorStr, ok := apr.GetValue(cli.AuthorParam); ok {
		name, email, err = cli.ParseAuthor(authorStr)
		if err != nil {
			return nil, err
		}
	} else {
		name = dSess.Username
		email = dSess.Email
	}

	// Get the commit message.
	msg, msgOk := apr.GetValue(cli.CommitMessageArg)
	if !msgOk {
		return nil, fmt.Errorf("Must provide commit message.")
	}

	// Specify the time if the date parameter is not.
	t := ctx.QueryTime()
	if commitTimeStr, ok := apr.GetValue(cli.DateParam); ok {
		var err error
		t, err = cli.ParseDate(commitTimeStr)

		if err != nil {
			return nil, fmt.Errorf(err.Error())
		}
	}

	h, err := actions.CommitStaged(ctx, dbData, actions.CommitStagedProps{
		Message:          msg,
		Date:             t,
		AllowEmpty:       apr.Contains(cli.AllowEmptyFlag),
		CheckForeignKeys: !apr.Contains(cli.ForceFlag),
		Name:             name,
		Email:            email,
	})

	return h, err
}

func hasWorkingSetChanges(rsr env.RepoStateReader) bool {
	return rsr.WorkingHash() != rsr.StagedHash()
}

// TODO: We should not be dealing with root objects here but commit specs.
func hasStagedSetChanges(ctx *sql.Context, ddb *doltdb.DoltDB, rsr env.RepoStateReader) (bool, error) {
	root, err := env.HeadRoot(ctx, ddb, rsr)

	if err != nil {
		return false, err
	}

	headHash, err := root.HashOf()

	if err != nil {
		return false, err
	}

	return rsr.StagedHash() != headHash, nil
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
