// Copyright 2022 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/expression"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const DoltTagFuncName = "dolt_tag"

// DoltTagFunc runs a `dolt commit` in the SQL context, committing staged changes to head.
// Deprecated: please use the version in the dprocedures package
type DoltTagFunc struct {
	expression.NaryExpression
}

// NewDoltTagFunc creates a new DoltTagFunc expression whose children represent the args passed in DOLT_COMMIT.
// Deprecated: please use the version in the dprocedures package
func NewDoltTagFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltTagFunc{expression.NaryExpression{ChildExpressions: args}}, nil
}

func (d DoltTagFunc) String() string {
	childrenStrings := make([]string, len(d.Children()))

	for i, child := range d.Children() {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("DOLT_BRANCH(%s)", strings.Join(childrenStrings, ","))
}

func (d DoltTagFunc) Type() sql.Type {
	return sql.Int8
}

func (d DoltTagFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltBranchFunc(children...)
}

func (d DoltTagFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	args, err := getDoltArgs(ctx, row, d.Children())
	if err != nil {
		return 1, err
	}
	return DoDoltTag(ctx, args)
}

func DoDoltTag(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()
	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}
	dSess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := dSess.GetDbData(ctx, dbName)
	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	apr, err := cli.CreateTagArgParser().Parse(args)
	if err != nil {
		return 1, err
	}

	// list tags
	if len(apr.Args) == 0 {
		return 1, InvalidArgErr
	}

	// delete tag
	if apr.Contains(cli.DeleteFlag) {
		if apr.Contains(cli.MessageArg) {
			return 1, fmt.Errorf("delete and tag message options are incompatible")
		} else if apr.Contains(cli.VerboseFlag) {
			return 1, fmt.Errorf("delete and verbose options are incompatible")
		}
		err = actions.DeleteTagsOnDB(ctx, dbData.Ddb, apr.Args...)
		if err != nil {
			return 1, err
		}
		return 0, nil
	}

	// create tag
	if apr.Contains(cli.VerboseFlag) {
		return 1, fmt.Errorf("verbose flag can only be used with tag listing")
	} else if len(apr.Args) > 2 {
		return 1, fmt.Errorf("create tag takes at most two args")
	}

	name := dSess.Username()
	email := dSess.Email()
	msg, _ := apr.GetValue(cli.MessageArg)

	props := actions.TagProps{
		TaggerName:  name,
		TaggerEmail: email,
		Description: msg,
	}

	tagName := apr.Arg(0)
	startPoint := "head"
	if len(apr.Args) > 1 {
		startPoint = apr.Arg(1)
	}
	headRef := dbData.Rsr.CWBHeadRef()
	err = actions.CreateTagOnDB(ctx, dbData.Ddb, tagName, startPoint, props, headRef)
	if err != nil {
		return 1, err
	}

	return 0, nil
}
