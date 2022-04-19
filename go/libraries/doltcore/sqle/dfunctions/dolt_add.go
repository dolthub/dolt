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
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const DoltAddFuncName = "dolt_add"

// Deprecated: please use the version in the dprocedures package
type DoltAddFunc struct {
	children []sql.Expression
}

func (d DoltAddFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	args, err := getDoltArgs(ctx, row, d.Children())
	if err != nil {
		return 1, err
	}
	return DoDoltAdd(ctx, args)
}

func DoDoltAdd(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}

	apr, err := cli.CreateAddArgParser().Parse(args)
	if err != nil {
		return 1, err
	}

	allFlag := apr.Contains(cli.AllFlag)

	dSess := dsess.DSessFromSess(ctx.Session)
	roots, ok := dSess.GetRoots(ctx, dbName)
	if apr.NArg() == 0 && !allFlag {
		return 1, fmt.Errorf("Nothing specified, nothing added. Maybe you wanted to say 'dolt add .'?")
	} else if allFlag || apr.NArg() == 1 && apr.Arg(0) == "." {
		if !ok {
			return 1, fmt.Errorf("db session not found")
		}

		roots, err = actions.StageAllTablesNoDocs(ctx, roots)
		if err != nil {
			return 1, err
		}

		err = dSess.SetRoots(ctx, dbName, roots)
		if err != nil {
			return 1, err
		}
	} else {
		roots, err = actions.StageTablesNoDocs(ctx, roots, apr.Args)
		if err != nil {
			return 1, err
		}

		err = dSess.SetRoots(ctx, dbName, roots)
		if err != nil {
			return 1, err
		}
	}

	return 0, nil
}

func (d DoltAddFunc) Resolved() bool {
	for _, child := range d.Children() {
		if !child.Resolved() {
			return false
		}
	}
	return true
}

func (d DoltAddFunc) String() string {
	childrenStrings := make([]string, len(d.children))

	for i, child := range d.children {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("DOLT_ADD(%s)", strings.Join(childrenStrings, ","))
}

func (d DoltAddFunc) Type() sql.Type {
	return sql.Int8
}

func (d DoltAddFunc) IsNullable() bool {
	for _, child := range d.Children() {
		if child.IsNullable() {
			return true
		}
	}
	return false
}

func (d DoltAddFunc) Children() []sql.Expression {
	return d.children
}

func (d DoltAddFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltAddFunc(children...)
}

// NewDoltAddFunc creates a new DoltAddFunc expression whose children represents the args passed in DOLT_ADD.
// Deprecated: please use the version in the dprocedures package
func NewDoltAddFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltAddFunc{children: args}, nil
}
