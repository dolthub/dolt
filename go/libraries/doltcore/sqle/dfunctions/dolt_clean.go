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

const DoltCleanFuncName = "dolt_clean"

// Deprecated: please use the version in the dprocedures package
type DoltCleanFunc struct {
	children []sql.Expression
}

func (d DoltCleanFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	args, err := getDoltArgs(ctx, row, d.Children())
	if err != nil {
		return 1, err
	}
	return DoDoltClean(ctx, args)
}

func DoDoltClean(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}

	dSess := dsess.DSessFromSess(ctx.Session)

	apr, err := cli.CreateCleanArgParser().Parse(args)
	if err != nil {
		return 1, err
	}

	// Get all the needed roots.
	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	roots, err = actions.CleanUntracked(ctx, roots, apr.Args, apr.ContainsAll(cli.DryRunFlag))
	if err != nil {
		return 1, fmt.Errorf("failed to clean; %w", err)
	}

	err = dSess.SetRoots(ctx, dbName, roots)
	if err != nil {
		return 1, err
	}
	return 0, nil
}

func (d DoltCleanFunc) Resolved() bool {
	for _, child := range d.Children() {
		if !child.Resolved() {
			return false
		}
	}
	return true
}

func (d DoltCleanFunc) String() string {
	childrenStrings := make([]string, len(d.children))

	for i, child := range d.children {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("DOLT_CLEAN(%s)", strings.Join(childrenStrings, ","))
}

func (d DoltCleanFunc) Type() sql.Type {
	return sql.Int8
}

func (d DoltCleanFunc) IsNullable() bool {
	for _, child := range d.Children() {
		if child.IsNullable() {
			return true
		}
	}
	return false
}

func (d DoltCleanFunc) Children() []sql.Expression {
	return d.children
}

func (d DoltCleanFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltCleanFunc(children...)
}

// Deprecated: please use the version in the dprocedures package
func NewDoltCleanFunc(args ...sql.Expression) (sql.Expression, error) {
	return DoltCleanFunc{children: args}, nil
}
