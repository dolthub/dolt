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
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

const DoltResetFuncName = "dolt_reset"

type DoltResetFunc struct {
	children []sql.Expression
}

func (d DoltResetFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}

	dSess := sqle.DSessFromSess(ctx.Session)
	dbData, ok := dSess.GetDbData(dbName)

	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	ap := cli.CreateResetArgParser()
	args, err := getDoltArgs(ctx, row, d.Children())

	if err != nil {
		return 1, err
	}

	apr := cli.ParseArgs(ap, args, nil)

	// Check if problems with args first.
	if apr.ContainsAll(cli.HardResetParam, cli.SoftResetParam) {
		return 1, fmt.Errorf("error: --%s and --%s are mutually exclusive options.", cli.HardResetParam, cli.SoftResetParam)
	}

	// Get all the needed roots.
	working, staged, head, err := env.GetRoots(ctx, dbData.Ddb, dbData.Rsr)

	if err != nil {
		return 1, err
	}

	if apr.Contains(cli.HardResetParam) {
		h, err := actions.ResetHardTables(ctx, dbData, apr, working, staged, head)
		if err != nil {
			return 1, err
		}

		// In this case we preserve untracked tables.
		if h == "" {
			headHash, err := dbData.Rsr.CWBHeadHash(ctx)
			if err != nil {
				return 1, err
			}

			h = headHash.String()
			err = setSessionRootExplicit(ctx, h, sqle.HeadKeySuffix)
			if err != nil {
				return 1, err
			}

			workingHash := dbData.Rsr.WorkingHash()
			err = setSessionRootExplicit(ctx, workingHash.String(), sqle.WorkingKeySuffix)
		} else {
			err = setHeadAndWorkingSessionRoot(ctx, h)
		}
	} else {
		_, err = actions.ResetSoftTables(ctx, dbData, apr, staged, head)
	}

	if err != nil {
		return 1, err
	}

	return 0, nil
}

func (d DoltResetFunc) Resolved() bool {
	for _, child := range d.Children() {
		if !child.Resolved() {
			return false
		}
	}
	return true
}

func (d DoltResetFunc) String() string {
	childrenStrings := make([]string, len(d.children))

	for i, child := range d.children {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("DOLT_RESET(%s)", strings.Join(childrenStrings, ","))
}

func (d DoltResetFunc) Type() sql.Type {
	return sql.Int8
}

func (d DoltResetFunc) IsNullable() bool {
	for _, child := range d.Children() {
		if child.IsNullable() {
			return true
		}
	}
	return false
}

func (d DoltResetFunc) Children() []sql.Expression {
	return d.children
}

func (d DoltResetFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltResetFunc(children...)
}

func NewDoltResetFunc(args ...sql.Expression) (sql.Expression, error) {
	return DoltResetFunc{children: args}, nil
}
