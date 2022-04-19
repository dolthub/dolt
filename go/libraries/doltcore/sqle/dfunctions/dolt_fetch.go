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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const DoltFetchFuncName = "dolt_fetch"

const (
	cmdFailure = 0
	cmdSuccess = 1
)

// Deprecated: please use the version in the dprocedures package
type DoltFetchFunc struct {
	expression.NaryExpression
}

// NewFetchFunc creates a new FetchFunc expression.
// Deprecated: please use the version in the dprocedures package
func NewFetchFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltFetchFunc{expression.NaryExpression{ChildExpressions: args}}, nil
}

func (d DoltFetchFunc) String() string {
	childrenStrings := make([]string, len(d.Children()))

	for i, child := range d.Children() {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("DOLT_FETCH(%s)", strings.Join(childrenStrings, ","))
}

func (d DoltFetchFunc) Type() sql.Type {
	return sql.Boolean
}

func (d DoltFetchFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewFetchFunc(children...)
}

func (d DoltFetchFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	args, err := getDoltArgs(ctx, row, d.Children())
	if err != nil {
		return cmdFailure, err
	}
	return DoDoltFetch(ctx, args)
}

func DoDoltFetch(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return cmdFailure, fmt.Errorf("empty database name")
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := sess.GetDbData(ctx, dbName)
	if !ok {
		return cmdFailure, fmt.Errorf("Could not load database %s", dbName)
	}

	apr, err := cli.CreateFetchArgParser().Parse(args)
	if err != nil {
		return cmdFailure, err
	}

	remote, refSpecs, err := env.NewFetchOpts(apr.Args, dbData.Rsr)
	if err != nil {
		return cmdFailure, err
	}

	updateMode := ref.UpdateMode{Force: apr.Contains(cli.ForceFlag)}

	err = actions.FetchRefSpecs(ctx, dbData, refSpecs, remote, updateMode, runProgFuncs, stopProgFuncs)
	if err != nil {
		return cmdFailure, fmt.Errorf("fetch failed: %w", err)
	}
	return cmdSuccess, nil
}
