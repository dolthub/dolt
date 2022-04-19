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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas"
)

const DoltPushFuncName = "dolt_push"

// Deprecated: please use the version in the dprocedures package
type DoltPushFunc struct {
	expression.NaryExpression
}

// NewPushFunc creates a new PushFunc expression.
// Deprecated: please use the version in the dprocedures package
func NewPushFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltPushFunc{expression.NaryExpression{ChildExpressions: args}}, nil
}

func (d DoltPushFunc) String() string {
	childrenStrings := make([]string, len(d.Children()))

	for i, child := range d.Children() {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("DOLT_PUSH(%s)", strings.Join(childrenStrings, ","))
}

func (d DoltPushFunc) Type() sql.Type {
	return sql.Boolean
}

func (d DoltPushFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewPushFunc(children...)
}

func (d DoltPushFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	args, err := getDoltArgs(ctx, row, d.Children())
	if err != nil {
		return cmdFailure, err
	}
	return DoDoltPush(ctx, args)
}

func DoDoltPush(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return cmdFailure, fmt.Errorf("empty database name")
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := sess.GetDbData(ctx, dbName)

	if !ok {
		return cmdFailure, fmt.Errorf("could not load database %s", dbName)
	}

	apr, err := cli.CreatePushArgParser().Parse(args)
	if err != nil {
		return cmdFailure, err
	}

	opts, err := env.NewPushOpts(ctx, apr, dbData.Rsr, dbData.Ddb, apr.Contains(cli.ForceFlag), apr.Contains(cli.SetUpstreamFlag))
	if err != nil {
		return cmdFailure, err
	}
	err = actions.DoPush(ctx, dbData.Rsr, dbData.Rsw, dbData.Ddb, dbData.Rsw.TempTableFilesDir(), opts, runProgFuncs, stopProgFuncs)
	if err != nil {
		switch err {
		case doltdb.ErrUpToDate:
			return cmdSuccess, nil
		case datas.ErrMergeNeeded:
			return cmdFailure, fmt.Errorf("%w; the tip of your current branch is behind its remote counterpart", err)
		default:
			return cmdFailure, err
		}
	}
	return cmdSuccess, nil
}
