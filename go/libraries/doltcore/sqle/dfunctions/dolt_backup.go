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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
)

const DoltBackupFuncName = "dolt_backup"

// Deprecated: please use the version in the dprocedures package
type DoltBackupFunc struct {
	expression.NaryExpression
}

// Deprecated: please use the version in the dprocedures package
func NewDoltBackupFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltBackupFunc{expression.NaryExpression{ChildExpressions: args}}, nil
}

func (d DoltBackupFunc) String() string {
	childrenStrings := make([]string, len(d.Children()))

	for i, child := range d.Children() {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("DOLT_BACKUP(%s)", strings.Join(childrenStrings, ","))
}

func (d DoltBackupFunc) Type() sql.Type {
	return sql.Int8
}

func (d DoltBackupFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltBackupFunc(children...)
}

func (d DoltBackupFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	args, err := getDoltArgs(ctx, row, d.Children())
	if err != nil {
		return 1, err
	}
	return DoDoltBackup(ctx, args)
}

func DoDoltBackup(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}

	_, err := cli.CreateBackupArgParser().Parse(args)
	if err != nil {
		return 1, err
	}

	return 1, fmt.Errorf("unimplemented")
}
