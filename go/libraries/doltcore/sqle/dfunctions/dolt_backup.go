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
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas/pull"
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

	apr, err := cli.CreateBackupArgParser().Parse(args)
	if err != nil {
		return 1, err
	}

	switch {
	case apr.NArg() == 0:
		return 1, fmt.Errorf("listing existing backups endpoints in sql is unimplemented.")
	case apr.Arg(0) == cli.AddBackupId:
		return 1, fmt.Errorf("adding backup endpoint in sql is unimplemented.")
	case apr.Arg(0) == cli.RemoveBackupId:
		return 1, fmt.Errorf("removing backup endpoint in sql is unimplemented.")
	case apr.Arg(0) == cli.RemoveBackupShortId:
		return 1, fmt.Errorf("removing backup endpoint in sql is unimplemented.")
	case apr.Arg(0) == cli.RestoreBackupId:
		return 1, fmt.Errorf("restoring backup endpoint in sql is unimplemented.")
	case apr.Arg(0) == cli.SyncBackupId:
		if apr.NArg() != 2 {
			return 1, fmt.Errorf("usage: dolt_backup('sync', BACKUP_NAME)")
		}

		backupName := strings.TrimSpace(apr.Arg(1))

		sess := dsess.DSessFromSess(ctx.Session)
		dbData, ok := sess.GetDbData(ctx, dbName)
		if !ok {
			return 1, sql.ErrDatabaseNotFound.New(dbName)
		}

		backups, err := dbData.Rsr.GetBackups()
		if err != nil {
			return 1, err
		}
		b, ok := backups[backupName]
		if !ok {
			return 1, fmt.Errorf("error: unknown backup: '%s'; %v", backupName, backups)
		}

		destDb, err := b.GetRemoteDB(ctx, dbData.Ddb.ValueReadWriter().Format())
		if err != nil {
			return 1, fmt.Errorf("error loading backup destination: %w", err)
		}

		err = actions.SyncRoots(ctx, dbData.Ddb, destDb, dbData.Rsw.TempTableFilesDir(), runProgFuncs, stopProgFuncs)
		if err != nil && err != pull.ErrDBUpToDate {
			return 1, fmt.Errorf("error syncing backup: %w", err)
		}
		return 0, nil
	default:
		return 1, fmt.Errorf("unrecognized dolt_backup parameter: %s", apr.Arg(0))
	}
}
