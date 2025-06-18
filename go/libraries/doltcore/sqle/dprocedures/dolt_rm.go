// Copyright 2025 Dolthub, Inc.
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

package dprocedures

import (
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/go-mysql-server/sql"
)

// doltAdd is the stored procedure version for the CLI command `dolt add`.
func doltRm(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, err := doDoltRm(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res)), nil
}

func doDoltRm(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return 1, err
	}

	apr, err := cli.CreateRmArgParser().Parse(args)
	if err != nil {
		return 1, err
	}

	if apr.NArg() == 0 {
		return 1, fmt.Errorf("Nothing specified, nothing added. Maybe you wanted to say 'dolt add .'?")
	}

	return 0, nil
}
