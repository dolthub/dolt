// Copyright 2023 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/errors"
)

// doltClean is the stored procedure version for the CLI command `dolt clean`.
func doltUndrop(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	doltSession := dsess.DSessFromSess(ctx.Session)
	provider := doltSession.Provider()

	// TODO: What are the right permissions for dolt_undrop?
	//       the same as drop? or create db?

	switch len(args) {
	case 0:
		availableDatabases, err := provider.ListDroppedDatabases(ctx)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("no database name specified. %s", errors.CreateUndropErrorMessage(availableDatabases))

	case 1:
		if err := provider.UndropDatabase(ctx, args[0]); err != nil {
			return nil, err
		}
		return rowToIter(int64(0)), nil

	default:
		return nil, fmt.Errorf("dolt_undrop called with too many arguments: " +
			"dolt_undrop only accepts one argument - the name of the dropped database to restore")
	}
}
