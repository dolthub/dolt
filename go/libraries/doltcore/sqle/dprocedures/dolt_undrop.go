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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"strings"
)

// doltClean is the stored procedure version for the CLI command `dolt clean`.
func doltUndrop(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	doltSession := dsess.DSessFromSess(ctx.Session)
	provider := doltSession.Provider()

	switch len(args) {
	case 0:
		// TODO: Are there any permission issues for undrop? probably the same as drop?
		undroppableDatabases, err := provider.ListUndroppableDatabases(ctx)
		if err != nil {
			return nil, err
		}

		extraInformation := "there are no databases that can currently be undropped."
		if len(undroppableDatabases) > 0 {
			extraInformation = fmt.Sprintf("the following dropped databases are availble to be undropped: %s",
				strings.Join(undroppableDatabases, ", "))
		}
		return nil, fmt.Errorf("no database name specified. %s", extraInformation)

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
