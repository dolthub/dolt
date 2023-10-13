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
)

func doltPurgeDroppedDatabases(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	if len(args) > 0 {
		return nil, fmt.Errorf("dolt_purge_dropped_databases does not take any arguments")
	}

	// Only allow admins to purge dropped databases
	if err := checkDoltPurgeDroppedDatabasesPrivs(ctx); err != nil {
		return nil, err
	}

	doltSession := dsess.DSessFromSess(ctx.Session)
	err := doltSession.Provider().PurgeDroppedDatabases(ctx)
	if err != nil {
		return nil, err
	}

	return rowToIter(int64(cmdSuccess)), nil
}

// checkDoltPurgeDroppedDatabasesPrivs returns an error if the user requesting to purge dropped databases
// does not have SUPER access. Since this is a permanent and destructive operation, we restrict it to admins,
// even though the SUPER privilege has been deprecated, since there isn't another appropriate global privilege.
func checkDoltPurgeDroppedDatabasesPrivs(ctx *sql.Context) error {
	privs, counter := ctx.GetPrivilegeSet()
	if counter == 0 {
		return fmt.Errorf("unable to check user privileges for dolt_purge_dropped_databases procedure")
	}
	if privs.Has(sql.PrivilegeType_Super) == false {
		return sql.ErrPrivilegeCheckFailed.New(ctx.Session.Client().User)
	}

	return nil
}
