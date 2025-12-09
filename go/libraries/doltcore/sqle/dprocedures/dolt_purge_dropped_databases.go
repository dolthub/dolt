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

	doltSession := dsess.DSessFromSess(ctx.Session)
	err := doltSession.Provider().PurgeDroppedDatabases(ctx)
	if err != nil {
		return nil, err
	}

	return rowToIter(int64(cmdSuccess)), nil
}
