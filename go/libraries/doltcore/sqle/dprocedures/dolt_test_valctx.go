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
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
)

// Only installed if valctx is enabled. This is only used in testing, and
// it lets tests assert that valctx is registered and working as expected.
//
// This stored procedure intentionally calls into the DoltDB layer with
// an unregistered valctx and allows a caller to look for the error which
// should get surfaced.
func doltTestValctx(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	dbName := ctx.GetCurrentDatabase()
	if len(dbName) == 0 {
		return rowToIter(int64(1)), fmt.Errorf("Empty database name.")
	}
	dSess := dsess.DSessFromSess(ctx.Session)
	ddb, ok := dSess.GetDoltDB(ctx, dbName)
	if !ok {
		return rowToIter(int64(1)), fmt.Errorf("Unable to get DoltDB")
	}
	// With valctx enabled, this should panic.  We are passing in
	// |context.Background()| here intentionally.  Note, that if
	// this does not panic, it will return an error, because a
	// RootValue with a |0| hash should not exist in the
	// database. We ignore that error here, and always return
	// success. If valctx changed to return an error instead of
	// panic, this would need to be reworked.
	ddb.ReadRootValue(context.Background(), hash.Hash{})
	return rowToIter(int64(0)), nil
}

func NewTestValctxProcedure() sql.ExternalStoredProcedureDetails {
	return sql.ExternalStoredProcedureDetails{
		Name: "dolt_test_valctx",
		Schema: int64Schema("status"),
		Function: doltTestValctx,
	}
}
