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

package dprocedures

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
)

// doltFetch is the stored procedure version of the function `dolt_fetch`.
func doltFetch(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, err := dfunctions.DoDoltFetch(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res)), nil
}
