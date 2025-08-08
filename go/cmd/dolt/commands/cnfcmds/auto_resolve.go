// Copyright 2019 Dolthub, Inc.
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

package cnfcmds

import (
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
)

type AutoResolveStrategy int

const (
	AutoResolveStrategyOurs AutoResolveStrategy = iota
	AutoResolveStrategyTheirs
)

// AutoResolveTables resolves all conflicts in the given tables according to the
// given |strategy|.
func AutoResolveTables(queryist cli.Queryist, sqlCtx *sql.Context, strategy AutoResolveStrategy, tbls []string) error {

	for _, tableName := range tbls {
		resolveQuery := "CALL dolt_conflicts_resolve(?, ?)"
		var resolveParams []interface{}
		switch strategy {
		case AutoResolveStrategyOurs:
			resolveParams = []interface{}{"--ours", tableName}
		case AutoResolveStrategyTheirs:
			resolveParams = []interface{}{"--theirs", tableName}
		default:
			return errors.New("invalid auto resolve strategy")
		}

		q, err := dbr.InterpolateForDialect(resolveQuery, resolveParams, dialect.MySQL)
		if err != nil {
			return fmt.Errorf("error interpolating resolve conflicts query for table %s: %w", tableName, err)
		}
		_, err = cli.GetRowsForSql(queryist, sqlCtx, q)
		if err != nil {
			return fmt.Errorf("error resolving conflicts for table %s: %w", tableName, err)
		}
	}

	return nil
}

func quoteWithPrefix(arr []string, prefix string) []string {
	out := make([]string, len(arr))
	for i := range arr {
		out[i] = fmt.Sprintf("`%s%s`", prefix, arr[i])
	}
	return out
}
