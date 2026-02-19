// Copyright 2026 Dolthub, Inc.
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

package prompt

import (
	"errors"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// Parts contains shell prompt components to render in the final prompt.
type Parts struct {
	BaseDatabase   string
	ActiveRevision string
	IsBranch       bool
	Dirty          bool
}

// Resolver resolves prompt Parts for the active session.
type Resolver interface {
	Resolve(sqlCtx *sql.Context, queryist cli.Queryist) (parts Parts, resolved bool, err error)
}

// doltSystemVariablesResolver can resolve [prompt.Parts] using [dsess] system variables. This supports revision
// qualified names can have two meanings (i.e., a normal database that uses the [doltdb.DbRevisionDelimiterAlias] in
// their names versus a revision qualified one).
type doltSystemVariablesResolver struct{}

// sqlDBActiveBranchResolver can resolve [prompt.Parts] using the SQL-specific functions. It is a fallback for older
// servers, and is the method older shells use in general. The resolver does not support
// [doltdb.DbRevisionDelimiterAlias] as a revision delimiter.
type sqlDBActiveBranchResolver struct{}

// chainedResolver can resolve [prompt.Parts] through the sequential execution [prompt.Resolver](s).
type chainedResolver struct {
	resolvers []Resolver
}

// NewPartsResolver constructs an up-to-date [prompt.Resolver].
func NewPartsResolver() Resolver {
	return chainedResolver{
		resolvers: []Resolver{
			doltSystemVariablesResolver{},
			sqlDBActiveBranchResolver{},
		},
	}
}

// Resolve resolves [prompt.Parts] through a chain of [prompt.Resolver](s) in sequential order. If a Resolver encounters
// an error, it is returned immediately and no other Resolver executes.
func (cr chainedResolver) Resolve(sqlCtx *sql.Context, queryist cli.Queryist) (parts Parts, resolved bool, err error) {
	for _, resolver := range cr.resolvers {
		parts, resolved, err := resolver.Resolve(sqlCtx, queryist)
		if err != nil {
			return Parts{}, false, err
		}
		if resolved {
			return parts, true, nil
		}
	}
	return Parts{}, false, nil
}

// Resolve resolves [prompt.Parts] by reading [dsess.DoltBaseDatabase] and [dsess.DoltActiveRevision] which certify the
// correct base (or repository) database when the [doltdb.DbRevisionDelimiterAlias] is in use in a revision qualified
// database name or normal database.
func (doltSystemVariablesResolver) Resolve(sqlCtx *sql.Context, queryist cli.Queryist) (parts Parts, resolved bool, err error) {
	parts = Parts{}
	variableValues, err := cli.GetSystemVariableValues(sqlCtx, queryist, dsess.DoltBaseDatabase, dsess.DoltActiveRevision)
	if err != nil {
		return parts, false, err
	}

	var hasBase, hasRevision bool
	parts.BaseDatabase, hasBase = variableValues[dsess.DoltBaseDatabase]
	parts.ActiveRevision, hasRevision = variableValues[dsess.DoltActiveRevision]
	if !hasBase || !hasRevision {
		return parts, false, nil
	}

	parts.Dirty, parts.IsBranch, err = resolveDirty(sqlCtx, queryist)
	if err != nil {
		return parts, false, err
	}

	return parts, true, nil
}

// Resolve resolves the base database and active revision through the SQL-specific functions `database()` and
// `active_branch()`. Unfortunately, to maintain support for ORMs that rely on the database in their connection URL,
// this method cannot interpret [doltdb.DbRevisionDelimiterAlias] as a revision delimiter.
func (sqlDBActiveBranchResolver) Resolve(sqlCtx *sql.Context, queryist cli.Queryist) (parts Parts, resolved bool, err error) {
	parts = Parts{}
	dbRows, err := cli.GetRowsForSql(queryist, sqlCtx, "select database() as db")
	if err != nil {
		return parts, false, err
	}
	if len(dbRows) > 0 && len(dbRows[0]) > 0 {
		dbName, err := cli.GetStringColumnValue(dbRows[0][0])
		if err != nil {
			return parts, false, err
		}
		parts.BaseDatabase, parts.ActiveRevision = doltdb.SplitRevisionDbName(dbName)
	}

	if parts.ActiveRevision == "" {
		rows, err := cli.GetRowsForSql(queryist, sqlCtx, "select active_branch() as branch")
		if err != nil {
			return parts, false, err
		}
		if len(rows) > 0 && len(rows[0]) > 0 {
			parts.ActiveRevision, err = cli.GetStringColumnValue(rows[0][0])
			if err != nil {
				return parts, false, err
			}
		}
	}

	parts.Dirty, parts.IsBranch, err = resolveDirty(sqlCtx, queryist)
	if err != nil {
		return parts, false, err
	}
	return parts, true, nil
}

// resolveDirty resolves the dirty state of the current branch. The isBranch bool is returned to differentiate other
// revision types.
func resolveDirty(sqlCtx *sql.Context, queryist cli.Queryist) (dirty bool, isBranch bool, err error) {
	rows, err := cli.GetRowsForSql(queryist, sqlCtx, "select count(table_name) > 0 as dirty from dolt_status")
	if errors.Is(err, doltdb.ErrOperationNotSupportedInDetachedHead) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return false, false, nil
	}
	dirty, err = cli.GetBoolColumnValue(rows[0][0])
	if err != nil {
		return false, false, err
	}

	return dirty, true, nil
}
