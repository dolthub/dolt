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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

// Parts contains shell prompt components to render the SQL shell prompt.
type Parts struct {
	BaseDatabase      string
	ActiveRevision    string
	RevisionDelimiter string
	IsBranch          bool
	Dirty             bool
}

// Resolver resolves prompt [prompt.Parts] for the active session.
type Resolver interface {
	Resolve(sqlCtx *sql.Context, queryist cli.Queryist) (parts Parts, resolved bool, err error)
}

// sqlDBActiveBranchResolver can resolve [prompt.Parts] using the Dolt SQL functions, and supports
// [doltdb.DbRevisionDelimiter] and [doltdb.DbRevisionDelimiterAlias].
type sqlDBActiveBranchResolver struct{}

// chainedResolver can resolve [prompt.Parts] through the sequential execution [prompt.Resolver](s).
type chainedResolver struct {
	resolvers []Resolver
}

// NewPromptResolver constructs an up-to-date [prompt.Resolver].
func NewPromptResolver() Resolver {
	return chainedResolver{
		resolvers: []Resolver{
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

// Resolve resolves the base DB and revision through the SQL function `database()` and Dolt-specific `active_branch()`.
func (sqlDBActiveBranchResolver) Resolve(sqlCtx *sql.Context, queryist cli.Queryist) (parts Parts, resolved bool, err error) {
	dbRows, err := cli.GetRowsForSql(queryist, sqlCtx, "select database() as db")
	if err != nil {
		return parts, false, err
	}
	if len(dbRows) > 0 && len(dbRows[0]) > 0 {
		dbName, err := cli.QueryValueAsString(dbRows[0][0])
		if err != nil {
			return parts, false, err
		}
		// Handles non-branch revisions (i.e., commit hash, tags, etc.).
		parts.BaseDatabase, parts.ActiveRevision = doltdb.SplitRevisionDbName(dbName)

		parts.RevisionDelimiter = doltdb.DbRevisionDelimiter
		for _, delimiter := range doltdb.DBRevisionDelimiters {
			if strings.Contains(dbName, delimiter) {
				parts.RevisionDelimiter = delimiter
				break
			}
		}
	}

	activeBranchRows, err := cli.GetRowsForSql(queryist, sqlCtx, "select active_branch() as branch")
	if err != nil {
		return parts, false, err
	}

	if len(activeBranchRows) > 0 && len(activeBranchRows[0]) > 0 {
		parts.IsBranch = activeBranchRows[0][0] != nil
		if parts.ActiveRevision == "" {
			parts.ActiveRevision, err = cli.QueryValueAsString(activeBranchRows[0][0])
			if err != nil {
				return parts, false, err
			}
		}
	}

	parts.Dirty, err = resolveDirty(sqlCtx, queryist, parts)
	if err != nil {
		return parts, false, err
	}

	return parts, true, nil
}

// resolveDirty resolves the dirty state of the current branch and whether the revision type is a branch.
func resolveDirty(sqlCtx *sql.Context, queryist cli.Queryist, parts Parts) (dirty bool, err error) {
	if doltdb.IsValidCommitHash(parts.ActiveRevision) {
		return false, nil
	}

	rows, err := cli.GetRowsForSql(queryist, sqlCtx, "select count(table_name) > 0 as dirty from dolt_status")
	// [sql.ErrTableNotFound] detects when viewing a non-Dolt database (e.g., information_schema). Older servers may
	// complain about [doltdb.ErrOperationNotSupportedInDetachedHead], but read-only revisions in newer versions this
	// issue should be gone.
	if errors.Is(err, doltdb.ErrOperationNotSupportedInDetachedHead) || sql.ErrTableNotFound.Is(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	if len(rows) == 0 || len(rows[0]) == 0 {
		return false, nil
	}
	dirty, err = cli.QueryValueAsBool(rows[0][0])
	if err != nil {
		return false, err
	}

	return dirty, nil
}
