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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// Context contains shell prompt database and revision values.
type Context struct {
	BaseDatabase   string
	ActiveRevision string
}

// Resolver resolves prompt context for the active SQL session. The bool return indicates whether context was resolved.
type Resolver interface {
	Resolve(sqlCtx *sql.Context, queryist cli.Queryist) (Context, bool, error)
}

type doltSystemVariablesResolver struct{}
type sqlDBActiveBranchResolver struct{}

type chainedResolver struct {
	resolvers []Resolver
}

// NewResolver returns prompt context resolution with a variables-first strategy and a legacy fallback.
func NewResolver() Resolver {
	return chainedResolver{
		resolvers: []Resolver{
			doltSystemVariablesResolver{},
			sqlDBActiveBranchResolver{},
		},
	}
}

func (p chainedResolver) Resolve(sqlCtx *sql.Context, queryist cli.Queryist) (Context, bool, error) {
	for _, resolver := range p.resolvers {
		context, ok, err := resolver.Resolve(sqlCtx, queryist)
		if err != nil {
			return Context{}, false, err
		}
		if ok {
			return context, true, nil
		}
	}
	return Context{}, false, nil
}

func (doltSystemVariablesResolver) Resolve(sqlCtx *sql.Context, queryist cli.Queryist) (Context, bool, error) {
	variableValues, err := cli.GetSystemVariableValues(queryist, sqlCtx, dsess.DoltBaseDatabase, dsess.DoltActiveRevision)
	if err != nil {
		return Context{}, false, err
	}

	baseDatabase, hasBase := variableValues[dsess.DoltBaseDatabase]
	activeRevision, hasRevision := variableValues[dsess.DoltActiveRevision]
	if !hasBase || !hasRevision {
		return Context{}, false, nil
	}

	return Context{
		BaseDatabase:   baseDatabase,
		ActiveRevision: activeRevision,
	}, true, nil
}

func (sqlDBActiveBranchResolver) Resolve(sqlCtx *sql.Context, queryist cli.Queryist) (Context, bool, error) {
	dbRows, err := cli.GetRowsForSql(queryist, sqlCtx, "select database() as db")
	if err != nil {
		return Context{}, false, err
	}
	if len(dbRows) == 0 || dbRows[0] == nil {
		return Context{}, true, nil
	}

	baseDatabase := ""
	activeRevision := ""
	dbName, err := cli.GetStringColumnValue(dbRows[0])
	if err != nil {
		return Context{}, false, err
	}
	baseDatabase, activeRevision = doltdb.SplitRevisionDbName(dbName)

	// Revision-qualified names already contain the revision and do not require active_branch().
	if activeRevision != "" {
		return Context{
			BaseDatabase:   baseDatabase,
			ActiveRevision: activeRevision,
		}, true, nil
	}

	branchRows, err := cli.GetRowsForSql(queryist, sqlCtx, "select active_branch() as branch")
	if err != nil {
		return Context{}, false, err
	}
	if len(branchRows) > 0 && branchRows[0] != nil {
		activeRevision, err = cli.GetStringColumnValue(branchRows[0])
		if err != nil {
			return Context{}, false, err
		}
	}

	return Context{
		BaseDatabase:   baseDatabase,
		ActiveRevision: activeRevision,
	}, true, nil
}
