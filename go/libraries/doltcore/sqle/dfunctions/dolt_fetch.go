// Copyright 2021 Dolthub, Inc.
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

package dfunctions

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const DoltFetchFuncName = "dolt_fetch"

type DoltFetchFunc struct {
	expression.NaryExpression
}

// NewFetchFunc creates a new FetchFunc expression.
func NewFetchFunc(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	return &DoltFetchFunc{expression.NaryExpression{ChildExpressions: args}}, nil
}

func (d DoltFetchFunc) String() string {
	childrenStrings := make([]string, len(d.Children()))

	for i, child := range d.Children() {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("DOLT_FETCH(%s)", strings.Join(childrenStrings, ","))
}

func (d DoltFetchFunc) Type() sql.Type {
	return sql.Boolean
}

func (d DoltFetchFunc) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewFetchFunc(ctx, children...)
}

func (d DoltFetchFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return noConflicts, fmt.Errorf("empty database name.")
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := sess.GetDbData(ctx, dbName)

	if !ok {
		return noConflicts, fmt.Errorf("Could not load database %s", dbName)
	}

	ap := cli.CreateMergeArgParser()
	args, err := getDoltArgs(ctx, row, d.Children())

	apr, err := ap.Parse(args)
	if err != nil {
		return noConflicts, err
	}

	remote, refSpecs, err := env.ParseFetchOpts(apr.Args(), dbData.Rsr)

	updateMode := ref.UpdateMode{Force: apr.Contains(cli.ForceFlag)}

	srcDB, err := remote.GetRemoteDBWithoutCaching(ctx, dbData.Ddb.ValueReadWriter().Format())
	if err != nil {
		return noConflicts, fmt.Errorf("failed to get remote db; %w", err)
	}

	for _, rs := range refSpecs {

		branchRefs, err := srcDB.GetHeadRefs(ctx)

		if err != nil {
			return nil, env.ErrFailedToReadDb
		}

		rsSeen := false

		for _, branchRef := range branchRefs {
			remoteTrackRef := rs.DestRef(branchRef)

			if remoteTrackRef != nil {
				rsSeen = true
				// todo: can we pass nil for either of the channels?
				srcDBCommit, err := actions.FetchRemoteBranch(ctx, dbData.Rsw.TempTableFilesDir(), remote, srcDB, dbData.Ddb, branchRef, remoteTrackRef, runProgFuncs, stopProgFuncs)
				if err != nil {
					return noConflicts, err
				}

				switch updateMode {
				case ref.ForceUpdate:
					err := dbData.Ddb.SetHeadToCommit(ctx, remoteTrackRef, srcDBCommit)
					if err != nil {
						//return errhand.BuildDError("error: fetch failed").AddCause(err).Build()
						return nil, err
					}
				case ref.FastForwardOnly:
					ok, err := dbData.Ddb.CanFastForward(ctx, remoteTrackRef, srcDBCommit)
					if !ok {
						return nil, fmt.Errorf("%w: %s", actions.ErrCantFF, err.Error())
					}

					if err == nil || err == doltdb.ErrUpToDate || err == doltdb.ErrIsAhead {
						err = dbData.Ddb.FastForward(ctx, remoteTrackRef, srcDBCommit)
						if err != nil {
							return nil, fmt.Errorf("%w: %s", actions.ErrCantFF, err.Error())
						}
					} else if err != nil {
						return nil, fmt.Errorf("%w: %s", actions.ErrCantFF, err.Error())
					}
				}
			}
			if !rsSeen {
				msg := "does not appear to be a dolt database. could not read from the remote database. please make sure you have the correct access rights and the database exists"
				if rb, ok := rs.(ref.BranchToTrackingBranchRefSpec); ok {
					return nil, fmt.Errorf("%w; '%s' %s", err, rb, msg)
				}
				return nil, fmt.Errorf("%w; %s %s", err, "remote", msg)
			}
		}
	}

	err = actions.FetchFollowTags(ctx, dbData.Rsw.TempTableFilesDir(), srcDB, dbData.Ddb, runProgFuncs, stopProgFuncs)
	if err != nil {
		return noConflicts, err
	}

	return noConflicts, nil
}
