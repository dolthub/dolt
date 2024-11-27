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

package dtablefunctions

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
)

type ReflogTableFunction struct {
	ctx            *sql.Context
	database       sql.Database
	refAndArgExprs []sql.Expression
}

var _ sql.TableFunction = (*ReflogTableFunction)(nil)
var _ sql.ExecSourceRel = (*ReflogTableFunction)(nil)

var reflogTableSchema = sql.Schema{
	&sql.Column{Name: "ref", Type: types.LongText},
	&sql.Column{Name: "ref_timestamp", Type: types.Timestamp, Nullable: true},
	&sql.Column{Name: "commit_hash", Type: types.LongText},
	&sql.Column{Name: "commit_message", Type: types.LongText},
}

func (rltf *ReflogTableFunction) NewInstance(ctx *sql.Context, database sql.Database, expressions []sql.Expression) (sql.Node, error) {
	newInstance := &ReflogTableFunction{
		ctx:      ctx,
		database: database,
	}

	node, err := newInstance.WithExpressions(expressions...)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (rltf *ReflogTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	sqlDb, ok := rltf.database.(dsess.SqlDatabase)
	if !ok {
		return nil, fmt.Errorf("unexpected database type: %T", rltf.database)
	}

	var refName string
	showAll := false
	for _, expr := range rltf.refAndArgExprs {
		target, err := expr.Eval(ctx, row)
		if err != nil {
			return nil, fmt.Errorf("error evaluating expression (%s): %s",
				expr.String(), err.Error())
		}
		targetStr, ok := target.(string)
		if !ok {
			return nil, fmt.Errorf("argument (%v) is not a string value, but a %T", target, target)
		}

		if targetStr == "--all" {
			if showAll {
				return nil, fmt.Errorf("error: multiple values provided for `all`")
			}
			showAll = true
		} else {
			if refName != "" {
				return nil, fmt.Errorf("error: %s has too many positional arguments. Expected at most %d, found %d: %s",
					rltf.Name(), 1, 2, rltf.refAndArgExprs)
			}
			refName = targetStr
		}
	}

	ddb := sqlDb.DbData().Ddb
	journal := ddb.ChunkJournal()
	if journal == nil {
		return sql.RowsToRowIter(), nil
	}

	previousCommitsByRef := make(map[string]string)
	rows := make([]sql.Row, 0)
	err := journal.IterateRoots(func(root string, timestamp *time.Time) error {
		hashof := hash.Parse(root)
		datasets, err := ddb.DatasetsByRootHash(ctx, hashof)
		if err != nil {
			return fmt.Errorf("unable to look up references for root hash %s: %s",
				hashof.String(), err.Error())
		}

		return datasets.IterAll(ctx, func(id string, addr hash.Hash) error {
			// Skip working set references (WorkingSetRefs can't always be resolved to commits)
			if ref.IsWorkingSet(id) {
				return nil
			}

			doltRef, err := ref.Parse(id)
			if err != nil {
				return err
			}

			// Skip any internal refs
			if doltRef.GetType() == ref.InternalRefType {
				return nil
			}
			// skip workspace refs by default
			if doltRef.GetType() == ref.WorkspaceRefType {
				if !showAll {
					return nil
				}
			}

			// If a ref expression to filter on was specified, see if we match the current ref
			if refName != "" {
				// If the caller has supplied a branch or tag name, without the fully qualified ref path,
				// take the first match and use that as the canonical ref to filter on
				if strings.HasSuffix(strings.ToLower(id), "/"+strings.ToLower(refName)) {
					refName = id
				}

				// Skip refs that don't match the target we're looking for
				if !strings.EqualFold(id, refName) {
					return nil
				}
			}

			// Skip ref entries where the commit didn't change from the previous ref entry
			if prev, ok := previousCommitsByRef[id]; ok && prev == addr.String() {
				return nil
			}

			commit, err := ddb.ResolveCommitRefAtRoot(ctx, doltRef, hashof)
			if err != nil {
				return err
			}
			commitMeta, err := commit.GetCommitMeta(ctx)
			if err != nil {
				return err
			}

			// TODO: We should be able to pass in a nil *time.Time, but it
			// currently triggers a problem in GMS' Time conversion logic.
			// Passing a nil any value works correctly though.
			var ts any = nil
			if timestamp != nil {
				ts = *timestamp
			}

			rows = append(rows, sql.UntypedSqlRow{
				id,                     // ref
				ts,                     // ref_timestamp
				addr.String(),          // commit_hash
				commitMeta.Description, // commit_message
			})
			previousCommitsByRef[id] = addr.String()
			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	// Reverse the results so that we return the most recent reflog entries first
	slices.Reverse(rows)

	return sql.RowsToRowIter(rows...), nil
}

func (rltf *ReflogTableFunction) Schema() sql.Schema {
	return reflogTableSchema
}

func (rltf *ReflogTableFunction) Resolved() bool {
	for _, expr := range rltf.refAndArgExprs {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

func (rltf *ReflogTableFunction) String() string {
	var args []string

	for _, expr := range rltf.refAndArgExprs {
		args = append(args, expr.String())
	}
	return fmt.Sprintf("DOLT_REFLOG(%s)", strings.Join(args, ", "))
}

func (rltf *ReflogTableFunction) Children() []sql.Node {
	return nil
}

func (rltf *ReflogTableFunction) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return rltf, nil
}

func (rltf *ReflogTableFunction) IsReadOnly() bool {
	return true
}

func (rltf *ReflogTableFunction) Expressions() []sql.Expression {
	return rltf.refAndArgExprs
}

func (rltf *ReflogTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	if len(expression) > 2 {
		return nil, sql.ErrInvalidArgumentNumber.New(rltf.Name(), "0 to 2", len(expression))
	}

	new := *rltf
	new.refAndArgExprs = expression

	return &new, nil
}

func (rltf *ReflogTableFunction) Name() string {
	return "dolt_reflog"
}

// Database implements the sql.Databaser interface
func (rltf *ReflogTableFunction) Database() sql.Database {
	return rltf.database
}

// WithDatabase implements the sql.Databaser interface
func (rltf *ReflogTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	new := *rltf
	new.database = database
	return &new, nil
}
