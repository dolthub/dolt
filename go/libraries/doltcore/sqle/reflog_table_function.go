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

package sqle

import (
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"strings"
)

type ReflogTableFunction struct {
	ctx      *sql.Context
	database sql.Database
	refExpr  sql.Expression
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

	target, err := rltf.refExpr.Eval(ctx, row)
	if err != nil {
		return nil, fmt.Errorf("error evaluating expression (%s): %s",
			rltf.refExpr.String(), err.Error())
	}

	refName, ok := target.(string)
	if !ok {
		return nil, fmt.Errorf("argument (%v) is not a string value, but a %T", target, target)
	}

	ddb := sqlDb.DbData().Ddb
	journal := ddb.ChunkJournal()
	if journal == nil {
		return sql.RowsToRowIter(), nil
	}

	previousCommit := ""
	rows := make([]sql.Row, 0)
	// journal.Roots are stored in chronological order, so we need to iterate over them backwards
	// so that we process the most recent root updates first.
	for i := len(journal.Roots) - 1; i >= 0; i-- {
		root := journal.Roots[i]
		timestamp := journal.RootTimestamps[i]

		hashof := hash.Parse(root)
		datasets, err := ddb.DatasetsByRootHash(ctx, hashof)
		if err != nil {
			return nil, fmt.Errorf("unable to look up references for root hash %s: %s",
				hashof.String(), err.Error())
		}

		err = datasets.IterAll(ctx, func(id string, addr hash.Hash) error {
			// If the caller has supplied a branch or tag name, without the fully qualified ref path,
			// take the first match and use that as the canonical
			if strings.HasSuffix(strings.ToLower(id), "/"+strings.ToLower(refName)) {
				refName = id
			}

			// Skip refs that don't match the target we're looking for
			if strings.ToLower(id) != strings.ToLower(refName) {
				return nil
			}

			// Skip ref entries where the commit didn't change from the previous ref entry
			if addr.String() == previousCommit {
				return nil
			}

			doltRef, err := ref.Parse(id)
			if err != nil {
				return err
			}
			commit, err := ddb.ResolveCommitRefAtRoot(ctx, doltRef, hashof)
			if err != nil {
				return err
			}
			commitMeta, err := commit.GetCommitMeta(ctx)
			if err != nil {
				return err
			}

			rows = append(rows, sql.Row{
				id,                     // ref
				timestamp,              // ref_timestamp
				addr.String(),          // commit_hash
				commitMeta.Description, // commit_message
			})
			previousCommit = addr.String()
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("unable to process references for root hash %s: %s",
				hashof.String(), err.Error())
		}
	}

	return sql.RowsToRowIter(rows...), nil
}

func (rltf *ReflogTableFunction) Schema() sql.Schema {
	return reflogTableSchema
}

func (rltf *ReflogTableFunction) Resolved() bool {
	return rltf.refExpr.Resolved()
}

func (rltf *ReflogTableFunction) String() string {
	return fmt.Sprintf("DOLT_REFLOG(%s)", rltf.refExpr.String())
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

func (rltf *ReflogTableFunction) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	// Currently, we only support viewing the reflog for the HEAD ref of the current session,
	// so no privileges need to be checked.
	return true
}

func (rltf *ReflogTableFunction) IsReadOnly() bool {
	return true
}

func (rltf *ReflogTableFunction) Expressions() []sql.Expression {
	return []sql.Expression{rltf.refExpr}
}

func (rltf *ReflogTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	if len(expression) != 1 {
		return nil, sql.ErrInvalidArgumentNumber.New(rltf.Name(), "1", len(expression))
	}

	new := *rltf
	new.refExpr = expression[0]
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
