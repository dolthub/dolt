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
	"errors"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
)

const DeprecatedHashOfFuncName = "hashof"
const HashOfFuncName = "dolt_hashof"

type HashOf struct {
	expression.UnaryExpression
	name string
}

var _ sql.FunctionExpression = (*HashOf)(nil)

// NewHashOfFunc creates a constructor for a Hashof function which will properly initialize the name
func NewHashOfFunc(name string) sql.CreateFunc1Args {
	return func(e sql.Expression) sql.Expression {
		return newHashOf(e, name)
	}
}

// newHashOf creates a new HashOf expression.
func newHashOf(e sql.Expression, name string) sql.Expression {
	return &HashOf{expression.UnaryExpression{Child: e}, name}
}

// Eval implements the Expression interface.
func (t *HashOf) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := t.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	paramStr, ok := val.(string)

	if !ok {
		return nil, errors.New("branch name is not a string")
	}

	name, as, err := doltdb.SplitAncestorSpec(paramStr)
	if err != nil {
		return nil, err
	}

	dbName := ctx.GetCurrentDatabase()
	ddb, ok := dsess.DSessFromSess(ctx.Session).GetDoltDB(ctx, dbName)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	var cm *doltdb.Commit
	if strings.EqualFold(name, "HEAD") {
		sess := dsess.DSessFromSess(ctx.Session)

		// TODO: this should resolve the current DB through the analyzer so it can use the revision qualified name here
		cm, err = sess.GetHeadCommit(ctx, dbName)
		if err != nil {
			return nil, err
		}
	} else {
		ref, err := ddb.GetRefByNameInsensitive(ctx, name)
		if err != nil {
			hsh, parsed := hash.MaybeParse(name)
			if parsed {
				orgErr := err
				optCmt, err := ddb.ReadCommit(ctx, hsh)
				if err != nil {
					return nil, orgErr
				}
				cm, ok = optCmt.ToCommit()
				if !ok {
					return nil, doltdb.ErrGhostCommitEncountered
				}
			} else {
				return nil, err
			}
		} else {
			cm, err = ddb.ResolveCommitRef(ctx, ref)
			if err != nil {
				return nil, err
			}
		}
	}

	optCmt, err := cm.GetAncestor(ctx, as)
	if err != nil {
		return nil, err
	}
	cm, ok = optCmt.ToCommit()
	if !ok {
		return nil, doltdb.ErrGhostCommitEncountered
	}

	h, err := cm.HashOf()
	if err != nil {
		return nil, err
	}

	return h.String(), nil
}

// String implements the Stringer interface.
func (t *HashOf) String() string {
	return fmt.Sprintf("%s(%s)", t.name, t.Child.String())
}

// FunctionName implements the FunctionExpression interface
func (t *HashOf) FunctionName() string {
	return t.name
}

// Description implements the FunctionExpression interface
func (t *HashOf) Description() string {
	return "returns the commit hash of a branch or other commit spec"
}

// IsNullable implements the Expression interface.
func (t *HashOf) IsNullable() bool {
	return t.Child.IsNullable()
}

// WithChildren implements the Expression interface.
func (t *HashOf) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 1)
	}
	return newHashOf(children[0], t.name), nil
}

// Type implements the Expression interface.
func (t *HashOf) Type() sql.Type {
	return types.Text
}
