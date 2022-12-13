// Copyright 2020 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const HashOfFuncName = "hashof"

type HashOf struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*HashOf)(nil)

// NewHashOf creates a new HashOf expression.
func NewHashOf(e sql.Expression) sql.Expression {
	return &HashOf{expression.UnaryExpression{Child: e}}
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
	if strings.ToUpper(name) == "HEAD" {
		sess := dsess.DSessFromSess(ctx.Session)

		cm, err = sess.GetHeadCommit(ctx, dbName)
		if err != nil {
			return nil, err
		}
	} else {
		ref, err := ddb.GetRefByNameInsensitive(ctx, name)
		if err != nil {
			return nil, err
		}

		cm, err = ddb.ResolveCommitRef(ctx, ref)
		if err != nil {
			return nil, err
		}
	}

	cm, err = cm.GetAncestor(ctx, as)
	if err != nil {
		return nil, err
	}

	h, err := cm.HashOf()
	if err != nil {
		return nil, err
	}

	return h.String(), nil
}

// String implements the Stringer interface.
func (t *HashOf) String() string {
	return fmt.Sprintf("HASHOF(%s)", t.Child.String())
}

// FunctionName implements the FunctionExpression interface
func (t *HashOf) FunctionName() string {
	return HashOfFuncName
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
	return NewHashOf(children[0]), nil
}

// Type implements the Expression interface.
func (t *HashOf) Type() sql.Type {
	return sql.Text
}
