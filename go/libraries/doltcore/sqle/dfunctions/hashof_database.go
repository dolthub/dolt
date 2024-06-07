// Copyright 2024 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/hash"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const HashOfDatabaseFuncName = "dolt_hashof_db"

type HashOfDatabase struct {
	children []sql.Expression
}

// NewHashOfDatabase creates a new HashOfDatabase expression.
func NewHashOfDatabase(args ...sql.Expression) (sql.Expression, error) {
	if len(args) > 1 {
		return nil, sql.ErrInvalidArgumentNumber.New(HashOfDatabaseFuncName, "0 or 1", len(args))
	}

	return &HashOfDatabase{
		children: args,
	}, nil
}

// Children implements the Expression interface.
func (t *HashOfDatabase) Children() []sql.Expression {
	return t.children
}

// Eval implements the Expression interface.
func (t *HashOfDatabase) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	var args []string
	for _, child := range t.children {
		args = append(args, child.String())
	}

	dbName := ctx.GetCurrentDatabase()
	ds := dsess.DSessFromSess(ctx.Session)

	roots, ok := ds.GetRoots(ctx, dbName)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	refStr := "WORKING"
	if len(args) != 0 {
		refStr = strings.TrimSpace(args[0])

		// Remove quotes if they are present at the beginning and end of the string
		for _, quote := range []rune{'\'', '"', '`'} {
			if rune(refStr[0]) == quote && rune(refStr[len(refStr)-1]) == quote {
				refStr = refStr[1 : len(refStr)-1]
				break
			}
		}
	}

	var h hash.Hash
	var err error
	switch {
	case len(refStr) == 0 || strings.EqualFold(refStr, doltdb.Working):
		h, err = roots.Working.HashOf()

	case strings.EqualFold(refStr, doltdb.Staged):
		h, err = roots.Staged.HashOf()

	case strings.EqualFold(refStr, "HEAD"):
		h, err = roots.Head.HashOf()

	default:
		dbData, ok := ds.GetDbData(ctx, dbName)
		if !ok {
			return nil, sql.ErrDatabaseNotFound.New(dbName)
		}

		ddb := dbData.Ddb
		r, err := ddb.GetRefByNameInsensitive(ctx, refStr)
		if err != nil {
			return nil, fmt.Errorf("error getting ref '%s' from database '%s': %w", refStr, dbName, err)
		}

		cm, err := ddb.ResolveCommitRef(ctx, r)
		if err != nil {
			return nil, fmt.Errorf("error resolving ref '%s' from database '%s': %w", refStr, dbName, err)
		}

		root, err := cm.GetRootValue(ctx)
		if err != nil {
			return nil, fmt.Errorf("error getting root value for commit '%s' in database '%s': %w", refStr, dbName, err)
		}

		h, err = root.HashOf()
	}

	if err != nil {
		return nil, fmt.Errorf("error getting dolt_hashof_db('%s'): %w", refStr, err)
	}

	return h.String(), nil
}

// String implements the Stringer interface.
func (t *HashOfDatabase) String() string {
	args := make([]string, 0, len(t.children))
	for _, child := range t.children {
		args = append(args, child.String())
	}

	argStr := strings.Join(args, ", ")
	return fmt.Sprintf("%s(%s)", HashOfDatabaseFuncName, argStr)
}

// Description implements the FunctionExpression interface
func (t *HashOfDatabase) Description() string {
	return "returns a hash of the contents of the current branch and database, typically used for detecting if a database has changed"
}

// IsNullable implements the Expression interface.
func (t *HashOfDatabase) IsNullable() bool {
	return false
}

// Resolved implements the Expression interface.
func (*HashOfDatabase) Resolved() bool {
	return true
}

// WithChildren implements the Expression interface.
func (t *HashOfDatabase) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewHashOfDatabase(children...)
}

// Type implements the Expression interface.
func (t *HashOfDatabase) Type() sql.Type {
	return types.Text
}
