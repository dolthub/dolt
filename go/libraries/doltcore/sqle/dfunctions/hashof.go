// Copyright 2020 Liquidata, Inc.
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
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

type HashOf struct {
	expression.UnaryExpression
	ddb *doltdb.DoltDB
}

func NewHashOfForDDB(ddb *doltdb.DoltDB) func(e sql.Expression) sql.Expression {
	return func(e sql.Expression) sql.Expression {
		return NewHashOf(e, ddb)
	}
}

// NewHashOf creates a new HashOf expression.
func NewHashOf(e sql.Expression, ddb *doltdb.DoltDB) sql.Expression {
	return &HashOf{expression.UnaryExpression{Child: e}, ddb}
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

	branchName, ok := val.(string)

	if !ok {
		return nil, errors.New("branch name is not a string")
	}

	branchName, err = getBranchIncensitive(ctx, branchName, t.ddb)

	if err != nil {
		return nil, err
	}

	cs, err := doltdb.NewCommitSpec("HEAD", branchName)

	if err != nil {
		return nil, err
	}

	cm, err := t.ddb.Resolve(ctx, cs)

	if err != nil {
		return nil, err
	}

	h, err := cm.HashOf()

	if err != nil {
		return "", err
	}

	return h.String(), nil
}

func getBranchIncensitive(ctx context.Context, branchName string, ddb *doltdb.DoltDB) (string, error) {
	branchRefs, err := ddb.GetBranches(ctx)

	if err != nil {
		return "", err
	}

	lowerNameToExact := make(map[string]string)
	for _, branchRef := range branchRefs {
		currName := branchRef.GetPath()
		if currName == branchName {
			return branchName, nil
		}

		lowerNameToExact[strings.ToLower(currName)] = currName
	}

	for lwr, exact := range lowerNameToExact {
		if lwr == strings.ToLower(branchName) {
			return exact, nil
		}
	}

	return "", doltdb.ErrBranchNotFound
}

// String implements the Stringer interface.
func (t *HashOf) String() string {
	return fmt.Sprintf("ABS(%s)", t.Child.String())
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
	return NewHashOf(children[0], t.ddb), nil
}

// Type implements the Expression interface.
func (t *HashOf) Type() sql.Type {
	return t.Child.Type()
}
