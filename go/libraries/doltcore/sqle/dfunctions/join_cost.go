// Copyright 2025 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/types"
	"strings"
)

const JoinCostFuncName = "dolt_join_cost"

type JoinCost struct {
	q sql.Expression
}

var _ sql.FunctionExpression = (*JoinCost)(nil)
var _ sql.CollationCoercible = (*JoinCost)(nil)

// NewJoinCost returns a new JoinCost expression.
func NewJoinCost(e sql.Expression) sql.Expression {
	return &JoinCost{q: e}
}

// FunctionName implements sql.FunctionExpression
func (c *JoinCost) FunctionName() string {
	return "JoinCost"
}

// Description implements sql.FunctionExpression
func (c *JoinCost) Description() string {
	return "print the memo tree"
}

// Type implements the Expression interface.
func (c *JoinCost) Type() sql.Type { return types.LongText }

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*JoinCost) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return ctx.GetCollation(), 4
}

// IsNullable implements the Expression interface.
func (c *JoinCost) IsNullable() bool {
	return false
}

func (c *JoinCost) String() string {
	return fmt.Sprintf("%s(%s)", c.FunctionName(), c.q)
}

// Eval implements the Expression interface.
func (c *JoinCost) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	q, err := exprToStringLit(ctx, c.q)
	if err != nil {
		return "", err
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.Provider()
	eng := gms.NewDefault(pro)

	binder := planbuilder.New(ctx, eng.Analyzer.Catalog, eng.EventScheduler, eng.Parser)
	parsed, _, _, qFlags, err := binder.Parse(q, nil, false)
	if err != nil {
		return nil, err
	}
	scope := plan.Scope{}
	_, err = eng.Analyzer.Analyze(ctx, parsed, &scope, qFlags)
	if err != nil {
		ctx.GetLogger().Debug("join cost error", err)
	}

	ret := strings.Builder{}
	sep := ""
	for _, t := range scope.JoinTrees {
		ret.WriteString(sep)
		ret.WriteString(t)
		sep = "\n"
	}
	return ret.String(), nil
}

// Resolved implements the Expression interface.
func (c *JoinCost) Resolved() bool {
	return true
}

// Children implements the Expression interface.
func (c *JoinCost) Children() []sql.Expression {
	return nil
}

// WithChildren implements the Expression interface.
func (c *JoinCost) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 3)
	}
	return c, nil
}

func exprToStringLit(ctx *sql.Context, e sql.Expression) (string, error) {
	q, err := e.Eval(ctx, nil)
	if err != nil {
		return "", err
	}
	qStr, isStr := q.(string)
	if !isStr {
		return "", fmt.Errorf("query must be a string, not %T", q)
	}
	return strings.TrimSpace(qStr), nil
}
