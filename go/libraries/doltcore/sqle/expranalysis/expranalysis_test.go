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

package expranalysis

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/variables"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/overrides"
)

// A context without a Dolt session parses with the caller's variables.
func TestResolveDefaultExpressionUsesCallerVariables(t *testing.T) {
	if overrides.EngineOverridesFromContext == nil {
		overrides.EngineOverridesFromContext = func(context.Context) sql.EngineOverrides { return sql.EngineOverrides{} }
		defer func() { overrides.EngineOverridesFromContext = nil }()
	}
	pk, err := schema.NewColumnWithTypeInfo("pk", 1, typeinfo.Int64Type, true, "", false, "")
	require.NoError(t, err)
	c, err := schema.NewColumnWithTypeInfo("c", 2, typeinfo.Int64Type, false, "(@@global.max_connections)", false, "")
	require.NoError(t, err)
	sch := schema.MustSchemaFromCols(schema.NewColCollection(pk, c))

	ctx := sql.NewContextWithVariables(context.Background(), variables.NewEngineVariables())
	expr, err := ResolveDefaultExpression(ctx, "t", sch, c)
	require.NoError(t, err)
	val, err := expr.Eval(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, val)
}
