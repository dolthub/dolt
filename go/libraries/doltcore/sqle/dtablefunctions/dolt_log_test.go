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

package dtablefunctions

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDoltLogBindVariables(t *testing.T) {
	ctx := sql.NewEmptyContext()
	
	// Test that bind variables are properly handled in evalArguments
	ltf := &LogTableFunction{ctx: ctx}
	
	// This should not fail during prepare phase
	node, err := ltf.evalArguments(expression.NewBindVar("v1"))
	assert.NoError(t, err)
	assert.NotNil(t, node)
	
	// Test mixed bind variables and literals
	node, err = ltf.evalArguments(
		expression.NewBindVar("v1"),
		expression.NewLiteral("--parents", types.Text),
	)
	assert.NoError(t, err)
	assert.NotNil(t, node)
}

func TestDoltLogExpressionsInterface(t *testing.T) {
	ctx := sql.NewEmptyContext()
	ltf := &LogTableFunction{
		ctx: ctx,
		revisionExprs: []sql.Expression{
			expression.NewBindVar("v1"),
			expression.NewLiteral("HEAD", types.Text),
		},
		notRevisionExprs: []sql.Expression{
			expression.NewBindVar("v2"),
		},
	}

	// Test that Expressions method returns all expressions for bind variable replacement
	exprs := ltf.Expressions()
	assert.Len(t, exprs, 3)
	
	// Test that WithExpressions method correctly reconstructs the function
	newExprs := []sql.Expression{
		expression.NewLiteral("main", types.Text),
		expression.NewLiteral("HEAD", types.Text),
		expression.NewLiteral("HEAD~1", types.Text),
	}
	
	newNode, err := ltf.WithExpressions(newExprs...)
	require.NoError(t, err)
	
	newLtf, ok := newNode.(*LogTableFunction)
	require.True(t, ok)
	assert.Len(t, newLtf.revisionExprs, 2)
	assert.Len(t, newLtf.notRevisionExprs, 1)
}

func TestDoltLogValidateRevisionExpressions(t *testing.T) {
	ctx := sql.NewEmptyContext()
	
	// Test that validation is skipped when bind variables are present
	ltf := &LogTableFunction{
		ctx: ctx,
		revisionExprs: []sql.Expression{
			expression.NewBindVar("v1"),
		},
	}
	
	err := ltf.validateRevisionExpressions()
	assert.NoError(t, err) // Should skip validation with bind variables
}