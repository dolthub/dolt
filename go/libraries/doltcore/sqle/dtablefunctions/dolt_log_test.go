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

	// Test the exact customer issue case: dolt_log(?, "--not", ?) #9508
	node, err = ltf.evalArguments(
		expression.NewBindVar("v1"),
		expression.NewLiteral("--not", types.Text),
		expression.NewBindVar("v2"),
	)
	assert.NoError(t, err)
	assert.NotNil(t, node)
}

func TestDoltLogExpressionsInterface(t *testing.T) {
	ctx := sql.NewEmptyContext()
	ltf := &LogTableFunction{
		ctx: ctx,
		argumentExprs: []sql.Expression{
			expression.NewBindVar("v1"),
			expression.NewLiteral("HEAD", types.Text),
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
	assert.Len(t, newLtf.argumentExprs, 3)
	assert.Equal(t, "'main'", newLtf.argumentExprs[0].String())
	assert.Equal(t, "'HEAD'", newLtf.argumentExprs[1].String())
	assert.Equal(t, "'HEAD~1'", newLtf.argumentExprs[2].String())
}

func TestDoltLogValidateRevisionStrings(t *testing.T) {
	ctx := sql.NewEmptyContext()

	// Test that validation works with parsed strings
	ltf := &LogTableFunction{
		ctx:          ctx,
		revisionStrs: []string{"HEAD"},
	}

	// Should validate normally
	err := ltf.validateRevisionStrings()
	assert.NoError(t, err)

	// Test range syntax conflict detection
	ltf.revisionStrs = []string{"HEAD..main", "other"}
	err = ltf.validateRevisionStrings()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "revision cannot contain '..' or '...' if multiple revisions exist")

	// Test --not revision validation
	ltf.revisionStrs = []string{"HEAD"}
	ltf.notRevisionStrs = []string{"main..other"}
	err = ltf.validateRevisionStrings()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "--not revision cannot contain '..'")
}

func TestDoltLogTypeValidation(t *testing.T) {
	ctx := sql.NewEmptyContext()

	// Test that type validation still works in addOptions via getDoltArgs
	// No type check in validateRevisionStrings because getDoltArgs already validates types
	ltf := &LogTableFunction{
		ctx: ctx,
	}

	// Test with non-text expression (integer)
	err := ltf.addOptions([]sql.Expression{
		expression.NewLiteral(123, types.Int32),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid argument to dolt_log: 123")

	// Test with text expression (should work)
	err = ltf.addOptions([]sql.Expression{
		expression.NewLiteral("HEAD", types.Text),
	})
	assert.NoError(t, err)
}

func TestDoltLogBindVariableWithParents(t *testing.T) {
	ctx := sql.NewEmptyContext()

	// Test bind variable with --parents flag to ensure schema is properly determined
	// during execution phase when bind variables are resolved
	ltf := &LogTableFunction{ctx: ctx}

	// Test case: dolt_log(?, "--parents") - bind variable with parents flag
	bindVarExprs := []sql.Expression{
		expression.NewBindVar("v1"),
		expression.NewLiteral("--parents", types.Text),
	}

	// During analysis phase, this should defer parsing due to bind variable
	node, err := ltf.evalArguments(bindVarExprs...)
	assert.NoError(t, err)
	assert.NotNil(t, node)

	newLtf, ok := node.(*LogTableFunction)
	assert.True(t, ok)

	// Should have stored the original expressions for deferred parsing
	assert.Equal(t, 2, len(newLtf.argumentExprs))
	assert.True(t, expression.IsBindVar(newLtf.argumentExprs[0]))
	assert.Equal(t, "'--parents'", newLtf.argumentExprs[1].String())

	// Should not have parsed the arguments yet (no revision strings)
	assert.Empty(t, newLtf.revisionStrs)
	assert.Empty(t, newLtf.notRevisionStrs)

	// showParents should be true during analysis when --parents is a literal flag
	assert.True(t, newLtf.showParents)

	// Schema should include parents column during analysis when --parents is literal
	schema := newLtf.Schema()
	parentColumn := false
	for _, col := range schema {
		if col.Name == "parents" {
			parentColumn = true
			break
		}
	}
	assert.True(t, parentColumn, "parents column should be in schema during analysis phase when --parents is literal")

	// Now test execution phase - simulate what happens in RowIter
	// Replace bind variable with actual value
	executionExprs := []sql.Expression{
		expression.NewLiteral("HEAD", types.Text),
		expression.NewLiteral("--parents", types.Text),
	}

	// This simulates what happens in RowIter when bind variables are resolved
	err = newLtf.addOptions(executionExprs)
	assert.NoError(t, err)

	// After execution parsing, showParents should still be true
	assert.True(t, newLtf.showParents)

	// Schema should still include parents column (unchanged from analysis)
	schemaAfterExecution := newLtf.Schema()
	parentColumnAfterExecution := false
	for _, col := range schemaAfterExecution {
		if col.Name == "parents" {
			parentColumnAfterExecution = true
			break
		}
	}
	assert.True(t, parentColumnAfterExecution, "parents column should remain in schema after execution parsing")
}
