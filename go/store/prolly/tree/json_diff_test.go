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

package tree

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/json"

	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func largeJsonDiffTests(t *testing.T) []JsonDiffTest {
	ctx := sql.NewEmptyContext()
	ns := NewTestNodeStore()

	emptyDocument := types.JSONDocument{Val: types.JsonObject{}}

	insert := func(document types.MutableJSON, path string, val interface{}) types.MutableJSON {
		jsonVal, inRange, err := types.JSON.Convert(ctx, val)
		require.NoError(t, err)
		require.True(t, (bool)(inRange))
		document = document.Clone(ctx).(types.MutableJSON)
		newDoc, changed, err := document.Insert(ctx, path, jsonVal.(sql.JSONWrapper))
		require.NoError(t, err)
		require.True(t, changed)
		return newDoc
	}

	set := func(document types.MutableJSON, path string, val interface{}) types.MutableJSON {
		jsonVal, inRange, err := types.JSON.Convert(ctx, val)
		require.NoError(t, err)
		require.True(t, (bool)(inRange))
		newDoc, changed, err := document.Replace(ctx, path, jsonVal.(sql.JSONWrapper))
		require.NoError(t, err)
		require.True(t, changed)
		return newDoc
	}

	lookup := func(document types.SearchableJSON, path string) sql.JSONWrapper {
		newDoc, err := document.Lookup(ctx, path)
		require.NoError(t, err)
		return newDoc
	}

	remove := func(document types.MutableJSON, path string) types.MutableJSON {
		newDoc, changed, err := document.Remove(ctx, path)
		require.True(t, changed)
		require.NoError(t, err)
		return newDoc
	}

	largeObject := createLargeArraylessDocumentForTesting(t, ctx, ns)
	return []JsonDiffTest{
		{
			Name: "nested insert",
			From: largeObject,
			To:   insert(largeObject, "$.level7.newKey", 2),
			ExpectedDiffs: []JsonDiff{
				{
					Key:  makeJsonPathKey(`level7`, `newKey`),
					From: nil,
					To:   &types.JSONDocument{Val: 2},
					Type: AddedDiff,
				},
			},
		},
		{
			Name: "nested remove",
			From: largeObject,
			To:   remove(largeObject, "$.level7.level6"),
			ExpectedDiffs: []JsonDiff{
				{
					Key:  makeJsonPathKey(`level7`, `level6`),
					From: lookup(largeObject, "$.level7.level6"),
					To:   nil,
					Type: RemovedDiff,
				},
			},
		},
		{
			Name: "nested modification 1",
			From: largeObject,
			To:   set(largeObject, "$.level7.level5", 2),
			ExpectedDiffs: []JsonDiff{
				{
					Key:  makeJsonPathKey(`level7`, `level5`),
					From: lookup(largeObject, "$.level7.level5"),
					To:   &types.JSONDocument{Val: 2},
					Type: ModifiedDiff,
				},
			},
		},
		{
			Name: "nested modification 2",
			From: largeObject,
			To:   set(largeObject, "$.level7.level4", 1),
			ExpectedDiffs: []JsonDiff{
				{
					Key:  makeJsonPathKey(`level7`, `level4`),
					From: lookup(largeObject, "$.level7.level4"),
					To:   &types.JSONDocument{Val: 1},
					Type: ModifiedDiff,
				},
			},
		},
		{
			Name: "convert object To array",
			From: largeObject,
			To:   set(largeObject, "$.level7.level6", []interface{}{}),
			ExpectedDiffs: []JsonDiff{
				{
					Key:  makeJsonPathKey(`level7`, `level6`),
					From: lookup(largeObject, "$.level7.level6"),
					To:   &types.JSONDocument{Val: []interface{}{}},
					Type: ModifiedDiff,
				},
			},
		},
		{
			Name: "convert array To object",
			From: set(largeObject, "$.level7.level6", []interface{}{}),
			To:   largeObject,
			ExpectedDiffs: []JsonDiff{
				{
					Key:  makeJsonPathKey(`level7`, `level6`),
					From: &types.JSONDocument{Val: []interface{}{}},
					To:   lookup(largeObject, "$.level7.level6"),
					Type: ModifiedDiff,
				},
			},
		},
		{
			// This is a regression test.
			// If:
			// - One document fits in a single chunk and the other doesn't
			// - The location of the chunk boundary in the larger document is also present in the smaller document
			// - The chunk boundary doesn't fall at the beginning of value.
			// Then the differ would fail To advance the prolly tree cursor and would incorrectly see the larger document as corrupt.
			// The values in this test case are specifically chosen To meet these conditions.
			Name: "no error when diffing large doc with small doc",
			From: largeObject,
			To:   insert(emptyDocument, "$.level6", insert(emptyDocument, "$.level4", lookup(largeObject, "$.level6.level4"))),
		},
		{
			// This is a regression test.
			//
			// If:
			// - A chunk begins with an object "A"
			// - If a value "A.b" within this object was modified
			// - The previous chunk was also modified
			// Then the differ would incorrectly report that the entire "A" object had been modified, instead of the sub-value "A.b"
			// The values in this test case are specifically chosen To meet these conditions,
			// as there is a chunk boundary immediately before "$.level5.level3.level1"
			Name: "correctly diff object that begins on chunk boundary",
			From: largeObject,
			To:   set(set(largeObject, "$.level5.level2.number", 2), "$.level5.level3.level1.number", 2),
			ExpectedDiffs: []JsonDiff{
				{
					Key:  makeJsonPathKey(`level5`, `level2`, `number`),
					From: types.JSONDocument{Val: 1},
					To:   types.JSONDocument{Val: 2},
					Type: ModifiedDiff,
				},
				{
					Key:  makeJsonPathKey(`level5`, `level3`, `level1`, `number`),
					From: types.JSONDocument{Val: 1},
					To:   types.JSONDocument{Val: 2},
					Type: ModifiedDiff,
				},
			},
		},
	}
}

// createLargeArraylessDocumentForTesting creates a JSON document large enough to be split across multiple chunks that
// does not contain arrays. This makes it easier to write tests for three-way merging, since we cant't currently merge
// concurrent changes to arrays.
func createLargeArraylessDocumentForTesting(t *testing.T, ctx *sql.Context, ns NodeStore) IndexedJsonDocument {
	leafDoc := make(map[string]interface{})
	leafDoc["number"] = float64(1.0)
	leafDoc["string"] = "dolt"
	var docExpression sql.Expression = expression.NewLiteral(newIndexedJsonDocumentFromValue(t, ctx, ns, leafDoc), types.JSON)
	var err error

	for level := 0; level < 8; level++ {
		docExpression, err = json.NewJSONInsert(docExpression, expression.NewLiteral(fmt.Sprintf("$.level%d", level), types.Text), docExpression)
		require.NoError(t, err)
	}
	doc, err := docExpression.Eval(ctx, nil)
	require.NoError(t, err)
	return newIndexedJsonDocumentFromValue(t, ctx, ns, doc)
}

func TestJsonDiff(t *testing.T) {
	t.Run("simple tests", func(t *testing.T) {
		runTestBatch(t, SimpleJsonDiffTests)
	})
	t.Run("large document tests", func(t *testing.T) {
		runTestBatch(t, largeJsonDiffTests(t))
	})
}

func runTestBatch(t *testing.T, tests []JsonDiffTest) {
	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			runTest(t, test)
		})
	}
}

func runTest(t *testing.T, test JsonDiffTest) {
	ctx := context.Background()
	ns := NewTestNodeStore()
	from := newIndexedJsonDocumentFromValue(t, ctx, ns, test.From)
	to := newIndexedJsonDocumentFromValue(t, ctx, ns, test.To)
	differ, err := NewIndexedJsonDiffer(ctx, from, to)
	require.NoError(t, err)
	var actualDiffs []JsonDiff
	for {
		diff, err := differ.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		actualDiffs = append(actualDiffs, diff)
	}

	diffsEqual := func(expected, actual JsonDiff) bool {
		if expected.Type != actual.Type {
			return false
		}
		if !bytes.Equal(expected.Key, actual.Key) {
			return false
		}
		cmp, err := types.CompareJSON(ctx, expected.From, actual.From)
		require.NoError(t, err)
		if cmp != 0 {
			return false
		}
		cmp, err = types.CompareJSON(ctx, expected.To, actual.To)
		require.NoError(t, err)

		return cmp == 0
	}
	if test.ExpectedDiffs != nil {

		if !assert.Equal(t, len(test.ExpectedDiffs), len(actualDiffs)) {
			require.Fail(t, "Diffs don't match", "Expected: %v\nActual: %v", test.ExpectedDiffs, actualDiffs)
		}
		for i, expected := range test.ExpectedDiffs {
			actual := actualDiffs[i]
			require.True(t, diffsEqual(expected, actual), fmt.Sprintf("Expected: %v\nActual: %v", expected, actual))
		}
	}
}
