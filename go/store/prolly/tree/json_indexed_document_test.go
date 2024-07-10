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

package tree

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/json"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function/json/jsontests"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
)

// newIndexedJsonDocumentFromValue creates an IndexedJsonDocument from a provided value.
func newIndexedJsonDocumentFromValue(t *testing.T, ctx context.Context, ns NodeStore, v interface{}) IndexedJsonDocument {
	doc, _, err := types.JSON.Convert(v)
	require.NoError(t, err)
	root, err := SerializeJsonToAddr(ctx, ns, doc.(sql.JSONWrapper))
	require.NoError(t, err)
	return NewIndexedJsonDocument(ctx, root, ns)
}

// createLargeDocumentForTesting creates a JSON document large enough to be split across multiple chunks.
// This is useful for testing mutation operations in large documents.
// Every different possible jsonPathType appears on a chunk boundary, for better test coverage:
// chunk 0 key: $[6].children[2].children[0].number(endOfValue)
// chunk 2 key: $[7].children[5].children[4].children[2].children(arrayInitialElement)
// chunk 5 key: $[8].children[6].children[4].children[3].children[0](startOfValue)
// chunk 8 key: $[8].children[7].children[6].children[5].children[3].children[2].children[1](objectInitialElement)
func createLargeDocumentForTesting(t *testing.T, ctx *sql.Context, ns NodeStore) IndexedJsonDocument {
	leafDoc := make(map[string]interface{})
	leafDoc["number"] = float64(1.0)
	leafDoc["string"] = "dolt"
	docExpression, err := json.NewJSONArray(expression.NewLiteral(newIndexedJsonDocumentFromValue(t, ctx, ns, leafDoc), types.JSON))
	require.NoError(t, err)

	for level := 0; level < 8; level++ {
		childObjectExpression, err := json.NewJSONObject(expression.NewLiteral("children", types.Text), docExpression)
		require.NoError(t, err)
		docExpression, err = json.NewJSONArrayAppend(docExpression, expression.NewLiteral("$", types.Text), childObjectExpression)
		require.NoError(t, err)
	}
	doc, err := docExpression.Eval(ctx, nil)
	require.NoError(t, err)
	return newIndexedJsonDocumentFromValue(t, ctx, ns, doc)
}

var jsonPathTypeNames = []string{
	"startOfValue",
	"objectInitialElement",
	"arrayInitialElement",
	"endOfValue",
}

type chunkBoundary struct {
	chunkId  int
	path     string
	pathType jsonPathType
}

var largeDocumentChunkBoundaries = []chunkBoundary{
	{
		chunkId:  0,
		path:     "$[6].children[2].children[0].number",
		pathType: endOfValue,
	},
	{
		chunkId:  2,
		path:     "$[7].children[5].children[4].children[2].children",
		pathType: arrayInitialElement,
	},
	{
		chunkId:  5,
		path:     "$[8].children[6].children[4].children[3].children[0]",
		pathType: startOfValue,
	},
	{
		chunkId:  8,
		path:     "$[8].children[7].children[6].children[5].children[3].children[2].children[1]",
		pathType: objectInitialElement,
	},
}

func jsonLocationFromMySqlPath(t *testing.T, path string, pathType jsonPathType) jsonLocation {
	result, err := jsonPathElementsFromMySQLJsonPath([]byte(path))
	require.NoError(t, err)
	result.setScannerState(pathType)
	return result
}

// TestIndexedJsonDocument_ValidateChunks asserts that the values defined largeDocumentChunkBoundaries are accurate,
// so they can be used in other tests.
func TestIndexedJsonDocument_ValidateChunks(t *testing.T) {
	ctx := sql.NewEmptyContext()
	ns := NewTestNodeStore()
	largeDoc := createLargeDocumentForTesting(t, ctx, ns)
	for _, boundary := range largeDocumentChunkBoundaries {
		t.Run(fmt.Sprintf("validate %v at chunk %v", jsonPathTypeNames[boundary.pathType], boundary.chunkId), func(t *testing.T) {
			expectedKey := jsonLocationFromMySqlPath(t, boundary.path, boundary.pathType)
			actualKey := []byte(largeDoc.m.Root.GetKey(boundary.chunkId))
			require.Equal(t, expectedKey.key, actualKey)
		})
	}
}

func TestIndexedJsonDocument_Insert(t *testing.T) {
	ctx := sql.NewEmptyContext()
	ns := NewTestNodeStore()
	convertToIndexedJsonDocument := func(t *testing.T, s interface{}) interface{} {
		return newIndexedJsonDocumentFromValue(t, ctx, ns, s)
	}

	testCases := jsontests.JsonInsertTestCases(t, convertToIndexedJsonDocument)
	jsontests.RunJsonTests(t, testCases)

	t.Run("large document inserts", func(t *testing.T) {

		largeDoc := createLargeDocumentForTesting(t, ctx, ns)

		// Generate a value large enough that, if it's inserted, will guarantee a change in chunk boundaries.
		valueToInsert, err := largeDoc.Lookup(ctx, "$[6]")
		require.NoError(t, err)

		for _, chunkBoundary := range largeDocumentChunkBoundaries {
			t.Run(jsonPathTypeNames[chunkBoundary.pathType], func(t *testing.T) {
				// Compute a location right before the chunk boundary, and insert a large value into it.
				insertionPoint := chunkBoundary.path[:strings.LastIndex(chunkBoundary.path, ".")]
				insertionPoint = fmt.Sprint(insertionPoint, ".a")
				newDoc, changed, err := largeDoc.Insert(ctx, insertionPoint, valueToInsert)
				require.NoError(t, err)
				require.True(t, changed)

				// test that the chunk boundary was moved as a result of the insert.
				newBoundary := []byte(largeDoc.m.Root.GetKey(chunkBoundary.chunkId))
				previousBoundary := jsonLocationFromMySqlPath(t, chunkBoundary.path, chunkBoundary.pathType)
				require.NotEqual(t, newBoundary, previousBoundary)

				// test that new value is valid by converting it to interface{}
				v, err := newDoc.ToInterface()
				require.NoError(t, err)
				newJsonDocument := types.JSONDocument{Val: v}

				// test that the JSONDocument compares equal to the IndexedJSONDocument
				cmp, err := types.JSON.Compare(newDoc, newJsonDocument)
				require.NoError(t, err)
				require.Equal(t, cmp, 0)

				// extract the inserted value and confirm it's equal to the original inserted value.
				result, err := newJsonDocument.Lookup(ctx, insertionPoint)
				require.NoError(t, err)
				require.NotNil(t, result)

				cmp, err = types.JSON.Compare(valueToInsert, result)
				require.NoError(t, err)
				require.Equal(t, cmp, 0)
			})
		}
	})

}

func TestIndexedJsonDocument_Remove(t *testing.T) {
	ctx := sql.NewEmptyContext()
	ns := NewTestNodeStore()
	convertToIndexedJsonDocument := func(t *testing.T, s interface{}) interface{} {
		return newIndexedJsonDocumentFromValue(t, ctx, ns, s)
	}

	testCases := jsontests.JsonRemoveTestCases(t, convertToIndexedJsonDocument)
	jsontests.RunJsonTests(t, testCases)

	t.Run("large document removals", func(t *testing.T) {

		largeDoc := createLargeDocumentForTesting(t, ctx, ns)

		for _, chunkBoundary := range largeDocumentChunkBoundaries {
			t.Run(jsonPathTypeNames[chunkBoundary.pathType], func(t *testing.T) {
				// For each tested chunk boundary, get the path to the object containing that crosses that boundary, and remove it.
				removalPointString := chunkBoundary.path[:strings.LastIndex(chunkBoundary.path, ".")]

				removalPointLocation, err := jsonPathElementsFromMySQLJsonPath([]byte(removalPointString))
				require.NoError(t, err)

				// Test that the value exists prior to removal
				result, err := largeDoc.Lookup(ctx, removalPointString)
				require.NoError(t, err)
				require.NotNil(t, result)

				newDoc, changed, err := largeDoc.Remove(ctx, removalPointString)
				require.NoError(t, err)
				require.True(t, changed)

				// test that new value is valid by calling ToInterface
				v, err := newDoc.ToInterface()
				require.NoError(t, err)

				// If the removed value was an object key, check that the key no longer exists.
				// If the removed value was an array element, check that the parent array has shrunk.
				if removalPointLocation.getLastPathElement().isArrayIndex {
					// Check that the parent array has shrunk.
					arrayIndex := int(removalPointLocation.getLastPathElement().getArrayIndex())
					parentLocation := removalPointLocation.Clone()
					parentLocation.pop()

					getParentArray := func(doc IndexedJsonDocument) []interface{} {
						arrayWrapper, err := doc.lookupByLocation(ctx, parentLocation)
						require.NoError(t, err)
						arrayInterface, err := arrayWrapper.ToInterface()
						require.NoError(t, err)
						require.IsType(t, []interface{}{}, arrayInterface)
						return arrayInterface.([]interface{})
					}

					oldArray := getParentArray(largeDoc)
					newArray := getParentArray(newDoc.(IndexedJsonDocument))

					oldArray = slices.Delete(oldArray, arrayIndex, arrayIndex+1)

					require.Equal(t, oldArray, newArray)
				} else {
					// Check that the key no longer exists.
					newJsonDocument := types.JSONDocument{Val: v}
					result, err = newJsonDocument.Lookup(ctx, removalPointString)
					require.NoError(t, err)
					require.Nil(t, result)
				}
			})
		}
	})
}

func TestIndexedJsonDocument_Extract(t *testing.T) {
	ctx := context.Background()
	ns := NewTestNodeStore()
	convertToIndexedJsonDocument := func(t *testing.T, s interface{}) interface{} {
		return newIndexedJsonDocumentFromValue(t, ctx, ns, s)
	}

	testCases := jsontests.JsonExtractTestCases(t, convertToIndexedJsonDocument)
	jsontests.RunJsonTests(t, testCases)
}

func TestIndexedJsonDocument_Value(t *testing.T) {
	ctx := context.Background()
	ns := NewTestNodeStore()
	convertToIndexedJsonDocument := func(t *testing.T, s interface{}) interface{} {
		return newIndexedJsonDocumentFromValue(t, ctx, ns, s)
	}

	jsontests.RunJsonValueTests(t, convertToIndexedJsonDocument)
}

func TestIndexedJsonDocument_ContainsPath(t *testing.T) {
	ctx := context.Background()
	ns := NewTestNodeStore()
	convertToIndexedJsonDocument := func(t *testing.T, s interface{}) interface{} {
		return newIndexedJsonDocumentFromValue(t, ctx, ns, s)
	}

	testCases := jsontests.JsonContainsPathTestCases(t, convertToIndexedJsonDocument)
	jsontests.RunJsonTests(t, testCases)
}
