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

	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type jsonDiffTest struct {
	name          string
	from, to      types.JsonObject
	expectedDiffs []JsonDiff
}

func makeJsonPathKey(parts ...string) []byte {
	result := []byte{byte(startOfValue)}
	for _, part := range parts {
		result = append(result, beginObjectKey)
		result = append(result, []byte(part)...)
	}
	return result
}

var simpleJsonDiffTests = []jsonDiffTest{
	{
		name:          "empty object, no modifications",
		from:          types.JsonObject{},
		to:            types.JsonObject{},
		expectedDiffs: nil,
	},
	{
		name: "insert into empty object",
		from: types.JsonObject{},
		to:   types.JsonObject{"a": 1},
		expectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`),
				From: nil,
				To:   &types.JSONDocument{Val: 1},
				Type: AddedDiff,
			},
		},
	},
	{
		name: "delete from object",
		from: types.JsonObject{"a": 1},
		to:   types.JsonObject{},
		expectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`),
				From: &types.JSONDocument{Val: 1},
				To:   nil,
				Type: RemovedDiff,
			},
		},
	},
	{
		name: "modify object",
		from: types.JsonObject{"a": 1},
		to:   types.JsonObject{"a": 2},
		expectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`),
				From: &types.JSONDocument{Val: 1},
				To:   &types.JSONDocument{Val: 2},
				Type: ModifiedDiff,
			},
		},
	},
	{
		name: "nested insert",
		from: types.JsonObject{"a": types.JsonObject{}},
		to:   types.JsonObject{"a": types.JsonObject{"b": 1}},
		expectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`, `b`),
				To:   &types.JSONDocument{Val: 1},
				Type: AddedDiff,
			},
		},
	},
	{
		name: "nested delete",
		from: types.JsonObject{"a": types.JsonObject{"b": 1}},
		to:   types.JsonObject{"a": types.JsonObject{}},
		expectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`, `b`),
				From: &types.JSONDocument{Val: 1},
				Type: RemovedDiff,
			},
		},
	},
	{
		name: "nested modify",
		from: types.JsonObject{"a": types.JsonObject{"b": 1}},
		to:   types.JsonObject{"a": types.JsonObject{"b": 2}},
		expectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`, `b`),
				From: &types.JSONDocument{Val: 1},
				To:   &types.JSONDocument{Val: 2},
				Type: ModifiedDiff,
			},
		},
	},
	{
		name: "insert object",
		from: types.JsonObject{"a": types.JsonObject{}},
		to:   types.JsonObject{"a": types.JsonObject{"b": types.JsonObject{"c": 3}}},
		expectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`, `b`),
				To:   &types.JSONDocument{Val: types.JsonObject{"c": 3}},
				Type: AddedDiff,
			},
		},
	},
	{
		name: "modify to object",
		from: types.JsonObject{"a": types.JsonObject{"b": 2}},
		to:   types.JsonObject{"a": types.JsonObject{"b": types.JsonObject{"c": 3}}},
		expectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`, `b`),
				From: &types.JSONDocument{Val: 2},
				To:   &types.JSONDocument{Val: types.JsonObject{"c": 3}},
				Type: ModifiedDiff,
			},
		},
	},
	{
		name: "modify from object",
		from: types.JsonObject{"a": types.JsonObject{"b": 2}},
		to:   types.JsonObject{"a": 1},
		expectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`),
				From: &types.JSONDocument{Val: types.JsonObject{"b": 2}},
				To:   &types.JSONDocument{Val: 1},
				Type: ModifiedDiff,
			},
		},
	},
	{
		name: "modify to array",
		from: types.JsonObject{"a": types.JsonObject{"b": "foo"}},
		to:   types.JsonObject{"a": types.JsonObject{"b": types.JsonArray{1, 2}}},
		expectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`, `b`),
				From: &types.JSONDocument{Val: "foo"},
				To:   &types.JSONDocument{Val: types.JsonArray{1, 2}},
				Type: ModifiedDiff,
			},
		},
	},
	{
		name: "modify from array",
		from: types.JsonObject{"a": types.JsonArray{1, 2}},
		to:   types.JsonObject{"a": 1},
		expectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`),
				From: &types.JSONDocument{Val: types.JsonArray{1, 2}},
				To:   &types.JSONDocument{Val: 1},
				Type: ModifiedDiff,
			},
		},
	},
	{
		name: "array to object",
		from: types.JsonObject{"a": types.JsonArray{1, 2}},
		to:   types.JsonObject{"a": types.JsonObject{"b": types.JsonObject{"c": 3}}},
		expectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`),
				From: &types.JSONDocument{Val: types.JsonArray{1, 2}},
				To:   &types.JSONDocument{Val: types.JsonObject{"b": types.JsonObject{"c": 3}}},
				Type: ModifiedDiff,
			},
		},
	},
	{
		name: "object to array",
		from: types.JsonObject{"a": types.JsonObject{"b": 2}},
		to:   types.JsonObject{"a": types.JsonArray{1, 2}},
		expectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`),
				From: &types.JSONDocument{Val: types.JsonObject{"b": 2}},
				To:   &types.JSONDocument{Val: types.JsonArray{1, 2}},
				Type: ModifiedDiff,
			},
		},
	},
	{
		name: "remove object",
		from: types.JsonObject{"a": types.JsonObject{"b": types.JsonObject{"c": 3}}},
		to:   types.JsonObject{"a": types.JsonObject{}},
		expectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`, `b`),
				From: &types.JSONDocument{Val: types.JsonObject{"c": 3}},
				Type: RemovedDiff,
			},
		},
	},
	{
		name: "insert escaped double quotes",
		from: types.JsonObject{"\"a\"": "1"},
		to:   types.JsonObject{"b": "\"2\""},
		expectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`"a"`),
				From: &types.JSONDocument{Val: "1"},
				To:   nil,
				Type: RemovedDiff,
			},
			{
				Key:  makeJsonPathKey(`b`),
				From: nil,
				To:   &types.JSONDocument{Val: "\"2\""},
				Type: AddedDiff,
			},
		},
	},
	{
		name: "modifications returned in lexographic order",
		from: types.JsonObject{"a": types.JsonObject{"1": "i"}, "aa": 2, "b": 6},
		to:   types.JsonObject{"": 1, "a": types.JsonObject{}, "aa": 3, "bb": 5},
		expectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(``),
				To:   &types.JSONDocument{Val: 1},
				Type: AddedDiff,
			},
			{
				Key:  makeJsonPathKey(`a`, `1`),
				From: &types.JSONDocument{Val: "i"},
				Type: RemovedDiff,
			},
			{
				Key:  makeJsonPathKey(`aa`),
				From: &types.JSONDocument{Val: 2},
				To:   &types.JSONDocument{Val: 3},
				Type: ModifiedDiff,
			},
			{
				Key:  makeJsonPathKey(`b`),
				From: &types.JSONDocument{Val: 6},
				Type: RemovedDiff,
			},
			{
				Key:  makeJsonPathKey(`bb`),
				To:   &types.JSONDocument{Val: 5},
				Type: AddedDiff,
			},
		},
	},
}

func TestJsonDiff(t *testing.T) {
	t.Run("simple tests", func(t *testing.T) {
		runTestBatch(t, simpleJsonDiffTests)
	})
}

func runTestBatch(t *testing.T, tests []jsonDiffTest) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runTest(t, test)
		})
	}
}

func runTest(t *testing.T, test jsonDiffTest) {
	ctx := context.Background()
	ns := NewTestNodeStore()
	from := newIndexedJsonDocumentFromValue(t, ctx, ns, test.from)
	to := newIndexedJsonDocumentFromValue(t, ctx, ns, test.to)
	differ, err := NewIndexedJsonDiffer(ctx, from, to)
	require.NoError(t, err)
	var actualDiffs []JsonDiff
	for {
		diff, err := differ.Next(ctx)
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		actualDiffs = append(actualDiffs, diff)
	}

	diffsEqual := func(expected, actual JsonDiff) bool {
		if expected.Type != actual.Type {
			return false
		}
		if !bytes.Equal(expected.Key, actual.Key) {
			return false
		}
		cmp, err := types.CompareJSON(expected.From, actual.From)
		require.NoError(t, err)
		if cmp != 0 {
			return false
		}
		cmp, err = types.CompareJSON(expected.To, actual.To)
		require.NoError(t, err)
		if cmp != 0 {
			return false
		}
		return true
	}
	require.Equal(t, len(test.expectedDiffs), len(actualDiffs))
	for i, expected := range test.expectedDiffs {
		actual := actualDiffs[i]
		require.True(t, diffsEqual(expected, actual), fmt.Sprintf("Expected: %v\nActual: %v", expected, actual))

	}
}
