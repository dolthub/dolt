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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type JsonDiffTest struct {
	Name          string
	From, To      sql.JSONWrapper
	ExpectedDiffs []JsonDiff
}

func makeJsonPathKey(parts ...interface{}) []byte {
	result := []byte{byte(startOfValue)}
	for _, part := range parts {
		switch p := part.(type) {
		case string:
			result = append(result, beginObjectKey)
			result = append(result, []byte(p)...)
		case int:
			result = append(result, beginArrayKey)
			result = append(result, makeVarInt(uint64(p))...)
		}
	}
	return result
}

var SimpleJsonDiffTests = []JsonDiffTest{
	{
		Name:          "empty object, no modifications",
		From:          types.JSONDocument{Val: types.JsonObject{}},
		To:            types.JSONDocument{Val: types.JsonObject{}},
		ExpectedDiffs: nil,
	},
	{
		Name: "insert into empty object",
		From: types.JSONDocument{Val: types.JsonObject{}},
		To:   types.JSONDocument{Val: types.JsonObject{"a": 1}},
		ExpectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`),
				From: nil,
				To:   &types.JSONDocument{Val: 1},
				Type: AddedDiff,
			},
		},
	},
	{
		Name: "delete From object",
		From: types.JSONDocument{Val: types.JsonObject{"a": 1}},
		To:   types.JSONDocument{Val: types.JsonObject{}},
		ExpectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`),
				From: &types.JSONDocument{Val: 1},
				To:   nil,
				Type: RemovedDiff,
			},
		},
	},
	{
		Name: "modify object",
		From: types.JSONDocument{Val: types.JsonObject{"a": 1}},
		To:   types.JSONDocument{Val: types.JsonObject{"a": 2}},
		ExpectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`),
				From: &types.JSONDocument{Val: 1},
				To:   &types.JSONDocument{Val: 2},
				Type: ModifiedDiff,
			},
		},
	},
	{
		Name: "nested insert",
		From: types.JSONDocument{Val: types.JsonObject{"a": types.JsonObject{}}},
		To:   types.JSONDocument{Val: types.JsonObject{"a": types.JsonObject{"b": 1}}},
		ExpectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`, `b`),
				To:   &types.JSONDocument{Val: 1},
				Type: AddedDiff,
			},
		},
	},
	{
		Name: "nested delete",
		From: types.JSONDocument{Val: types.JsonObject{"a": types.JsonObject{"b": 1}}},
		To:   types.JSONDocument{Val: types.JsonObject{"a": types.JsonObject{}}},
		ExpectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`, `b`),
				From: &types.JSONDocument{Val: 1},
				Type: RemovedDiff,
			},
		},
	},
	{
		Name: "nested modify",
		From: types.JSONDocument{Val: types.JsonObject{"a": types.JsonObject{"b": 1}}},
		To:   types.JSONDocument{Val: types.JsonObject{"a": types.JsonObject{"b": 2}}},
		ExpectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`, `b`),
				From: &types.JSONDocument{Val: 1},
				To:   &types.JSONDocument{Val: 2},
				Type: ModifiedDiff,
			},
		},
	},
	{
		Name: "insert object",
		From: types.JSONDocument{Val: types.JsonObject{"a": types.JsonObject{}}},
		To:   types.JSONDocument{Val: types.JsonObject{"a": types.JsonObject{"b": types.JsonObject{"c": 3}}}},
		ExpectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`, `b`),
				To:   &types.JSONDocument{Val: types.JsonObject{"c": 3}},
				Type: AddedDiff,
			},
		},
	},
	{
		Name: "modify to object",
		From: types.JSONDocument{Val: types.JsonObject{"a": types.JsonObject{"b": 2}}},
		To:   types.JSONDocument{Val: types.JsonObject{"a": types.JsonObject{"b": types.JsonObject{"c": 3}}}},
		ExpectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`, `b`),
				From: &types.JSONDocument{Val: 2},
				To:   &types.JSONDocument{Val: types.JsonObject{"c": 3}},
				Type: ModifiedDiff,
			},
		},
	},
	{
		Name: "modify from object",
		From: types.JSONDocument{Val: types.JsonObject{"a": types.JsonObject{"b": 2}}},
		To:   types.JSONDocument{Val: types.JsonObject{"a": 1}},
		ExpectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`),
				From: &types.JSONDocument{Val: types.JsonObject{"b": 2}},
				To:   &types.JSONDocument{Val: 1},
				Type: ModifiedDiff,
			},
		},
	},
	{
		Name: "modify to array",
		From: types.JSONDocument{Val: types.JsonObject{"a": types.JsonObject{"b": "foo"}}},
		To:   types.JSONDocument{Val: types.JsonObject{"a": types.JsonObject{"b": types.JsonArray{1, 2}}}},
		ExpectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`, `b`),
				From: &types.JSONDocument{Val: "foo"},
				To:   &types.JSONDocument{Val: types.JsonArray{1, 2}},
				Type: ModifiedDiff,
			},
		},
	},
	{
		Name: "modify from array",
		From: types.JSONDocument{Val: types.JsonObject{"a": types.JsonArray{1, 2}}},
		To:   types.JSONDocument{Val: types.JsonObject{"a": 1}},
		ExpectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`),
				From: &types.JSONDocument{Val: types.JsonArray{1, 2}},
				To:   &types.JSONDocument{Val: 1},
				Type: ModifiedDiff,
			},
		},
	},
	{
		Name: "array to object",
		From: types.JSONDocument{Val: types.JsonObject{"a": types.JsonArray{1, 2}}},
		To:   types.JSONDocument{Val: types.JsonObject{"a": types.JsonObject{"b": types.JsonObject{"c": 3}}}},
		ExpectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`),
				From: &types.JSONDocument{Val: types.JsonArray{1, 2}},
				To:   &types.JSONDocument{Val: types.JsonObject{"b": types.JsonObject{"c": 3}}},
				Type: ModifiedDiff,
			},
		},
	},
	{
		Name: "object to array",
		From: types.JSONDocument{Val: types.JsonObject{"a": types.JsonObject{"b": 2}}},
		To:   types.JSONDocument{Val: types.JsonObject{"a": types.JsonArray{1, 2}}},
		ExpectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`),
				From: &types.JSONDocument{Val: types.JsonObject{"b": 2}},
				To:   &types.JSONDocument{Val: types.JsonArray{1, 2}},
				Type: ModifiedDiff,
			},
		},
	},
	{
		Name: "array modification",
		From: types.JSONDocument{Val: types.JsonArray{1, 2}},
		To:   types.JSONDocument{Val: types.JsonArray{1, 3}},
		ExpectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(1),
				From: &types.JSONDocument{Val: 2},
				To:   &types.JSONDocument{Val: 3},
				Type: ModifiedDiff,
			},
		},
	},
	{
		Name: "array insert at end",
		From: types.JSONDocument{Val: types.JsonArray{1, 2}},
		To:   types.JSONDocument{Val: types.JsonArray{1, 2, types.JsonObject{"a": 2}}},
		ExpectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(2),
				From: nil,
				To:   &types.JSONDocument{Val: types.JsonObject{"a": 2}},
				Type: AddedDiff,
			},
		},
	},
	{
		Name: "array removal at end",
		From: types.JSONDocument{Val: types.JsonArray{1, 2, types.JsonArray{3}}},
		To:   types.JSONDocument{Val: types.JsonArray{1, 2}},
		ExpectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(2),
				From: &types.JSONDocument{Val: types.JsonArray{3}},
				To:   nil,
				Type: RemovedDiff,
			},
		},
	},
	{
		Name: "array modification in object",
		From: types.JSONDocument{Val: types.JsonObject{"a": types.JsonArray{1, 2}}},
		To:   types.JSONDocument{Val: types.JsonObject{"a": types.JsonArray{1, 3}}},
		ExpectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`, 1),
				From: &types.JSONDocument{Val: 2},
				To:   &types.JSONDocument{Val: 3},
				Type: ModifiedDiff,
			},
		},
	},
	{
		Name: "remove object",
		From: types.JSONDocument{Val: types.JsonObject{"a": types.JsonObject{"b": types.JsonObject{"c": 3}}}},
		To:   types.JSONDocument{Val: types.JsonObject{"a": types.JsonObject{}}},
		ExpectedDiffs: []JsonDiff{
			{
				Key:  makeJsonPathKey(`a`, `b`),
				From: &types.JSONDocument{Val: types.JsonObject{"c": 3}},
				Type: RemovedDiff,
			},
		},
	},
	{
		Name: "insert escaped double quotes",
		From: types.JSONDocument{Val: types.JsonObject{"\"a\"": "1"}},
		To:   types.JSONDocument{Val: types.JsonObject{"b": "\"2\""}},
		ExpectedDiffs: []JsonDiff{
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
		Name: "modifications returned in lexographic order",
		From: types.JSONDocument{Val: types.JsonObject{"a": types.JsonObject{"1": "i"}, "aa": 2, "b": 6}},
		To:   types.JSONDocument{Val: types.JsonObject{"": 1, "a": types.JsonObject{}, "aa": 3, "bb": 5}},
		ExpectedDiffs: []JsonDiff{
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
