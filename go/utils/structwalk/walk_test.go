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

package structwalk

import (
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestWalk(t *testing.T) {
	type innerStruct struct {
		Five *string `json:"five"`
	}

	type testStruct struct {
		One   string       `json:"one"`
		Two   int          `json:"two"`
		Three bool         `json:"three"`
		Four  *innerStruct `json:"four"`
		Six   []string     `json:"six"`
	}

	expected := []struct {
		name    string
		typeStr string
		depth   int
		json    string
	}{
		{
			name:    "One",
			typeStr: "string",
			depth:   0,
			json:    "one",
		},
		{
			name:    "Two",
			typeStr: "int",
			depth:   0,
			json:    "two",
		},
		{
			name:    "Three",
			typeStr: "bool",
			depth:   0,
			json:    "three",
		},
		{
			name:    "Four",
			typeStr: "*structwalk.innerStruct",
			depth:   0,
			json:    "four",
		},
		{
			name:    "Five",
			typeStr: "*string",
			depth:   1,
			json:    "five",
		},
		{
			name:    "Six",
			typeStr: "[]string",
			depth:   0,
			json:    "six",
		},
	}

	var n int
	err := Walk(&testStruct{}, func(sf reflect.StructField, depth int) error {
		require.Equal(t, expected[n].name, sf.Name)
		require.Equal(t, expected[n].typeStr, sf.Type.String())
		require.Equal(t, expected[n].depth, depth)
		require.Equal(t, expected[n].json, sf.Tag.Get("json"))
		n++
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, len(expected), n)
}
