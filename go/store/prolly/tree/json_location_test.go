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

package tree

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestJsonLocationKeyOrdering covers path elements, escaped names, and ancestor scanner states.
func TestJsonLocationKeyOrdering(t *testing.T) {
	for _, test := range []struct {
		left, right           string
		leftState, rightState jsonPathType
		want                  int
	}{
		{"$.a.child", "$.aa", startOfValue, startOfValue, -1},
		{`$."\n"`, `$."\u000a"`, startOfValue, startOfValue, 0},
		{`$."a\\b"`, `$."a\u005cb"`, startOfValue, startOfValue, 0},
		{"$.a", "$.a.x", startOfValue, startOfValue, -1},
		{"$.a", "$.a.x", endOfValue, startOfValue, 1},
		{"$.a", "$.a[0]", objectInitialElement, startOfValue, 1},
		{"$.a", "$.a[0].x", objectInitialElement, startOfValue, -1},
		{"$.a[0]", "$.a.x", startOfValue, startOfValue, -1},
		{"$.a", "$.a", startOfValue, endOfValue, -1},
		{"$", "$.a", objectInitialElement, arrayInitialElement, 1},
		{"$", "$[0]", objectInitialElement, objectInitialElement, -1},
	} {
		t.Run(test.left+" before "+test.right+" "+jsonPathTypeNames[test.leftState], func(t *testing.T) {
			left := jsonLocationFromMySqlPath(t, test.left, test.leftState)
			right := jsonLocationFromMySqlPath(t, test.right, test.rightState)
			leftBytes, rightBytes := bytes.Clone(left.key), bytes.Clone(right.key)
			cmp, err := CompareJsonDiffKeys(left.key, right.key)
			require.NoError(t, err)
			require.Equal(t, test.want, cmp)
			cmp, err = CompareJsonDiffKeys(right.key, left.key)
			require.NoError(t, err)
			require.Equal(t, -test.want, cmp)
			cmp, err = compareJsonLocations(left, right)
			require.NoError(t, err)
			require.Equal(t, test.want, cmp)
			cmp, err = compareJsonLocations(right, left)
			require.NoError(t, err)
			require.Equal(t, -test.want, cmp)
			require.Equal(t, leftBytes, []byte(left.key))
			require.Equal(t, rightBytes, []byte(right.key))
		})
	}
}

// TestJsonLocationArrayIndices separates varint payload bytes from object delimiters and escapes.
func TestJsonLocationArrayIndices(t *testing.T) {
	var previous jsonLocationKey
	for _, index := range []uint64{0, 1, 92, 240, 241, 254, 255, 494, 495, 65535, 1 << 48, ^uint64(0)} {
		key := jsonLocationKey{byte(startOfValue)}.appendArrayIndexU64(index)
		child := jsonLocationKey(bytes.Clone(key)).appendObjectKey([]byte("child"))
		prefix, err := IsJsonKeyPrefix(child, key)
		require.NoError(t, err)
		require.True(t, prefix)
		if previous != nil {
			cmp, err := CompareJsonDiffKeys(previous, child)
			require.NoError(t, err)
			require.Negative(t, cmp)
		}
		previous = child
	}
}

// TestJsonLocationKeyRelations preserves strict prefixes and equivalent escaped spellings.
func TestJsonLocationKeyRelations(t *testing.T) {
	for _, test := range []struct {
		path, prefix string
		want         bool
	}{
		{"$.a.x", "$.a", true},
		{"$.aa", "$.a", false},
		{"$.a", "$.a", false},
		{`$."\n".x`, `$."\u000a"`, true},
		{`$."\n"`, `$."\u000a"`, false},
		{"$.a[92].x", "$.a[92]", true},
	} {
		path := jsonLocationFromMySqlPath(t, test.path, startOfValue)
		prefix := jsonLocationFromMySqlPath(t, test.prefix, startOfValue)
		got, err := IsJsonKeyPrefix(path.key, prefix.key)
		require.NoError(t, err)
		require.Equal(t, test.want, got)
	}
	left := jsonLocationFromMySqlPath(t, `$."\n"`, startOfValue)
	right := jsonLocationFromMySqlPath(t, `$."\u000a"`, startOfValue)
	equal, err := EqualJsonDiffKeys(left.key, right.key)
	require.NoError(t, err)
	require.True(t, equal)
	equal, err = EqualJsonDiffKeys(nil, left.key)
	require.NoError(t, err)
	require.False(t, equal)
	equal, err = EqualJsonDiffKeys(nil, nil)
	require.NoError(t, err)
	require.True(t, equal)
}

// TestJsonLocationKeyErrors retains lazy checks for unsupported states and invalid escapes.
func TestJsonLocationKeyErrors(t *testing.T) {
	bad := jsonLocationKey{byte(startOfValue)}.appendObjectKey([]byte(`\q`))
	plain := jsonLocationKey{byte(startOfValue)}.appendObjectKey([]byte("a"))
	_, err := CompareJsonDiffKeys(bad, plain)
	require.Error(t, err)
	prefix, err := IsJsonKeyPrefix(plain, jsonLocationKey(bytes.Clone(bad)).appendObjectKey([]byte("x")))
	require.NoError(t, err)
	require.False(t, prefix)
	prefix, err = IsJsonKeyPrefix(nil, bad)
	require.NoError(t, err)
	require.False(t, prefix)
	_, err = CompareJsonDiffKeys([]byte{byte(jsonPathTypeNumElements)}, []byte{byte(startOfValue)})
	require.ErrorIs(t, err, unknownLocationKeyError)
	future := jsonPathFromKey([]byte{byte(jsonPathTypeNumElements)})
	_, err = compareJsonLocations(future, future)
	require.ErrorIs(t, err, unknownLocationKeyError)
}

// TestJsonLocationKeyComparisonAllocations keeps ordinary path comparisons allocation-free.
func TestJsonLocationKeyComparisonAllocations(t *testing.T) {
	for _, paths := range [][2]string{{"$.a.b.c", "$.a.b.d"},
		{"$.a[92].c", "$.a[92].d"}} {
		left := jsonLocationFromMySqlPath(t, paths[0], startOfValue)
		right := jsonLocationFromMySqlPath(t, paths[1], startOfValue)
		allocations := testing.AllocsPerRun(1000, func() {
			if _, err := CompareJsonDiffKeys(left.key, right.key); err != nil {
				panic(err)
			}
		})
		require.Zero(t, allocations)
	}
}
