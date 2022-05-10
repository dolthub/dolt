// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package diff

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func TestPatchPathPartCompare(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()
	defer vs.Close()

	fieldPath1 := mustParsePath(assert, `.field1`)[0]
	fieldPath2 := mustParsePath(assert, `.field2`)[0]
	indexPath1 := mustParsePath(assert, `["field1"]`)[0]
	indexPath2 := mustParsePath(assert, `["field2"]`)[0]
	indexPathKey1 := mustParsePath(assert, `["field1"]@key`)[0]
	indexPathKey2 := mustParsePath(assert, `["field2"]@key`)[0]
	hashIndexPath1 := mustParsePath(assert, `[#01234567890123456789012345678901]`)[0]
	hashIndexPath2 := mustParsePath(assert, `[#0123456789abcdef0123456789abcdef]`)[0]
	hashIndexPathKey1 := mustParsePath(assert, `[#01234567890123456789012345678901]`)[0]
	hashIndexPathKey2 := mustParsePath(assert, `[#0123456789abcdef0123456789abcdef]`)[0]

	testCases := [][]types.PathPart{
		{fieldPath1, fieldPath2},
		{indexPath1, indexPath2},
		{indexPathKey1, indexPathKey2},
		{hashIndexPath1, hashIndexPath2},
		{hashIndexPathKey1, hashIndexPathKey2},
		{fieldPath2, indexPath1},
		{fieldPath2, indexPathKey1},
		{fieldPath2, hashIndexPath1},
		{fieldPath2, hashIndexPathKey1},
		{indexPath2, hashIndexPath1},
		{indexPath2, hashIndexPathKey1},
	}

	for i, tc := range testCases {
		res01, err := pathPartCompare(vs.Format(), tc[0], tc[1])
		require.NoError(t, err)
		res00, err := pathPartCompare(vs.Format(), tc[0], tc[0])
		require.NoError(t, err)
		res10, err := pathPartCompare(vs.Format(), tc[1], tc[0])
		require.NoError(t, err)

		assert.Equal(-1, res01, "test case %d failed, pp0: %s, pp1: %s", i, tc[0], tc[1])
		assert.Equal(0, res00, "test case %d failed, pp0: %s, pp1: %s", i, tc[0], tc[1])
		assert.Equal(1, res10, "test case %d failed, pp0: %s, pp1: %s", i, tc[0], tc[1])
	}
}

func TestPatchPathIsLess(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()
	defer vs.Close()

	testCases := [][]string{
		{``, `["field1"]`},
		{`["field1"]`, `["field1"].f1`},
		{`["field1"].f1`, `["field1"]["f1"]`},
		{`["field1"]["f1"]@key`, `["field1"]["f1"]`},
		{`["field1"]["f1"]`, `["field1"][#01234567890123456789012345678901]`},
		{`["field1"][#01234567890123456789012345678901]`, `["field1"][#0123456789abcdef0123456789abcdef]`},
	}

	for i, tc := range testCases {
		p0 := mustParsePath(assert, tc[0])
		p1 := mustParsePath(assert, tc[1])
		zeroLTOne, err := pathIsLess(vs.Format(), p0, p1)
		require.NoError(t, err)
		zeroLTZero, err := pathIsLess(vs.Format(), p0, p0)
		require.NoError(t, err)
		oneLTZero, err := pathIsLess(vs.Format(), p1, p0)
		require.NoError(t, err)
		assert.True(zeroLTOne, "test case %d failed", i)
		assert.False(zeroLTZero, "test case %d failed", i)
		assert.False(oneLTZero, "test case %d failed", i)
	}
	//p := mustParsePath(assert, `#0123456789abcdef0123456789abcdef.value`)
	//fmt.Printf("p[0]: %s, type: %T\n", p[0], p[0])
}

func TestPatchSort(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()
	defer vs.Close()

	sortedPaths := Patch{
		{Path: mustParsePath(assert, `["field1"]`)},
		{Path: mustParsePath(assert, `["field1"].f1`)},
		{Path: mustParsePath(assert, `["field1"]["f1"]`), ChangeType: types.DiffChangeRemoved},
		{Path: mustParsePath(assert, `["field1"]["f1"]`), ChangeType: types.DiffChangeModified},
		{Path: mustParsePath(assert, `["field1"]["f1"]`), ChangeType: types.DiffChangeAdded},
		{Path: mustParsePath(assert, `["field1"][#01234567890123456789012345678901]`)},
		{Path: mustParsePath(assert, `["field1"][#0123456789abcdef0123456789abcdef]`)},
	}

	rand.Perm(len(sortedPaths))
	shuffledPaths := Patch{}
	for _, idx := range rand.Perm(len(sortedPaths)) {
		shuffledPaths = append(shuffledPaths, sortedPaths[idx])
	}

	types.SortWithErroringLess(PatchSort{shuffledPaths, vs.Format()})
	assert.Equal(sortedPaths, shuffledPaths)
}
