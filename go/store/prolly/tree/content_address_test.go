// Copyright 2022 Dolthub, Inc.
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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

var goldenHash = hash.Hash{
	0x33, 0xba, 0x6a, 0x18, 0xcb,
	0xcb, 0xa7, 0x41, 0x4a, 0xdb,
	0x1e, 0x3d, 0xbf, 0x3f, 0x1e,
	0xea, 0x7d, 0x47, 0x69, 0x6c,
}

// todo(andy): need an analogous test in pkg prolly
func TestContentAddress(t *testing.T) {
	tups, _ := AscendingUintTuples(12345)
	m, err := MakeTreeForTest(tups)
	assert.NoError(t, err)
	require.NotNil(t, m)
	require.Equal(t, goldenHash, m.HashOf())
	tc, err := m.TreeCount()
	require.NoError(t, err)
	assert.Equal(t, 12345, tc)
}
