// Copyright 2021 Dolthub, Inc.
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

package prolly

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/val"
)

func Test3WayMapMerge(t *testing.T) {
	scales := []int{
		10,
		100,
		1000,
		10000,
	}

	for _, s := range scales {
		name := fmt.Sprintf("test proCur map at scale %d", s)
		t.Run(name, func(t *testing.T) {
			prollyMap, tuples := makeProllyMap(t, s)

			t.Run("merge identical maps", func(t *testing.T) {
				testEqualMapMerge(t, prollyMap.(Map))
			})
			t.Run("3way merge inserts", func(t *testing.T) {
				for k := 0; k < 10; k++ {
					testMapMergeInserts(t, prollyMap.(Map), tuples, s/10)
				}
			})
		})
	}
}

func testEqualMapMerge(t *testing.T, m Map) {
	ctx := context.Background()
	mm, err := ThreeWayMerge(ctx, m, m, m, panicOnConflict)
	require.NoError(t, err)
	assert.NotNil(t, mm)
	//assert.Equal(t, m.Count(), mm.Count())
}

func testMapMergeInserts(t *testing.T, final Map, tups [][2]val.Tuple, sz int) {
	testRand.Shuffle(len(tups), func(i, j int) {
		tups[i], tups[j] = tups[j], tups[i]
	})

	left := makeMapWithDeletes(t, final, tups[:sz]...)
	right := makeMapWithDeletes(t, final, tups[sz:sz*2]...)
	base := makeMapWithDeletes(t, final, tups[:sz*2]...)

	ctx := context.Background()
	final2, err := ThreeWayMerge(ctx, left, right, base, panicOnConflict)
	require.NoError(t, err)
	assert.Equal(t, final.HashOf(), final2.HashOf())

	cnt := 0
	err = DiffMaps(ctx, final, final2, func(ctx context.Context, diff Diff) error {
		cnt++
		return nil
	})
	require.Error(t, io.EOF, err)
	assert.Equal(t, 0, cnt)
}

func panicOnConflict(left, right Diff) (Diff, bool) {
	panic("cannot merge cells")
}
