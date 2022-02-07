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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/val"
)

func TestMapDiff(t *testing.T) {
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

			t.Run("empty map diff", func(t *testing.T) {
				testEmptyMapDiff(t, prollyMap.(Map))
			})
			t.Run("single map diff", func(t *testing.T) {
				for k := 0; k < 100; k++ {
					testSingleMapDiff(t, prollyMap.(Map), tuples)
				}
			})
		})
	}
}

func testEmptyMapDiff(t *testing.T, m Map) {
	ctx := context.Background()
	var counter int
	err := DiffMaps(ctx, m, m, func(ctx context.Context, diff Diff) error {
		counter++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 0, counter)
}

func testSingleMapDiff(t *testing.T, from Map, tuples [][2]val.Tuple) {
	ctx := context.Background()

	idx := testRand.Int() % len(tuples)
	to := deleteKeys(t, from, tuples[idx])

	var counter int
	err := DiffMaps(ctx, from, to, func(ctx context.Context, diff Diff) error {
		assert.Equal(t, RemovedDiff, diff.Type)
		counter++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, counter)
}

func deleteKeys(t *testing.T, m Map, deletes ...[2]val.Tuple) Map {
	ctx := context.Background()
	mut := m.Mutate()
	for _, pair := range deletes {
		err := mut.Put(ctx, pair[0], nil)
		require.NoError(t, err)
	}
	mm, err := mut.Map(ctx)
	require.NoError(t, err)
	return mm
}
