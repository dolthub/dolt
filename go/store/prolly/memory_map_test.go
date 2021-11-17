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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/val"
)

func TestMemMap(t *testing.T) {
	scales := []int{
		10,
		100,
		1000,
		10_000,
	}

	for _, s := range scales {
		name := fmt.Sprintf("test memory map at scale %d", s)
		t.Run(name, func(t *testing.T) {
			memMap, tuples := makeMemMap(t, s)

			t.Run("get item from map", func(t *testing.T) {
				testOrderedMapGetAndHas(t, memMap, tuples)
			})
			t.Run("iter all from map", func(t *testing.T) {
				testOrderedMapIterAll(t, memMap, tuples)
			})
			t.Run("iter value range", func(t *testing.T) {
				//testOrderedMapIterValueRange(t, memMap, tuples)
			})
		})
	}
}

func makeMemMap(t *testing.T, count int) (orderedMap, [][2]val.Tuple) {
	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: false},
	)
	vd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
	)

	tuples := randomTuplePairs(count, kd, vd)

	mm := newMemoryMap(kd)
	for _, pair := range tuples {
		ok := mm.Put(pair[0], pair[1])
		require.True(t, ok)
	}

	return mm, tuples
}
