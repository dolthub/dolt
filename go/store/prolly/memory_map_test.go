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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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
				testGet(t, memMap, tuples)
			})
			t.Run("iter all from map", func(t *testing.T) {
				testIterAll(t, memMap, tuples)
			})
			t.Run("iter range", func(t *testing.T) {
				testIterRange(t, memMap, tuples)
			})
			t.Run("iter prefix range", func(t *testing.T) {
				testIterPrefixRange(t, memMap, tuples)
			})

			memMap2, tuples2, deletes := makeMemMapWithDeletes(t, s)
			t.Run("get item from map with deletes", func(t *testing.T) {
				testMemoryMapGetAndHas(t, memMap2, tuples2, deletes)
			})
			t.Run("iter all from map with deletes", func(t *testing.T) {
				testIterAll(t, memMap2, tuples2)
			})
			t.Run("iter range", func(t *testing.T) {
				testIterRange(t, memMap2, tuples2)
			})
			t.Run("iter prefix range", func(t *testing.T) {
				testIterPrefixRange(t, memMap2, tuples2)
			})
		})
	}
}

var memKeyDesc = val.NewTupleDescriptor(
	val.Type{Enc: val.Uint32Enc, Nullable: false},
)
var memValueDesc = val.NewTupleDescriptor(
	val.Type{Enc: val.Uint32Enc, Nullable: true},
	val.Type{Enc: val.Uint32Enc, Nullable: true},
	val.Type{Enc: val.Uint32Enc, Nullable: true},
)

func makeMemMap(t *testing.T, count int) (orderedMap, [][2]val.Tuple) {
	tuples := randomTuplePairs(count, memKeyDesc, memValueDesc)
	mm := newMemoryMap(memKeyDesc)
	for _, pair := range tuples {
		mm.Put(pair[0], pair[1])
	}

	return mm, tuples
}

func makeMemMapWithDeletes(t *testing.T, count int) (mut memoryMap, tuples, deletes [][2]val.Tuple) {
	om, tuples := makeMemMap(t, count)
	mut = om.(memoryMap)

	testRand.Shuffle(count, func(i, j int) {
		tuples[i], tuples[j] = tuples[j], tuples[i]
	})

	// delete 1/4 of tuples
	deletes = tuples[:count/4]

	// re-sort the remaining tuples
	tuples = tuples[count/4:]
	desc := getKeyDesc(om)
	sortTuplePairs(tuples, desc)

	for _, kv := range deletes {
		mut.Put(kv[0], nil)
	}

	return mut, tuples, deletes
}

func testMemoryMapGetAndHas(t *testing.T, mem memoryMap, tuples, deletes [][2]val.Tuple) {
	ctx := context.Background()
	for _, kv := range tuples {
		err := mem.Get(ctx, kv[0], func(key, val val.Tuple) (err error) {
			assert.NotNil(t, kv[0])
			assert.Equal(t, kv[0], key)
			assert.Equal(t, kv[1], val)
			return
		})
		require.NoError(t, err)
	}

	for _, kv := range deletes {
		err := mem.Get(ctx, kv[0], func(key, value val.Tuple) (err error) {
			assert.NotNil(t, kv[0])
			assert.Equal(t, val.Tuple(nil), key)
			assert.Equal(t, val.Tuple(nil), value)
			return
		})
		require.NoError(t, err)
	}
}

func debugFmt(tup val.Tuple, desc val.TupleDesc) (s string) {
	if tup == nil {
		s = "[ nil ]"
	} else {
		s = desc.Format(tup)
	}
	return
}

func fmtMany(tuples, deletes [][2]val.Tuple) string {
	tuples = append(tuples, deletes...)
	sortTuplePairs(tuples, memKeyDesc)

	var sb strings.Builder
	sb.WriteString("{ ")
	for _, kv := range tuples {
		sb.WriteString(debugFmt(kv[0], memKeyDesc))
		sb.WriteString(": ")
		sb.WriteString(debugFmt(kv[1], memValueDesc))
		sb.WriteString(", ")
	}
	sb.WriteString("}")
	return sb.String()
}
