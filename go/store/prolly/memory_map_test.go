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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/val"
)

func TestMemoryMap(t *testing.T) {
	t.Run("get item from map", func(t *testing.T) {
		testOrderedMapGetAndHas(t, makeMemoryMap, 10)
		testOrderedMapGetAndHas(t, makeMemoryMap, 100)
		testOrderedMapGetAndHas(t, makeMemoryMap, 1000)
		testOrderedMapGetAndHas(t, makeMemoryMap, 10_000)
	})
	//t.Run("get from map at index", func(t *testing.T) {
	//	testOrderedMapGetIndex(t, makeMemoryMap, 10)
	//	testOrderedMapGetIndex(t, makeMemoryMap, 100)
	//	testOrderedMapGetIndex(t, makeMemoryMap, 1000)
	//	testOrderedMapGetIndex(t, makeMemoryMap, 10_000)
	//})
	//t.Run("get value range from map", func(t *testing.T) {
	//	testMapIterValueRange(t, 10)
	//	testMapIterValueRange(t, 100)
	//	testMapIterValueRange(t, 1000)
	//	testMapIterValueRange(t, 10_000)
	//})
	//t.Run("get index range from map", func(t *testing.T) {
	//	testOrderedMapIterIndexRange(t, makeMemoryMap, 10)
	//	testOrderedMapIterIndexRange(t, makeMemoryMap, 100)
	//	testOrderedMapIterIndexRange(t, makeMemoryMap, 1000)
	//	testOrderedMapIterIndexRange(t, makeMemoryMap, 10_000)
	//})
}

func makeMemoryMap(t *testing.T, kd, vd val.TupleDesc, items [][2]val.Tuple) orderedMap {
	mm := newMemoryMap(kd)
	for _, item := range items {
		ok := mm.Put(item[0], item[1])
		require.True(t, ok)
	}
	return mm
}

var _ cartographer = makeMemoryMap
