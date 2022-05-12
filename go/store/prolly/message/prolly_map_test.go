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

package message

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/pool"
)

var sharedPool = pool.NewBuffPool()

func TestGetKeyValueOffsetsVectors(t *testing.T) {
	for trial := 0; trial < 100; trial++ {
		keys, values := randomByteSlices(t, (testRand.Int()%101)+50)
		require.True(t, sumSize(keys)+sumSize(values) < MaxVectorOffset)
		s := ProllyMapSerializer{Pool: sharedPool}
		msg := s.Serialize(keys, values, nil, 0)

		// uses hard-coded vtable slot
		keyBuf, valBuf, _ := getProllyMapKeysAndValues(msg)

		for i := range keys {
			assert.Equal(t, keys[i], keyBuf.GetSlice(i))
		}
		for i := range values {
			assert.Equal(t, values[i], valBuf.GetSlice(i))
		}
	}
}

func randomByteSlices(t *testing.T, count int) (keys, values [][]byte) {
	keys = make([][]byte, count)
	for i := range keys {
		sz := (testRand.Int() % 41) + 10
		keys[i] = make([]byte, sz)
		_, err := testRand.Read(keys[i])
		assert.NoError(t, err)
	}

	values = make([][]byte, count)
	copy(values, keys)
	testRand.Shuffle(len(values), func(i, j int) {
		values[i], values[j] = values[j], values[i]
	})

	return
}

func sumSize(items [][]byte) (sz uint64) {
	for _, item := range items {
		sz += uint64(len(item))
	}
	return
}
