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
	"testing"

	"github.com/dolthub/dolt/go/store/val"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMap(t *testing.T) {
	t.Run("get item from map", func(t *testing.T) {
		testMapGetItem(t, 10)
		testMapGetItem(t, 100)
		testMapGetItem(t, 1000)
	})
}

func testMapGetItem(t *testing.T, count int) {
	ctx := context.Background()
	m, items := randomMap(t, count)

	for _, kv := range items {
		k, v := val.Tuple(kv[0]), val.Tuple(kv[1])
		err := m.Get(ctx, k, func(key, val val.Tuple) (err error) {
			assert.NotNil(t, k)
			assert.Equal(t, k, key)
			assert.Equal(t, v, val)
			return
		})
		require.NoError(t, err)
	}
}

func randomMap(t *testing.T, count int) (Map, [][2]nodeItem) {
	t1 := val.Type{Coll: val.ByteOrderCollation, Nullable: false}
	t2 := val.Type{Coll: val.ByteOrderCollation, Nullable: true}

	root, items, nrw := randomTree(t, count, 3)
	assert.NotNil(t, root)

	m := Map{
		root: root,
		// non-null keys, nullable values
		keyDesc: val.NewTupleDescriptor(t1, t1, t1),
		valDesc: val.NewTupleDescriptor(t2, t2, t2),
		nrw:     nrw,
	}

	return m, items
}
