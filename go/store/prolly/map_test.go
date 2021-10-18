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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMap(t *testing.T) {
	t.Run("get item from map", func(t *testing.T) {
		testMapGetItem(t)
	})
}

func testMapGetItem(t *testing.T) {
	root, items, nrw := randomTree(t, 1000)
	assert.NotNil(t, root)

	ctx := context.Background()
	for _, kv := range items {
		key, value := kv[0], kv[1]
		err := newCursorAtItem(ctx, nrw, root, key, compareRandomTuples, func(cur *nodeCursor) (err error) {
			assert.Equal(t, key, cur.current())
			_, err = cur.advance(ctx)
			require.NoError(t, err)
			assert.Equal(t, value, cur.current())
			return
		})
		require.NoError(t, err)
	}

	validateTreeItems(t, nrw, root, items)
}
