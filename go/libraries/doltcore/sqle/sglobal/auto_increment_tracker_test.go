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

package sglobal

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNextHasNoRepeats(t *testing.T) {
	allVals := make(map[uint64]int)
	var mu sync.Mutex

	aiTracker := NewAutoIncrementTracker()

	for i := 0; i < 100; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				nxt, err := aiTracker.Next("test", nil, 1)
				require.NoError(t, err)

				val, err := convertIntTypeToUint(nxt)
				require.NoError(t, err)

				mu.Lock()
				allVals[val]++
				mu.Unlock()
			}
		}()
	}

	for _, val := range allVals {
		require.Equal(t, 1, val)
	}
}
