// Copyright 2023 Dolthub, Inc.
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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/val"
)

func TestStatsLevel(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		count int
		level int
	}{
		{
			count: 10,
			level: 0,
		},
		{
			count: 1e3,
			level: 0,
		},
		{
			count: 1e6,
			level: 1,
		},
	}
	lowBucketCnt := 20
	hashFanout := 200
	highBucketCnt := lowBucketCnt * hashFanout
	for _, tt := range tests {
		t.Run(fmt.Sprintf("stats histogram level test count: %d", tt.count), func(t *testing.T) {
			root, _, ns := randomTree(t, tt.count*2)
			m := StaticMap[val.Tuple, val.Tuple, val.TupleDesc]{
				Root:      root,
				NodeStore: ns,
				Order:     keyDesc,
			}
			levelNodes, err := GetHistogramLevel(ctx, m, lowBucketCnt)
			require.NoError(t, err)

			require.Equal(t, tt.level, levelNodes[0].Level())

			if root.level == 0 {
				require.Equal(t, 1, len(levelNodes))
			} else if root.level == 1 {
				require.Equal(t, root.Count(), len(levelNodes))
			} else {
				if len(levelNodes) < lowBucketCnt || len(levelNodes) >= highBucketCnt {
					t.Errorf("expected histogram bucket level to be in range: [%d,%d); found: %d", lowBucketCnt, highBucketCnt, len(levelNodes))
				}
			}
			require.Equal(t, tt.count, histLevelCount(t, levelNodes))
		})
	}
}

func histLevelCount(t *testing.T, nodes []Node) int {
	cnt := 0
	for _, n := range nodes {
		switch n.level {
		case 0:
			cnt += n.Count()
		default:
			n, err := n.loadSubtrees()
			require.NoError(t, err)
			for i := 0; i < n.Count(); i++ {
				subCnt := n.GetSubtreeCount(i)
				cnt += int(subCnt)
			}
		}
	}
	return cnt
}
