// Copyright 2022 Dolthub, Inc.
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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/prolly/message"
)

func TestWriteImmutableTree(t *testing.T) {
	tests := []struct {
		inputSize int
		chunkSize int
		err       error
	}{
		{
			inputSize: 100,
			chunkSize: 5,
		},
		{
			inputSize: 100,
			chunkSize: 100,
		},
		{
			inputSize: 100,
			chunkSize: 101,
		},
		{
			inputSize: 255,
			chunkSize: 5,
		},
		{
			inputSize: 243,
			chunkSize: 5,
		},
		{
			inputSize: 47,
			chunkSize: 3,
		},
		{
			inputSize: 200,
			chunkSize: 7,
		},
		{
			inputSize: 200,
			chunkSize: 40,
		},
		{
			inputSize: 1,
			chunkSize: 5,
		},
		{
			inputSize: 20,
			chunkSize: 500,
		},
		{
			inputSize: 10,
			chunkSize: 1,
			err:       ErrInvalidChunkSize,
		},
		{
			inputSize: 10,
			chunkSize: -1,
			err:       ErrInvalidChunkSize,
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("inputSize=%d; chunkSize=%d", tt.inputSize, tt.chunkSize), func(t *testing.T) {
			buf := make([]byte, tt.inputSize)
			for i := range buf {
				buf[i] = byte(i)
			}
			ctx := context.Background()
			r := bytes.NewReader(buf)
			ns := NewTestNodeStore()
			serializer := message.ProllyMapSerializer{Pool: ns.Pool()}
			root, err := buildImmutableTree(ctx, r, ns, serializer, tt.chunkSize)
			if tt.err != nil {
				require.True(t, errors.Is(err, tt.err))
				return
			}
			require.NoError(t, err)

			expSubtrees := expectedSubtrees(tt.inputSize, tt.chunkSize)
			expLevel := expectedLevel(tt.inputSize, tt.chunkSize)
			expSum := expectedSum(tt.inputSize)
			expUnfilled := expectedUnfilled(tt.inputSize, tt.chunkSize)

			unfilledCnt := 0
			sum := 0
			byteCnt := 0
			WalkNodes(ctx, root, ns, func(ctx context.Context, n Node) error {
				var keyCnt int
				if n.IsLeaf() {
					byteCnt += len(n.values.Buf)
					for _, i := range n.getValue(0) {
						sum += int(i)
					}
					keyCnt = len(n.values.Buf)
				} else {
					keyCnt = n.Count()
				}
				if keyCnt != tt.chunkSize {
					unfilledCnt += 1
				}
				return nil
			})

			require.Equal(t, expLevel, root.Level())
			require.Equal(t, expSum, sum)
			require.Equal(t, tt.inputSize, byteCnt)
			require.Equal(t, expUnfilled, unfilledCnt)
			require.Equal(t, expSubtrees, root.getSubtreeCounts())
		})
	}
}

func expectedLevel(size, chunk int) int {
	l := 0
	for size > chunk {
		size = size / chunk
		l += 1
	}
	return l
}

func expectedSubtrees(size, chunk int) SubtreeCounts {
	if size <= chunk {
		return SubtreeCounts{0}
	}
	size = int(math.Ceil(float64(size) / float64(chunk)))
	l := chunk
	for l < size {
		l *= chunk
	}
	l /= chunk

	res := make(SubtreeCounts, 0)
	for size > l {
		res = append(res, uint64(l))
		size -= l
	}
	res = append(res, uint64(size))

	return res
}

func expectedSum(size int) int {
	return (size * (size + 1) / 2) - size
}

func expectedUnfilled(size, chunk int) int {
	l := chunk
	for l < size {
		l *= chunk
	}
	l /= chunk
	size -= l
	cnt := 0
	i := 1
	for size > 0 {
		if l > size {
			if i < chunk-1 {
				cnt += 1
			}
			l /= chunk
			i = 0
		} else {
			size -= l
			i++
		}
	}
	if i < chunk {
		cnt += 1
	}
	return cnt
}
