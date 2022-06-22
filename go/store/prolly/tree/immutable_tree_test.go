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
	"github.com/dolthub/dolt/go/store/val"
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

func TestImmutableTreeWalk(t *testing.T) {
	tests := []struct {
		blobLen   int
		chunkSize int
		keyCnt    int
	}{
		{
			blobLen:   25,
			chunkSize: 6,
			keyCnt:    4,
		},
		{
			blobLen:   25,
			chunkSize: 5,
			keyCnt:    4,
		},
		{
			blobLen:   378,
			chunkSize: 5,
			keyCnt:    12,
		},
		{
			blobLen:   5000,
			chunkSize: 12,
			keyCnt:    6,
		},
		{
			blobLen:   1,
			chunkSize: 12,
			keyCnt:    6,
		},
		{
			blobLen:   0,
			chunkSize: 12,
			keyCnt:    6,
		},
	}

	ns := NewTestNodeStore()
	for _, tt := range tests {
		t.Run(fmt.Sprintf("inputSize=%d; chunkSize=%d; keyCnt=%d", tt.blobLen, tt.chunkSize, tt.keyCnt), func(t *testing.T) {
			r := newTree(t, ns, tt.keyCnt, tt.blobLen, tt.chunkSize)
			var cnt int
			walkOpaqueNodes(context.Background(), r, ns, func(ctx context.Context, n Node) error {
				cnt++
				return nil
			})
			require.Equal(t, leafAddrCnt(tt.blobLen, tt.chunkSize)*tt.keyCnt+1, cnt)
		})
	}
}

func leafAddrCnt(size, chunk int) int {
	if size == 0 {
		return 0
	}
	l := 1
	for size > chunk {
		size = int(math.Ceil(float64(size) / float64(chunk)))
		l += size
	}
	return l
}

func newTree(t *testing.T, ns NodeStore, keyCnt, blobLen, chunkSize int) Node {
	ctx := context.Background()

	keyDesc := val.NewTupleDescriptor(val.Type{Enc: val.Uint32Enc})
	valDesc := val.NewTupleDescriptor(val.Type{Enc: val.BytesAddrEnc})

	tuples := make([][2]val.Tuple, keyCnt)
	keyBld := val.NewTupleBuilder(keyDesc)
	valBld := val.NewTupleBuilder(valDesc)
	for i := range tuples {
		keyBld.PutUint32(0, uint32(i))
		tuples[i][0] = keyBld.Build(sharedPool)

		b := mustNewBlob(ctx, ns, blobLen, chunkSize)
		valBld.PutBytesAddr(0, b.Addr)
		tuples[i][1] = valBld.Build(sharedPool)
	}

	serializer := message.ProllyMapSerializer{Pool: ns.Pool(), ValDesc: valDesc}
	chunker, err := newEmptyChunker(ctx, ns, serializer)
	require.NoError(t, err)
	for _, pair := range tuples {
		err := chunker.AddPair(ctx, Item(pair[0]), Item(pair[1]))
		require.NoError(t, err)
	}
	root, err := chunker.Done(ctx)
	require.NoError(t, err)
	return root
}

func mustNewBlob(ctx context.Context, ns NodeStore, len, chunkSize int) *ImmutableTree {
	buf := make([]byte, len)
	for i := range buf {
		buf[i] = byte(i)
	}
	r := bytes.NewReader(buf)
	root, err := NewImmutableTreeFromReader(ctx, r, ns, chunkSize)
	if err != nil {
		panic(err)
	}
	return root
}
