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

package tree

import (
	"context"
	"math"
	"math/rand"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func TestRoundTripInts(t *testing.T) {
	tups, _ := AscendingUintTuples(10)
	keys := make([]val.Tuple, len(tups))
	values := make([]val.Tuple, len(tups))
	for i := range tups {
		keys[i] = tups[i][0]
		values[i] = tups[i][1]
	}
	require.True(t, sumTupleSize(keys)+sumTupleSize(values) < message.MaxVectorOffset)

	nd := NewTupleLeafNode(keys, values)
	assert.True(t, nd.IsLeaf())
	assert.Equal(t, len(keys), int(nd.count))
	for i := range keys {
		assert.Equal(t, keys[i], val.Tuple(nd.GetKey(i)))
		assert.Equal(t, values[i], val.Tuple(nd.GetValue(i)))
	}
}

func TestRoundTripNodeItems(t *testing.T) {
	for trial := 0; trial < 100; trial++ {
		keys, values := randomNodeItemPairs(t, (rand.Int()%101)+50)
		require.True(t, sumSize(keys)+sumSize(values) < message.MaxVectorOffset)

		nd := newLeafNode(keys, values)
		assert.True(t, nd.IsLeaf())
		assert.Equal(t, len(keys), int(nd.count))
		for i := range keys {
			assert.Equal(t, keys[i], nd.GetKey(i))
			assert.Equal(t, values[i], nd.GetValue(i))
		}
	}
}

func TestNodeSize(t *testing.T) {
	sz := unsafe.Sizeof(Node{})
	assert.Equal(t, 88, int(sz))
}

func BenchmarkNodeGet(b *testing.B) {
	const (
		count int = 128
		mask  int = 0x7f
	)
	tuples, _ := AscendingUintTuples(count)
	assert.Len(b, tuples, count)
	keys := make([]Item, count)
	vals := make([]Item, count)
	for i := range tuples {
		keys[i] = Item(tuples[i][0])
		vals[i] = Item(tuples[i][1])
	}
	nd := newLeafNode(keys, vals)

	var pm serial.ProllyTreeNode
	err := serial.InitProllyTreeNodeRoot(&pm, nd.msg, serial.MessagePrefixSz)
	require.NoError(b, err)
	b.ResetTimer()

	b.Run("ItemAccess Get", func(b *testing.B) {
		var k Item
		for i := 0; i < b.N; i++ {
			k = nd.GetKey(i & mask)
		}
		assert.NotNil(b, k)
	})
	b.Run("Flatbuffers Get", func(b *testing.B) {
		var k Item
		for i := 0; i < b.N; i++ {
			k = flatbuffersGetKey(i&mask, pm)
		}
		assert.NotNil(b, k)
	})
}

// Node.Get() without cached offset metadata
func flatbuffersGetKey(i int, pm serial.ProllyTreeNode) (key []byte) {
	buf := pm.KeyItemsBytes()
	start := pm.KeyOffsets(i)
	stop := pm.KeyOffsets(i + 1)
	key = buf[start:stop]
	return
}

func TestNodeHashValueCompatibility(t *testing.T) {
	keys, values := randomNodeItemPairs(t, (rand.Int()%101)+50)
	nd := newLeafNode(keys, values)
	nbf := types.Format_DOLT
	th, err := ValueFromNode(nd).Hash(nbf)
	require.NoError(t, err)
	assert.Equal(t, nd.HashOf(), th)

	h1 := hash.Parse("kvup5vdur99ush7c18g0kjc6rhdkfdgo")
	h2 := hash.Parse("7e54ill10nji9oao1ja88buh9itaj7k9")
	msg := message.NewAddressMapSerializer(sharedPool).Serialize(
		[][]byte{[]byte("chopin"), []byte("listz")},
		[][]byte{h1[:], h2[:]},
		[]uint64{},
		0)
	nd, fileId, err := NodeFromBytes(msg)
	require.NoError(t, err)
	assert.Equal(t, fileId, serial.AddressMapFileID)
	th, err = ValueFromNode(nd).Hash(nbf)
	require.NoError(t, err)
	assert.Equal(t, nd.HashOf(), th)
}

func TestNodeDecodeValueCompatibility(t *testing.T) {
	keys, values := randomNodeItemPairs(t, (rand.Int()%101)+50)
	nd := newLeafNode(keys, values)

	ts := &chunks.TestStorage{}
	cs := ts.NewView()
	ns := NewNodeStore(cs)
	vs := types.NewValueStore(cs)
	h, err := ns.Write(context.Background(), nd)
	require.NoError(t, err)

	v, err := vs.ReadValue(context.Background(), h)
	require.NoError(t, err)
	assert.Equal(t, nd.bytes(), []byte(v.(types.SerialMessage)))
}

func randomNodeItemPairs(t *testing.T, count int) (keys, values []Item) {
	keys = make([]Item, count)
	for i := range keys {
		sz := (rand.Int() % 41) + 10
		keys[i] = make(Item, sz)
		_, err := rand.Read(keys[i])
		assert.NoError(t, err)
	}

	values = make([]Item, count)
	copy(values, keys)
	rand.Shuffle(len(values), func(i, j int) {
		values[i], values[j] = values[j], values[i]
	})

	return
}

func sumSize(items []Item) (sz uint64) {
	for _, item := range items {
		sz += uint64(len(item))
	}
	return
}

func sumTupleSize(items []val.Tuple) (sz uint64) {
	for _, item := range items {
		sz += uint64(len(item))
	}
	return
}

func TestSamples(t *testing.T) {
	tests := []struct {
		data Samples
		sum  float64
		mean float64
		std  float64
	}{
		{
			data: Samples{1},
			sum:  1.0,
			mean: 1.0,
			std:  0.0,
		},
		{
			data: Samples{1, 2, 3, 4, 5},
			sum:  15.0,
			mean: 3.0,
			std:  math.Sqrt(2),
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.sum, test.data.sum())
		assert.Equal(t, test.mean, test.data.mean())
		assert.Equal(t, test.std, test.data.stdDev())
	}
}
