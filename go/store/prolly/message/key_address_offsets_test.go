// Copyright 2026 Dolthub, Inc.
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
	"context"
	"fmt"
	"testing"

	"github.com/mohae/uvarint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

// inlineAdaptiveValue returns the inline encoding of an adaptive value.
func inlineAdaptiveValue(content string) []byte {
	return append([]byte{0}, content...)
}

// outOfBandAdaptiveValue returns the out-of-band encoding of an adaptive value:
// varint(length) followed by the content address.
func outOfBandAdaptiveValue(length uint64, addr hash.Hash) []byte {
	// uvarint.Encode writes by index, so the buffer needs length (not just capacity) for the
	// largest possible varint
	buf := make([]byte, 9)
	n := uvarint.Encode(buf, length)
	return append(buf[:n], addr[:]...)
}

func testAddr(i byte) hash.Hash {
	var a hash.Hash
	for j := range a {
		a[j] = i
	}
	return a
}

func collectAddresses(t *testing.T, msg serial.Message) hash.HashSet {
	addrs := hash.NewHashSet()
	err := WalkAddresses(context.Background(), msg, func(_ context.Context, addr hash.Hash) error {
		addrs.Insert(addr)
		return nil
	})
	require.NoError(t, err)
	return addrs
}

// TestKeyAddressOffsets exercises the key_address_offsets field of ProllyTreeNode: leaf nodes with
// out-of-band adaptive values in key tuples record their addresses, WalkAddresses visits them, and
// nodes without any such address omit the field entirely, remaining readable by older clients that
// predate it.
func TestKeyAddressOffsets(t *testing.T) {
	kd := val.NewTupleDescriptor(val.Type{Enc: val.StringAdaptiveEnc, Nullable: true})
	vd := val.NewTupleDescriptor(val.Type{Enc: val.StringAdaptiveEnc, Nullable: true})
	s := NewProllyMapSerializer(kd, vd, sharedPool)

	newTuple := func(field []byte) []byte {
		return val.NewTuple(sharedPool, field)
	}

	t.Run("inline values omit the field and stay readable by older clients", func(t *testing.T) {
		var keys, values [][]byte
		for i := 0; i < 4; i++ {
			keys = append(keys, newTuple(inlineAdaptiveValue(fmt.Sprintf("key-%d", i))))
			values = append(values, newTuple(inlineAdaptiveValue(fmt.Sprintf("value-%d", i))))
		}
		msg := s.Serialize(keys, values, nil, 0)

		var pm serial.ProllyTreeNode
		require.NoError(t, serial.InitProllyTreeNodeRoot(&pm, msg, serial.MessagePrefixSz))
		assert.Zero(t, pm.KeyAddressOffsetsLength())
		assert.Zero(t, pm.ValueAddressOffsetsLength())
		// The trailing unset field is trimmed from the vtable, so clients that predate
		// key_address_offsets can still read this node.
		assert.Less(t, int(pm.Table().NumFields()), serial.ProllyTreeNodeNumFields)
		assert.Empty(t, collectAddresses(t, msg))
	})

	t.Run("out-of-band key and value addresses are recorded and walked in leaf nodes", func(t *testing.T) {
		keyAddrs := []hash.Hash{testAddr(1), testAddr(2), testAddr(3)}
		valueAddrs := []hash.Hash{testAddr(11), testAddr(12)}
		keys := [][]byte{
			newTuple(outOfBandAdaptiveValue(3000, keyAddrs[0])),
			newTuple(outOfBandAdaptiveValue(4000, keyAddrs[1])),
			newTuple(inlineAdaptiveValue("small")),
			newTuple(outOfBandAdaptiveValue(5000, keyAddrs[2])),
		}
		values := [][]byte{
			newTuple(outOfBandAdaptiveValue(6000, valueAddrs[0])),
			newTuple(inlineAdaptiveValue("small")),
			newTuple(outOfBandAdaptiveValue(7000, valueAddrs[1])),
			newTuple(nil), // NULL
		}
		msg := s.Serialize(keys, values, nil, 0)

		var pm serial.ProllyTreeNode
		require.NoError(t, serial.InitProllyTreeNodeRoot(&pm, msg, serial.MessagePrefixSz))
		require.Equal(t, len(keyAddrs), pm.KeyAddressOffsetsLength())
		require.Equal(t, len(valueAddrs), pm.ValueAddressOffsetsLength())
		assert.Equal(t, serial.ProllyTreeNodeNumFields, int(pm.Table().NumFields()))

		// each recorded offset points at the address bytes within the key items buffer
		keyItems := pm.KeyItemsBytes()
		recorded := hash.NewHashSet()
		for i := 0; i < pm.KeyAddressOffsetsLength(); i++ {
			o := pm.KeyAddressOffsets(i)
			recorded.Insert(hash.New(keyItems[o : o+hash.ByteLen]))
		}
		for _, addr := range keyAddrs {
			assert.True(t, recorded.Has(addr), "missing key address %s", addr)
		}

		// the address walk visits every key and value address
		walked := collectAddresses(t, msg)
		require.Equal(t, len(keyAddrs)+len(valueAddrs), walked.Size())
		for _, addr := range append(keyAddrs, valueAddrs...) {
			assert.True(t, walked.Has(addr), "missing address %s", addr)
		}
	})

	t.Run("internal nodes record key addresses as bookkeeping, but they are not walked", func(t *testing.T) {
		keyAddrs := []hash.Hash{testAddr(1), testAddr(2)}
		keys := [][]byte{
			newTuple(outOfBandAdaptiveValue(3000, keyAddrs[0])),
			newTuple(outOfBandAdaptiveValue(4000, keyAddrs[1])),
		}
		child1, child2 := testAddr(21), testAddr(22)
		children := [][]byte{child1[:], child2[:]}
		msg := s.Serialize(keys, children, []uint64{10, 20}, 1)

		var pm serial.ProllyTreeNode
		require.NoError(t, serial.InitProllyTreeNodeRoot(&pm, msg, serial.MessagePrefixSz))
		require.Equal(t, len(keyAddrs), pm.KeyAddressOffsetsLength())
		assert.Equal(t, serial.ProllyTreeNodeNumFields, int(pm.Table().NumFields()))

		// each recorded offset points at the address bytes within the key items buffer
		keyItems := pm.KeyItemsBytes()
		recorded := hash.NewHashSet()
		for i := 0; i < pm.KeyAddressOffsetsLength(); i++ {
			o := pm.KeyAddressOffsets(i)
			recorded.Insert(hash.New(keyItems[o : o+hash.ByteLen]))
		}
		for _, addr := range keyAddrs {
			assert.True(t, recorded.Has(addr), "missing key address %s", addr)
		}

		// the walk visits only the child addresses
		walked := collectAddresses(t, msg)
		require.Equal(t, 2, walked.Size())
		assert.True(t, walked.Has(testAddr(21)))
		assert.True(t, walked.Has(testAddr(22)))
	})

	t.Run("internal nodes with inline boundary keys omit the field and stay readable by older clients", func(t *testing.T) {
		keys := [][]byte{
			newTuple(inlineAdaptiveValue("small-1")),
			newTuple(inlineAdaptiveValue("small-2")),
		}
		child1, child2 := testAddr(21), testAddr(22)
		children := [][]byte{child1[:], child2[:]}
		msg := s.Serialize(keys, children, []uint64{10, 20}, 1)

		var pm serial.ProllyTreeNode
		require.NoError(t, serial.InitProllyTreeNodeRoot(&pm, msg, serial.MessagePrefixSz))
		assert.Zero(t, pm.KeyAddressOffsetsLength())
		assert.Less(t, int(pm.Table().NumFields()), serial.ProllyTreeNodeNumFields)
	})
}
