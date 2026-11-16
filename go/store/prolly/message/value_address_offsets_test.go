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
	"testing"

	fb "github.com/dolthub/flatbuffers/v23/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

// mergeArtifactsNumFieldsBeforeValueAddresses is serial.MergeArtifactsNumFields in clients that predate
// value_address_offsets. Those clients reject a MergeArtifacts message whose vtable has more fields.
const mergeArtifactsNumFieldsBeforeValueAddresses = 9

// readableByOlderClients returns whether a client that predates value_address_offsets accepts |msg|, by the check its
// generated serial.MergeArtifacts.Init makes.
func readableByOlderClients(t *testing.T, msg serial.Message) bool {
	var ma serial.MergeArtifacts
	require.NoError(t, serial.InitMergeArtifactsRoot(&ma, msg, serial.MessagePrefixSz))
	return int(ma.Table().NumFields()) <= mergeArtifactsNumFieldsBeforeValueAddresses
}

// TestMergeArtifactValueAddressOffsets exercises the value_address_offsets field of MergeArtifacts: leaf nodes with
// artifact values holding the address of a conflict's base record it, WalkAddresses visits it, and nodes without any
// such address omit the field, so that older clients can still read them.
func TestMergeArtifactValueAddressOffsets(t *testing.T) {
	// An artifact key is the source key, the source commit, the artifact type and a violation info hash. Its value is
	// the JSON metadata and the address of the conflict's base.
	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int32Enc},
		val.Type{Enc: val.CommitAddrEnc},
		val.Type{Enc: val.Uint8Enc},
		val.Type{Enc: val.ByteStringEnc},
	)
	vd := val.NewTupleDescriptor(
		val.Type{Enc: val.JSONEnc},
		val.Type{Enc: val.CommitAddrEnc, Nullable: true},
	)
	s := NewMergeArtifactSerializer(kd, vd, sharedPool)

	theirs := testAddr(1)
	key := func(pk int32) []byte {
		pkb := make([]byte, 4)
		pkb[0] = byte(pk)
		return val.NewTuple(sharedPool, pkb, theirs[:], []byte{1}, make([]byte, hash.ByteLen))
	}
	value := func(base *hash.Hash) []byte {
		if base == nil {
			return val.NewTuple(sharedPool, []byte(`{"bc":"x"}`))
		}
		return val.NewTuple(sharedPool, []byte(`{"bc":"x"}`), base[:])
	}

	t.Run("values without a base address omit the field and stay readable by older clients", func(t *testing.T) {
		msg := s.Serialize([][]byte{key(1), key(2)}, [][]byte{value(nil), value(nil)}, nil, 0)

		var ma serial.MergeArtifacts
		require.NoError(t, serial.InitMergeArtifactsRoot(&ma, msg, serial.MessagePrefixSz))
		assert.Zero(t, ma.ValueAddressOffsetsLength())
		assert.True(t, readableByOlderClients(t, msg))
		walked := collectAddresses(t, msg)
		assert.Equal(t, 1, walked.Size())
		assert.True(t, walked.Has(theirs))
	})

	t.Run("a base address is recorded and walked, and older clients reject the node", func(t *testing.T) {
		base := testAddr(2)
		msg := s.Serialize([][]byte{key(1), key(2), key(3)}, [][]byte{value(&base), value(nil), value(&base)}, nil, 0)

		var ma serial.MergeArtifacts
		require.NoError(t, serial.InitMergeArtifactsRoot(&ma, msg, serial.MessagePrefixSz))
		require.Equal(t, 2, ma.ValueAddressOffsetsLength())
		assert.Equal(t, serial.MergeArtifactsNumFields, int(ma.Table().NumFields()))
		assert.False(t, readableByOlderClients(t, msg))

		valueItems := ma.ValueItemsBytes()
		for i := 0; i < ma.ValueAddressOffsetsLength(); i++ {
			o := ma.ValueAddressOffsets(i)
			assert.Equal(t, base, hash.New(valueItems[o:o+hash.ByteLen]))
		}

		walked := collectAddresses(t, msg)
		assert.Equal(t, 2, walked.Size())
		assert.True(t, walked.Has(theirs))
		assert.True(t, walked.Has(base))
	})

	t.Run("values written by older clients lack the base address", func(t *testing.T) {
		// Older clients' artifact values have only the JSON metadata.
		oldVD := val.NewTupleDescriptor(val.Type{Enc: val.JSONEnc})
		old := NewMergeArtifactSerializer(kd, oldVD, sharedPool).Serialize([][]byte{key(1)}, [][]byte{value(nil)}, nil, 0)
		cur := s.Serialize([][]byte{key(1)}, [][]byte{value(nil)}, nil, 0)
		assert.Equal(t, []byte(old), []byte(cur))
		assert.Nil(t, vd.GetField(1, val.Tuple(value(nil))))
	})

	t.Run("internal nodes never record value addresses", func(t *testing.T) {
		left, right := testAddr(3), testAddr(4)
		msg := s.Serialize([][]byte{key(1), key(2)}, [][]byte{left[:], right[:]}, []uint64{1, 1}, 1)
		assert.True(t, readableByOlderClients(t, msg))
		walked := collectAddresses(t, msg)
		assert.True(t, walked.Has(testAddr(3)))
		assert.True(t, walked.Has(testAddr(4)))
	})

	t.Run("an older client's check rejects the node with unknown fields", func(t *testing.T) {
		base := testAddr(2)
		msg := s.Serialize([][]byte{key(1)}, [][]byte{value(&base)}, nil, 0)
		var ma serial.MergeArtifacts
		require.NoError(t, serial.InitMergeArtifactsRoot(&ma, msg, serial.MessagePrefixSz))
		// The check of serial.MergeArtifacts.Init, with the field count of older clients.
		var err error
		if mergeArtifactsNumFieldsBeforeValueAddresses < int(ma.Table().NumFields()) {
			err = fb.ErrTableHasUnknownFields
		}
		assert.ErrorIs(t, err, fb.ErrTableHasUnknownFields)
	})
}
