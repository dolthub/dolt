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

package message

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	fb "github.com/dolthub/flatbuffers/v23/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
)

const (
	// These constants are mirrored from serial.VectorIndexNode
	// They are only as stable as the flatbuffers schema that define them.
	vectorIvfKeyItemBytesVOffset      fb.VOffsetT = 4
	vectorIvfKeyOffsetsVOffset        fb.VOffsetT = 6
	vectorIvfValueItemBytesVOffset    fb.VOffsetT = 8
	vectorIvfValueOffsetsVOffset      fb.VOffsetT = 10
	vectorIvfAddressArrayBytesVOffset fb.VOffsetT = 12
)

var vectorIvfFileID = []byte(serial.VectorIndexNodeFileID)

func NewVectorIndexSerializer(pool pool.BuffPool, logChunkSize uint8) VectorIndexSerializer {
	return VectorIndexSerializer{pool: pool, logChunkSize: logChunkSize}
}

type VectorIndexSerializer struct {
	pool         pool.BuffPool
	logChunkSize uint8
}

var _ Serializer = VectorIndexSerializer{}

func (s VectorIndexSerializer) Serialize(keys, values [][]byte, subtrees []uint64, level int) serial.Message {
	var (
		keyTups, keyOffs fb.UOffsetT
		valTups, valOffs fb.UOffsetT
		refArr, cardArr  fb.UOffsetT
	)

	keySz, valSz, bufSz := estimateVectorIndexSize(keys, values, subtrees)
	b := getFlatbufferBuilder(s.pool, bufSz)

	// serialize keys and offStart
	keyTups = writeItemBytes(b, keys, keySz)
	serial.VectorIndexNodeStartKeyOffsetsVector(b, len(keys)+1)
	keyOffs = writeItemOffsets32(b, keys, keySz)

	if level == 0 {
		// serialize value tuples for leaf nodes
		valTups = writeItemBytes(b, values, valSz)
		serial.VectorIndexNodeStartValueOffsetsVector(b, len(values)+1)
		valOffs = writeItemOffsets32(b, values, valSz)
	} else {
		// serialize child refs and subtree counts for internal nodes
		refArr = writeItemBytes(b, values, valSz)
		cardArr = writeCountArray(b, subtrees)
	}

	// populate the node's vtable
	serial.VectorIndexNodeStart(b)
	serial.VectorIndexNodeAddKeyItems(b, keyTups)
	serial.VectorIndexNodeAddKeyOffsets(b, keyOffs)
	if level == 0 {
		serial.VectorIndexNodeAddValueItems(b, valTups)
		serial.VectorIndexNodeAddValueOffsets(b, valOffs)
		serial.VectorIndexNodeAddTreeCount(b, uint64(len(keys)))
	} else {
		serial.VectorIndexNodeAddAddressArray(b, refArr)
		serial.VectorIndexNodeAddSubtreeCounts(b, cardArr)
		serial.VectorIndexNodeAddTreeCount(b, sumSubtrees(subtrees))
	}
	serial.VectorIndexNodeAddTreeLevel(b, uint8(level))
	serial.VectorIndexNodeAddLogChunkSize(b, s.logChunkSize)

	return serial.FinishMessage(b, serial.VectorIndexNodeEnd(b), vectorIvfFileID)
}

func getVectorIndexKeysAndValues(msg serial.Message) (keys, values ItemAccess, level, count uint16, err error) {
	keys.offsetSize = OFFSET_SIZE_32
	values.offsetSize = OFFSET_SIZE_32
	var pm serial.VectorIndexNode
	err = serial.InitVectorIndexNodeRoot(&pm, msg, serial.MessagePrefixSz)
	if err != nil {
		return
	}
	keys.bufStart = lookupVectorOffset(vectorIvfKeyItemBytesVOffset, pm.Table())
	keys.bufLen = uint32(pm.KeyItemsLength())
	keys.offStart = lookupVectorOffset(vectorIvfKeyOffsetsVOffset, pm.Table())
	keys.offLen = uint32(pm.KeyOffsetsLength() * uint16Size)

	count = uint16(keys.offLen/2) - 1
	level = uint16(pm.TreeLevel())

	vv := pm.ValueItemsBytes()
	if vv != nil {
		values.bufStart = lookupVectorOffset(vectorIvfValueItemBytesVOffset, pm.Table())
		values.bufLen = uint32(pm.ValueItemsLength())
		values.offStart = lookupVectorOffset(vectorIvfValueOffsetsVOffset, pm.Table())
		values.offLen = uint32(pm.ValueOffsetsLength() * uint16Size)
	} else {
		values.bufStart = lookupVectorOffset(vectorIvfAddressArrayBytesVOffset, pm.Table())
		values.bufLen = uint32(pm.AddressArrayLength())
		values.itemWidth = hash.ByteLen
	}
	return
}

func walkVectorIndexAddresses(ctx context.Context, msg serial.Message, cb func(ctx context.Context, addr hash.Hash) error) error {
	var pm serial.VectorIndexNode
	err := serial.InitVectorIndexNodeRoot(&pm, msg, serial.MessagePrefixSz)
	if err != nil {
		return err
	}
	arr := pm.AddressArrayBytes()
	for i := 0; i < len(arr)/hash.ByteLen; i++ {
		addr := hash.New(arr[i*addrSize : (i+1)*addrSize])
		if err := cb(ctx, addr); err != nil {
			return err
		}
	}

	return nil
}

func getVectorIndexCount(msg serial.Message) (uint16, error) {
	var pm serial.VectorIndexNode
	err := serial.InitVectorIndexNodeRoot(&pm, msg, serial.MessagePrefixSz)
	if err != nil {
		return 0, err
	}
	return uint16(pm.KeyOffsetsLength() - 1), nil
}

func getVectorIndexTreeLevel(msg serial.Message) (int, error) {
	var pm serial.VectorIndexNode
	err := serial.InitVectorIndexNodeRoot(&pm, msg, serial.MessagePrefixSz)
	if err != nil {
		return 0, fb.ErrTableHasUnknownFields
	}
	return int(pm.TreeLevel()), nil
}

func getVectorIndexTreeCount(msg serial.Message) (int, error) {
	var pm serial.VectorIndexNode
	err := serial.InitVectorIndexNodeRoot(&pm, msg, serial.MessagePrefixSz)
	if err != nil {
		return 0, fb.ErrTableHasUnknownFields
	}
	return int(pm.TreeCount()), nil
}

func getVectorIndexSubtrees(msg serial.Message) ([]uint64, error) {
	sz, err := getVectorIndexCount(msg)
	if err != nil {
		return nil, err
	}

	var pm serial.VectorIndexNode
	n := fb.GetUOffsetT(msg[serial.MessagePrefixSz:])
	err = pm.Init(msg, serial.MessagePrefixSz+n)
	if err != nil {
		return nil, err
	}

	counts := make([]uint64, sz)

	return decodeVarints(pm.SubtreeCountsBytes(), counts), nil
}

// estimateVectorIndexSize returns the exact Size of the tuple vectors for keys and values,
// and an estimate of the overall Size of the final flatbuffer.
func estimateVectorIndexSize(keys, values [][]byte, subtrees []uint64) (int, int, int) {
	var keySz, valSz, bufSz int
	for i := range keys {
		keySz += len(keys[i])
		valSz += len(values[i])
	}
	subtreesSz := len(subtrees) * binary.MaxVarintLen64

	// constraints enforced upstream
	if keySz > math.MaxUint32 {
		panic(fmt.Sprintf("key vector exceeds Size limit ( %d > %d )", keySz, math.MaxUint32))
	}
	if valSz > math.MaxUint32 {
		panic(fmt.Sprintf("value vector exceeds Size limit ( %d > %d )", valSz, math.MaxUint32))
	}

	// The following estimates the final size of the message based on the expected size of the flatbuffer components.
	bufSz += keySz + valSz               // tuples
	bufSz += subtreesSz                  // subtree counts
	bufSz += len(keys)*4 + len(values)*4 // offStart
	bufSz += 8 + 1 + 1 + 1               // metadata
	bufSz += 72                          // vtable (approx)
	bufSz += 100                         // padding?
	bufSz += serial.MessagePrefixSz

	return keySz, valSz, bufSz
}
