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

	fb "github.com/dolthub/flatbuffers/v23/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/val"
)

const (
	// These constants are mirrored from serial.MergeArtifacts.KeyOffsets()
	// and serial.MergeArtifacts.ValueOffsets() respectively.
	// They are only as stable as the flatbuffers schema that define them.

	mergeArtifactKeyItemBytesVOffset   fb.VOffsetT = 4
	mergeArtifactKeyOffsetsVOffset     fb.VOffsetT = 6
	mergeArtifactValueItemBytesVOffset fb.VOffsetT = 10
	mergeArtifactValueOffsetsVOffset   fb.VOffsetT = 12
	mergeArtifactAddressArrayVOffset   fb.VOffsetT = 14
)

var mergeArtifactFileID = []byte(serial.MergeArtifactsFileID)

func NewMergeArtifactSerializer(keyDesc val.TupleDesc, pool pool.BuffPool) MergeArtifactSerializer {
	return MergeArtifactSerializer{
		keyDesc: keyDesc,
		pool:    pool,
	}
}

type MergeArtifactSerializer struct {
	keyDesc val.TupleDesc
	pool    pool.BuffPool
}

var _ Serializer = MergeArtifactSerializer{}

func (s MergeArtifactSerializer) Serialize(keys, values [][]byte, subtrees []uint64, level int) serial.Message {
	var (
		keyTups, keyOffs fb.UOffsetT
		valTups, valOffs fb.UOffsetT
		keyAddrOffs      fb.UOffsetT
		refArr, cardArr  fb.UOffsetT
	)

	keySz, valSz, bufSz := estimateMergeArtifactSize(keys, values, subtrees, s.keyDesc.AddressFieldCount())
	b := getFlatbufferBuilder(s.pool, bufSz)

	// serialize keys and offStart
	keyTups = writeItemBytes(b, keys, keySz)
	serial.MergeArtifactsStartKeyOffsetsVector(b, len(keys)+1)
	keyOffs = writeItemOffsets(b, keys, keySz)

	if level == 0 {
		// serialize value tuples for leaf nodes
		valTups = writeItemBytes(b, values, valSz)
		serial.MergeArtifactsStartValueOffsetsVector(b, len(values)+1)
		valOffs = writeItemOffsets(b, values, valSz)
		// serialize offStart of chunk addresses within |keyTups|
		if s.keyDesc.AddressFieldCount() > 0 {
			serial.MergeArtifactsStartKeyAddressOffsetsVector(b, countAddresses(keys, s.keyDesc))
			keyAddrOffs = writeAddressOffsets(b, keys, keySz, s.keyDesc)
		}
	} else {
		// serialize child refs and subtree counts for internal nodes
		refArr = writeItemBytes(b, values, valSz)
		cardArr = writeCountArray(b, subtrees)
	}

	// populate the node's vtable
	serial.MergeArtifactsStart(b)
	serial.MergeArtifactsAddKeyItems(b, keyTups)
	serial.MergeArtifactsAddKeyOffsets(b, keyOffs)
	if level == 0 {
		serial.MergeArtifactsAddValueItems(b, valTups)
		serial.MergeArtifactsAddValueOffsets(b, valOffs)
		serial.MergeArtifactsAddTreeCount(b, uint64(len(keys)))
		serial.MergeArtifactsAddKeyAddressOffsets(b, keyAddrOffs)
	} else {
		serial.MergeArtifactsAddAddressArray(b, refArr)
		serial.MergeArtifactsAddSubtreeCounts(b, cardArr)
		serial.MergeArtifactsAddTreeCount(b, sumSubtrees(subtrees))
	}
	serial.MergeArtifactsAddTreeLevel(b, uint8(level))

	return serial.FinishMessage(b, serial.MergeArtifactsEnd(b), mergeArtifactFileID)
}

func getArtifactMapKeysAndValues(msg serial.Message) (keys, values ItemAccess, level, count uint16, err error) {
	var ma serial.MergeArtifacts
	err = serial.InitMergeArtifactsRoot(&ma, msg, serial.MessagePrefixSz)
	if err != nil {
		return
	}
	keys.bufStart = lookupVectorOffset(mergeArtifactKeyItemBytesVOffset, ma.Table())
	keys.bufLen = uint32(ma.KeyItemsLength())
	keys.offStart = lookupVectorOffset(mergeArtifactKeyOffsetsVOffset, ma.Table())
	keys.offLen = uint32(ma.KeyOffsetsLength() * uint16Size)

	count = uint16(keys.offLen/2) - 1
	level = uint16(ma.TreeLevel())

	vv := ma.ValueItemsBytes()
	if vv != nil {
		values.bufStart = lookupVectorOffset(mergeArtifactValueItemBytesVOffset, ma.Table())
		values.bufLen = uint32(ma.ValueItemsLength())
		values.offStart = lookupVectorOffset(mergeArtifactValueOffsetsVOffset, ma.Table())
		values.offLen = uint32(ma.ValueOffsetsLength() * uint16Size)
	} else {
		values.bufStart = lookupVectorOffset(mergeArtifactAddressArrayVOffset, ma.Table())
		values.bufLen = uint32(ma.AddressArrayLength())
		values.itemWidth = hash.ByteLen
	}
	return
}

func walkMergeArtifactAddresses(ctx context.Context, msg serial.Message, cb func(ctx context.Context, addr hash.Hash) error) error {
	var ma serial.MergeArtifacts
	err := serial.InitMergeArtifactsRoot(&ma, msg, serial.MessagePrefixSz)
	if err != nil {
		return err
	}
	arr := ma.AddressArrayBytes()
	for i := 0; i < len(arr)/hash.ByteLen; i++ {
		addr := hash.New(arr[i*addrSize : (i+1)*addrSize])
		if err := cb(ctx, addr); err != nil {
			return err
		}
	}

	cnt := ma.KeyAddressOffsetsLength()
	arr2 := ma.KeyItemsBytes()
	for i := 0; i < cnt; i++ {
		o := ma.KeyAddressOffsets(i)
		addr := hash.New(arr2[o : o+addrSize])
		if err := cb(ctx, addr); err != nil {
			return err
		}
	}

	return nil
}

func getMergeArtifactCount(msg serial.Message) (uint16, error) {
	var ma serial.MergeArtifacts
	err := serial.InitMergeArtifactsRoot(&ma, msg, serial.MessagePrefixSz)
	if err != nil {
		return 0, err
	}
	if ma.KeyItemsLength() == 0 {
		return 0, nil
	}
	// zeroth offset omitted from array
	return uint16(ma.KeyOffsetsLength() + 1), nil
}

func getMergeArtifactTreeLevel(msg serial.Message) (int, error) {
	var ma serial.MergeArtifacts
	err := serial.InitMergeArtifactsRoot(&ma, msg, serial.MessagePrefixSz)
	if err != nil {
		return 0, err
	}
	return int(ma.TreeLevel()), nil
}

func getMergeArtifactTreeCount(msg serial.Message) (int, error) {
	var ma serial.MergeArtifacts
	err := serial.InitMergeArtifactsRoot(&ma, msg, serial.MessagePrefixSz)
	if err != nil {
		return 0, err
	}
	return int(ma.TreeCount()), nil
}

func getMergeArtifactSubtrees(msg serial.Message) ([]uint64, error) {
	sz, err := getMergeArtifactCount(msg)
	if err != nil {
		return nil, err
	}
	counts := make([]uint64, sz)
	var ma serial.MergeArtifacts
	err = serial.InitMergeArtifactsRoot(&ma, msg, serial.MessagePrefixSz)
	if err != nil {
		return nil, err
	}
	return decodeVarints(ma.SubtreeCountsBytes(), counts), nil
}

// estimateMergeArtifact>Size returns the exact Size of the tuple vectors for keys and values,
// and an estimate of the overall Size of the final flatbuffer.
func estimateMergeArtifactSize(keys, values [][]byte, subtrees []uint64, keyAddrs int) (int, int, int) {
	var keySz, valSz, bufSz int
	for i := range keys {
		keySz += len(keys[i])
		valSz += len(values[i])
	}
	refCntSz := len(subtrees) * binary.MaxVarintLen64

	// constraints enforced upstream
	if keySz > int(MaxVectorOffset) {
		panic(fmt.Sprintf("key vector exceeds Size limit ( %d > %d )", keySz, MaxVectorOffset))
	}
	if valSz > int(MaxVectorOffset) {
		panic(fmt.Sprintf("value vector exceeds Size limit ( %d > %d )", valSz, MaxVectorOffset))
	}

	bufSz += keySz + valSz               // tuples
	bufSz += refCntSz                    // subtree counts
	bufSz += len(keys)*2 + len(values)*2 // offStart
	bufSz += 8 + 1 + 1 + 1               // metadata
	bufSz += 72                          // vtable (approx)
	bufSz += 100                         // padding?
	bufSz += keyAddrs * len(keys) * 2
	bufSz += serial.MessagePrefixSz

	return keySz, valSz, bufSz
}
