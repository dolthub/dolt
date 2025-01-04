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
	// These constants are mirrored from serial.ProllyTreeNode.KeyOffsets()
	// and serial.ProllyTreeNode.ValueOffsets() respectively.
	// They are only as stable as the flatbuffers schema that define them.
	prollyMapKeyItemBytesVOffset      fb.VOffsetT = 4
	prollyMapKeyOffsetsVOffset        fb.VOffsetT = 6
	prollyMapValueItemBytesVOffset    fb.VOffsetT = 10
	prollyMapValueOffsetsVOffset      fb.VOffsetT = 12
	prollyMapAddressArrayBytesVOffset fb.VOffsetT = 18
)

var prollyMapFileID = []byte(serial.ProllyTreeNodeFileID)

func NewProllyMapSerializer(valueDesc val.TupleDesc, pool pool.BuffPool) ProllyMapSerializer {
	return ProllyMapSerializer{valDesc: valueDesc, pool: pool}
}

type ProllyMapSerializer struct {
	valDesc val.TupleDesc
	pool    pool.BuffPool
}

var _ Serializer = ProllyMapSerializer{}

func (s ProllyMapSerializer) Serialize(keys, values [][]byte, subtrees []uint64, level int) serial.Message {
	var (
		keyTups, keyOffs fb.UOffsetT
		valTups, valOffs fb.UOffsetT
		valAddrOffs      fb.UOffsetT
		refArr, cardArr  fb.UOffsetT
	)

	keySz, valSz, bufSz := estimateProllyMapSize(keys, values, subtrees, s.valDesc.AddressFieldCount())
	b := getFlatbufferBuilder(s.pool, bufSz)

	// serialize keys and offStart
	keyTups = writeItemBytes(b, keys, keySz)
	serial.ProllyTreeNodeStartKeyOffsetsVector(b, len(keys)+1)
	keyOffs = writeItemOffsets(b, keys, keySz)

	if level == 0 {
		// serialize value tuples for leaf nodes
		valTups = writeItemBytes(b, values, valSz)
		serial.ProllyTreeNodeStartValueOffsetsVector(b, len(values)+1)
		valOffs = writeItemOffsets(b, values, valSz)
		// serialize offStart of chunk addresses within |valTups|
		if s.valDesc.AddressFieldCount() > 0 {
			serial.ProllyTreeNodeStartValueAddressOffsetsVector(b, countAddresses(values, s.valDesc))
			valAddrOffs = writeAddressOffsets(b, values, valSz, s.valDesc)
		}
	} else {
		// serialize child refs and subtree counts for internal nodes
		refArr = writeItemBytes(b, values, valSz)
		cardArr = writeCountArray(b, subtrees)
	}

	// populate the node's vtable
	serial.ProllyTreeNodeStart(b)
	serial.ProllyTreeNodeAddKeyItems(b, keyTups)
	serial.ProllyTreeNodeAddKeyOffsets(b, keyOffs)
	if level == 0 {
		serial.ProllyTreeNodeAddValueItems(b, valTups)
		serial.ProllyTreeNodeAddValueOffsets(b, valOffs)
		serial.ProllyTreeNodeAddTreeCount(b, uint64(len(keys)))
		serial.ProllyTreeNodeAddValueAddressOffsets(b, valAddrOffs)
	} else {
		serial.ProllyTreeNodeAddAddressArray(b, refArr)
		serial.ProllyTreeNodeAddSubtreeCounts(b, cardArr)
		serial.ProllyTreeNodeAddTreeCount(b, sumSubtrees(subtrees))
	}
	serial.ProllyTreeNodeAddKeyType(b, serial.ItemTypeTupleFormatAlpha)
	serial.ProllyTreeNodeAddValueType(b, serial.ItemTypeTupleFormatAlpha)
	serial.ProllyTreeNodeAddTreeLevel(b, uint8(level))

	return serial.FinishMessage(b, serial.ProllyTreeNodeEnd(b), prollyMapFileID)
}

func getProllyMapKeysAndValues(msg serial.Message) (keys, values ItemAccess, level, count uint16, err error) {
	var pm serial.ProllyTreeNode
	err = serial.InitProllyTreeNodeRoot(&pm, msg, serial.MessagePrefixSz)
	if err != nil {
		return
	}
	keys.bufStart = lookupVectorOffset(prollyMapKeyItemBytesVOffset, pm.Table())
	keys.bufLen = uint32(pm.KeyItemsLength())
	keys.offStart = lookupVectorOffset(prollyMapKeyOffsetsVOffset, pm.Table())
	keys.offLen = uint32(pm.KeyOffsetsLength() * uint16Size)

	count = uint16(keys.offLen/2) - 1
	level = uint16(pm.TreeLevel())

	vv := pm.ValueItemsBytes()
	if vv != nil {
		values.bufStart = lookupVectorOffset(prollyMapValueItemBytesVOffset, pm.Table())
		values.bufLen = uint32(pm.ValueItemsLength())
		values.offStart = lookupVectorOffset(prollyMapValueOffsetsVOffset, pm.Table())
		values.offLen = uint32(pm.ValueOffsetsLength() * uint16Size)
	} else {
		values.bufStart = lookupVectorOffset(prollyMapAddressArrayBytesVOffset, pm.Table())
		values.bufLen = uint32(pm.AddressArrayLength())
		values.itemWidth = hash.ByteLen
	}
	return
}

func walkProllyMapAddresses(ctx context.Context, msg serial.Message, cb func(ctx context.Context, addr hash.Hash) error) error {
	var pm serial.ProllyTreeNode
	err := serial.InitProllyTreeNodeRoot(&pm, msg, serial.MessagePrefixSz)
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

	cnt := pm.ValueAddressOffsetsLength()
	arr2 := pm.ValueItemsBytes()
	for i := 0; i < cnt; i++ {
		o := pm.ValueAddressOffsets(i)
		addr := hash.New(arr2[o : o+addrSize])
		if err := cb(ctx, addr); err != nil {
			return err
		}
	}
	assertFalse((arr != nil) && (arr2 != nil), "cannot WalkAddresses for ProllyTreeNode with both AddressArray and ValueAddressOffsets")
	return nil
}

func getProllyMapCount(msg serial.Message) (uint16, error) {
	var pm serial.ProllyTreeNode
	err := serial.InitProllyTreeNodeRoot(&pm, msg, serial.MessagePrefixSz)
	if err != nil {
		return 0, err
	}
	return uint16(pm.KeyOffsetsLength() - 1), nil
}

func getProllyMapTreeLevel(msg serial.Message) (int, error) {
	var pm serial.ProllyTreeNode
	err := serial.InitProllyTreeNodeRoot(&pm, msg, serial.MessagePrefixSz)
	if err != nil {
		return 0, fb.ErrTableHasUnknownFields
	}
	return int(pm.TreeLevel()), nil
}

func getProllyMapTreeCount(msg serial.Message) (int, error) {
	var pm serial.ProllyTreeNode
	err := serial.InitProllyTreeNodeRoot(&pm, msg, serial.MessagePrefixSz)
	if err != nil {
		return 0, fb.ErrTableHasUnknownFields
	}
	return int(pm.TreeCount()), nil
}

func getProllyMapSubtrees(msg serial.Message) ([]uint64, error) {
	sz, err := getProllyMapCount(msg)
	if err != nil {
		return nil, err
	}

	var pm serial.ProllyTreeNode
	n := fb.GetUOffsetT(msg[serial.MessagePrefixSz:])
	pm.Init(msg, serial.MessagePrefixSz+n)
	if serial.ProllyTreeNodeNumFields < pm.Table().NumFields() {
		return nil, fb.ErrTableHasUnknownFields
	}

	counts := make([]uint64, sz)

	return decodeVarints(pm.SubtreeCountsBytes(), counts), nil
}

// estimateProllyMapSize returns the exact Size of the tuple vectors for keys and values,
// and an estimate of the overall Size of the final flatbuffer.
func estimateProllyMapSize(keys, values [][]byte, subtrees []uint64, valAddrsCnt int) (int, int, int) {
	var keySz, valSz, bufSz int
	for i := range keys {
		keySz += len(keys[i])
		valSz += len(values[i])
	}
	subtreesSz := len(subtrees) * binary.MaxVarintLen64

	// constraints enforced upstream
	if keySz > int(MaxVectorOffset) {
		panic(fmt.Sprintf("key vector exceeds Size limit ( %d > %d )", keySz, MaxVectorOffset))
	}
	if valSz > int(MaxVectorOffset) {
		panic(fmt.Sprintf("value vector exceeds Size limit ( %d > %d )", valSz, MaxVectorOffset))
	}

	bufSz += keySz + valSz               // tuples
	bufSz += subtreesSz                  // subtree counts
	bufSz += len(keys)*2 + len(values)*2 // offStart
	bufSz += 8 + 1 + 1 + 1               // metadata
	bufSz += 72                          // vtable (approx)
	bufSz += 100                         // padding?
	bufSz += valAddrsCnt * len(values) * 2
	bufSz += serial.MessagePrefixSz

	return keySz, valSz, bufSz
}
