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

	fb "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/val"
)

const (
	// These constants are mirrored from serial.ProllyTreeNode.KeyOffsets()
	// and serial.ProllyTreeNode.ValueOffsets() respectively.
	// They are only as stable as the flatbuffers schema that define them.
	prollyMapKeyOffsetsVOffset   = 6
	prollyMapValueOffsetsVOffset = 12
)

var prollyMapFileID = []byte(serial.ProllyTreeNodeFileID)

type ProllyMapSerializer struct {
	Pool pool.BuffPool
}

var _ Serializer = ProllyMapSerializer{}

func (s ProllyMapSerializer) Serialize(keys, values [][]byte, subtrees []uint64, level int) Message {
	var (
		keyTups, keyOffs fb.UOffsetT
		valTups, valOffs fb.UOffsetT
		refArr, cardArr  fb.UOffsetT
	)

	keySz, valSz, bufSz := estimateProllyMapSize(keys, values, subtrees)
	b := getFlatbufferBuilder(s.Pool, bufSz)

	// serialize keys and offsets
	keyTups = writeItemBytes(b, keys, keySz)
	serial.ProllyTreeNodeStartKeyOffsetsVector(b, len(keys)-1)
	keyOffs = writeItemOffsets(b, keys, keySz)

	if level == 0 {
		// serialize value tuples for leaf nodes
		valTups = writeItemBytes(b, values, valSz)
		serial.ProllyTreeNodeStartValueOffsetsVector(b, len(values)-1)
		valOffs = writeItemOffsets(b, values, valSz)
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
	} else {
		serial.ProllyTreeNodeAddAddressArray(b, refArr)
		serial.ProllyTreeNodeAddSubtreeCounts(b, cardArr)
		serial.ProllyTreeNodeAddTreeCount(b, sumSubtrees(subtrees))
	}
	serial.ProllyTreeNodeAddKeyType(b, serial.ItemTypeTupleFormatAlpha)
	serial.ProllyTreeNodeAddValueType(b, serial.ItemTypeTupleFormatAlpha)
	serial.ProllyTreeNodeAddTreeLevel(b, uint8(level))

	return finishMessage(b, serial.ProllyTreeNodeEnd(b), prollyMapFileID)
}

func getProllyMapKeysAndValues(msg Message) (keys, values val.SlicedBuffer, cnt uint16) {
	pm := serial.GetRootAsProllyTreeNode(msg, messagePrefixSz)

	keys.Buf = pm.KeyItemsBytes()
	keys.Offs = getProllyMapKeyOffsets(pm)
	if len(keys.Buf) == 0 {
		cnt = 0
	} else {
		cnt = 1 + uint16(len(keys.Offs)/2)
	}

	vv := pm.ValueItemsBytes()
	if vv != nil {
		values.Buf = vv
		values.Offs = getProllyMapValueOffsets(pm)
	} else {
		values.Buf = pm.AddressArrayBytes()
		values.Offs = offsetsForAddressArray(values.Buf)
	}

	return
}

func walkProllyMapAddresses(ctx context.Context, msg Message, cb func(ctx context.Context, addr hash.Hash) error) error {
	pm := serial.GetRootAsProllyTreeNode(msg, messagePrefixSz)
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
		addr := hash.New(arr[o : o+addrSize])
		if err := cb(ctx, addr); err != nil {
			return err
		}
	}
	assertFalse((arr != nil) && (arr2 != nil))
	return nil
}

func getProllyMapCount(msg Message) uint16 {
	pm := serial.GetRootAsProllyTreeNode(msg, messagePrefixSz)
	if pm.KeyItemsLength() == 0 {
		return 0
	}
	// zeroth offset ommitted from array
	return uint16(pm.KeyOffsetsLength() + 1)
}

func getProllyMapTreeLevel(msg Message) int {
	pm := serial.GetRootAsProllyTreeNode(msg, messagePrefixSz)
	return int(pm.TreeLevel())
}

func getProllyMapTreeCount(msg Message) int {
	pm := serial.GetRootAsProllyTreeNode(msg, messagePrefixSz)
	return int(pm.TreeCount())
}

func getProllyMapSubtrees(msg Message) []uint64 {
	counts := make([]uint64, getProllyMapCount(msg))
	pm := serial.GetRootAsProllyTreeNode(msg, messagePrefixSz)
	return decodeVarints(pm.SubtreeCountsBytes(), counts)
}

func getProllyMapKeyOffsets(pm *serial.ProllyTreeNode) []byte {
	sz := pm.KeyOffsetsLength() * 2
	tab := pm.Table()
	vec := tab.Offset(prollyMapKeyOffsetsVOffset)
	start := int(tab.Vector(fb.UOffsetT(vec)))
	stop := start + sz

	return tab.Bytes[start:stop]
}

func getProllyMapValueOffsets(pm *serial.ProllyTreeNode) []byte {
	sz := pm.ValueOffsetsLength() * 2
	tab := pm.Table()
	vec := tab.Offset(prollyMapValueOffsetsVOffset)
	start := int(tab.Vector(fb.UOffsetT(vec)))
	stop := start + sz

	return tab.Bytes[start:stop]
}

// estimateProllyMapSize returns the exact Size of the tuple vectors for keys and values,
// and an estimate of the overall Size of the final flatbuffer.
func estimateProllyMapSize(keys, values [][]byte, subtrees []uint64) (keySz, valSz, bufSz int) {
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

	// todo(andy): better estimates
	bufSz += keySz + valSz               // tuples
	bufSz += refCntSz                    // subtree counts
	bufSz += len(keys)*2 + len(values)*2 // offsets
	bufSz += 8 + 1 + 1 + 1               // metadata
	bufSz += 72                          // vtable (approx)
	bufSz += 100                         // padding?
	bufSz += messagePrefixSz

	return
}
