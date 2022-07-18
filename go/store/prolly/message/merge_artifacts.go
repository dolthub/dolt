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
	// These constants are mirrored from serial.MergeArtifacts.KeyOffsets()
	// and serial.MergeArtifacts.ValueOffsets() respectively.
	// They are only as stable as the flatbuffers schema that define them.
	mergeArtifactKeyOffsetsVOffset   = 6
	mergeArtifactValueOffsetsVOffset = 14
)

var mergeArtifactFileID = []byte(serial.MergeArtifactsFileID)

type MergeArtifactSerializer struct {
	KeyDesc val.TupleDesc
	Pool    pool.BuffPool
}

var _ Serializer = MergeArtifactSerializer{}

func (s MergeArtifactSerializer) Serialize(keys, values [][]byte, subtrees []uint64, level int) Message {
	var (
		keyTups, keyOffs fb.UOffsetT
		valTups, valOffs fb.UOffsetT
		keyAddrOffs      fb.UOffsetT
		refArr, cardArr  fb.UOffsetT
	)

	keySz, valSz, bufSz := estimateMergeArtifactSize(keys, values, subtrees, s.KeyDesc.AddressTypeCount())
	b := getFlatbufferBuilder(s.Pool, bufSz)

	// serialize keys and offsets
	keyTups = writeItemBytes(b, keys, keySz)
	serial.MergeArtifactsStartKeyOffsetsVector(b, len(keys)-1)
	keyOffs = writeItemOffsets(b, keys, keySz)

	if level == 0 {
		// serialize value tuples for leaf nodes
		valTups = writeItemBytes(b, values, valSz)
		serial.MergeArtifactsStartValueOffsetsVector(b, len(values)-1)
		valOffs = writeItemOffsets(b, values, valSz)
		// serialize offsets of chunk addresses within |keyTups|
		if s.KeyDesc.AddressTypeCount() > 0 {
			serial.MergeArtifactsStartKeyAddressOffsetsVector(b, s.KeyDesc.AddressTypeCount())
			keyAddrOffs = writeAddressOffsets(b, keys, keySz, s.KeyDesc)
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
	serial.MergeArtifactsAddKeyType(b, serial.ArtifactTypeArifactFormatAlpha)
	serial.MergeArtifactsAddValueType(b, serial.ArtifactTypeArifactFormatAlpha)
	serial.MergeArtifactsAddTreeLevel(b, uint8(level))

	return finishMessage(b, serial.MergeArtifactsEnd(b), mergeArtifactFileID)
}

func getMergeArtifactKeys(msg Message) (keys val.SlicedBuffer) {
	ma := serial.GetRootAsMergeArtifacts(msg, messagePrefixSz)
	keys.Buf = ma.KeyItemsBytes()
	keys.Offs = getMergeArtifactKeyOffsets(ma)
	return
}

func getMergeArtifactValues(msg Message) (values val.SlicedBuffer) {
	ma := serial.GetRootAsMergeArtifacts(msg, messagePrefixSz)
	values.Buf = ma.ValueItemsBytes()
	values.Offs = getMergeArtifactValueOffsets(ma)
	return
}

func walkMergeArtifactAddresses(ctx context.Context, msg Message, cb func(ctx context.Context, addr hash.Hash) error) error {
	ma := serial.GetRootAsMergeArtifacts(msg, messagePrefixSz)
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
	assertFalse((arr != nil) && (arr2 != nil))
	return nil
}

func getMergeArtifactCount(msg Message) uint16 {
	ma := serial.GetRootAsMergeArtifacts(msg, messagePrefixSz)
	if ma.KeyItemsLength() == 0 {
		return 0
	}
	// zeroth offset ommitted from array
	return uint16(ma.KeyOffsetsLength() + 1)
}

func getMergeArtifactTreeLevel(msg Message) int {
	ma := serial.GetRootAsMergeArtifacts(msg, messagePrefixSz)
	return int(ma.TreeLevel())
}

func getMergeArtifactTreeCount(msg Message) int {
	ma := serial.GetRootAsMergeArtifacts(msg, messagePrefixSz)
	return int(ma.TreeCount())
}

func getMergeArtifactSubtrees(msg Message) []uint64 {
	counts := make([]uint64, getMergeArtifactCount(msg))
	ma := serial.GetRootAsMergeArtifacts(msg, messagePrefixSz)
	return decodeVarints(ma.SubtreeCountsBytes(), counts)
}

func getMergeArtifactKeyOffsets(ma *serial.MergeArtifacts) []byte {
	sz := ma.KeyOffsetsLength() * 2
	tab := ma.Table()
	vec := tab.Offset(mergeArtifactKeyOffsetsVOffset)
	start := int(tab.Vector(fb.UOffsetT(vec)))
	stop := start + sz
	return tab.Bytes[start:stop]
}

func getMergeArtifactValueOffsets(ma *serial.MergeArtifacts) []byte {
	sz := ma.ValueOffsetsLength() * 2
	tab := ma.Table()
	vec := tab.Offset(mergeArtifactValueOffsetsVOffset)
	start := int(tab.Vector(fb.UOffsetT(vec)))
	stop := start + sz
	return tab.Bytes[start:stop]
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

	// todo(andy): better estimates
	bufSz += keySz + valSz               // tuples
	bufSz += refCntSz                    // subtree counts
	bufSz += len(keys)*2 + len(values)*2 // offsets
	bufSz += 8 + 1 + 1 + 1               // metadata
	bufSz += 72                          // vtable (approx)
	bufSz += 100                         // padding?
	bufSz += keyAddrs * len(keys) * 2
	bufSz += messagePrefixSz

	return keySz, valSz, bufSz
}
