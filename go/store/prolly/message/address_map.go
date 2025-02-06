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

package message

import (
	"context"
	"encoding/binary"

	fb "github.com/dolthub/flatbuffers/v23/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
)

const (
	// This constant is mirrored from serial.AddressMap.KeyOffsetsLength()
	// It is only as stable as the flatbuffers schema that defines it.
	addressMapKeyItemsBytesVOffset   fb.VOffsetT = 4
	addressMapKeyItemsOffsetsVOffset fb.VOffsetT = 6
	addressMapAddressArrayVOffset    fb.VOffsetT = 8
)

var addressMapFileID = []byte(serial.AddressMapFileID)

func NewAddressMapSerializer(pool pool.BuffPool) AddressMapSerializer {
	return AddressMapSerializer{pool: pool}
}

type AddressMapSerializer struct {
	pool pool.BuffPool
}

var _ Serializer = AddressMapSerializer{}

func (s AddressMapSerializer) Serialize(keys, addrs [][]byte, subtrees []uint64, level int) serial.Message {
	var (
		keyArr, keyOffs  fb.UOffsetT
		addrArr, cardArr fb.UOffsetT
	)

	keySz, addrSz, totalSz := estimateAddressMapSize(keys, addrs, subtrees)
	b := getFlatbufferBuilder(s.pool, totalSz)

	// keys
	keyArr = writeItemBytes(b, keys, keySz)
	serial.AddressMapStartKeyOffsetsVector(b, len(keys)+1)
	keyOffs = writeItemOffsets(b, keys, keySz)

	// addresses
	addrArr = writeItemBytes(b, addrs, addrSz)

	// subtree cardinalities
	if level > 0 {
		cardArr = writeCountArray(b, subtrees)
	}

	serial.AddressMapStart(b)
	serial.AddressMapAddKeyItems(b, keyArr)
	serial.AddressMapAddKeyOffsets(b, keyOffs)
	serial.AddressMapAddAddressArray(b, addrArr)

	if level > 0 {
		serial.AddressMapAddSubtreeCounts(b, cardArr)
		serial.AddressMapAddTreeCount(b, sumSubtrees(subtrees))
	} else {
		serial.AddressMapAddTreeCount(b, uint64(len(keys)))
	}
	serial.AddressMapAddTreeLevel(b, uint8(level))

	return serial.FinishMessage(b, serial.AddressMapEnd(b), addressMapFileID)
}

func getAddressMapKeys(msg serial.Message) (keys ItemAccess, err error) {
	var am serial.AddressMap
	err = serial.InitAddressMapRoot(&am, msg, serial.MessagePrefixSz)
	if err != nil {
		return keys, err
	}
	keys.bufStart = lookupVectorOffset(addressMapKeyItemsBytesVOffset, am.Table())
	keys.bufLen = uint32(am.KeyItemsLength())
	keys.offStart = lookupVectorOffset(addressMapKeyItemsOffsetsVOffset, am.Table())
	keys.offLen = uint32(am.KeyOffsetsLength() * uint16Size)
	return
}

func getAddressMapValues(msg serial.Message) (values ItemAccess, err error) {
	var am serial.AddressMap
	err = serial.InitAddressMapRoot(&am, msg, serial.MessagePrefixSz)
	if err != nil {
		return values, err
	}
	values.bufStart = lookupVectorOffset(addressMapAddressArrayVOffset, am.Table())
	values.bufLen = uint32(am.AddressArrayLength())
	values.itemWidth = hash.ByteLen
	return
}

func walkAddressMapAddresses(ctx context.Context, msg serial.Message, cb func(ctx context.Context, addr hash.Hash) error) error {
	var am serial.AddressMap
	err := serial.InitAddressMapRoot(&am, msg, serial.MessagePrefixSz)
	if err != nil {
		return err
	}
	arr := am.AddressArrayBytes()
	for i := 0; i < len(arr)/hash.ByteLen; i++ {
		addr := hash.New(arr[i*addrSize : (i+1)*addrSize])
		if err := cb(ctx, addr); err != nil {
			return err
		}
	}
	return nil
}

func getAddressMapCount(msg serial.Message) (uint16, error) {
	var am serial.AddressMap
	err := serial.InitAddressMapRoot(&am, msg, serial.MessagePrefixSz)
	if err != nil {
		return 0, err
	}
	return uint16(am.KeyOffsetsLength() - 1), nil
}

func getAddressMapTreeLevel(msg serial.Message) (uint16, error) {
	var am serial.AddressMap
	err := serial.InitAddressMapRoot(&am, msg, serial.MessagePrefixSz)
	if err != nil {
		return 0, err
	}
	return uint16(am.TreeLevel()), nil
}

func getAddressMapTreeCount(msg serial.Message) (int, error) {
	var am serial.AddressMap
	err := serial.InitAddressMapRoot(&am, msg, serial.MessagePrefixSz)
	if err != nil {
		return 0, err
	}
	return int(am.TreeCount()), nil
}

func getAddressMapSubtrees(msg serial.Message) ([]uint64, error) {
	sz, err := getAddressMapCount(msg)
	if err != nil {
		return nil, err
	}
	counts := make([]uint64, sz)
	var am serial.AddressMap
	err = serial.InitAddressMapRoot(&am, msg, serial.MessagePrefixSz)
	if err != nil {
		return nil, err
	}
	return decodeVarints(am.SubtreeCountsBytes(), counts), nil
}

func estimateAddressMapSize(keys, addresses [][]byte, subtrees []uint64) (keySz, addrSz, totalSz int) {
	assertTrue(len(keys) == len(addresses), "num keys != num addresses for AddressMap")
	for i := range keys {
		keySz += len(keys[i])
		addrSz += len(addresses[i])
	}
	totalSz += keySz + addrSz
	totalSz += len(keys) * uint16Size
	totalSz += len(subtrees) * binary.MaxVarintLen64
	totalSz += 8 + 1 + 1 + 1
	totalSz += 72
	totalSz += serial.MessagePrefixSz
	return
}

func getAddressMapKeysAndValues(msg serial.Message) (keys, values ItemAccess, level, count uint16, err error) {
	keys, err = getAddressMapKeys(msg)
	if err != nil {
		return
	}
	values, err = getAddressMapValues(msg)
	if err != nil {
		return
	}
	level, err = getAddressMapTreeLevel(msg)
	if err != nil {
		return
	}
	count, err = getAddressMapCount(msg)
	return
}
