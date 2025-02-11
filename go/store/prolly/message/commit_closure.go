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

	fb "github.com/dolthub/flatbuffers/v23/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
)

var commitClosureEmptyValueBytes = []byte{}

const (
	commitClosureKeyItemBytesVOffset fb.VOffsetT = 4
	commitClosureAddressArrayVOffset fb.VOffsetT = 6
)

func getCommitClosureKeys(msg serial.Message) (ItemAccess, error) {
	var ret ItemAccess
	var m serial.CommitClosure
	err := serial.InitCommitClosureRoot(&m, msg, serial.MessagePrefixSz)
	if err != nil {
		return ret, err
	}
	ret.bufStart = lookupVectorOffset(commitClosureKeyItemBytesVOffset, m.Table())
	ret.bufLen = uint32(m.KeyItemsLength())
	ret.itemWidth = uint32(commitClosureKeyLength)
	return ret, nil
}

func getCommitClosureValues(msg serial.Message) (ItemAccess, error) {
	var ret ItemAccess
	var m serial.CommitClosure
	err := serial.InitCommitClosureRoot(&m, msg, serial.MessagePrefixSz)
	if err != nil {
		return ret, err
	}
	if m.AddressArrayLength() == 0 {
		ret.bufStart = 0
		ret.bufLen = 0
		ret.itemWidth = 0
	} else {
		ret.bufStart = lookupVectorOffset(commitClosureAddressArrayVOffset, m.Table())
		ret.bufLen = uint32(m.AddressArrayLength())
		ret.itemWidth = hash.ByteLen
	}
	return ret, nil
}

// uint64 + hash.
const commitClosureKeyLength = 8 + 20

func getCommitClosureCount(msg serial.Message) (uint16, error) {
	var m serial.CommitClosure
	err := serial.InitCommitClosureRoot(&m, msg, serial.MessagePrefixSz)
	if err != nil {
		return 0, err
	}
	return uint16(m.KeyItemsLength() / commitClosureKeyLength), nil
}

func getCommitClosureTreeLevel(msg serial.Message) (uint16, error) {
	var m serial.CommitClosure
	err := serial.InitCommitClosureRoot(&m, msg, serial.MessagePrefixSz)
	if err != nil {
		return 0, err
	}
	return uint16(m.TreeLevel()), nil
}

func getCommitClosureTreeCount(msg serial.Message) (int, error) {
	var m serial.CommitClosure
	err := serial.InitCommitClosureRoot(&m, msg, serial.MessagePrefixSz)
	if err != nil {
		return 0, err
	}
	return int(m.TreeCount()), nil
}

func getCommitClosureSubtrees(msg serial.Message) ([]uint64, error) {
	cnt, err := getCommitClosureCount(msg)
	if err != nil {
		return nil, err
	}
	counts := make([]uint64, cnt)
	m, err := serial.TryGetRootAsCommitClosure(msg, serial.MessagePrefixSz)
	if err != nil {
		return nil, err
	}
	return decodeVarints(m.SubtreeCountsBytes(), counts), nil
}

func walkCommitClosureAddresses(ctx context.Context, msg serial.Message, cb func(ctx context.Context, addr hash.Hash) error) error {
	m, err := serial.TryGetRootAsCommitClosure(msg, serial.MessagePrefixSz)
	if err != nil {
		return err
	}
	arr := m.AddressArrayBytes()
	for i := 0; i < len(arr)/hash.ByteLen; i++ {
		addr := hash.New(arr[i*addrSize : (i+1)*addrSize])
		if err := cb(ctx, addr); err != nil {
			return err
		}
	}
	if m.TreeLevel() == 0 {
		// If Level() == 0, walk addresses in keys.
		keybytes := m.KeyItemsBytes()
		for i := 8; i < len(keybytes); i += commitClosureKeyLength {
			addr := hash.New(keybytes[i : i+addrSize])
			if err := cb(ctx, addr); err != nil {
				return err
			}
		}
	}
	return nil
}

var commitClosureFileID = []byte(serial.CommitClosureFileID)

func NewCommitClosureSerializer(pool pool.BuffPool) CommitClosureSerializer {
	return CommitClosureSerializer{pool: pool}
}

type CommitClosureSerializer struct {
	pool pool.BuffPool
}

var _ Serializer = CommitClosureSerializer{}

func (s CommitClosureSerializer) Serialize(keys, addrs [][]byte, subtrees []uint64, level int) serial.Message {
	var keyArr, addrArr, cardArr fb.UOffsetT

	keySz, addrSz, totalSz := estimateCommitClosureSize(keys, addrs, subtrees)
	b := getFlatbufferBuilder(s.pool, totalSz)

	// keys
	keyArr = writeItemBytes(b, keys, keySz)

	if level > 0 {
		// addresses
		addrArr = writeItemBytes(b, addrs, addrSz)

		// subtree cardinalities
		cardArr = writeCountArray(b, subtrees)
	}

	serial.CommitClosureStart(b)
	serial.CommitClosureAddKeyItems(b, keyArr)

	if level > 0 {
		serial.CommitClosureAddAddressArray(b, addrArr)
		serial.CommitClosureAddSubtreeCounts(b, cardArr)
		serial.CommitClosureAddTreeCount(b, sumSubtrees(subtrees))
	} else {
		serial.CommitClosureAddTreeCount(b, uint64(len(keys)))
	}
	serial.CommitClosureAddTreeLevel(b, uint8(level))

	return serial.FinishMessage(b, serial.CommitClosureEnd(b), commitClosureFileID)
}

func estimateCommitClosureSize(keys, addresses [][]byte, subtrees []uint64) (keySz, addrSz, totalSz int) {
	keySz = commitClosureKeyLength * len(keys)
	addrSz = addrSize * len(addresses)
	totalSz += keySz + addrSz
	totalSz += len(subtrees) * binary.MaxVarintLen64
	totalSz += 8 + 1 + 1 + 1
	totalSz += 72
	totalSz += serial.MessagePrefixSz
	return
}
