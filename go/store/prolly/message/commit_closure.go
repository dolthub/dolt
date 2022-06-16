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

	fb "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/val"
)

var commitClosureKeyOffsets []byte
var commitClosureValueOffsets []byte
var commitClosureEmptyValueBytes []byte

func init() {
	commitClosureKeyOffsets = make([]byte, (maxChunkSz/commitClosureKeyLength)*uint16Size)
	commitClosureValueOffsets = make([]byte, (maxChunkSz/commitClosureKeyLength)*uint16Size)
	commitClosureEmptyValueBytes = make([]byte, 0)

	buf := commitClosureKeyOffsets
	off := uint16(commitClosureKeyLength)
	for len(buf) > 0 {
		binary.LittleEndian.PutUint16(buf, off)
		buf = buf[uint16Size:]
		off += uint16(commitClosureKeyLength)
	}
}

func offsetsForCommitClosureKeys(buf []byte) []byte {
	cnt := len(buf) / commitClosureKeyLength
	return commitClosureKeyOffsets[:cnt*uint16Size]
}

func getCommitClosureKeys(msg Message) val.SlicedBuffer {
	var ret val.SlicedBuffer
	m := serial.GetRootAsCommitClosure(msg, messagePrefixSz)
	ret.Buf = m.KeyItemsBytes()
	ret.Offs = offsetsForCommitClosureKeys(ret.Buf)
	return ret
}

func getCommitClosureValues(msg Message) val.SlicedBuffer {
	var ret val.SlicedBuffer
	m := serial.GetRootAsCommitClosure(msg, messagePrefixSz)
	if m.AddressArrayLength() == 0 {
		ret.Buf = commitClosureEmptyValueBytes
		ret.Offs = commitClosureValueOffsets[:getCommitClosureCount(msg)*uint16Size]
		return ret
	}
	ret.Buf = m.AddressArrayBytes()
	ret.Offs = offsetsForAddressArray(ret.Buf)
	return ret
}

// uint64 + hash.
const commitClosureKeyLength = 8 + 20

func getCommitClosureCount(msg Message) uint16 {
	m := serial.GetRootAsCommitClosure(msg, messagePrefixSz)
	return uint16(m.KeyItemsLength() / commitClosureKeyLength)
}

func getCommitClosureTreeLevel(msg Message) int {
	m := serial.GetRootAsCommitClosure(msg, messagePrefixSz)
	return int(m.TreeLevel())
}

func getCommitClosureTreeCount(msg Message) int {
	m := serial.GetRootAsCommitClosure(msg, messagePrefixSz)
	return int(m.TreeCount())
}

func getCommitClosureSubtrees(msg Message) []uint64 {
	counts := make([]uint64, getCommitClosureCount(msg))
	m := serial.GetRootAsCommitClosure(msg, messagePrefixSz)
	return decodeVarints(m.SubtreeCountsBytes(), counts)
}

func walkCommitClosureAddresses(ctx context.Context, msg Message, cb func(ctx context.Context, addr hash.Hash) error) error {
	m := serial.GetRootAsCommitClosure(msg, messagePrefixSz)
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

type CommitClosureSerializer struct {
	Pool pool.BuffPool
}

var _ Serializer = CommitClosureSerializer{}

func (s CommitClosureSerializer) Serialize(keys, addrs [][]byte, subtrees []uint64, level int) Message {
	var keyArr, addrArr, cardArr fb.UOffsetT

	keySz, addrSz, totalSz := estimateCommitClosureSize(keys, addrs, subtrees)
	b := getFlatbufferBuilder(s.Pool, totalSz)

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

	return finishMessage(b, serial.CommitClosureEnd(b), commitClosureFileID)
}

func estimateCommitClosureSize(keys, addresses [][]byte, subtrees []uint64) (keySz, addrSz, totalSz int) {
	keySz = commitClosureKeyLength * len(keys)
	addrSz = addrSize * len(addresses)
	totalSz += keySz + addrSz
	totalSz += len(subtrees) * binary.MaxVarintLen64
	totalSz += 8 + 1 + 1 + 1
	totalSz += 72
	totalSz += messagePrefixSz
	return
}
