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

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
)

var blobFileID = []byte(serial.BlobFileID)

type BlobSerializer struct {
	Pool pool.BuffPool
}

var _ Serializer = BlobSerializer{}

func (s BlobSerializer) Serialize(keys, values [][]byte, subtrees []uint64, level int) serial.Message {
	bufSz := estimateBlobSize(values, subtrees)
	b := getFlatbufferBuilder(s.Pool, bufSz)

	if level == 0 {
		assertTrue(len(values) == 1)
		assertTrue(len(subtrees) == 1)
		payload := b.CreateByteVector(values[0])

		serial.BlobStart(b)
		serial.BlobAddPayload(b, payload)
	} else {
		addrs := writeItemBytes(b, values, len(values)*hash.ByteLen)
		cards := writeCountArray(b, subtrees)

		serial.BlobStart(b)
		serial.BlobAddAddressArray(b, addrs)
		serial.BlobAddSubtreeSizes(b, cards)
	}
	serial.BlobAddTreeSize(b, sumSubtrees(subtrees))
	serial.BlobAddTreeLevel(b, uint8(level))
	return serial.FinishMessage(b, serial.BlobEnd(b), blobFileID)
}

func getBlobKeys(msg serial.Message) ItemArray {
	cnt := getBlobCount(msg)
	buf := make([]byte, cnt)
	for i := range buf {
		buf[i] = 0
	}
	offs := make([]byte, cnt*2)
	for i := 0; i < int(cnt); i++ {
		b := offs[i*2 : (i+1)*2]
		binary.LittleEndian.PutUint16(b, uint16(i))
	}
	return ItemArray{
		Items: buf,
		Offs:  offs,
	}
}

func getBlobValues(msg serial.Message) ItemArray {
	b := serial.GetRootAsBlob(msg, serial.MessagePrefixSz)
	if b.TreeLevel() > 0 {
		arr := b.AddressArrayBytes()
		off := offsetsForAddressArray(arr)
		return ItemArray{
			Items: arr,
			Offs:  off,
		}
	}

	buf := b.PayloadBytes()
	offs := make([]byte, 4)
	binary.LittleEndian.PutUint16(offs[2:], uint16(len(buf)))

	return ItemArray{Items: buf, Offs: offs}
}

func getBlobCount(msg serial.Message) uint16 {
	b := serial.GetRootAsBlob(msg, serial.MessagePrefixSz)
	if b.TreeLevel() == 0 {
		return 1
	}
	return uint16(b.AddressArrayLength() / hash.ByteLen)
}

func walkBlobAddresses(ctx context.Context, msg serial.Message, cb func(ctx context.Context, addr hash.Hash) error) error {
	b := serial.GetRootAsBlob(msg, serial.MessagePrefixSz)
	arr := b.AddressArrayBytes()
	for i := 0; i < len(arr)/hash.ByteLen; i++ {
		addr := hash.New(arr[i*addrSize : (i+1)*addrSize])
		if err := cb(ctx, addr); err != nil {
			return err
		}
	}
	return nil
}

func getBlobTreeLevel(msg serial.Message) int {
	b := serial.GetRootAsBlob(msg, serial.MessagePrefixSz)
	return int(b.TreeLevel())
}

func getBlobTreeCount(msg serial.Message) int {
	b := serial.GetRootAsBlob(msg, serial.MessagePrefixSz)
	return int(b.TreeSize())
}

func getBlobSubtrees(msg serial.Message) []uint64 {
	b := serial.GetRootAsBlob(msg, serial.MessagePrefixSz)
	if b.TreeLevel() == 0 {
		return nil
	}
	counts := make([]uint64, b.AddressArrayLength()/hash.ByteLen)
	return decodeVarints(b.SubtreeSizesBytes(), counts)
}

func estimateBlobSize(values [][]byte, subtrees []uint64) (bufSz int) {
	for i := range values {
		bufSz += len(values[i])
	}
	bufSz += len(subtrees) * binary.MaxVarintLen64
	bufSz += 200 // overhead
	return
}
