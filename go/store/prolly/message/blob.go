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

const (
	blobPayloadBytesVOffset fb.VOffsetT = 4
	blobAddressArrayVOffset fb.VOffsetT = 6
)

var blobFileID = []byte(serial.BlobFileID)

func NewBlobSerializer(pool pool.BuffPool) BlobSerializer {
	return BlobSerializer{pool: pool}
}

type BlobSerializer struct {
	pool pool.BuffPool
}

var _ Serializer = BlobSerializer{}

func (s BlobSerializer) Serialize(keys, values [][]byte, subtrees []uint64, level int) serial.Message {
	bufSz := estimateBlobSize(values, subtrees)
	b := getFlatbufferBuilder(s.pool, bufSz)

	if level == 0 {
		assertTrue(len(values) == 1, "num values != 1 when serialize Blob")
		assertTrue(len(subtrees) == 1, "num subtrees != 1 when serialize Blob")
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

func getBlobKeys(msg serial.Message) (ItemAccess, error) {
	return ItemAccess{}, nil
}

func getBlobValues(msg serial.Message) (values ItemAccess, err error) {
	var b serial.Blob
	err = serial.InitBlobRoot(&b, msg, serial.MessagePrefixSz)
	if err != nil {
		return ItemAccess{}, err
	}
	if b.TreeLevel() > 0 {
		values.bufStart = lookupVectorOffset(blobAddressArrayVOffset, b.Table())
		values.bufLen = uint16(b.AddressArrayLength())
		values.itemWidth = hash.ByteLen
	} else {
		values.bufStart = lookupVectorOffset(blobPayloadBytesVOffset, b.Table())
		values.bufLen = uint16(b.PayloadLength())
		values.itemWidth = uint16(b.PayloadLength())
	}
	return
}

func getBlobCount(msg serial.Message) (uint16, error) {
	var b serial.Blob
	err := serial.InitBlobRoot(&b, msg, serial.MessagePrefixSz)
	if err != nil {
		return 0, err
	}
	if b.TreeLevel() == 0 {
		return 1, nil
	}
	return uint16(b.AddressArrayLength() / hash.ByteLen), nil
}

func walkBlobAddresses(ctx context.Context, msg serial.Message, cb func(ctx context.Context, addr hash.Hash) error) error {
	var b serial.Blob
	err := serial.InitBlobRoot(&b, msg, serial.MessagePrefixSz)
	if err != nil {
		return err
	}
	arr := b.AddressArrayBytes()
	for i := 0; i < len(arr)/hash.ByteLen; i++ {
		addr := hash.New(arr[i*addrSize : (i+1)*addrSize])
		if err := cb(ctx, addr); err != nil {
			return err
		}
	}
	return nil
}

func getBlobTreeLevel(msg serial.Message) (uint16, error) {
	var b serial.Blob
	err := serial.InitBlobRoot(&b, msg, serial.MessagePrefixSz)
	if err != nil {
		return 0, err
	}
	return uint16(b.TreeLevel()), nil
}

func getBlobTreeCount(msg serial.Message) (int, error) {
	var b serial.Blob
	err := serial.InitBlobRoot(&b, msg, serial.MessagePrefixSz)
	if err != nil {
		return 0, err
	}
	return int(b.TreeSize()), nil
}

func getBlobSubtrees(msg serial.Message) ([]uint64, error) {
	var b serial.Blob
	err := serial.InitBlobRoot(&b, msg, serial.MessagePrefixSz)
	if err != nil {
		return nil, err
	}
	if b.TreeLevel() == 0 {
		return nil, nil
	}
	counts := make([]uint64, b.AddressArrayLength()/hash.ByteLen)
	return decodeVarints(b.SubtreeSizesBytes(), counts), nil
}

func estimateBlobSize(values [][]byte, subtrees []uint64) (bufSz int) {
	for i := range values {
		bufSz += len(values[i])
	}
	bufSz += len(subtrees) * binary.MaxVarintLen64
	bufSz += 200 // overhead
	return
}
