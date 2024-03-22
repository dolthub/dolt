// Copyright 2019-2024 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"errors"

	"encoding/binary"

	"github.com/golang/snappy"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

var EmptyChunkRecord ChunkRecord

func init() {
	EmptyChunkRecord = ChunkToChunkRecord(chunks.EmptyChunk, BetaV)
}

// ChunkRecord represents a chunk of data in a table file which is still compressed, either raw data compressed with
// snappy, or delta compressed type info preserved.
type ChunkRecord struct {
	// H is the hash of the chunk
	H hash.Hash

	// FullCompressedChunk is the entirety of the compressed chunk data including the crc
	fullCompressedChunk []byte

	// CompressedData is just the snappy encoded byte buffer that stores the chunk data
	compressedData []byte

	nbsVer NbsVersion // NM4 - not sure if we want this. Everywhere we use these objects we should have the version near at hand. TBD.

	// The full size of the chunk, including the type byte and the crc. This is how much space the chunk takes up on disk.
	fullSize uint32

	rawChunkSize uint32
}

func (cmp ChunkRecord) Size() uint32 {
	return cmp.fullSize
}

func (cmp ChunkRecord) RawChunkSize() uint32 {
	return cmp.rawChunkSize
}

func (cmp ChunkRecord) WritableData() []byte {
	return cmp.fullCompressedChunk
}

// NBSVersion returns the version of the noms data format that this chunk is encoded with. This is required in cases
// where the origin of the ChunkRecord us unknown, and user of the object must ensure that the data is encoded
// in the correct format.
func (cmp ChunkRecord) NBSVersion() NbsVersion {
	return cmp.nbsVer
}

// NewChunkRecord creates a ChunkRecord, using the full chunk record bytes, which includes the crc.
// Will error in the event that the crc does not match the data.
func NewChunkRecord(h hash.Hash, buff []byte, nbsVersion NbsVersion) (ChunkRecord, error) {
	fullSize := uint32(len(buff))
	dataLen := uint64(len(buff)) - checksumSize
	chksum := binary.BigEndian.Uint32(buff[dataLen:])

	crcSum := crc(buff[:dataLen])

	fullbuff := buff
	if nbsVersion >= Dolt1V {
		// First byte is indicating metadata about the chunk. It is not part of the compressed payload, but is included
		// in the CRC calculation.
		// We don't use yet, but we need to skip it.
		if buff[0] != 0 {
			return ChunkRecord{}, errors.New("invalid chunk data - chunk type byte is not 0")
		}

		dataLen--
		buff = buff[1:]
	}

	compressedData := buff[:dataLen]

	uncLen, err := snappy.DecodedLen(compressedData)
	if err != nil {
		return ChunkRecord{}, err
	}

	if chksum != crcSum {
		return ChunkRecord{}, errors.New("checksum error")
	}

	// NM4 - not sure if we want to continue carrying around the originam buff.
	return ChunkRecord{H: h,
		fullCompressedChunk: fullbuff,
		fullSize:            fullSize,
		rawChunkSize:        uint32(uncLen),
		compressedData:      compressedData,
		nbsVer:              nbsVersion}, nil
}

// ToChunk snappy decodes the compressed data and returns a chunks.Chunk
func (cmp ChunkRecord) ToChunk() (chunks.Chunk, error) {
	data, err := snappy.Decode(nil, cmp.compressedData)

	if err != nil {
		return chunks.Chunk{}, err
	}

	// NM4 - We don't verify the hash?? See comment in NewChunkWithHash which assume the caller trusts the Hash. We
	// verify the CRC in NewChunkRecord, but not the hash. I believe this is for per reasons. For my testing, I'd
	// like to verify the hash here.

	return chunks.NewChunkWithHash(cmp.H, data), nil
}

func ChunkToChunkRecord(chunk chunks.Chunk, nbsVersion NbsVersion) ChunkRecord {
	// NM4 - need to figure out the optimal allocation strategy for this.
	// See previous comment in history about snappy.MaxEncodedLen
	rawChunkSize := chunk.Size()
	raw := make([]byte, 1+checksumSize+snappy.MaxEncodedLen(rawChunkSize))
	offset := 0

	if nbsVersion >= Dolt1V {
		raw = append(raw, 0) // NM4.
		offset++
	}

	compressed := snappy.Encode(raw[offset:], chunk.Data())
	offset += len(compressed)

	decodeLen, err := snappy.DecodedLen(compressed)
	if err != nil {
		panic(err)
	}

	if decodeLen != rawChunkSize {
		panic("snappy encoded length does not match raw chunk size")
	}

	raw = append(raw, []byte{0, 0, 0, 0}...)
	binary.BigEndian.PutUint32(raw[offset:], crc(raw[:offset]))
	return ChunkRecord{H: chunk.Hash(),
		fullCompressedChunk: raw[:(offset + checksumSize)],
		compressedData:      raw[:offset],
		fullSize:            uint32(offset + checksumSize),
		rawChunkSize:        uint32(rawChunkSize),
		nbsVer:              nbsVersion}
}

// Hash returns the hash of the data
func (cmp ChunkRecord) Hash() hash.Hash {
	return cmp.H
}

// IsEmpty returns true if the chunk contains no data.
func (cmp ChunkRecord) IsEmpty() bool {
	return len(cmp.compressedData) == 0 || (len(cmp.compressedData) == 1 && cmp.compressedData[0] == 0)
}
