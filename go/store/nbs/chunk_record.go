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
	// h is the hash of the chunk
	h hash.Hash

	// fullCompressedChunk is the entirety of the compressed chunk data including the crc
	fullCompressedChunk []byte

	// compressedData is just the snappy encoded byte buffer that stores the chunk data. Currently a subslice of
	// fullCompressedChunk, but that may change in the future.
	compressedData []byte

	// nbsVer is the version of the nbs format that this chunk is encoded with. This must be set whenever creating
	// the ChunkRecord because it's necesary to interpret the bytes of the chunk.
	nbsVer NbsVersion

	// The full size of the chunk, including the type byte and the crc. This is how much space the chunk takes up on disk.
	fullSize uint32

	// The size of the chunk data when it is fully resolved.
	rawChunkSize uint32
}

// NBSVersion returns the version of the noms data format that this chunk is encoded with. This is required in cases
// where the origin of the ChunkRecord us unknown, and user of the object must ensure that the data is encoded
// in the correct format.
func (cmp ChunkRecord) NBSVersion() NbsVersion {
	return cmp.nbsVer
}

// DeserializeChunkRecord creates a ChunkRecord, using the full chunk record bytes stored in the table file. This includes the crc.
// Will error in the event that the crc does not match the data. The nbsVersion is required to interpret the bytes of the
// input appropriately.
func DeserializeChunkRecord(h hash.Hash, buff []byte, nbsVersion NbsVersion) (ChunkRecord, error) {
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

	return ChunkRecord{h: h,
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

	return chunks.NewChunkWithHash(cmp.h, data), nil
}

func ChunkToChunkRecord(chunk chunks.Chunk, nbsVersion NbsVersion) ChunkRecord {
	// NM4 - need to figure out the optimal allocation strategy for this.
	// See previous comment in history about snappy.MaxEncodedLen
	rawChunkSize := chunk.Size()
	raw := make([]byte, 1+checksumSize+snappy.MaxEncodedLen(rawChunkSize))
	offset := 0

	if nbsVersion >= Dolt1V {
		// Currently the metadata byte is always 0. Will change when we use delta encoding. 0 effectively
		// gets handled in the same way as legacy data.
		raw = append(raw, 0)
		offset++
	}

	compressed := snappy.Encode(raw[offset:], chunk.Data())
	offset += len(compressed)

	raw = append(raw, []byte{0, 0, 0, 0}...)
	binary.BigEndian.PutUint32(raw[offset:], crc(raw[:offset]))
	return ChunkRecord{
		h:                   chunk.Hash(),
		fullCompressedChunk: raw[:(offset + checksumSize)],
		compressedData:      raw[:offset],
		fullSize:            uint32(offset + checksumSize),
		rawChunkSize:        uint32(rawChunkSize),
		nbsVer:              nbsVersion}
}

// Size returns the bytes that the chunk record takes up in a table file.
func (cmp ChunkRecord) Size() uint32 {
	return cmp.fullSize
}

// RawChunkSize returns the size of the chunk data when it is fully resolved.
func (cmp ChunkRecord) RawChunkSize() uint32 {
	return cmp.rawChunkSize
}

// WritableData returns the full binary ChunkRecord that can be written to a table file.
func (cmp ChunkRecord) WritableData() []byte {
	return cmp.fullCompressedChunk
}

// Hash returns the hash of the data
func (cmp ChunkRecord) Hash() hash.Hash {
	return cmp.h
}

// IsEmpty returns true if the chunk contains no data.
func (cmp ChunkRecord) IsEmpty() bool {
	return len(cmp.compressedData) == 0 || (len(cmp.compressedData) == 1 && cmp.compressedData[0] == 0)
}
