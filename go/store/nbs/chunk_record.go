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

var EmptyCompressedChunk CompressedChunk

func init() {
	EmptyCompressedChunk = ChunkToCompressedChunk(chunks.EmptyChunk, nomsBetaVersion)
}

// CompressedChunk represents a chunk of data in a table file which is still compressed, either raw data compressed with
// snappy, or delta compressed type info preserved.
type CompressedChunk struct {
	// H is the hash of the chunk
	H hash.Hash

	// FullCompressedChunk is the entirety of the compressed chunk data including the crc
	fullCompressedChunk []byte

	// CompressedData is just the snappy encoded byte buffer that stores the chunk data
	CompressedData []byte

	nbsVer uint8 // NM4 - not sure if we want this. Everywhere we use these objects we should have the version near at hand. TBD.

	// The full size of the chunk, including the type byte and the crc. This is how much space the chunk takes up on disk.
	fullSize uint32
}

func (cmp CompressedChunk) Size() uint32 {
	return cmp.fullSize
}

func (cmp CompressedChunk) WritableData() []byte {
	return cmp.fullCompressedChunk
}

// NBSVersion returns the version of the noms data format that this chunk is encoded with. This is required in cases
// where the origin of the CompressedChunk us unknown, and user of the object must ensure that the data is encoded
// in the correct format.
func (cmp CompressedChunk) NBSVersion() uint8 {
	return cmp.nbsVer
}

// NewCompressedChunk creates a CompressedChunk, using the full chunk record bytes, which includes the crc.
// Will error in the event that the crc does not match the data.
func NewCompressedChunk(h hash.Hash, buff []byte, nbsVersion uint8) (CompressedChunk, error) {
	fullSize := uint32(len(buff))
	dataLen := uint64(len(buff)) - checksumSize
	chksum := binary.BigEndian.Uint32(buff[dataLen:])

	crcSum := crc(buff[:dataLen])

	fullbuff := buff
	if nbsVersion >= doltRev1Version {
		// First byte is indicating metadata about the chunk. It is not part of the compressed payload, but is included
		// in the CRC calculation.
		// We don't use yet, but we need to skip it.
		if buff[0] != 0 {
			return CompressedChunk{}, errors.New("invalid chunk data - chunk type byte is not 0")
		}

		dataLen--
		buff = buff[1:]
	}

	compressedData := buff[:dataLen]

	if chksum != crcSum {
		return CompressedChunk{}, errors.New("checksum error")
	}

	// NM4 - not sure if we want to continue carrying around the originam buff.
	return CompressedChunk{H: h, fullCompressedChunk: fullbuff, fullSize: fullSize, CompressedData: compressedData, nbsVer: nbsVersion}, nil
}

// ToChunk snappy decodes the compressed data and returns a chunks.Chunk
func (cmp CompressedChunk) ToChunk() (chunks.Chunk, error) {
	data, err := snappy.Decode(nil, cmp.CompressedData)

	if err != nil {
		return chunks.Chunk{}, err
	}

	// NM4 - We don't verify the hash?? See comment in NewChunkWithHash which assume the caller trusts the Hash. We
	// verify the CRC in NewCompressedChunk, but not the hash. I believe this is for per reasons. For my testing, I'd
	// like to verify the hash here.

	return chunks.NewChunkWithHash(cmp.H, data), nil
}

func ChunkToCompressedChunk(chunk chunks.Chunk, nbsVersion uint8) CompressedChunk {
	// NM4 - need to figure out the optimal allocation strategy for this.
	// See previous comment in history about snappy.MaxEncodedLen
	raw := make([]byte, 1+checksumSize+snappy.MaxEncodedLen(chunk.Size()))
	offset := 0

	if nbsVersion >= doltRev1Version {
		raw = append(raw, 0) // NM4.
		offset++
	}

	compressed := snappy.Encode(raw[offset:], chunk.Data())
	offset += len(compressed)

	raw = append(raw, []byte{0, 0, 0, 0}...)
	binary.BigEndian.PutUint32(raw[offset:], crc(raw[:offset]))
	return CompressedChunk{H: chunk.Hash(), fullCompressedChunk: raw[:(offset + checksumSize)], CompressedData: raw[:offset], fullSize: uint32(offset + checksumSize), nbsVer: nbsVersion}
}

// Hash returns the hash of the data
func (cmp CompressedChunk) Hash() hash.Hash {
	return cmp.H
}

// IsEmpty returns true if the chunk contains no data.
func (cmp CompressedChunk) IsEmpty() bool {
	return len(cmp.CompressedData) == 0 || (len(cmp.CompressedData) == 1 && cmp.CompressedData[0] == 0)
}

// CompressedSize returns the size of this CompressedChunk.
func (cmp CompressedChunk) CompressedSize() int {
	return len(cmp.CompressedData)
}
