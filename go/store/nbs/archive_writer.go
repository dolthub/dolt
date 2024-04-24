// Copyright 2024 Dolthub, Inc.
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

package nbs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sort"

	"github.com/dolthub/dolt/go/store/hash"
)

/*
A Dolt Archive is a file format for storing a collection of Chunks in a single file. The archive is essentially many
byte spans concatenated together, with an index at the end of the file. Chunk Addresses are used to lookup and retrieve
Chunks from the Archive.

There are byte spans with in the archive which are not addressable by a Chunk Address. These are used as internal data
to aid in the compression of the Chunks.

A Dolt Archive file follows the following format:
   +------------+------------+-----+------------+-------+----------+--------+
   | ByteSpan 1 | ByteSpan 2 | ... | ByteSpan N | Index | Metadata | Footer |
   +------------+------------+-----+------------+-------+----------+--------+

In reverse order, since that's how we read it

Footer:
   +----------------------+-------------------------+----------------------+--------------------------+-----------------+------------------------+--------------------+
   | (Uint32) IndexLength | (Uint32) ByteSpan Count | (Uint32) Chunk Count | (Uint32) Metadata Length | (192) CheckSums | (Uint8) Format Version | (7) File Signature |
   +----------------------+-------------------------+----------------------+--------------------------+-----------------+------------------------+--------------------+
   - Index Length: The length of the Index in bytes.
   - ByteSpan Count: (N) The number of ByteSpans in the Archive. (does not include the null ByteSpan)
   - Chunk Count: (M) The number of Chunk Records in the Archive.
      * These 3 values are all required to properly parse the Index. Note that the NBS Index has a deterministic size
        based on the Chunk Count. This is not the case with a Dolt Archive.
   - Metadata Length: The length of the Metadata in bytes.
   - CheckSums: See Below.
   - Format Version: Sequence starting at 1.
   - File Signature: Some would call this a magic number. Not on my watch. Dolt Archives have a 7 byte signature: "DOLTARC"

   CheckSums:
   +----------------------------+-------------------+----------------------+
   | (64) Sha512 ByteSpan 1 - N | (64) Sha512 Index | (64) Sha512 Metadata |
   +----------------------------+-------------------+----------------------+
   - The Sha512 checksums of the ByteSpans, Index, and Metadata. Currently unused, but may be used in the future. Leaves
     the opening to verify integrity manually at least, but could be used in the future to allow to break the file into
     parts, and ensure we can verify the integrity of each part.

Index:
   +--------------+------------+-----------------+----------+
   | ByteSpan Map | Prefix Map | ChunkReferences | Suffixes |
   +--------------+------------+-----------------+----------+

   ByteSpan Map:
       +------------------+------------------+-----+------------------+
       | ByteSpanRecord 1 | ByteSpanRecord 2 | ... | ByteSpanRecord N |
       +------------------+------------------+-----+------------------+
       ByteSpanRecord:
           +------------------+------------------+
           | (uvarint) Offset | (uvarint) Length |
           +------------------+------------------+
           - Offset: The byte offset of the ByteSpan in the archive
           - Length: The byte length of the ByteSpan (includes the CRC32)

       The ByteSpan Map contains N ByteSpan Records. The index in the map is considered the ByteSpan's ID, and
       is used to reference the ByteSpan in the ChunkRefs. Note that the ByteSpan ID is 1-based, as 0 is reserved to indicate
       an empty ByteSpan.

   Prefix Map:
       +-------------------+-------------------+-----+---------------------------+
       | (Uint64) Prefix 0 | (Uint64) Prefix 1 | ... | (Uint64) Prefix Tuple M-1 |
       +-------------------+-------------------+-----+---------------------------+
       - The Prefix Map contains M Prefixes - one for each Chunk Record in the Table.
       - The Prefix Tuples are sorted, allowing for a binary search.
       - NB: THE SAME PREFIX MAY APPEAR MULTIPLE TIMES, as distinct Hashes (referring to distinct Chunks) may share the same Prefix.
       - The index into this map is the Ordinal of the Chunk Record.

   ChunkReferences:
       +------------+------------+-----+--------------+
       | ChunkRef 0 | ChunkRef 1 | ... | ChunkRef M-1 |
       +------------+------------+-----+--------------+
       ChunkRef:
           +-------------------------------+--------------------------+
           | (uvarint) Dictionary ByteSpan | (uvarint) Chunk ByteSpan |
           +-------------------------------+--------------------------+
        - Dictionary: ID for a ByteSpan to be used as zstd dictionary. 0 refers to the empty ByteSpan, which indicates no dictionary.
        - Chunk: ID for the ByteSpan containing the Chunk data. Never 0.

   Suffixes:
       +--------------------+--------------------+-----+----------------------+
       | (12) Hash Suffix 0 | (12) Hash Suffix 1 | ... | (12) Hash Suffix M-1 |
       +--------------------+--------------------+-----+----------------------+

     - Each Hash Suffix is the last 12 bytes of a Chunk in this Table.
     - Hash Suffix M must correspond to Prefix M and Chunk Record M

Metadata:
   The Metadata section is intended to be used for additional information about the Archive. This may include the version
   of Dolt that created the archive, possibly references to other archives, or other information. For Format version 1,
   We use a simple JSON object. The Metadata Length is the length of the JSON object in bytes. Could be a Flatbuffer in
   the future, which would mandate a format version bump.

ByteSpan:
   +----------------+
   | Data as []byte |
   +----------------+
     - Self Explanatory.
     - zStd automatically applies and checks CRC.

Chunk Retrieval (phase 1 is similar to NBS):

  Phase one: Chunk Presence
  - Slice off the first 8 bytes of your Hash to create a Prefix
  - Since the Prefix Tuples in the Prefix Map are in lexicographic order, binary search the Prefix Map for the desired
    Prefix. To not mix terms with Index, we'll call this the Chunk Id, which is the 0-based index into the Prefix Map.
  - Using the Chunk Id found with a binary search, search locally for additional matching Prefixes. The matching indexes
    are all potential matches for the chunk you are looking for.
    - For each Chunk Id found, grab the corresponding Suffix, and compare to the Suffix of the Hash you are looking for.
    - If they match, your chunk is in this file in the Chunk Id which matched.
    - If they don't match, continue to the next matching Chunk Id.
  - If not found, your chunk is not in this Table.
  - If found, the given Chunk Id is the same index into the ChunkRef Map for the desired chunk.

  Phase two: Loading Chunk data
  - Take the Chunk Id discovered in Phase one, and use it to grap that index from the ChunkRefs Map.
  - Retrieve the ByteSpan Id for the Chunk data. Verify integrity with CRC.
  - If Dictionary is 0:
    - Decompress the Chunk data using zstd (no dictionary)
  - Otherwise:
    - Retrieve the ByteSpan ID for the Dictionary data.
    - Decompress the Chunk data using zstd with the Dictionary data.
*/

const (
	archiveFormatVersion = 1
	archiveFileSignature = "DOLTARC"
	archiveFileSigSize   = uint64(len(archiveFileSignature))
	archiveCheckSumSize  = 64 * 3       // sha512 3 times.
	archiveFooterSize    = uint32Size + // index length
		uint32Size + // byte span count
		uint32Size + // chunk count
		uint32Size + // metadata length
		archiveCheckSumSize +
		1 + // version byte
		archiveFileSigSize
)

var ErrInvalidChunkRange = errors.New("invalid chunk range")
var ErrInvalidDictionaryRange = errors.New("invalid dictionary range")
var ErrInvalidFileSignature = errors.New("invalid file signature")
var ErrInvalidFormatVersion = errors.New("invalid format version")

type stagedByteSpan struct {
	offset uint64
	length uint32
}
type stagedByteSpanSlice []stagedByteSpan

type stagedChunkRef struct {
	hash             hash.Hash
	dictionary, data uint32
}
type stagedChunkRefSlice []stagedChunkRef

type sha512Sum [64]byte

type archiveWriter struct {
	output           HashingByteSink
	bytesWritten     uint64
	stagedBytes      stagedByteSpanSlice
	stagedChunks     stagedChunkRefSlice
	indexLen         uint32
	metadataLen      uint32
	dataCheckSum     sha512Sum
	indexCheckSum    sha512Sum
	metadataCheckSum sha512Sum
}

func newArchiveWriter(output ByteSink) *archiveWriter {
	hbs := NewSHA512HashingByteSink(output)
	return &archiveWriter{output: *hbs}
}

// writeByteSpan writes a byte span to the archive, returning the ByteSpan ID if the write was successful. Note
// that writing an empty byte span is a no-op and will return 0. Also, the slice passed in is copied, so the caller
// can reuse the slice after this call.
func (aw *archiveWriter) writeByteSpan(b []byte) (uint32, error) {
	if len(b) == 0 {
		return 0, nil
	}

	offset := aw.bytesWritten

	written, err := aw.output.Write(b)
	if err != nil {
		return 0, err
	}
	if written != len(b) {
		return 0, io.ErrShortWrite
	}
	aw.bytesWritten += uint64(written)

	aw.stagedBytes = append(aw.stagedBytes, stagedByteSpan{offset, uint32(written)})

	return uint32(len(aw.stagedBytes)), nil
}

func (aw *archiveWriter) stageChunk(hash hash.Hash, dictionary, data uint32) error {
	if data == 0 || data > uint32(len(aw.stagedBytes)) {
		return ErrInvalidChunkRange
	}

	if dictionary > uint32(len(aw.stagedBytes)) {
		return ErrInvalidDictionaryRange
	}

	aw.stagedChunks = append(aw.stagedChunks, stagedChunkRef{hash, dictionary, data})
	return nil
}

func (scrs stagedChunkRefSlice) Len() int {
	return len(scrs)
}
func (scrs stagedChunkRefSlice) Less(i, j int) bool {
	return scrs[i].hash.Prefix() < scrs[j].hash.Prefix()
}
func (scrs stagedChunkRefSlice) Swap(i, j int) {
	scrs[i], scrs[j] = scrs[j], scrs[i]
}

// finalizeByteSpans writes the ... NM4
func (aw *archiveWriter) finalizeByteSpans() {
	// Get the checksum for the data written so far
	aw.dataCheckSum = sha512Sum(aw.output.GetSum())
	aw.output.ResetHasher()
}

// Helper method.
func (aw *archiveWriter) writeUint32(val uint32) error {
	bb := &bytes.Buffer{}
	err := binary.Write(bb, binary.BigEndian, val)
	if err != nil {
		return err
	}

	i := bb.Len()
	_ = i

	n, err := aw.output.Write(bb.Bytes())
	if err != nil {
		return err
	}
	if n != uint32Size {
		return io.ErrShortWrite
	}

	aw.bytesWritten += uint32Size
	return nil
}

func (aw *archiveWriter) writeUint64(val uint64) error {
	bb := &bytes.Buffer{}
	err := binary.Write(bb, binary.BigEndian, val)
	if err != nil {
		return err
	}

	n, err := aw.output.Write(bb.Bytes())
	if err != nil {
		return err
	}
	if n != uint64Size {
		return io.ErrShortWrite
	}

	aw.bytesWritten += uint64Size
	return nil
}

// writeIndex writes the index to the archive. Expects the hasher to be reset before be called, and will reset it. It
// sets the indexLen and indexCheckSum fields on the archiveWriter, and updates the bytesWritten field.
func (aw *archiveWriter) writeIndex() error {
	startingByteCount := aw.bytesWritten

	varIbuf := make([]byte, binary.MaxVarintLen64)

	// Write out the stagedByteSpans
	for _, bs := range aw.stagedBytes {
		n := binary.PutUvarint(varIbuf, bs.offset)
		written, err := aw.output.Write(varIbuf[:n])
		if err != nil {
			return err
		}
		if written != n {
			return io.ErrShortWrite
		}
		aw.bytesWritten += uint64(written)

		n = binary.PutUvarint(varIbuf, uint64(bs.length))
		written, err = aw.output.Write(varIbuf[:n])
		if err != nil {
			return err
		}
		if written != n {
			return io.ErrShortWrite
		}
		aw.bytesWritten += uint64(written)
	}

	// sort stagedChunks by hash.Prefix(). Note this isn't a perfect sort for hashes, we are just grouping them by prefix
	sort.Sort(aw.stagedChunks)

	// We lay down the sorted chunk list in it's three forms.
	// Prefix Map
	for _, scr := range aw.stagedChunks {
		err := aw.writeUint64(scr.hash.Prefix())
		if err != nil {
			return err
		}
	}
	// ChunkReferences
	for _, scr := range aw.stagedChunks {
		n := binary.PutUvarint(varIbuf, uint64(scr.dictionary))
		written, err := aw.output.Write(varIbuf[:n])
		if err != nil {
			return err
		}
		if written != n {
			return io.ErrShortWrite
		}
		aw.bytesWritten += uint64(written)

		n = binary.PutUvarint(varIbuf, uint64(scr.data))
		written, err = aw.output.Write(varIbuf[:n])
		if err != nil {
			return err
		}
		if written != n {
			return io.ErrShortWrite
		}
		aw.bytesWritten += uint64(written)
	}
	// Suffixes
	for _, scr := range aw.stagedChunks {
		n, err := aw.output.Write(scr.hash.Suffix())
		if err != nil {
			return err
		}
		if n != hash.SuffixLen {
			return io.ErrShortWrite
		}
		aw.bytesWritten += uint64(hash.SuffixLen)
	}

	aw.indexLen = uint32(aw.bytesWritten - startingByteCount)
	aw.indexCheckSum = sha512Sum(aw.output.GetSum())
	aw.output.ResetHasher()

	return nil
}

// writeMetadata writes the metadata to the archive. Expects the hasher to be reset before be called, and will reset it.
// It sets the metadataLen and metadataCheckSum fields on the archiveWriter, and updates the bytesWritten field.
//
// Empty input is allowed.
func (aw *archiveWriter) writeMetadata(data []byte) error {
	if data == nil || len(data) == 0 {
		aw.metadataCheckSum = sha512Sum(aw.output.GetSum())
		aw.metadataLen = 0
		aw.output.ResetHasher()
		return nil
	}

	written, err := aw.output.Write(data)
	if err != nil {
		return err
	}
	if written != len(data) {
		return io.ErrShortWrite
	}
	aw.bytesWritten += uint64(written)
	aw.metadataLen = uint32(written)
	aw.metadataCheckSum = sha512Sum(aw.output.GetSum())
	aw.output.ResetHasher()

	return nil
}

func (aw *archiveWriter) writeFooter() error {
	// Write out the index length
	err := aw.writeUint32(aw.indexLen)
	if err != nil {
		return err
	}

	// Write out the byte span count
	err = aw.writeUint32(uint32(len(aw.stagedBytes)))
	if err != nil {
		return err
	}

	// Write out the chunk count
	err = aw.writeUint32(uint32(len(aw.stagedChunks)))
	if err != nil {
		return err
	}

	// Write out the metadata length
	err = aw.writeUint32(aw.metadataLen)
	if err != nil {
		return err
	}

	err = aw.writeCheckSums()
	if err != nil {
		return err
	}

	// Write out the format version
	_, err = aw.output.Write([]byte{archiveFormatVersion})
	if err != nil {
		return err
	}
	aw.bytesWritten++

	// Write out the file signature
	_, err = aw.output.Write([]byte(archiveFileSignature))
	if err != nil {
		return err
	}
	aw.bytesWritten += archiveFileSigSize

	return nil
}

func (aw *archiveWriter) writeCheckSums() error {
	_, err := aw.output.Write(aw.dataCheckSum[:])
	if err != nil {
		return err
	}

	_, err = aw.output.Write(aw.indexCheckSum[:])
	if err != nil {
		return err
	}

	_, err = aw.output.Write(aw.metadataCheckSum[:])
	if err != nil {
		return err
	}
	aw.bytesWritten += archiveCheckSumSize
	return nil
}
