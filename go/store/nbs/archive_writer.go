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
   +------------+------------+-----+------------+-------+--------+
   | ByteSpan 1 | ByteSpan 2 | ... | ByteSpan N | Index | Footer |
   +------------+------------+-----+------------+-------+--------+

In reverse order, since that's how we read it

Footer:
   +----------------------+-------------------------+----------------------+--------------------+--------------------+
   | (Uint32) IndexLength | (Uint32) ByteSpan Count | (Uint32) Chunk Count | (1) Format Version | (7) File Signature |
   +----------------------+-------------------------+----------------------+--------------------+--------------------+
   - Index Length: The length of the Index in bytes.
   - ByteSpan Count: (N) The number of ByteSpans in the Archive. (does not include the null ByteSpan)
   - Chunk Count: (M) The number of Chunk Records in the Archive.
      * These 3 values are all required to properly parse the Index. Note that the NBS Index has a deterministic size
        based on the Chunk Count. This is not the case with a Dolt Archive.
   - Format Version: Sequence starting at 1.
   - File Signature: Some would call this a magic number. Not on my watch. Dolt Archives have a 7 byte signature: "DOLTARC"

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

ByteSpan:
   +----------------+----------------+
   | Data as []byte | (Uint32) CRC32 |
   +----------------+----------------+
     - Self Explanatory.


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
    - Retrieve the ByteSpan ID for the Dictionary data. Verify integrity with CRC.
    - Decompress the Chunk data using zstd with the Dictionary data.
*/

const archiveFormatVersion = 1
const archiveFileSignature = "DOLTARC"
const archiveFooterSize = 4 + 4 + 4 + 1 + 7

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

type archiveWriter struct {
	output       io.Writer
	bytesWritten uint64
	stagedBytes  stagedByteSpanSlice
	stagedChunks stagedChunkRefSlice
}

func newArchiveWriter(output io.Writer) *archiveWriter {
	return &archiveWriter{output: output}
}

// writeByteSpan writes a byte span to the archive, returning the ByteSpan ID if the write was successful. Note
// that writing an empty byte span is a no-op and will return 0.
func (aw *archiveWriter) writeByteSpan(b []byte) (uint32, error) {
	if len(b) == 0 {
		return 0, nil
	}

	offset := aw.bytesWritten

	cr := crc(b)
	written, err := aw.output.Write(b)
	if err != nil {
		return 0, err
	}
	if written != len(b) {
		return 0, io.ErrShortWrite
	}
	aw.bytesWritten += uint64(written)

	err = binary.Write(aw.output, binary.BigEndian, cr)
	if err != nil {
		return 0, err
	}
	aw.bytesWritten += uint32Size

	aw.stagedBytes = append(aw.stagedBytes, stagedByteSpan{offset, uint32(aw.bytesWritten - offset)})

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

func (aw *archiveWriter) finalize() error {
	indexLen, err := aw.writeIndex()
	if err != nil {
		return err
	}

	return aw.writeFooter(indexLen)
}

func (aw *archiveWriter) writeFooter(indexLen uint32) error {
	// Write out the index length
	err := binary.Write(aw.output, binary.BigEndian, indexLen)
	if err != nil {
		return err
	}
	aw.bytesWritten += uint32Size

	// Write out the byte span count
	err = binary.Write(aw.output, binary.BigEndian, uint32(len(aw.stagedBytes)))
	if err != nil {
		return err
	}
	aw.bytesWritten += uint32Size

	// Write out the chunk count
	err = binary.Write(aw.output, binary.BigEndian, uint32(len(aw.stagedChunks)))
	if err != nil {
		return err
	}
	aw.bytesWritten += uint32Size

	// Write out the format version
	err = binary.Write(aw.output, binary.BigEndian, uint8(archiveFormatVersion))
	if err != nil {
		return err
	}
	aw.bytesWritten++

	// Write out the file signature
	_, err = aw.output.Write([]byte(archiveFileSignature))
	if err != nil {
		return err
	}
	aw.bytesWritten += 7

	return nil
}

func (aw *archiveWriter) writeIndex() (uint32, error) {
	startingByteCount := aw.bytesWritten

	varIbuf := make([]byte, binary.MaxVarintLen64)

	// Write out the stagedByteSpans
	for _, bs := range aw.stagedBytes {
		n := binary.PutUvarint(varIbuf, bs.offset)
		written, err := aw.output.Write(varIbuf[:n])
		if err != nil {
			return 0, err
		}
		if written != n {
			return 0, io.ErrShortWrite
		}
		aw.bytesWritten += uint64(written)

		n = binary.PutUvarint(varIbuf, uint64(bs.length))
		written, err = aw.output.Write(varIbuf[:n])
		if err != nil {
			return 0, err
		}
		if written != n {
			return 0, io.ErrShortWrite
		}
		aw.bytesWritten += uint64(written)
	}

	// sort stagedChunks by hash.Prefix(). Note this isn't a perfect sort for hashes, we are just grouping them by prefix
	sort.Sort(aw.stagedChunks)

	// We lay down the sorted chunk list in it's three forms.
	// Prefix Map
	for _, scr := range aw.stagedChunks {
		err := binary.Write(aw.output, binary.BigEndian, scr.hash.Prefix())
		if err != nil {
			return 0, err
		}
		aw.bytesWritten += uint64Size
	}
	// ChunkReferences
	for _, scr := range aw.stagedChunks {
		n := binary.PutUvarint(varIbuf, uint64(scr.dictionary))
		written, err := aw.output.Write(varIbuf[:n])
		if err != nil {
			return 0, err
		}
		if written != n {
			return 0, io.ErrShortWrite
		}
		aw.bytesWritten += uint64(written)

		n = binary.PutUvarint(varIbuf, uint64(scr.data))
		written, err = aw.output.Write(varIbuf[:n])
		if err != nil {
			return 0, err
		}
		if written != n {
			return 0, io.ErrShortWrite
		}
		aw.bytesWritten += uint64(written)
	}
	// Suffixes
	for _, scr := range aw.stagedChunks {
		n, err := aw.output.Write(scr.hash.Suffix())
		if err != nil {
			return 0, err
		}
		if n != hash.SuffixLen {
			return 0, io.ErrShortWrite
		}

		aw.bytesWritten += uint64(hash.SuffixLen)
	}

	return uint32(aw.bytesWritten - startingByteCount), nil
}
