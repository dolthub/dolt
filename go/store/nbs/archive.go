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
	"crypto/sha512"
	"errors"
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
   - The Index is a concatenation of 4 sections, the first three of which are compressed as one stream. The Suffixes are
     are not compressed because they won't compress well. For this reason there are two methods on the footer to get the
     the two spans individually.

   ByteSpan Map:
       +------------------+------------------+-----+------------------+
       | ByteSpanLength 1 | ByteSpanLength 2 | ... | ByteSpanLength N |
       +------------------+------------------+-----+------------------+
       - The Length of each ByteSpan is recorded as a varuint, and as we read them we will calculate the offset of each.

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
        - Dictionary and Chunk ByteSpans are constrained to be uint32, which is plenty. Varints can exceed this value, but we constrain them.

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
  - Take the Chunk Id discovered in Phase one, and use it to grab that index from the ChunkRefs Map.
  - Retrieve the ByteSpan Id for the Chunk data. Verify integrity with CRC.
  - If Dictionary is 0:
    - Decompress the Chunk data using zstd (no dictionary)
  - Otherwise:
    - Retrieve the ByteSpan ID for the Dictionary data.
    - Decompress the Chunk data using zstd with the Dictionary data.
*/

const (
	archiveFormatVersion = uint8(1)
	archiveFileSignature = "DOLTARC"
	archiveFileSigSize   = uint64(len(archiveFileSignature))
	archiveCheckSumSize  = sha512.Size * 3 // sha512 3 times.
	archiveFooterSize    = uint32Size +    // index length
		uint32Size + // byte span count
		uint32Size + // chunk count
		uint32Size + // metadataSpan length
		archiveCheckSumSize +
		1 + // version byte
		archiveFileSigSize
	archiveFileSuffix = ".darc"
)

/*
+----------------------+-------------------------+----------------------+--------------------------+-----------------+------------------------+--------------------+
| (Uint32) IndexLength | (Uint32) ByteSpan Count | (Uint32) Chunk Count | (Uint32) Metadata Length | (192) CheckSums | (Uint8) Format Version | (7) File Signature |
+----------------------+-------------------------+----------------------+--------------------------+-----------------+------------------------+--------------------+
*/
const ( // afr = Archive FooteR
	afrIndexLenOffset    = 0
	afrByteSpanOffset    = afrIndexLenOffset + uint32Size
	afrChunkCountOffset  = afrByteSpanOffset + uint32Size
	afrMetaLenOffset     = afrChunkCountOffset + uint32Size
	afrDataChkSumOffset  = afrMetaLenOffset + uint32Size
	afrIndexChkSumOffset = afrDataChkSumOffset + sha512.Size
	afrMetaChkSumOffset  = afrIndexChkSumOffset + sha512.Size
	afrVersionOffset     = afrMetaChkSumOffset + sha512.Size
	afrSigOffset         = afrVersionOffset + 1
)

var ErrInvalidChunkRange = errors.New("invalid chunk range")
var ErrInvalidDictionaryRange = errors.New("invalid dictionary range")
var ErrInvalidFileSignature = errors.New("invalid file signature")
var ErrInvalidFormatVersion = errors.New("invalid format version")

type sha512Sum [sha512.Size]byte

type byteSpan struct {
	offset uint64
	length uint64
}
