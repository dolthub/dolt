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
ByteSpans concatenated together, with an index at the end of the file. Chunk Addresses are used to lookup and retrieve
Chunks from the Archive.

ByteSpans are arbitrary offset/lengths into the file which store (1) zstd dictionary data, and (2) compressed chunk
data.

Each Chunk is stored as one or two ByteSpans. Dictionary ByteSpans can (should) be used by multiple
Chunks, so there are more ByteSpans than Chunks. The Index is used to map Chunks to ByteSpan pairs. These pairs are
called  ChunkRefs, and we store them as [uint32,uint32] on disk. This allows us to quickly find the ByteSpans for a
given Chunk with minimal processing at load time.

Format Version Differences:
  - Version 1: All chunks are compressed with zStd. Dictionaries are stored as a chunk ref with dictionary ID 0.
               The dictionaries themselves are zStd compressed. Chunks are stored with a pair of ByteSpans, the first
               being the dictionary, and the second being the chunk data.
  - Version 2: In addition to zStd compressed chunks, we also support Snappy compressed chunks, in the same format
               as Noms table files. Any Snappy compressed chunk will have a dictionary ID of 0, and the chunk data
               will be stored in the second Bytespan. It is stored with 32 bit CRC, just like Noms table files.

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
   - Format Version: Sequence starting at 1. Currently, 1 and 2 are supported.
   - File Signature: Some would call this a magic number. Not on my watch. Dolt Archives have a 7 byte signature: "DOLTARC"

   CheckSums:
   +----------------------------+-------------------+----------------------+
   | (64) Sha512 ByteSpan 1 - N | (64) Sha512 Index | (64) Sha512 Metadata |
   +----------------------------+-------------------+----------------------+
   - The Sha512 checksums of the ByteSpans, Index, and Metadata. Currently unused, but may be used in the future. Leaves
     the opening to verify integrity manually at least, but could be used in the future to allow to break the file into
     parts, and ensure we can verify the integrity of each part.

Index:
   The Index is a concatenation of 4 sections, all of which are stored in raw form on disk.
   +-----------+------------+-----------------+----------+
   | SpanIndex | Prefix Map | ChunkReferences | Suffixes |
   +-----------+------------+-----------------+----------+

   SpanIndex:
       SpanIndex contains information required to lookup N ByteSpan Records. ByteSpan IDs are 1-based, where a 0 ID
       indicates an empty ByteSpan. The SpanIndex is a list of Uint64s, where each Uint64 is the offset of the _end_ of
       each ByteSpan. This allows for quick calculation of the offset/length of each ByteSpan.

       +------------------+------------------+-----+------------------+
       | ByteSpanOffset 1 | ByteSpanOffset 2 | ... | ByteSpanOffset N |
       +------------------+------------------+-----+------------------+

       An example:
       +-------------------+-------------------+-------------------+-------------------+
       | ByteSpan 1, len 7 | ByteSpan 2, len 3 | ByteSpan 3, len 5 | ByteSpan 4, len 9 |
       +-------------------+-------------------+-------------------+-------------------+

       Written as the following Uint64 on disk: [7, 10, 15, 24]
         - The first ByteSpan is 7 bytes long, and starts at offset 0.
         - The second ByteSpan is 3 bytes long, and starts at offset 7.
         - The third ByteSpan is 5 bytes long, and starts at offset 10.
         - The fourth ByteSpan is 9 bytes long, and starts at offset 15.

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
           +--------------------------------+---------------------------+
           | (Uint32) Dictionary ByteSpanId | (Uint32) Chunk ByteSpanId |
           +--------------------------------+---------------------------+
        - Dictionary: ID for a ByteSpan to be used as zstd dictionary. 0 refers to the empty ByteSpan, which indicates no dictionary.
        - Chunk: ID for the ByteSpan containing the Chunk data. Never 0.
        - ChunkRefs with a Dictionary ID of 0 are zStd compressed Chunks. The Chunk data is stored in the second ByteSpan. (version 1)
        - ChunkRefs with a Dictionary ID of 0 are Snappy compressed Chunks. The Chunk data is stored in the second ByteSpan. (version 2)

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
    - Decompress the Chunk data using zstd (no dictionary, version 1).
    - Decompress the Chunk data using snappy (no dictionary, version 2).
  - Otherwise:
    - Retrieve the ByteSpan ID for the Dictionary data.
    - Decompress the Chunk data using zstd with the Dictionary data.
*/

const (
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
	ArchiveFileSuffix = ".darc"
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

// Archive Format Versions.
const (
	archiveVersionInitial       = uint8(1)
	archiveVersionSnappySupport = uint8(2)
	archiveFormatVersionMax     = archiveVersionSnappySupport
)

// Archive Metadata Data Keys are the fields in the archive metadata that are stored in the footer. These are used
// to store information about the archive that is semi-structured. The data is stored in JSON format, all values are strings.
const ( //amdk = Archive Metadata Data Key
	// The version of Dolt that created the archive.
	amdkDoltVersion = "dolt_version"
	// The id of the table file that the archive was created from. This value can be used during the reverse process
	// to quickly get back to the original table file if it is still available.
	amdkOriginTableFile = "origin_table_file"
	// The timestamp of when the archive was created.
	amdkConversionTime = "conversion_time"
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
