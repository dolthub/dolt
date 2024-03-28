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
   | (Uint64) IndexLength | (Uint32) ByteSpan Count | (Uint32) Chunk Count | (1) Format Version | (7) File Signature |
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
       +----------------+----------------+-----+------------------+
       | Prefix Tuple 0 | Prefix Tuple 1 | ... | Prefix Tuple M-1 |
       +----------------+----------------+-----+------------------+
       - The Prefix Map contains M Prefix Tuples - one for each Chunk Record in the Table.
       - The Prefix Tuples are sorted in increasing lexicographic order within the Prefix Map.
       - NB: THE SAME PREFIX MAY APPEAR MULTIPLE TIMES, as distinct Hashes (referring to distinct Chunks) may share the same Prefix.

       Prefix Tuple:
           +----------------------+------------------+
           | (Uint64) Hash Prefix | (varint) Ordinal |
           +----------------------+------------------+
           - First 8 bytes of a Chunk's Hash
           - Ordinal is the 0-based ordinal position of the ChunkRecord in the Index. 0 <= Ordinal < N

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
     - Hash Suffix M must correspond to Chunk Record M.

ByteSpan:
   +----------------+----------------+
   | Data as []byte | (Uint32) CRC32 |
   +----------------+----------------+
	 - Self Explanatory.


Chunk Retrieval (phase 1 is identical to NBS):

  Phase one: Chunk presence
  - Slice off the first 8 bytes of your Hash to create a Prefix
  - Since the Prefix Tuples in the Prefix Map are in lexicographic order, binary search the Prefix Map for the desired Prefix.
  - For all Prefix Tuples with a matching Prefix:
    - Load the Ordinal
    - Use the Ordinal to index into Suffixes
    - Check the Suffix of your Hash against the loaded Suffix
    - If they match, your chunk is in this Table in the Chunk Record indicated by Ordinal
    - If they don't match, continue to the next matching Prefix Tuple
  - If not found, your chunk is not in this Table.

  Phase two: Loading Chunk data
  - Take the Ordinal discovered in Phase one
  - Use the Ordinal to index into ChunkReferences
  - Retrieve the ByteSpan ID for the Chunk data. Verify integrity with CRC.
  - If Dictionary is 0:
    - Decompress the Chunk data using zstd (no dictionary)
  - Otherwise:
    - Retrieve the ByteSpan ID for the Dictionary data. Verify integrity with CRC.
    - Decompress the Chunk data using zstd with the Dictionary data
*/
