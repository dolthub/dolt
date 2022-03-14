// Copyright 2019 Dolthub, Inc.
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
	"bytes"
	"context"
	"crypto/sha512"
	"encoding/base32"
	"encoding/binary"
	"hash/crc32"
	"io"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

/*
   An NBS Table stores N byte slices ("chunks") which are addressed by a 20-byte hash of their
   contents. The footer encodes N as well as the total bytes consumed by all contained chunks.
   An Index maps each address to the position of its corresponding chunk. Addresses are logically sorted within the Index, but the corresponding chunks need not be.

   Table:
   +----------------+----------------+-----+----------------+-------+--------+
   | Chunk Record 0 | Chunk Record 1 | ... | Chunk Record N | Index | Footer |
   +----------------+----------------+-----+----------------+-------+--------+

   Chunk Record:
   +---------------------------+----------------+
   | (Chunk Length) Chunk Data | (Uint32) CRC32 |
   +---------------------------+----------------+

   Index:
   +------------+---------+----------+
   | Prefix Map | Lengths | Suffixes |
   +------------+---------+----------+

   Prefix Map:
   +--------------+--------------+-----+----------------+
   | Prefix Tuple | Prefix Tuple | ... | Prefix Tuple N |
   +--------------+--------------+-----+----------------+

     -The Prefix Map contains N Prefix Tuples.
     -Each Prefix Tuple corresponds to a unique Chunk Record in the Table.
     -The Prefix Tuples are sorted in increasing lexicographic order within the Prefix Map.
     -NB: THE SAME PREFIX MAY APPEAR MULTIPLE TIMES, as distinct Hashes (referring to distinct Chunks) may share the same Prefix.

   Prefix Tuple:
   +-----------------+------------------+
   | (8) Hash Prefix | (Uint32) Ordinal |
   +-----------------+------------------+

     -First 8 bytes of a Chunk's Hash
     -Ordinal is the 0-based ordinal position of the associated record within the sequence of chunk records, the associated Length within Lengths, and the associated Hash Suffix within Suffixes.

   Lengths:
   +-----------------+-----------------+-----+-------------------+
   | (Uint32) Length | (Uint32) Length | ... | (Uint32) Length N |
   +-----------------+-----------------+-----+-------------------+

     - Each Length is the length of a Chunk Record in this Table.
     - Length M must correspond to Chunk Record M for 0 <= M <= N

   Suffixes:
   +------------------+------------------+-----+--------------------+
   | (12) Hash Suffix | (12) Hash Suffix | ... | (12) Hash Suffix N |
   +------------------+------------------+-----+--------------------+

     - Each Hash Suffix is the last 12 bytes of a Chunk in this Table.
     - Hash Suffix M must correspond to Chunk Record M for 0 <= M <= N

   Footer:
   +----------------------+----------------------------------------+------------------+
   | (Uint32) Chunk Count | (Uint64) Total Uncompressed Chunk Data | (8) Magic Number |
   +----------------------+----------------------------------------+------------------+

     -Total Uncompressed Chunk Data is the sum of the uncompressed byte lengths of all contained chunk byte slices.
     -Magic Number is the first 8 bytes of the SHA256 hash of "https://github.com/attic-labs/nbs".

    NOTE: Unsigned integer quanities, hashes and hash suffix are all encoded big-endian


  Looking up Chunks in an NBS Table
  There are two phases to loading chunk data for a given Hash from an NBS Table: Checking for the chunk's presence, and fetching the chunk's bytes. When performing a has-check, only the first phase is necessary.

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
  - Calculate the Offset of your desired Chunk Record: Sum(Lengths[0]...Lengths[Ordinal-1])
  - Load Lengths[Ordinal] bytes from Table[Offset]
  - Check the first 4 bytes of the loaded data against the last 4 bytes of your desired Hash. They should match, and the rest of the data is your Chunk data.
*/

const (
	addrSize        = 20
	addrPrefixSize  = 8
	addrSuffixSize  = addrSize - addrPrefixSize
	uint64Size      = 8
	uint32Size      = 4
	ordinalSize     = uint32Size
	lengthSize      = uint32Size
	offsetSize      = uint64Size
	magicNumber     = "\xff\xb5\xd8\xc2\x24\x63\xee\x50"
	magicNumberSize = 8 //len(magicNumber)
	footerSize      = uint32Size + uint64Size + magicNumberSize
	prefixTupleSize = addrPrefixSize + ordinalSize
	checksumSize    = uint32Size
	maxChunkSize    = 0xffffffff // Snappy won't compress slices bigger than this
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func crc(b []byte) uint32 {
	return crc32.Update(0, crcTable, b)
}

func computeAddrDefault(data []byte) addr {
	r := sha512.Sum512(data)
	h := addr{}
	copy(h[:], r[:addrSize])
	return h
}

var computeAddr = computeAddrDefault

type addr [addrSize]byte

var encoding = base32.NewEncoding("0123456789abcdefghijklmnopqrstuv")

func (a addr) String() string {
	return encoding.EncodeToString(a[:])
}

func (a addr) Prefix() uint64 {
	return binary.BigEndian.Uint64(a[:])
}

func (a addr) Checksum() uint32 {
	return binary.BigEndian.Uint32(a[addrSize-checksumSize:])
}

func parseAddr(str string) (addr, error) {
	var h addr
	_, err := encoding.Decode(h[:], []byte(str))
	return h, err
}

func ValidateAddr(s string) bool {
	_, err := encoding.DecodeString(s)
	return err == nil
}

type addrSlice []addr

func (hs addrSlice) Len() int           { return len(hs) }
func (hs addrSlice) Less(i, j int) bool { return bytes.Compare(hs[i][:], hs[j][:]) < 0 }
func (hs addrSlice) Swap(i, j int)      { hs[i], hs[j] = hs[j], hs[i] }

type hasRecord struct {
	a      *addr
	prefix uint64
	order  int
	has    bool
}

type hasRecordByPrefix []hasRecord

func (hs hasRecordByPrefix) Len() int           { return len(hs) }
func (hs hasRecordByPrefix) Less(i, j int) bool { return hs[i].prefix < hs[j].prefix }
func (hs hasRecordByPrefix) Swap(i, j int)      { hs[i], hs[j] = hs[j], hs[i] }

type hasRecordByOrder []hasRecord

func (hs hasRecordByOrder) Len() int           { return len(hs) }
func (hs hasRecordByOrder) Less(i, j int) bool { return hs[i].order < hs[j].order }
func (hs hasRecordByOrder) Swap(i, j int)      { hs[i], hs[j] = hs[j], hs[i] }

type getRecord struct {
	a      *addr
	prefix uint64
	found  bool
}

type getRecordByPrefix []getRecord

func (hs getRecordByPrefix) Len() int           { return len(hs) }
func (hs getRecordByPrefix) Less(i, j int) bool { return hs[i].prefix < hs[j].prefix }
func (hs getRecordByPrefix) Swap(i, j int)      { hs[i], hs[j] = hs[j], hs[i] }

type extractRecord struct {
	a    addr
	data []byte
	err  error
}

type chunkReader interface {
	has(h addr) (bool, error)
	hasMany(addrs []hasRecord) (bool, error)
	get(ctx context.Context, h addr, stats *Stats) ([]byte, error)
	getMany(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, *chunks.Chunk), stats *Stats) (bool, error)
	getManyCompressed(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, CompressedChunk), stats *Stats) (bool, error)
	extract(ctx context.Context, chunks chan<- extractRecord) error
	count() (uint32, error)
	uncompressedLen() (uint64, error)

	// Close releases resources retained by the |chunkReader|.
	Close() error
}

type chunkReadPlanner interface {
	findOffsets(reqs []getRecord) (ors offsetRecSlice, remaining bool, err error)
	getManyAtOffsets(
		ctx context.Context,
		eg *errgroup.Group,
		offsetRecords offsetRecSlice,
		found func(context.Context, *chunks.Chunk),
		stats *Stats,
	) error
	getManyCompressedAtOffsets(
		ctx context.Context,
		eg *errgroup.Group,
		offsetRecords offsetRecSlice,
		found func(context.Context, CompressedChunk),
		stats *Stats,
	) error
}

type chunkSource interface {
	chunkReader
	hash() (addr, error)
	calcReads(reqs []getRecord, blockSize uint64) (reads int, remaining bool, err error)

	// opens a Reader to the first byte of the chunkData segment of this table.
	reader(context.Context) (io.Reader, error)
	// size returns the total size of the chunkSource: chunks, index, and footer
	size() (uint64, error)
	index() (tableIndex, error)

	// Clone returns a |chunkSource| with the same contents as the
	// original, but with independent |Close| behavior. A |chunkSource|
	// cannot be |Close|d more than once, so if a |chunkSource| is being
	// retained in two objects with independent life-cycle, it should be
	// |Clone|d first.
	Clone() (chunkSource, error)
}

type chunkSources []chunkSource

// TableFile is an interface for working with an existing table file
type TableFile interface {
	// FileID gets the id of the file
	FileID() string

	// NumChunks returns the number of chunks in a table file
	NumChunks() int

	// Open returns an io.ReadCloser which can be used to read the bytes of a table file. The total length of the
	// table file in bytes can be optionally returned.
	Open(ctx context.Context) (io.ReadCloser, uint64, error)
}

// Describes what is possible to do with TableFiles in a TableFileStore.
type TableFileStoreOps struct {
	// True is the TableFileStore supports reading table files.
	CanRead bool
	// True is the TableFileStore supports writing table files.
	CanWrite bool
	// True is the TableFileStore supports pruning unused table files.
	CanPrune bool
	// True is the TableFileStore supports garbage collecting chunks.
	CanGC bool
}

// TableFileStore is an interface for interacting with table files directly
type TableFileStore interface {
	// Sources retrieves the current root hash, a list of all the table files (which may include appendix table files),
	// and a second list containing only appendix table files.
	Sources(ctx context.Context) (hash.Hash, []TableFile, []TableFile, error)

	// Size  returns the total size, in bytes, of the table files in this Store.
	Size(ctx context.Context) (uint64, error)

	// WriteTableFile will read a table file from the provided reader and write it to the TableFileStore.
	WriteTableFile(ctx context.Context, fileId string, numChunks int, rd io.Reader, contentLength uint64, contentHash []byte) error

	// AddTableFilesToManifest adds table files to the manifest
	AddTableFilesToManifest(ctx context.Context, fileIdToNumChunks map[string]int) error

	// PruneTableFiles deletes old table files that are no longer referenced in the manifest.
	PruneTableFiles(ctx context.Context) error

	// SetRootChunk changes the root chunk hash from the previous value to the new root.
	SetRootChunk(ctx context.Context, root, previous hash.Hash) error

	// SupportedOperations returns a description of the support TableFile operations. Some stores only support reading table files, not writing.
	SupportedOperations() TableFileStoreOps
}
