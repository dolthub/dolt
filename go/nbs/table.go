// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"crypto/sha512"
	"encoding/base32"
	"encoding/binary"
	"hash/crc32"
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

     -Address suffix is the 4 least-significant bytes of the Chunk's address. Used (e.g. in place
      of CRC32) as a checksum and a filter against false positive reads costing more than one IOP.

   Index:
   +------------+-------+----------+
   | Prefix Map | Sizes | Suffixes |
   +------------+-------+----------+

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
   +----------------------+---------------------------+------------------+
   | (Uint32) Chunk Count | (Uint64) Total Chunk Data | (8) Magic Number |
   +----------------------+---------------------------+------------------+

     -Total Chunk Data is the sum of the logical byte lengths of all contained chunk byte slices.
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
	addrSize           uint64 = 20
	addrPrefixSize     uint64 = 8
	addrSuffixSize            = addrSize - addrPrefixSize
	uint64Size         uint64 = 8
	uint32Size         uint64 = 4
	ordinalSize        uint64 = uint32Size
	lengthSize         uint64 = uint32Size
	magicNumber               = "\xff\xb5\xd8\xc2\x24\x63\xee\x50"
	magicNumberSize    uint64 = uint64(len(magicNumber))
	footerSize                = uint32Size + uint64Size + magicNumberSize
	prefixTupleSize           = addrPrefixSize + ordinalSize
	checksumSize       uint64 = uint32Size
	maxChunkLengthSize uint64 = binary.MaxVarintLen64
	maxChunkSize       uint64 = 0xffffffff // Snappy won't compress slices bigger than this
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

func ParseAddr(b []byte) (h addr) {
	encoding.Decode(h[:], b)
	return
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
	order  int
	data   []byte
}

type getRecordByPrefix []getRecord

func (hs getRecordByPrefix) Len() int           { return len(hs) }
func (hs getRecordByPrefix) Less(i, j int) bool { return hs[i].prefix < hs[j].prefix }
func (hs getRecordByPrefix) Swap(i, j int)      { hs[i], hs[j] = hs[j], hs[i] }

type getRecordByOrder []getRecord

func (hs getRecordByOrder) Len() int           { return len(hs) }
func (hs getRecordByOrder) Less(i, j int) bool { return hs[i].order < hs[j].order }
func (hs getRecordByOrder) Swap(i, j int)      { hs[i], hs[j] = hs[j], hs[i] }

type chunkReader interface {
	has(h addr) bool
	hasMany(addrs []hasRecord) bool
	get(h addr) []byte
	getMany(reqs []getRecord) bool
	count() uint32
}

type chunkSource interface {
	chunkReader
	close() error
	hash() addr
}
