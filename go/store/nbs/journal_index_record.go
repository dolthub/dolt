// Copyright 2023 Dolthub, Inc.
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
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

// indexRec is a record in a chunk journal index file. Index records
// serve as out-of-band chunk indexes into the chunk journal that allow
// bootstrapping the journal without reading each record in the journal.
//
// Like journalRec, its serialization format uses uint8 tag prefixes
// to identify fields and allow for format evolution.
type indexRec struct {
	// encoded chunk index
	payload []byte
	// file offsets for the region of the journal file
	// that |payload| indexes. end points to a root hash
	// record in the journal containing |lastRoot|.
	// we expect a sequence of index records to cover
	// contiguous regions of the journal file.tart uint64
	start uint64
	end   uint64
	// index record length
	length uint32
	// index record crc32 checksum
	checksum uint32
	// root hash of commit when this index record was written
	lastRoot hash.Hash
	// index record kind
	kind indexRecKind
}

type indexRecKind uint8

const (
	unknownIndexRecKind indexRecKind = 0
	tableIndexRecKind   indexRecKind = 1
)

type indexRecTag uint8

const (
	unknownIndexRecTag     indexRecTag = 0
	lastRootIndexRecTag    indexRecTag = 1
	startOffsetIndexRecTag indexRecTag = 2
	endOffsetIndexRecTag   indexRecTag = 3
	kindIndexRecTag        indexRecTag = 4
	payloadIndexRecTag     indexRecTag = 5
)

const (
	indexRecTagSz      = 1
	indexRecLenSz      = 4
	indexRecKindSz     = 1
	indexRecLastRootSz = 20
	indexRecOffsetSz   = 8
	indexRecChecksumSz = 4
	lookupSz           = 16 + uint64Size + uint32Size
	lookupMetaSz       = uint64Size + uint64Size + uint32Size + hash.ByteLen
)

func journalIndexRecordSize(idx []byte) (recordSz uint32) {
	recordSz += indexRecLenSz
	recordSz += indexRecTagSz + indexRecLastRootSz
	recordSz += indexRecTagSz + indexRecOffsetSz
	recordSz += indexRecTagSz + indexRecOffsetSz
	recordSz += indexRecTagSz + indexRecKindSz
	recordSz += indexRecTagSz // payload tag
	recordSz += uint32(len(idx))
	recordSz += indexRecChecksumSz
	return
}

func writeJournalIndexRecord(buf []byte, root hash.Hash, start, end uint64, idx []byte) (n uint32) {
	//defer trace.StartRegion(ctx, "writeJournalIndexRecord").End()

	// length
	l := journalIndexRecordSize(idx)
	writeUint32(buf[:indexRecLenSz], l)
	n += indexRecLenSz
	// last root
	buf[n] = byte(lastRootIndexRecTag)
	n += indexRecTagSz
	copy(buf[n:], root[:])
	n += indexRecLastRootSz
	// start offset
	buf[n] = byte(startOffsetIndexRecTag)
	n += indexRecTagSz
	writeUint64(buf[n:], start)
	n += indexRecOffsetSz
	// end offset
	buf[n] = byte(endOffsetIndexRecTag)
	n += indexRecTagSz
	writeUint64(buf[n:], end)
	n += indexRecOffsetSz
	// kind
	buf[n] = byte(kindIndexRecTag)
	n += indexRecTagSz
	buf[n] = byte(tableIndexRecKind)
	n += indexRecKindSz
	// payload
	buf[n] = byte(payloadIndexRecTag)
	n += indexRecTagSz
	copy(buf[n:], idx)
	n += uint32(len(idx))
	// checksum
	writeUint32(buf[n:], crc(buf[:n]))
	n += indexRecChecksumSz
	d.PanicIfFalse(l == n)
	return
}

func readJournalIndexRecord(buf []byte) (rec indexRec, err error) {
	rec.length = readUint32(buf)
	buf = buf[indexRecLenSz:]
	for len(buf) > indexRecChecksumSz {
		tag := indexRecTag(buf[0])
		buf = buf[indexRecTagSz:]
		switch tag {
		case lastRootIndexRecTag:
			copy(rec.lastRoot[:], buf)
			buf = buf[indexRecLastRootSz:]
		case startOffsetIndexRecTag:
			rec.start = readUint64(buf)
			buf = buf[indexRecOffsetSz:]
		case endOffsetIndexRecTag:
			rec.end = readUint64(buf)
			buf = buf[indexRecOffsetSz:]
		case kindIndexRecTag:
			rec.kind = indexRecKind(buf[0])
			buf = buf[indexRecKindSz:]
		case payloadIndexRecTag:
			sz := len(buf) - indexRecChecksumSz
			rec.payload = buf[:sz]
			buf = buf[sz:]
		case unknownIndexRecTag:
			fallthrough
		default:
			err = fmt.Errorf("unknown record field tag: %d", tag)
			return
		}
	}
	rec.checksum = readUint32(buf[:indexRecChecksumSz])
	return
}

func validateIndexRecord(buf []byte) bool {
	if len(buf) < (indexRecLenSz + indexRecChecksumSz) {
		return false
	}
	off := readUint32(buf)
	if int(off) > len(buf) {
		return false
	}
	off -= indexRecChecksumSz
	return crc(buf[:off]) == readUint32(buf[off:])
}

type lookupMeta struct {
	batchStart int64
	batchEnd   int64
	checkSum   uint32
	latestHash hash.Hash
}

const indexRecTypeSize = 1
const (
	indexRecChunk byte = iota
	indexRecMeta
)

// processIndexRecords reads batches of chunk index lookups into the journal.
// An index batch looks like |lookup|lookup|...|meta|. The first byte of a record
// indicates whether it is a |lookup| or |meta|. Only callback errors are returned.
// The caller is expected to track the latest lookupMeta end offset and truncate
// the index to compensate for partially written batches.
func processIndexRecords(rd *bufio.Reader, sz int64, cb func(lookupMeta, []lookup, uint32) error) (off int64, err error) {
	var batchCrc uint32
	var batch []lookup
	var batchOff int64
	for off < sz {
		recTag, err := rd.ReadByte()
		if err != nil {
			return off, nil
		}
		batchOff += 1

		switch recTag {
		case indexRecChunk:
			l, err := readIndexLookup(rd)
			if err != nil {
				return off, nil
			}
			batchOff += lookupSz
			batch = append(batch, l)
			batchCrc = crc32.Update(batchCrc, crcTable, l.a[:])

		case indexRecMeta:
			m, err := readIndexMeta(rd)
			if err != nil {
				return off, nil
			}
			if err := cb(m, batch, batchCrc); err != nil {
				return off, err
			}
			batch = nil
			batchCrc = 0
			off += batchOff + lookupMetaSz
			batchOff = 0
		default:
			return off, ErrMalformedIndex
		}
	}
	return off, nil
}

var ErrMalformedIndex = errors.New("journal index is malformed")

// readIndexLookup reads a sequence of |chunkAddress|journalOffset|chunkLength|
// that is used to speed up |journal.ranges| initialization.
func readIndexLookup(r *bufio.Reader) (lookup, error) {
	addr := addr16{}
	if _, err := io.ReadFull(r, addr[:]); err != nil {
		return lookup{}, err
	}

	var offsetBuf [uint64Size]byte
	if _, err := io.ReadFull(r, offsetBuf[:]); err != nil {
		return lookup{}, err
	}
	offset := binary.BigEndian.Uint64(offsetBuf[:])

	var lengthBuf [uint32Size]byte
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		return lookup{}, err
	}
	length := binary.BigEndian.Uint32(lengthBuf[:])

	return lookup{a: addr, r: Range{Offset: offset, Length: length}}, nil
}

// readIndexMeta reads a sequence of |journalStart|journalEnd|lastRootHash|checksum|
// that is used to validate a range of lookups on read. A corrupted lookup in the
// start-end range will cause the checksum/crc check to fail. The last root hash
// is a duplicate sanity check.
func readIndexMeta(r *bufio.Reader) (lookupMeta, error) {
	var startBuf [offsetSize]byte
	if _, err := io.ReadFull(r, startBuf[:]); err != nil {
		return lookupMeta{}, err
	}
	startOff := binary.BigEndian.Uint64(startBuf[:])

	var endBuf [offsetSize]byte
	if _, err := io.ReadFull(r, endBuf[:]); err != nil {
		return lookupMeta{}, err
	}
	endOff := binary.BigEndian.Uint64(endBuf[:])

	var checksumBuf [checksumSize]byte
	if _, err := io.ReadFull(r, checksumBuf[:]); err != nil {
		return lookupMeta{}, err
	}
	checksum := binary.BigEndian.Uint32(checksumBuf[:])

	addr := hash.Hash{}
	if _, err := io.ReadFull(r, addr[:]); err != nil {
		return lookupMeta{}, err
	}

	return lookupMeta{
		batchStart: int64(startOff),
		batchEnd:   int64(endOff),
		checkSum:   checksum,
		latestHash: addr,
	}, nil
}

func writeIndexLookup(w *bufio.Writer, l lookup) error {
	w.WriteByte(indexRecChunk)

	if _, err := w.Write(l.a[:]); err != nil {
		return err
	}

	var offsetBuf [offsetSize]byte
	binary.BigEndian.PutUint64(offsetBuf[:], l.r.Offset)
	if _, err := w.Write(offsetBuf[:]); err != nil {
		return err
	}

	var lengthBuf [lengthSize]byte
	binary.BigEndian.PutUint32(lengthBuf[:], l.r.Length)
	if _, err := w.Write(lengthBuf[:]); err != nil {
		return err
	}

	return nil
}

// writeJournalIndexMeta writes a metadata record for an index range to verify
// index bootstrapping integrity. Includes the range of index lookups, a CRC
// checksum, and the latest root hash before |end|.
func writeJournalIndexMeta(w *bufio.Writer, root hash.Hash, start, end int64, checksum uint32) error {
	// |journal start|journal end|last root hash|range checkSum|

	if err := w.WriteByte(indexRecMeta); err != nil {
		return err
	}

	startBuf := make([]byte, offsetSize)
	binary.BigEndian.PutUint64(startBuf, uint64(start))
	if _, err := w.Write(startBuf); err != nil {
		return err
	}

	endBuf := make([]byte, offsetSize)
	binary.BigEndian.PutUint64(endBuf, uint64(end))
	if _, err := w.Write(endBuf); err != nil {
		return err
	}

	checksumBuf := make([]byte, checksumSize)
	binary.BigEndian.PutUint32(checksumBuf, checksum)
	if _, err := w.Write(checksumBuf); err != nil {
		return err
	}

	if _, err := w.Write(root[:]); err != nil {
		return err
	}

	return nil
}

type lookup struct {
	a addr16
	r Range
}
