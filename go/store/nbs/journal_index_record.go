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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sort"

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
	// index record length
	length uint32

	// root hash of commit when this index record was written
	lastRoot hash.Hash

	// file offsets for the region of the journal file
	// that |payload| indexes. end points to a root hash
	// record in the journal containing |lastRoot|.
	// we expect a sequence of index records to cover
	// contiguous regions of the journal file.
	start, end uint64

	// index record kind
	kind indexRecKind

	// encoded chunk index
	payload []byte

	// index record crc32 checksum
	checksum uint32
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

func validateIndexRecord(buf []byte) (ok bool) {
	if len(buf) > (indexRecLenSz + indexRecChecksumSz) {
		off := len(buf) - indexRecChecksumSz
		ok = crc(buf[:off]) == readUint32(buf[off:])
	}
	return
}

// processIndexRecords reads a sequence of index records from |r| and passes them to the callback. While reading records
// it makes some basic assertions that the sequence is well-formed and indexes a contiguous region for the journal file.
func processIndexRecords(ctx context.Context, r io.ReadSeeker, sz int64, cb func(o int64, r indexRec) error) (err error) {
	var (
		buf  []byte
		off  int64
		prev uint64
	)

	rdr := bufio.NewReader(r)
	for off < sz {
		// peek to read next record size
		if buf, err = rdr.Peek(uint32Size); err != nil {
			break
		}

		l := readUint32(buf)
		if int64(l) > sz {
			return fmt.Errorf("invalid record size %d for index file of size %d", l, sz)
		}
		if len(buf) < int(l) {
			buf = make([]byte, l)
		}
		if _, err = io.ReadFull(rdr, buf); err != nil {
			break
		}

		// we do not zero-fill the journal index and expect
		// only complete records that will checksum
		if !validateIndexRecord(buf) {
			return fmt.Errorf("failed to checksum index record at %d", off)
		}

		var rec indexRec
		if rec, err = readJournalIndexRecord(buf); err != nil {
			return err
		} else if rec.start != prev {
			return fmt.Errorf("index records do not cover contiguous region (%d != %d)", rec.end, prev)
		}

		if err = cb(off, rec); err != nil {
			return err
		}
		prev = rec.end
		off += int64(len(buf))
	}
	if err == nil && off != sz {
		err = fmt.Errorf("failed to process entire journal index (%d < %d)", off, sz)
	} else if err == io.EOF {
		err = nil
	}
	return
}

type lookup struct {
	a addr
	r Range
}

const lookupSize = addrSize + offsetSize + lengthSize

// serializeLookups serializes |lookups| using the table file chunk index format.
func serializeLookups(lookups []lookup) (index []byte) {
	index = make([]byte, len(lookups)*lookupSize)
	sort.Slice(lookups, func(i, j int) bool { // sort by addr
		return bytes.Compare(lookups[i].a[:], lookups[j].a[:]) < 0
	})
	buf := index
	for _, l := range lookups {
		copy(buf, l.a[:])
		buf = buf[addrSize:]
		binary.BigEndian.PutUint64(buf, l.r.Offset)
		buf = buf[offsetSize:]
		binary.BigEndian.PutUint32(buf, l.r.Length)
		buf = buf[lengthSize:]
	}
	return
}

func deserializeLookups(index []byte) (lookups []lookup) {
	lookups = make([]lookup, len(index)/lookupSize)
	for i := range lookups {
		copy(lookups[i].a[:], index)
		index = index[addrSize:]
		lookups[i].r.Offset = binary.BigEndian.Uint64(index)
		index = index[offsetSize:]
		lookups[i].r.Length = binary.BigEndian.Uint32(index)
		index = index[lengthSize:]
	}
	return
}
