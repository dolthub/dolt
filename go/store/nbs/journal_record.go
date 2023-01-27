// Copyright 2022 Dolthub, Inc.
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
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/store/d"
)

// journalRec is a record in a chunk journal. It's serialization format uses
// uint8 tag prefixes to identify fields and allow for format evolution.
//
// There are two kinds of journalRecs: chunk records and root hash records.
// Chunk records store chunks from persisted memTables. Root hash records
// store root hash updates to the manifest state.
// Future records kinds may include other updates to manifest state such as
// updates to GC generation or the table set lock hash.
//
// +-----------------+-------+---------+-----+-------------------+
// | length (uint32) | tag 0 | field 0 | ... | checksum (uint32) |
// +-----------------+-------+---------+-----+-------------------+
//
// Currently, the payload field is always written as the penultimate field,
// followed only by the fixed-width record checksum. This allows the payload
// to be extracted from the journalRec using only the record length and payload
// offset. See recLookup for more detail.
type journalRec struct {
	length   uint32
	kind     recKind
	address  addr
	payload  []byte
	checksum uint32
}

// payloadOffset returns the journalOffset of the payload within the record
// assuming only the checksum field follows the payload.
func (r journalRec) payloadOffset() uint32 {
	return r.length - uint32(len(r.payload)+recChecksumSz)
}

// uncompressedPayloadSize returns the uncompressed size of the payload.
func (r journalRec) uncompressedPayloadSize() (sz uint64) {
	// |r.payload| is snappy-encoded and starts with
	// the uvarint-encoded uncompressed data size
	sz, _ = binary.Uvarint(r.payload)
	return
}

type recKind uint8

const (
	unknownKind     recKind = 0
	rootHashRecKind recKind = 1
	chunkRecKind    recKind = 2
)

type recTag uint8

const (
	unknownTag recTag = 0
	kindTag    recTag = 1
	addrTag    recTag = 2
	payloadTag recTag = 3
)

const (
	recTagSz      = 1
	recLenSz      = 4
	recKindSz     = 1
	recAddrSz     = 20
	recChecksumSz = 4

	// todo(andy): less arbitrary
	recMaxSz = 128 * 1024
)

func chunkRecordSize(c CompressedChunk) (recordSz, payloadOff uint32) {
	recordSz += recLenSz
	recordSz += recTagSz + recKindSz
	recordSz += recTagSz + recAddrSz
	recordSz += recTagSz // payload tag
	payloadOff = recordSz
	recordSz += uint32(len(c.FullCompressedChunk))
	recordSz += recChecksumSz
	return
}

func rootHashRecordSize() (recordSz int) {
	recordSz += recLenSz
	recordSz += recTagSz + recKindSz
	recordSz += recTagSz + recAddrSz
	recordSz += recChecksumSz
	return
}

func writeChunkRecord(buf []byte, c CompressedChunk) (n uint32) {
	// length
	l, _ := chunkRecordSize(c)
	writeUint(buf[:recLenSz], l)
	n += recLenSz
	// kind
	buf[n] = byte(kindTag)
	n += recTagSz
	buf[n] = byte(chunkRecKind)
	n += recKindSz
	// address
	buf[n] = byte(addrTag)
	n += recTagSz
	copy(buf[n:], c.H[:])
	n += recAddrSz
	// payload
	buf[n] = byte(payloadTag)
	n += recTagSz
	copy(buf[n:], c.FullCompressedChunk)
	n += uint32(len(c.FullCompressedChunk))
	// checksum
	writeUint(buf[n:], crc(buf[:n]))
	n += recChecksumSz
	d.PanicIfFalse(l == n)
	return
}

func writeRootHashRecord(buf []byte, root addr) (n uint32) {
	// length
	l := rootHashRecordSize()
	writeUint(buf[:recLenSz], uint32(l))
	n += recLenSz
	// kind
	buf[n] = byte(kindTag)
	n += recTagSz
	buf[n] = byte(rootHashRecKind)
	n += recKindSz
	// address
	buf[n] = byte(addrTag)
	n += recTagSz
	copy(buf[n:], root[:])
	n += recAddrSz
	// empty payload
	// checksum
	writeUint(buf[n:], crc(buf[:n]))
	n += recChecksumSz
	return
}

func readJournalRecord(buf []byte) (rec journalRec, err error) {
	rec.length = readUint(buf)
	buf = buf[recLenSz:]
	for len(buf) > recChecksumSz {
		tag := recTag(buf[0])
		buf = buf[recTagSz:]
		switch tag {
		case kindTag:
			rec.kind = recKind(buf[0])
			buf = buf[recKindSz:]
		case addrTag:
			copy(rec.address[:], buf)
			buf = buf[recAddrSz:]
		case payloadTag:
			sz := len(buf) - recChecksumSz
			rec.payload = buf[:sz]
			buf = buf[sz:]
		case unknownTag:
			fallthrough
		default:
			err = fmt.Errorf("unknown record field tag: %d", tag)
			return
		}
	}
	rec.checksum = readUint(buf[:recChecksumSz])
	return
}

func validateJournalRecord(buf []byte) (ok bool) {
	if len(buf) > (recLenSz + recChecksumSz) {
		off := len(buf) - recChecksumSz
		ok = crc(buf[:off]) == readUint(buf[off:])
	}
	return
}

func processRecords(ctx context.Context, r io.ReadSeeker, cb func(o int64, r journalRec) error) (int64, error) {
	var (
		buf []byte
		off int64
		err error
	)

	rdr := bufio.NewReaderSize(r, journalWriterBuffSize)
	for {
		// peek to read next record size
		if buf, err = rdr.Peek(uint32Size); err != nil {
			break
		}

		l := readUint(buf)
		if l > recMaxSz {
			break
		} else if buf, err = rdr.Peek(int(l)); err != nil {
			break
		}

		if !validateJournalRecord(buf) {
			break // stop if we can't validate |rec|
		}

		var rec journalRec
		if rec, err = readJournalRecord(buf); err != nil {
			break // failed to read valid record
		}
		if err = cb(off, rec); err != nil {
			break
		}

		// advance |rdr| state by |l| bytes
		if _, err = io.ReadFull(rdr, buf); err != nil {
			break
		}
		off += int64(len(buf))
	}
	if err != nil && err != io.EOF {
		return 0, err
	}
	// reset the file pointer to end of the last
	// successfully processed journal record
	if _, err = r.Seek(off, 0); err != nil {
		return 0, err
	}
	return off, nil
}

func readUint(buf []byte) uint32 {
	return binary.BigEndian.Uint32(buf)
}

func writeUint(buf []byte, u uint32) {
	binary.BigEndian.PutUint32(buf, u)
}
