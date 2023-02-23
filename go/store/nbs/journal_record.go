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
	"github.com/dolthub/dolt/go/store/hash"
)

// journalRec is a record in a chunk journal. Its serialization format uses
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
	kind     journalRecKind
	address  addr
	payload  []byte
	checksum uint32
}

// payloadOffset returns the journalOffset of the payload within the record
// assuming only the checksum field follows the payload.
func (r journalRec) payloadOffset() uint32 {
	return r.length - uint32(len(r.payload)+journalRecChecksumSz)
}

// uncompressedPayloadSize returns the uncompressed size of the payload.
func (r journalRec) uncompressedPayloadSize() (sz uint64) {
	// |r.payload| is snappy-encoded and starts with
	// the uvarint-encoded uncompressed data size
	sz, _ = binary.Uvarint(r.payload)
	return
}

type journalRecKind uint8

const (
	unknownJournalRecKind  journalRecKind = 0
	rootHashJournalRecKind journalRecKind = 1
	chunkJournalRecKind    journalRecKind = 2
)

type journalRecTag uint8

const (
	unknownJournalRecTag journalRecTag = 0
	kindJournalRecTag    journalRecTag = 1
	addrJournalRecTag    journalRecTag = 2
	payloadJournalRecTag journalRecTag = 3
)

const (
	journalRecTagSz      = 1
	journalRecLenSz      = 4
	journalRecKindSz     = 1
	journalRecAddrSz     = 20
	journalRecChecksumSz = 4

	// todo(andy): less arbitrary
	journalRecMaxSz = 128 * 1024
)

func chunkRecordSize(c CompressedChunk) (recordSz, payloadOff uint32) {
	recordSz += journalRecLenSz
	recordSz += journalRecTagSz + journalRecKindSz
	recordSz += journalRecTagSz + journalRecAddrSz
	recordSz += journalRecTagSz // payload tag
	payloadOff = recordSz
	recordSz += uint32(len(c.FullCompressedChunk))
	recordSz += journalRecChecksumSz
	return
}

func rootHashRecordSize() (recordSz int) {
	recordSz += journalRecLenSz
	recordSz += journalRecTagSz + journalRecKindSz
	recordSz += journalRecTagSz + journalRecAddrSz
	recordSz += journalRecChecksumSz
	return
}

func writeChunkRecord(buf []byte, c CompressedChunk) (n uint32) {
	// length
	l, _ := chunkRecordSize(c)
	writeUint32(buf[:journalRecLenSz], l)
	n += journalRecLenSz
	// kind
	buf[n] = byte(kindJournalRecTag)
	n += journalRecTagSz
	buf[n] = byte(chunkJournalRecKind)
	n += journalRecKindSz
	// address
	buf[n] = byte(addrJournalRecTag)
	n += journalRecTagSz
	copy(buf[n:], c.H[:])
	n += journalRecAddrSz
	// payload
	buf[n] = byte(payloadJournalRecTag)
	n += journalRecTagSz
	copy(buf[n:], c.FullCompressedChunk)
	n += uint32(len(c.FullCompressedChunk))
	// checksum
	writeUint32(buf[n:], crc(buf[:n]))
	n += journalRecChecksumSz
	d.PanicIfFalse(l == n)
	return
}

func writeRootHashRecord(buf []byte, root addr) (n uint32) {
	// length
	l := rootHashRecordSize()
	writeUint32(buf[:journalRecLenSz], uint32(l))
	n += journalRecLenSz
	// kind
	buf[n] = byte(kindJournalRecTag)
	n += journalRecTagSz
	buf[n] = byte(rootHashJournalRecKind)
	n += journalRecKindSz
	// address
	buf[n] = byte(addrJournalRecTag)
	n += journalRecTagSz
	copy(buf[n:], root[:])
	n += journalRecAddrSz
	// empty payload
	// checksum
	writeUint32(buf[n:], crc(buf[:n]))
	n += journalRecChecksumSz
	return
}

func readJournalRecord(buf []byte) (rec journalRec, err error) {
	rec.length = readUint32(buf)
	buf = buf[journalRecLenSz:]
	for len(buf) > journalRecChecksumSz {
		tag := journalRecTag(buf[0])
		buf = buf[journalRecTagSz:]
		switch tag {
		case kindJournalRecTag:
			rec.kind = journalRecKind(buf[0])
			buf = buf[journalRecKindSz:]
		case addrJournalRecTag:
			copy(rec.address[:], buf)
			buf = buf[journalRecAddrSz:]
		case payloadJournalRecTag:
			sz := len(buf) - journalRecChecksumSz
			rec.payload = buf[:sz]
			buf = buf[sz:]
		case unknownJournalRecTag:
			fallthrough
		default:
			err = fmt.Errorf("unknown record field tag: %d", tag)
			return
		}
	}
	rec.checksum = readUint32(buf[:journalRecChecksumSz])
	return
}

func validateJournalRecord(buf []byte) (ok bool) {
	if len(buf) > (journalRecLenSz + journalRecChecksumSz) {
		off := len(buf) - journalRecChecksumSz
		ok = crc(buf[:off]) == readUint32(buf[off:])
	}
	return
}

func processJournalRecords(ctx context.Context, r io.ReadSeeker, off int64, cb func(o int64, r journalRec) error) (int64, error) {
	var (
		buf []byte
		err error
	)

	// start processing records from |off|
	if _, err = r.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}

	rdr := bufio.NewReaderSize(r, journalWriterBuffSize)
	for {
		// peek to read next record size
		if buf, err = rdr.Peek(uint32Size); err != nil {
			break
		}

		l := readUint32(buf)
		if l > journalRecMaxSz {
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

func peekRootHashAt(journal io.ReaderAt, offset int64) (root hash.Hash, err error) {
	buf := make([]byte, 1024) // assumes len(rec) < 1024
	if _, err = journal.ReadAt(buf, offset); err != nil {
		return
	}
	sz := readUint32(buf)
	if sz > journalRecMaxSz {
		err = fmt.Errorf("invalid root hash record size at %d", offset)
		return
	}
	buf = buf[:sz]
	if !validateIndexRecord(buf) {
		err = fmt.Errorf("failed to validate root hash record at %d", offset)
		return
	}
	var rec journalRec
	if rec, err = readJournalRecord(buf); err != nil {
		return
	} else if rec.kind != rootHashJournalRecKind {
		err = fmt.Errorf("expected root hash record, got kind: %d", rec.kind)
		return
	}
	return hash.Hash(rec.address), nil
}

func readUint32(buf []byte) uint32 {
	return binary.BigEndian.Uint32(buf)
}

func writeUint32(buf []byte, u uint32) {
	binary.BigEndian.PutUint32(buf, u)
}

func readUint64(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}

func writeUint64(buf []byte, u uint64) {
	binary.BigEndian.PutUint64(buf, u)
}
