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
	"io"

	"github.com/dolthub/dolt/go/store/d"

	"github.com/dolthub/dolt/go/store/hash"
)

// journalRec is a record in a chunk journal
// containing a chunk and its metadata.
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
	recLenSz      = 4
	recKindSz     = 1
	recAddrSz     = 20
	recChecksumSz = 4

	// todo(andy): less arbitrary
	recMaxSz = 128 * 1024

	rootHashRecordSize = recLenSz + recKindSz + addrSize + checksumSize
)

func chunkRecordSize(c CompressedChunk) (recordLen, payloadOff uint32) {
	recordLen += recLenSz
	recordLen += recKindSz
	recordLen += recAddrSz
	payloadOff = recordLen
	recordLen += uint32(len(c.FullCompressedChunk))
	recordLen += recChecksumSz
	return
}

func writeChunkRecord(buf []byte, c CompressedChunk) (n uint32) {
	l, _ := chunkRecordSize(c)
	writeUint(buf[:recLenSz], l)
	n += recLenSz
	buf[n] = byte(chunkRecKind)
	n += recKindSz
	copy(buf[n:], c.H[:])
	n += recAddrSz
	copy(buf[n:], c.FullCompressedChunk)
	n += uint32(len(c.FullCompressedChunk))
	writeUint(buf[n:], crc(buf[:n]))
	n += recChecksumSz
	d.PanicIfFalse(l == n)
	return
}

func writeRootHashRecord(buf []byte, root addr) (n uint32) {
	writeUint(buf[:recLenSz], rootHashRecordSize)
	n += recLenSz
	buf[n] = byte(rootHashRecKind)
	n += recKindSz
	copy(buf[n:], root[:])
	n += recAddrSz
	writeUint(buf[n:], crc(buf[:n]))
	n += recChecksumSz
	return
}

func readJournalRecord(buf []byte) (rec journalRec) {
	rec.length = readUint(buf)
	buf = buf[recLenSz:]
	rec.kind = recKind(buf[0])
	buf = buf[recKindSz:]
	copy(rec.address[:], buf)
	buf = buf[recAddrSz:]
	rec.payload = buf[:len(buf)-recChecksumSz]
	rec.checksum = readUint(buf[len(buf)-recChecksumSz:])
	return
}

func safeReadJournalRecord(buf []byte) (journalRec, bool) {
	o := len(buf) - recChecksumSz
	if crc(buf[:o]) != readUint(buf[o:]) {
		return journalRec{}, false
	}

	rec := readJournalRecord(buf)
	switch rec.kind {
	case rootHashRecKind:
		return rec, true

	case chunkRecKind:
		_, err := NewCompressedChunk(hash.Hash(rec.address), rec.payload)
		if err != nil {
			return journalRec{}, false
		}
		return rec, true

	default:
		return journalRec{}, false
	}
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
		if l < (recLenSz+recChecksumSz) || l > recMaxSz {
			break
		} else if buf, err = rdr.Peek(int(l)); err != nil {
			break
		}

		rec, ok := safeReadJournalRecord(buf)
		if !ok {
			break // stop if we can't validate |rec|
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
