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

const (
	unknownKind  jrecordKind = 0
	rootHashKind jrecordKind = 1
	chunkKind    jrecordKind = 2

	recKindSz = 1
	recLenSz  = uint32Size
	recMinSz  = recLenSz + recKindSz + addrSize + checksumSize
	recMaxSz  = 128 * 1024 // todo(andy): less arbitrary

	chunkRecordHeaderSize = recLenSz + recKindSz + addrSize
	rootHashRecordSize    = recMinSz
)

type jrecordKind uint8

type jrecord struct {
	length   uint32
	kind     jrecordKind
	address  addr
	payload  []byte
	checksum uint32
}

func chunkRecordSize(c CompressedChunk) uint32 {
	return uint32(len(c.FullCompressedChunk)) + recMinSz
}

func writeChunkRecord(buf []byte, c CompressedChunk) (n uint32) {
	l := chunkRecordSize(c)
	writeUint(buf[:recLenSz], l)
	n += recLenSz
	buf[n] = byte(chunkKind)
	n += recKindSz
	copy(buf[n:], c.H[:])
	n += addrSize
	copy(buf[n:], c.FullCompressedChunk)
	n += uint32(len(c.FullCompressedChunk))
	writeUint(buf[n:], crc(buf[:n]))
	n += checksumSize
	d.PanicIfFalse(l == n)
	return
}

func writeRootHashRecord(buf []byte, root addr) (n uint32) {
	writeUint(buf[:recLenSz], rootHashRecordSize)
	n += recLenSz
	buf[n] = byte(rootHashKind)
	n += recKindSz
	copy(buf[n:], root[:])
	n += addrSize
	writeUint(buf[n:], crc(buf[:n]))
	n += checksumSize
	return
}

func readJournalRecord(buf []byte) (rec jrecord) {
	rec.length = readUint(buf)
	buf = buf[recLenSz:]
	rec.kind = jrecordKind(buf[0])
	buf = buf[recKindSz:]
	copy(rec.address[:], buf)
	buf = buf[addrSize:]
	rec.payload = buf[:len(buf)-checksumSize]
	rec.checksum = readUint(buf[len(buf)-checksumSize:])
	return
}

func safeReadJournalRecord(buf []byte) (jrecord, bool) {
	o := len(buf) - checksumSize
	if crc(buf[:o]) != readUint(buf[o:]) {
		return jrecord{}, false
	}

	rec := readJournalRecord(buf)
	switch rec.kind {
	case rootHashKind:
		return rec, true

	case chunkKind:
		_, err := NewCompressedChunk(hash.Hash(rec.address), rec.payload)
		if err != nil {
			return jrecord{}, false
		}
		return rec, true

	default:
		return jrecord{}, false
	}
}

func processRecords(ctx context.Context, r io.ReadSeeker, cb func(o int64, r jrecord) error) (int64, error) {
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
		if l < recMinSz || l > recMaxSz {
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
