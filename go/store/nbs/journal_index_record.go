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
	"context"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/store/d"
)

// indexRec is a record in a chunk journal index file. Index records
// serve as out-of-band chunk indexes into the chunk journal that allow
// bootstrapping the journal without reading each record in the journal.
//
// Like journalRec, its serialization format uses uint8 tag prefixes
// to identify fields and allow for format evolution.
type indexRec struct {
	length   uint32
	kind     indexRecKind
	payload  []byte
	checksum uint32
}

type indexRecKind uint8

const (
	unknownIndexRecKind indexRecKind = 0
	tableIndexRecKind   indexRecKind = 1
)

type indexRecTag uint8

const (
	unknownIndexRecTag indexRecTag = 0
	kindIndexRecTag    indexRecTag = 1
	payloadIndexRecTag indexRecTag = 2
)

const (
	indexRecTagSz      = 1
	indexRecLenSz      = 4
	indexRecKindSz     = 1
	indexRecChecksumSz = 4
)

func tableIndexRecordSize(idx []byte) (recordSz uint32) {
	recordSz += indexRecLenSz
	recordSz += indexRecTagSz + indexRecKindSz
	recordSz += indexRecTagSz // payload tag
	recordSz += uint32(len(idx))
	recordSz += indexRecChecksumSz
	return
}

func writeTableIndexRecord(buf []byte, idx []byte) (n uint32) {
	// length
	l := tableIndexRecordSize(idx)
	writeUint(buf[:indexRecLenSz], l)
	n += indexRecLenSz
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
	writeUint(buf[n:], crc(buf[:n]))
	n += indexRecChecksumSz
	d.PanicIfFalse(l == n)
	return
}

func readTableIndexRecord(buf []byte) (rec indexRec, err error) {
	rec.length = readUint(buf)
	buf = buf[indexRecLenSz:]
	for len(buf) > indexRecChecksumSz {
		tag := indexRecTag(buf[0])
		buf = buf[indexRecTagSz:]
		switch tag {
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
	rec.checksum = readUint(buf[:indexRecChecksumSz])
	return
}

func validateIndexRecord(buf []byte) (ok bool) {
	if len(buf) > (indexRecLenSz + indexRecChecksumSz) {
		off := len(buf) - indexRecChecksumSz
		ok = crc(buf[:off]) == readUint(buf[off:])
	}
	return
}

func processIndexRecords(ctx context.Context, r io.ReadSeeker, sz int, cb func(o int64, r indexRec) error) (int64, error) {
	var (
		buf []byte
		off int64
		err error
	)

	rdr := bufio.NewReaderSize(r, sz)
	for {
		// peek to read next record size
		if buf, err = rdr.Peek(uint32Size); err != nil {
			break
		}

		l := readUint(buf)
		if buf, err = rdr.Peek(int(l)); err != nil {
			break
		}

		if !validateIndexRecord(buf) {
			break // stop if we can't validate |rec|
		}

		var rec indexRec
		if rec, err = readTableIndexRecord(buf); err != nil {
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
	// successfully processed index record
	if _, err = r.Seek(off, 0); err != nil {
		return 0, err
	}
	return off, nil
}
