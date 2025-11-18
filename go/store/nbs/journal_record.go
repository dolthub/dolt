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
	"errors"
	"fmt"
	"io"
	"math"
	"time"

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
	timestamp time.Time
	payload   []byte
	length    uint32
	checksum  uint32
	address   hash.Hash
	kind      journalRecKind
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
	unknownJournalRecTag   journalRecTag = 0
	kindJournalRecTag      journalRecTag = 1
	addrJournalRecTag      journalRecTag = 2
	payloadJournalRecTag   journalRecTag = 3
	timestampJournalRecTag journalRecTag = 4
)

const (
	journalRecTagSz       = 1
	journalRecLenSz       = 4
	journalRecKindSz      = 1
	journalRecAddrSz      = 20
	journalRecChecksumSz  = 4
	journalRecTimestampSz = 8
)

// journalRecordTimestampGenerator returns the current time in Unix epoch seconds. This function is stored in a
// variable so that unit tests can override it to ensure the journal record timestamps are a known, expected value.
var journalRecordTimestampGenerator = func() uint64 {
	return uint64(time.Now().Unix())
}

func chunkRecordSize(c CompressedChunk) (recordSz, payloadOff uint32) {
	payloadOff += journalRecLenSz
	payloadOff += journalRecTagSz + journalRecKindSz
	payloadOff += journalRecTagSz + journalRecAddrSz
	payloadOff += journalRecTagSz // payload tag

	// Make sure the size of the chunk wouldn't overflow the uint32 record length
	maxCompressedChunkSize := math.MaxUint32 - int(payloadOff+journalRecChecksumSz)
	if len(c.FullCompressedChunk) > maxCompressedChunkSize {
		panic(fmt.Sprintf("compressed chunk size (%d) is larger than max size allowed "+
			"for chunk record (%d)", len(c.FullCompressedChunk), maxCompressedChunkSize))
	}

	recordSz = payloadOff
	recordSz += uint32(len(c.FullCompressedChunk))
	recordSz += journalRecChecksumSz

	return recordSz, payloadOff
}

func rootHashRecordSize() (recordSz int) {
	recordSz += journalRecLenSz
	recordSz += journalRecTagSz + journalRecKindSz
	recordSz += journalRecTagSz + journalRecAddrSz
	recordSz += journalRecTagSz + journalRecTimestampSz
	recordSz += journalRecChecksumSz
	return
}

func writeChunkRecord(buf []byte, c CompressedChunk) (n uint32) {
	// length – comes back as an unsigned 32 bit int, which aligns with the four bytes used
	// in the journal storage protocol to store the total record length, assuring that we can't
	// read a length that is too large to safely write.
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

func writeRootHashRecord(buf []byte, root hash.Hash) (n uint32) {
	// length
	l := rootHashRecordSize()
	writeUint32(buf[:journalRecLenSz], uint32(l))
	n += journalRecLenSz

	// kind
	buf[n] = byte(kindJournalRecTag)
	n += journalRecTagSz
	buf[n] = byte(rootHashJournalRecKind)
	n += journalRecKindSz

	// timestamp
	buf[n] = byte(timestampJournalRecTag)
	n += journalRecTagSz
	writeUint64(buf[n:], journalRecordTimestampGenerator())
	n += journalRecTimestampSz

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
		case timestampJournalRecTag:
			unixSeconds := readUint64(buf)
			rec.timestamp = time.Unix(int64(unixSeconds), 0)
			buf = buf[journalRecTimestampSz:]
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

// validateJournalRecord performs some sanity checks on the buffer |buf| containing a journal
// record, such as checking that the length of the record is not too short, and checking the
// checksum. If any problems are detected, an error is returned, otherwise nil is returned.
func validateJournalRecord(buf []byte) error {
	if len(buf) < (journalRecLenSz + journalRecChecksumSz) {
		return fmt.Errorf("invalid journal record: buffer length too small (%d < %d)", len(buf), (journalRecLenSz + journalRecChecksumSz))
	}

	off := readUint32(buf)
	if int(off) > len(buf) {
		return fmt.Errorf("invalid journal record: offset is greater than length of buffer (%d > %d)",
			off, len(buf))
	}

	off -= journalRecChecksumSz
	crcMatches := crc(buf[:off]) == readUint32(buf[off:])
	if !crcMatches {
		return fmt.Errorf("invalid journal record: CRC checksum does not match")
	}

	return nil
}

// processJournalRecords iterates over a chunk journal's records by reading from disk using |r|, starting at
// offset |off|, and calls the callback function |cb| with each journal record. The offset where reading was stopped
// is returned, or any error encountered along the way.
// If an invalid journal record is found, it is not included and this function stops processing journal
// entries, but does not return an error. Journal records may be incomplete if the system crashes while
// records are being persisted to disk. This isn't likely, but the OS filesystem write is not an atomic
// operation, so it's possible to leave the journal in a corrupted state. We must gracefully recover
// without preventing the server from starting up, so we are careful to only return the journal file
// offset that points to end of the last valid record.
//
// The |recoveryCb| callback is called with any errors encountered that we automatically recover from. This allows the caller
// to handle the situation in a context specific way.
func processJournalRecords(ctx context.Context, r io.ReadSeeker, off int64, cb func(o int64, r journalRec) error, recoveryCb func(error)) (int64, error) {
	var (
		buf []byte
		err error
	)

	// start processing records from |off|
	if _, err = r.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}

	// There are a few ways we can recover from journals which seem truncated or corrupted, but we want to be sure
	// to scan to the end of the file in these cases to ensure there is no indication of data loss.
	recovered := false

	rdr := bufio.NewReaderSize(r, journalWriterBuffSize)
	for {
		// peek to read next record size
		if buf, err = rdr.Peek(uint32Size); err != nil {
			// If we hit EOF here, it's expected. We can have no more than 3 bytes of data left, and we don't really have
			// a way to determine if that was valid data or just padding. So we simply stop scanning.
			break
		}

		// The first 4 bytes in the journal record are the total length of the record (including
		// these first four bytes)
		l := readUint32(buf)

		// The journal file data is initialized to a block of zero bytes, so if we read a record
		// length of 0, we know we've reached the end of the journal records and are starting to
		// read the zero padding.
		if l == 0 {
			recovered = true
			break
		}

		if l > journalWriterBuffSize {
			// Probably hit a corrupted record. Report error and stop processing.
			if recoveryCb != nil {
				// We don't assign this error to err, because we want to recover from this.
				jErr := fmt.Errorf("invalid journal record length: %d exceeds max allowed size of %d", l, journalWriterBuffSize)
				recoveryCb(jErr)
			}
			recovered = true
			break
		}

		if buf, err = rdr.Peek(int(l)); err != nil {
			if recoveryCb != nil {
				// We probably wend off the end of the file. Report error and recover.
				jErr := fmt.Errorf("failed to read full journal record of length %d at offset %d: %w", l, off, err)
				recoveryCb(jErr)
			}
			recovered = true
			break
		}

		// TODO: The NomsBlockStore manifest is updated when the sql engine is shutdown cleanly. In the clean shutdown
		//       case, we have the root hash value and the number of chunks written to the journal from the manifest
		//       and we could use that to more aggressively validate journal records. When we are starting up from a
		//       clean shutdown, we expect all journal records to be valid, and could safely error out during startup
		//       for invalid records.
		if validationErr := validateJournalRecord(buf); validationErr != nil {
			if recoveryCb != nil {
				// We don't assign the validation error to err, because we want to recover from this.
				jErr := fmt.Errorf("invalid journal record at offset %d: %w", off, validationErr)
				recoveryCb(jErr)
			}
			recovered = true
			break
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

	// If a non-EOF error was captured while processing journal records, return a
	// journal offset of 0 and the error, which will cause startup to halt.
	if err != nil && err != io.EOF {
		return 0, err
	}

	// If we hit a recovery state, we want to check if there is any parseable data in the remainder of the journal.
	if recovered {
		dataLossFound, dErr := possibleDataLossCheck(rdr)
		if dErr != nil {
			// Finding an error at this point is considered recoverable since we were already in a recovery state.
			// Report the error and continue.
			if recoveryCb != nil {
				recoveryCb(fmt.Errorf("error while checking for possible data loss in journal at offset %d: %w", off, dErr))
			}
		}
		if dataLossFound {
			err = fmt.Errorf("possible data loss detected in journal file at offset %d", off)
			return 0, err
		}
	}

	// reset the file pointer to end of the last
	// successfully processed journal record
	if _, err = r.Seek(off, 0); err != nil {
		return 0, err
	}

	return off, nil
}

// possibleDataLossCheck checks for non-zero data starting at |off| in |r|. When calling this method, we've already hit
// as state where we can't read a record at |off|, so we are going to scan forward from |off| to see if there is any
// valid root record followed by another record (root hash or chunk).
func possibleDataLossCheck(reader *bufio.Reader) (dataLoss bool, err error) {
	firstRootFound := false
	secondRecordFound := false

	remainingData, err := io.ReadAll(reader)
	if err != nil {
		return false, err
	}

	idx := 0
	for idx <= len(remainingData)-rootHashRecordSize() {
		sz := readUint32(remainingData[idx : idx+uint32Size])
		if sz > 0 && sz <= journalWriterBuffSize {
			// in the right range.
			if int(sz) <= len(remainingData[idx:]) {
				// try to validate it.
				candidate := remainingData[idx : idx+int(sz)]
				e := validateJournalRecord(candidate)
				if e == nil {
					record, err := readJournalRecord(candidate)
					if err != nil {
						// Unexpected, since we already validated it.
						return false, err
					}
					if firstRootFound {
						// found the second record!
						secondRecordFound = true
						break
					}
					if record.kind == rootHashJournalRecKind {
						// found the first root hash record
						firstRootFound = true
					}
					// If we have a valid record, skip ahead by its size even if it's not an interesting record.
					idx += int(sz)
					continue
				}
			}
		}
		// Found unparsable data. Reset any found root hash state and continue scanning.
		firstRootFound = false
		idx++
	}

	return firstRootFound && secondRecordFound, nil
}

func peekRootHashAt(journal io.ReaderAt, offset int64) (root hash.Hash, err error) {
	expSz := rootHashRecordSize()
	buf := make([]byte, expSz) // assumes len(rec) is exactly rootHashRecordSize
	n, err := journal.ReadAt(buf, offset)
	if errors.Is(err, io.EOF) {
		err = nil // EOF is expected for last record
	} else if err != nil {
		return
	} else if n != expSz {
		err = fmt.Errorf("invalid root hash record at %d: %d", offset, n)
		return
	}
	return rootHashFromBuffer(buf, offset)
}

// rootHashFromBuffer extracts the root hash from a root hash journal record stored in |buf|. The buffer must always
// be exactly the size of a root hash record (i.e. rootHashRecordSize()), and the CRC is checked before parsing. |offset|
// is only used for error reporting, so it should be relative to the start of the journal file.
func rootHashFromBuffer(buf []byte, offset int64) (root hash.Hash, err error) {
	sz := readUint32(buf)
	if sz > uint32(rootHashRecordSize()) {
		err = fmt.Errorf("invalid root hash record size at %d", offset)
		return
	}
	buf = buf[:sz]
	err = validateJournalRecord(buf)
	if err != nil {
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
	return rec.address, nil
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
