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
	"os"
	"path/filepath"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	chunkJournalFileSize = 256 * 1024 * 1024

	// todo(andy): buffer must be able to hold an entire record,
	//   but we don't have a hard limit on record size right now
	journalWriterBuffSize = 1024 * 1024

	chunkJournalAddr = "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"
)

var (
	journalAddr = addr(hash.Parse(chunkJournalAddr))
)

func isJournalAddr(a addr) bool {
	return a == journalAddr
}

func journalFileExists(path string) (bool, error) {
	var err error
	if path, err = filepath.Abs(path); err != nil {
		return false, err
	}

	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if info.IsDir() {
		return true, fmt.Errorf("expected file %s found directory", chunkJournalName)
	}
	return true, nil
}

func openJournalWriter(ctx context.Context, path string) (wr *journalWriter, exists bool, err error) {
	var f *os.File
	if path, err = filepath.Abs(path); err != nil {
		return nil, false, err
	}

	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	} else if info.IsDir() {
		return nil, true, fmt.Errorf("expected file %s found directory", chunkJournalName)
	}
	if f, err = os.OpenFile(path, os.O_RDWR, 0666); err != nil {
		return nil, true, err
	}

	return &journalWriter{
		buf:  make([]byte, 0, journalWriterBuffSize),
		file: f,
		path: path,
	}, true, nil
}

func createJournalWriter(ctx context.Context, path string) (wr *journalWriter, err error) {
	var f *os.File
	if path, err = filepath.Abs(path); err != nil {
		return nil, err
	}

	_, err = os.Stat(path)
	if err == nil {
		return nil, fmt.Errorf("journal file %s already exists", chunkJournalName)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	if f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666); err != nil {
		return nil, err
	}
	const batch = 1024 * 1024
	b := make([]byte, batch)
	for i := 0; i < chunkJournalFileSize; i += batch {
		if _, err = f.Write(b); err != nil { // zero fill |f|
			return nil, err
		}
	}
	if err = f.Sync(); err != nil {
		return nil, err
	}
	if o, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	} else if o != 0 {
		return nil, fmt.Errorf("expected file offset 0, got %d", o)
	}

	return &journalWriter{
		buf:  make([]byte, 0, journalWriterBuffSize),
		file: f,
		path: path,
	}, nil
}

type snapshotReader interface {
	io.ReaderAt
	// Snapshot returns an io.Reader that provides a consistent view
	// of the current state of the snapshotReader.
	snapshot() (io.Reader, int64, error)

	// currentSize returns the current size.
	currentSize() int64
}

type journalWriter struct {
	buf  []byte
	file *os.File
	off  int64
	path string
}

var _ io.WriteCloser = &journalWriter{}
var _ snapshotReader = &journalWriter{}

func (wr *journalWriter) filepath() string {
	return wr.path
}

func (wr *journalWriter) ReadAt(p []byte, off int64) (n int, err error) {
	var bp []byte
	if off < wr.off {
		// fill some or all of |p| from |wr.file|
		fread := int(wr.off - off)
		if len(p) > fread {
			// straddled read
			bp = p[fread:]
			p = p[:fread]
		}
		if n, err = wr.file.ReadAt(p, off); err != nil {
			return 0, err
		}
		off = 0
	} else {
		// fill all of |p| from |wr.buf|
		bp = p
		off -= wr.off
	}
	n += copy(bp, wr.buf[off:])
	return
}

func (wr *journalWriter) snapshot() (io.Reader, int64, error) {
	if err := wr.flush(); err != nil {
		return nil, 0, err
	}
	// open a new file descriptor with an
	// independent lifecycle from |wr.file|
	f, err := os.Open(wr.path)
	if err != nil {
		return nil, 0, err
	}
	return io.LimitReader(f, wr.off), wr.off, nil
}

func (wr *journalWriter) currentSize() int64 {
	return wr.off
}

func (wr *journalWriter) Write(p []byte) (n int, err error) {
	if len(p) > len(wr.buf) {
		// write directly to |wr.file|
		if err = wr.flush(); err != nil {
			return 0, err
		}
		n, err = wr.file.WriteAt(p, wr.off)
		wr.off += int64(n)
		return
	}
	var buf []byte
	if buf, err = wr.getBytes(len(p)); err != nil {
		return 0, err
	}
	n = copy(buf, p)
	return
}

func (wr *journalWriter) processJournal(ctx context.Context) (last hash.Hash, cs journalChunkSource, err error) {
	// maybeInitJournal chunk journal from |wr.file|
	src := journalChunkSource{
		journal: wr,
		address: journalAddr,
		lookups: make(map[addr]jrecordLookup),
	}
	wr.off, err = processRecords(ctx, wr.file, func(o int64, r jrecord) error {
		switch r.kind {
		case chunkKind:
			src.lookups[r.address] = jrecordLookup{offset: o, length: r.length}
			src.compressedSz += uint64(r.length)
			// todo(andy): uncompressed size
		case rootHashKind:
			last = hash.Hash(r.address)
		default:
			return fmt.Errorf("unknown journal record kind (%d)", r.kind)
		}
		return nil
	})
	if err != nil {
		return hash.Hash{}, journalChunkSource{}, err
	}
	cs = src
	return
}

func (wr *journalWriter) writeChunk(cc CompressedChunk) (jrecordLookup, error) {
	rec := jrecordLookup{
		offset: wr.offset(),
		length: chunkRecordSize(cc),
	}
	buf, err := wr.getBytes(int(rec.length))
	if err != nil {
		return jrecordLookup{}, err
	}
	_ = writeChunkRecord(buf, cc)
	return rec, nil
}

func (wr *journalWriter) writeRootHash(root hash.Hash) error {
	buf, err := wr.getBytes(rootHashRecordSize)
	if err != nil {
		return err
	}
	_ = writeRootHashRecord(buf, addr(root))

	if err = wr.flush(); err != nil {
		return err
	}
	return wr.file.Sync()
}

func (wr *journalWriter) offset() int64 {
	return wr.off + int64(len(wr.buf))
}

func (wr *journalWriter) getBytes(n int) (buf []byte, err error) {
	c, l := cap(wr.buf), len(wr.buf)
	if n > c {
		err = fmt.Errorf("requested bytes (%d) exceeds capacity (%d)", n, c)
		return
	} else if n > c-l {
		if err = wr.flush(); err != nil {
			return
		}
	}
	l = len(wr.buf)
	wr.buf = wr.buf[:l+n]
	buf = wr.buf[l : l+n]
	return
}

func (wr *journalWriter) flush() (err error) {
	if _, err = wr.file.WriteAt(wr.buf, wr.off); err != nil {
		return err
	}
	wr.off += int64(len(wr.buf))
	wr.buf = wr.buf[:0]
	return
}

func (wr *journalWriter) Close() (err error) {
	if err = wr.flush(); err != nil {
		return err
	}
	if cerr := wr.file.Sync(); cerr != nil {
		err = cerr
	}
	if cerr := wr.file.Close(); cerr != nil {
		err = cerr
	}
	return
}

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

// todo(andy): extensible record format
type jrecord struct {
	length   uint32
	kind     jrecordKind
	address  addr
	payload  []byte
	checksum uint32
}

type jrecordLookup struct {
	offset int64
	length uint32
}

func rangeFromLookup(l jrecordLookup) Range {
	return Range{
		Offset: uint64(l.offset) + chunkRecordHeaderSize,
		// jrecords are currently double check-summed
		Length: uint32(l.length) - (chunkRecordHeaderSize + checksumSize),
	}
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
