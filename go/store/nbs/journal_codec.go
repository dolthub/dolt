// Copyright 2026 Dolthub, Inc.
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
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
)

// An encrypted journal uses only these record kinds, and introduces each key before any record sealed with it:
//
//	key record:        | kind | addr (zero) | payload: key |
//	chunk record:      | kind | addr | payload: sealed chunk |
//	root hash record:  | kind | timestamp | addr | payload: authentication |
//
// journal.idx mirrors the key records, and may end each batch with an authentication record.
const (
	keyJournalRecKind         journalRecKind = 0x80
	encChunkJournalRecKind    journalRecKind = 0x81
	encRootHashJournalRecKind journalRecKind = 0x82

	indexRecKey  byte = 0x80
	indexRecAuth byte = 0x81

	// encJournalRecOverhead is what sealing adds to a payload: a key id, a nonce and a tag.
	encJournalRecOverhead = 16 + 12 + 16
)

func isEncryptedRecKind(k journalRecKind) bool {
	return k == keyJournalRecKind || k == encChunkJournalRecKind || k == encRootHashJournalRecKind
}

func isChunkRecKind(k journalRecKind) bool {
	return k == chunkJournalRecKind || k == encChunkJournalRecKind
}

func isRootHashRecKind(k journalRecKind) bool {
	return k == rootHashJournalRecKind || k == encRootHashJournalRecKind
}

// sealedRecord is a record as a journalCodec seals it, to be written after a record introducing |key| if it is set.
type sealedRecord struct {
	kind    journalRecKind
	payload []byte
	key     []byte
}

// journalCodec seals what a chunk journal writes, and checks and opens what it reads. The journal writer holds its
// lock for every call but chunker and locate, which may run concurrently under its read lock or without it. Slices
// passed to the codec are valid only during the call.
type journalCodec interface {
	// replay checks a record read from the journal.
	replay(ctx context.Context, off int64, r journalRec) error
	// checkBatch checks an index batch whose checksum matched.
	checkBatch(ctx context.Context, m lookupMeta, batch []lookup) error
	sealChunk(ctx context.Context, cc CompressedChunk) (sealedRecord, error)
	sealRoot(ctx context.Context, root hash.Hash, timestamp uint64) (sealedRecord, error)
	// indexedLookup and indexedKey see each lookup and durable key added to the open index batch.
	indexedLookup(l lookup)
	indexedKey(key []byte)
	// sealBatch returns the authentication record of index batch |m|, or false to extend the batch instead.
	sealBatch(m lookupMeta) (auth []byte, ok bool, err error)
	// chunker returns chunk |h| from its record's payload.
	chunker(h hash.Hash, payload []byte) (ToChunker, error)
	// locate returns the range of chunk |h|'s record payload |r| that a remote client reads, and the stored key that
	// sealed it.
	locate(h hash.Hash, r Range, rd journalReaderAt) (Range, []byte, error)
	// reset forgets what replay learned, before the journal is replayed from its start.
	reset()
}

type plainJournal struct{}

func (plainJournal) replay(_ context.Context, off int64, r journalRec) error {
	if isEncryptedRecKind(r.kind) {
		return fmt.Errorf("%w: encrypted journal record kind %d at offset %d", ErrEncryptionMismatch, r.kind, off)
	}
	return nil
}

func (plainJournal) checkBatch(_ context.Context, m lookupMeta, _ []lookup) error {
	if len(m.keys) > 0 || m.auth != nil {
		return fmt.Errorf("%w: encryption records in a plaintext journal index", ErrMalformedIndex)
	}
	return nil
}

func (plainJournal) sealChunk(_ context.Context, cc CompressedChunk) (sealedRecord, error) {
	return sealedRecord{kind: chunkJournalRecKind, payload: cc.FullCompressedChunk}, nil
}

func (plainJournal) sealRoot(context.Context, hash.Hash, uint64) (sealedRecord, error) {
	return sealedRecord{kind: rootHashJournalRecKind}, nil
}

func (plainJournal) indexedLookup(lookup) {}

func (plainJournal) indexedKey([]byte) {}

func (plainJournal) sealBatch(lookupMeta) ([]byte, bool, error) {
	return nil, true, nil
}

func (plainJournal) chunker(h hash.Hash, payload []byte) (ToChunker, error) {
	return NewCompressedChunk(h, payload)
}

// journalReaderAt reads the journal at an offset.
type journalReaderAt interface {
	readAt(p []byte, off int64) (int, error)
}

func (plainJournal) locate(_ hash.Hash, r Range, _ journalReaderAt) (Range, []byte, error) {
	return r, nil, nil
}

func (plainJournal) reset() {}

// writeKeyRecord introduces |key| to the journal and syncs it, so it is durable before any record sealed with it.
func (wr *journalWriter) writeKeyRecord(ctx context.Context, behavior dherrors.FatalBehavior, key []byte) error {
	if key == nil {
		return nil
	}
	if len(key) > math.MaxUint16 {
		return fmt.Errorf("journal key of %d bytes is too large", len(key))
	}
	recordLen, _ := journalRecordSize(len(key))
	buf, err := wr.getBytes(ctx, behavior, int(recordLen))
	if err != nil {
		return err
	}
	_ = writeJournalRecord(buf, keyJournalRecKind, hash.Hash{}, key)
	if err = wr.flush(ctx, behavior); err != nil {
		return err
	}
	if err = wr.journal.syncData(); err != nil {
		return dherrors.Fatalf(behavior, "%w: error syncing journal", err)
	}
	return wr.indexKey(key)
}

func (wr *journalWriter) indexLookup(l lookup) error {
	if err := writeIndexLookup(wr.indexWriter, l); err != nil {
		return err
	}
	// Hashing a copy keeps |l| itself off the heap.
	a := l.a
	wr.batchCrc = crc32.Update(wr.batchCrc, crcTable, a[:])
	wr.codec.indexedLookup(l)
	return nil
}

func (wr *journalWriter) indexKey(key []byte) error {
	if err := writeIndexBytes(wr.indexWriter, indexRecKey, key); err != nil {
		return err
	}
	wr.batchCrc = crc32.Update(wr.batchCrc, crcTable, key)
	wr.codec.indexedKey(key)
	return nil
}

func readIndexBytes(r *bufio.Reader) ([]byte, error) {
	var lenBuf [uint16Size]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	b := make([]byte, binary.BigEndian.Uint16(lenBuf[:]))
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}

func writeIndexBytes(w *bufio.Writer, tag byte, b []byte) error {
	if len(b) > math.MaxUint16 {
		return fmt.Errorf("journal index record of %d bytes is too large", len(b))
	}
	if err := w.WriteByte(tag); err != nil {
		return err
	}
	if _, err := w.Write(binary.BigEndian.AppendUint16(nil, uint16(len(b)))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

const uint16Size = 2

// readPayload reads the payload at |r| of a chunk record; the caller holds the writer's lock.
func (wr *journalWriter) readPayload(r Range) ([]byte, error) {
	buf := make([]byte, r.Length)
	if _, err := wr.readAt(buf, int64(r.Offset)); err != nil {
		return nil, err
	}
	return buf, nil
}

// chunkerAtRange returns the chunk whose record payload is at |r| under the address the record holds, since |h| may
// be only a prefix of it; the caller holds the writer's lock.
func (wr *journalWriter) chunkerAtRange(r Range, h hash.Hash) (ToChunker, error) {
	const prefix = journalRecTagSz + journalRecAddrSz + journalRecTagSz
	if r.Offset < prefix {
		return nil, fmt.Errorf("invalid journal range offset %d", r.Offset)
	}
	buf := make([]byte, prefix+r.Length)
	if _, err := wr.readAt(buf, int64(r.Offset)-prefix); err != nil {
		return nil, err
	}
	addr := hash.New(buf[journalRecTagSz : journalRecTagSz+journalRecAddrSz])
	if journalRecTag(buf[0]) != addrJournalRecTag || journalRecTag(buf[prefix-1]) != payloadJournalRecTag || toAddr16(addr) != toAddr16(h) {
		return nil, fmt.Errorf("journal range at offset %d is not a chunk record for %s", r.Offset, h.String())
	}
	return wr.codec.chunker(addr, buf[prefix:])
}

// visitJournalRecords calls |cb| with each chunk of the journal read by |r|, which starts at offset |off|.
func visitJournalRecords(ctx context.Context, r io.Reader, off int64, codec journalCodec, cb func(ToChunker) error) error {
	var warningErr error
	_, _, _, err := processJournalRecordsReader(ctx, r, off, func(off int64, rec journalRec) error {
		if warningErr != nil {
			return warningErr
		}
		if err := codec.replay(ctx, off, rec); err != nil {
			return err
		}
		if !isChunkRecKind(rec.kind) {
			return nil
		}
		// The record's buffer is reused by our caller, and the chunk may outlive this call.
		c, err := codec.chunker(rec.address, bytes.Clone(rec.payload))
		if err != nil {
			return fmt.Errorf("error making compressed chunk at off %v for address %v: %w", off, rec.address, err)
		}
		return cb(c)
	}, func(err error) {
		warningErr = err
	})
	if err == io.EOF {
		err = nil
	}
	return errors.Join(err, warningErr)
}

// peekStoreVersion returns the format of the journaling store in |dir| from its manifest, or failing that its journal.
func peekStoreVersion(dir string) (string, bool, error) {
	f, err := os.Open(filepath.Join(dir, manifestFileName))
	if errors.Is(err, os.ErrNotExist) {
		return peekJournalVersion(filepath.Join(dir, chunkJournalName))
	} else if err != nil {
		return "", false, err
	}
	defer f.Close()
	contents, err := parseManifest(f)
	if err != nil {
		return "", false, err
	}
	return contents.nbfVers, true, nil
}

// peekJournalVersion returns the format a journal written before its manifest has, from its first record.
func peekJournalVersion(path string) (string, bool, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	} else if err != nil {
		return "", false, err
	}
	defer f.Close()
	var hdr [journalRecLenSz + journalRecTagSz + journalRecKindSz]byte
	if _, err = io.ReadFull(f, hdr[:]); errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "", false, nil
	} else if err != nil {
		return "", false, err
	}
	if journalRecTag(hdr[journalRecLenSz]) != kindJournalRecTag {
		return "", false, nil
	}
	switch kind := journalRecKind(hdr[journalRecLenSz+journalRecTagSz]); {
	case isEncryptedRecKind(kind):
		return constants.EncryptedFormatPrefix, true, nil
	case kind == chunkJournalRecKind || kind == rootHashJournalRecKind:
		return constants.FormatDoltString, true, nil
	}
	return "", false, nil
}
