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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync/atomic"

	"github.com/dolthub/gozstd"
	"github.com/golang/snappy"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
)

// toyCodec is a store codec for tests of the encryption seams. It "seals" a record by XORing it with a fixed key and
// appending a tag of the hash it holds, so a record read as plaintext, or as the wrong record, fails. It is not
// encryption.
type toyCodec struct {
	// located counts the records it located for remote clients.
	located atomic.Int32
}

var errToyTag = errors.New("toy record does not authenticate")

// toyFormat is the format string of stores the toy codec encrypts.
const toyFormat = constants.EncryptedFormatPrefix + "TOY__"

// withToy opens a store under |c|, creating it encrypted.
func withToy(c *toyCodec) StoreOption {
	return func(o *storeOptions) {
		o.newCodec = func(exists bool, format string) (string, storeCodec, error) {
			if !exists || format == toyFormat || format == constants.EncryptedFormatPrefix {
				return toyFormat, c, nil
			}
			return plainStoreCodec(exists, format)
		}
	}
}

const toyKey = 0x5c

var toyKeyBlock = []byte("TOY")

func checkToyKeyBlock(kb []byte) error {
	if !bytes.Equal(kb, toyKeyBlock) {
		return fmt.Errorf("toy key block %x", kb)
	}
	return nil
}

// toyTagSize makes a sealed record larger than its payload, as sealing does.
const toyTagSize = 8

func toyTag(h hash.Hash, plain []byte) []byte {
	crc := crc32.Update(crc32.ChecksumIEEE(h[:]), crc32.IEEETable, plain)
	return binary.BigEndian.AppendUint32(h[:4:4], crc)
}

func toySeal(dst []byte, h hash.Hash, plain []byte) []byte {
	for _, b := range plain {
		dst = append(dst, b^toyKey)
	}
	return append(dst, toyTag(h, plain)...)
}

func toyOpen(h hash.Hash, sealed []byte) ([]byte, error) {
	if len(sealed) < toyTagSize {
		return nil, errToyTag
	}
	plain := make([]byte, len(sealed)-toyTagSize, len(sealed))
	for i := range plain {
		plain[i] = sealed[i] ^ toyKey
	}
	if !bytes.Equal(sealed[len(plain):], toyTag(h, plain)) {
		return nil, errToyTag
	}
	return plain, nil
}

func (c *toyCodec) encrypted() bool {
	return true
}

func (c *toyCodec) checkAccess() error {
	return nil
}

func (c *toyCodec) newTableSealer() (recordSealer, error) {
	return toySealer{}, nil
}

func (c *toyCodec) newSpanSealer(bool) (spanSealer, error) {
	return toySpanSealer{}, nil
}

func (c *toyCodec) openTable(ti onHeapTableIndex, _ bool) (recordOpener, error) {
	if err := checkToyKeyBlock(ti.keyBlock()); err != nil {
		return nil, err
	}
	return toyOpener{c}, nil
}

func (c *toyCodec) openArchive(keyBlock []byte, _ uint32, _ bool) (spanOpener, error) {
	if err := checkToyKeyBlock(keyBlock); err != nil {
		return nil, err
	}
	return toySpans{c}, nil
}

func (c *toyCodec) conjoinKeyBlock([]sourceWithSize) ([]byte, error) {
	return toyKeyBlock, nil
}

func (c *toyCodec) newJournalCodec() journalCodec {
	return &toyJournal{codec: c}
}

func (c *toyCodec) checkOpened(context.Context, *NomsBlockStore, chunkSourceSet, hash.Hash) error {
	return nil
}

func (c *toyCodec) checkAdded(*NomsBlockStore, chunkSourceSet) (func() error, error) {
	return func() error { return nil }, nil
}

type toySealer struct{}

func (toySealer) seal(dst []byte, _ uint32, h hash.Hash, payload []byte) ([]byte, error) {
	return toySeal(dst, h, payload), nil
}

// add seals |tc| again, where Dolt Enterprise may copy its record as it is.
func (toySealer) add(tw *CmpChunkTableWriter, tc ToChunker) (uint32, error) {
	chk, err := tc.ToChunk()
	if err != nil {
		return 0, err
	}
	return tw.writeRecord(chk.Hash(), toySeal(nil, chk.Hash(), snappy.Encode(nil, chk.Data())), len(chk.Data()))
}

func (toySealer) keyBlock() ([]byte, error) {
	return toyKeyBlock, nil
}

type toySpanSealer struct {
	toySealer
}

func (toySpanSealer) seal(_ uint32, ref spanRef, b []byte) ([]byte, error) {
	switch {
	case ref.isDict:
		return toySeal(nil, hash.Hash{}, b), nil
	case ref.dict == 0:
		b = b[:len(b)-checksumSize]
	}
	return toySeal(nil, ref.h, b), nil
}

func (toySpanSealer) add(asw *ArchiveStreamWriter, sc SealedChunk) (uint32, error) {
	chk, err := sc.ToChunk()
	if err != nil {
		return 0, err
	}
	return asw.writeCompressedChunk(ChunkToCompressedChunk(chk))
}

type toyOpener struct {
	codec *toyCodec
}

func (o toyOpener) open(_ context.Context, _ uint64, h hash.Hash, sealed []byte) (CompressedChunk, error) {
	payload, err := toyOpen(h, sealed)
	if err != nil {
		return CompressedChunk{}, err
	}
	full := binary.BigEndian.AppendUint32(payload, crc(payload))
	return CompressedChunk{H: h, FullCompressedChunk: full, CompressedData: full[:len(payload)], verify: true}, nil
}

func (o toyOpener) chunker(_ context.Context, _ uint64, h hash.Hash, sealed []byte) (ToChunker, error) {
	return &toyChunk{h: h, record: sealed}, nil
}

func (o toyOpener) locate(_ context.Context, ranges map[hash.Hash]Range) error {
	o.codec.located.Add(int32(len(ranges)))
	return nil
}

type toySpans struct {
	codec *toyCodec
}

func (s toySpans) openDict(_ context.Context, _ uint32, stored []byte) (*DecompBundle, error) {
	raw, err := toyOpen(hash.Hash{}, stored)
	if err != nil {
		return nil, err
	}
	return NewDecompBundle(raw)
}

func (s toySpans) chunkData(_ context.Context, rc resolvedChunk, dict *DecompBundle, stored []byte) ([]byte, error) {
	chk, err := (&toyChunk{h: rc.h, record: stored, dict: dict}).ToChunk()
	return chk.Data(), err
}

func (s toySpans) chunker(_ context.Context, rc resolvedChunk, dict *DecompBundle, stored []byte) (ToChunker, error) {
	return &toyChunk{h: rc.h, record: stored, dict: dict}, nil
}

func (s toySpans) locate(_ context.Context, resolved []resolvedChunk, _ map[hash.Hash]Range) error {
	s.codec.located.Add(int32(len(resolved)))
	return nil
}

// toyChunk is a sealed toy record: snappy data, or zstd data compressed with |dict|.
type toyChunk struct {
	h      hash.Hash
	record []byte
	dict   *DecompBundle
}

func (tc *toyChunk) ToChunk() (chunks.Chunk, error) {
	payload, err := toyOpen(tc.h, tc.record)
	if err != nil {
		return chunks.EmptyChunk, err
	}
	var data []byte
	if tc.dict == nil {
		data, err = snappy.Decode(nil, payload)
	} else {
		data, err = gozstd.DecompressDict(nil, payload, tc.dict.dDict)
	}
	if err != nil {
		return chunks.EmptyChunk, err
	}
	return chunks.NewChunkWithHash(tc.h, data), nil
}

func (tc *toyChunk) Hash() hash.Hash {
	return tc.h
}

func (tc *toyChunk) IsEmpty() bool {
	return len(tc.record) == 0
}

func (tc *toyChunk) IsGhost() bool {
	return false
}

func (tc *toyChunk) CompressedSize() uint32 {
	return uint32(len(tc.record))
}

func (tc *toyChunk) sealed() {}

// toyJournal introduces its key with a key record that it also indexes, and authenticates each index batch.
type toyJournal struct {
	codec    *toyCodec
	wroteKey bool
	resets   int
}

func (j *toyJournal) replay(_ context.Context, wr *journalWriter, canWrite bool, off int64, r journalRec) error {
	switch r.kind {
	case keyJournalRecKind:
		j.wroteKey = true
		if err := checkToyKeyBlock(r.payload); err != nil || !canWrite {
			return err
		}
		return wr.indexKey(r.payload)
	case encChunkJournalRecKind, encRootHashJournalRecKind:
		return nil
	}
	return fmt.Errorf("%w: journal record kind %d at %d", ErrEncryptionMismatch, r.kind, off)
}

func (j *toyJournal) checkBatch(_ context.Context, m lookupMeta, _ []lookup) error {
	for _, k := range m.keys {
		if err := checkToyKeyBlock(k); err != nil {
			return err
		}
		j.wroteKey = true
	}
	if !bytes.Equal(m.auth, toyTag(m.latestHash, nil)) {
		return errToyTag
	}
	return nil
}

// writeKey introduces the toy key to |wr|'s journal before the first record sealed with it.
func (j *toyJournal) writeKey(ctx context.Context, wr *journalWriter, behavior dherrors.FatalBehavior) error {
	if j.wroteKey {
		return nil
	}
	n, _ := journalRecordSize(len(toyKeyBlock))
	buf, err := wr.getBytes(ctx, behavior, int(n))
	if err != nil {
		return err
	}
	writeJournalRecord(buf, keyJournalRecKind, hash.Hash{}, toyKeyBlock)
	j.wroteKey = true
	return wr.indexKey(toyKeyBlock)
}

func (j *toyJournal) sealChunk(ctx context.Context, wr *journalWriter, behavior dherrors.FatalBehavior, cc CompressedChunk) (sealedRecord, error) {
	if err := j.writeKey(ctx, wr, behavior); err != nil {
		return sealedRecord{}, err
	}
	return sealedRecord{kind: encChunkJournalRecKind, payload: toySeal(nil, cc.H, cc.CompressedData)}, nil
}

func (j *toyJournal) sealRoot(ctx context.Context, wr *journalWriter, behavior dherrors.FatalBehavior, root hash.Hash, timestamp uint64) (sealedRecord, error) {
	if err := j.writeKey(ctx, wr, behavior); err != nil {
		return sealedRecord{}, err
	}
	return sealedRecord{kind: encRootHashJournalRecKind, payload: toySeal(nil, root, binary.BigEndian.AppendUint64(nil, timestamp))}, nil
}

func (j *toyJournal) indexedLookup(lookup) {}

func (j *toyJournal) sealBatch(wr *journalWriter, m lookupMeta) (bool, error) {
	return true, writeIndexBytes(wr.indexWriter, indexRecAuth, toyTag(m.latestHash, nil))
}

func (j *toyJournal) chunker(h hash.Hash, payload []byte) (ToChunker, error) {
	return &toyChunk{h: h, record: payload}, nil
}

func (j *toyJournal) locate(_ context.Context, _ hash.Hash, r Range, _ journalReaderAt) (Range, error) {
	j.codec.located.Add(1)
	return r, nil
}

func (j *toyJournal) reset() {
	j.resets++
}
