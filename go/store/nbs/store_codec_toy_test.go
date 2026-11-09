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
// appending a tag that binds what the record holds, so a record read as plaintext, or as the wrong record, fails.
// It is not encryption.
type toyCodec struct {
	keyless bool
	// added counts the commits of files added to the store, and located the records it located for remote clients.
	added   atomic.Int32
	located atomic.Int32
	// refuseOpened, if set, is the error checkOpened refuses files with.
	refuseOpened error
}

var errToyKeyless = errors.New("toy codec has no keys")

var errToyTag = errors.New("toy record does not authenticate")

func newToyCodec() *toyCodec {
	return &toyCodec{}
}

// toyFormat is the format string of stores the toy codec encrypts.
const toyFormat = constants.EncryptedFormatPrefix + "TOY__"

// withToy opens a store under |c|, creating it encrypted.
func withToy(c *toyCodec) StoreOption {
	return withToyFormat(c, toyFormat)
}

// withToyFormat opens a store under |c| in |own| format, and refuses other encrypted formats.
func withToyFormat(c *toyCodec, own string) StoreOption {
	return func(o *storeOptions) {
		o.newCodec = func(exists bool, format string) (string, storeCodec, error) {
			switch {
			case !exists || format == own || format == constants.EncryptedFormatPrefix:
				return own, c, nil
			case constants.IsEncryptedFormatString(format):
				return "", nil, ErrEncryptionUnsupported
			}
			return format, plainCodec{}, nil
		}
	}
}

const (
	toyKey = 0x5c

	toyKindChunk = 'c'
	toyKindDict  = 'd'
	toyKindRoot  = 'r'
)

// toyKeyBlock is the key block of every toy file.
var toyKeyBlock = []byte("TOY")

func toyTag(kind byte, h hash.Hash, plain []byte) []byte {
	crc := crc32.Update(crc32.ChecksumIEEE([]byte{kind}), crc32.IEEETable, h[:])
	return binary.BigEndian.AppendUint32(nil, crc32.Update(crc, crc32.IEEETable, plain)^0x5a5a5a5a)
}

func toySeal(dst []byte, kind byte, h hash.Hash, plain []byte) []byte {
	for _, b := range plain {
		dst = append(dst, b^toyKey)
	}
	return append(dst, toyTag(kind, h, plain)...)
}

func toyOpen(kind byte, h hash.Hash, sealed []byte) ([]byte, error) {
	if len(sealed) < 4 {
		return nil, errToyTag
	}
	plain := make([]byte, len(sealed)-4, len(sealed))
	for i := range plain {
		plain[i] = sealed[i] ^ toyKey
	}
	if !bytes.Equal(sealed[len(plain):], toyTag(kind, h, plain)) {
		return nil, errToyTag
	}
	return plain, nil
}

func (c *toyCodec) encrypted() bool {
	return true
}

func (c *toyCodec) checkAccess() error {
	if c.keyless {
		return errToyKeyless
	}
	return nil
}

func (c *toyCodec) newTableSealer() (recordSealer, error) {
	if c.keyless {
		return nil, errToyKeyless
	}
	return toySealer{}, nil
}

func (c *toyCodec) newSpanSealer(bool) (spanSealer, error) {
	if c.keyless {
		return nil, errToyKeyless
	}
	return toySpanSealer{}, nil
}

func (c *toyCodec) openTable(ti onHeapTableIndex, _ bool) (recordOpener, error) {
	if err := checkToyKeyBlock(ti.keyBlock()); err != nil {
		return nil, err
	}
	return &toyOpener{codec: c}, nil
}

func (c *toyCodec) openArchive(ar *archiveReader, _ bool) (archiveSpans, error) {
	if err := checkToyKeyBlock(ar.keyBlock); err != nil {
		return nil, err
	}
	return &toySpans{codec: c}, nil
}

func checkToyKeyBlock(kb []byte) error {
	if !bytes.Equal(kb, toyKeyBlock) {
		return fmt.Errorf("bad toy key block %x", kb)
	}
	return nil
}

func (c *toyCodec) conjoinKeyBlock([]sourceWithSize) ([]byte, error) {
	return toyKeyBlock, nil
}

func (c *toyCodec) newJournalCodec() journalCodec {
	return &toyJournal{codec: c}
}

func (c *toyCodec) checkOpened(context.Context, *NomsBlockStore, chunkSourceSet, hash.Hash) error {
	return c.refuseOpened
}

func (c *toyCodec) checkAdded(_ *NomsBlockStore, added chunkSourceSet) (func() error, error) {
	for _, cs := range added {
		if _, ok := cs.(journalChunkSource); ok {
			continue
		}
		if encrypted, err := sourceEncrypted(cs); err != nil {
			return nil, err
		} else if !encrypted {
			return nil, ErrEncryptionMismatch
		}
	}
	return func() error {
		c.added.Add(1)
		return nil
	}, nil
}

type toySealer struct{}

func (toySealer) seal(dst []byte, _ uint32, h hash.Hash, payload []byte) ([]byte, error) {
	return toySeal(dst, toyKindChunk, h, payload), nil
}

// add seals |tc| again, where Dolt Enterprise may copy its record as it is.
func (toySealer) add(tw *CmpChunkTableWriter, tc ToChunker) (uint32, error) {
	chk, err := tc.ToChunk()
	if err != nil {
		return 0, err
	}
	record := toySeal(nil, toyKindChunk, chk.Hash(), snappy.Encode(nil, chk.Data()))
	return tw.writeRecord(chk.Hash(), record, len(chk.Data()))
}

func (toySealer) keyBlock() ([]byte, error) {
	return toyKeyBlock, nil
}

type toySpanSealer struct{}

func (toySpanSealer) seal(_ uint32, ref spanRef, b []byte) ([]byte, error) {
	switch {
	case ref.isDict:
		return toySeal(nil, toyKindDict, hash.Hash{}, b), nil
	case ref.dict == 0:
		return toySeal(nil, toyKindChunk, ref.h, b[:len(b)-checksumSize]), nil
	default:
		return toySeal(nil, toyKindChunk, ref.h, b), nil
	}
}

func (toySpanSealer) add(asw *ArchiveStreamWriter, sc SealedChunk) (uint32, error) {
	chk, err := sc.ToChunk()
	if err != nil {
		return 0, err
	}
	return asw.writeCompressedChunk(ChunkToCompressedChunk(chk))
}

func (toySpanSealer) keyBlock() ([]byte, error) {
	return toyKeyBlock, nil
}

type toyOpener struct {
	codec *toyCodec
}

func (o *toyOpener) open(_ context.Context, _ uint64, h hash.Hash, sealed []byte) (CompressedChunk, error) {
	if o.codec.keyless {
		return CompressedChunk{}, errToyKeyless
	}
	payload, err := toyOpen(toyKindChunk, h, sealed)
	if err != nil {
		return CompressedChunk{}, err
	}
	full := binary.BigEndian.AppendUint32(payload, crc(payload))
	return CompressedChunk{H: h, FullCompressedChunk: full, CompressedData: full[:len(payload)], verify: true}, nil
}

func (o *toyOpener) chunker(_ context.Context, _ uint64, h hash.Hash, sealed []byte) (ToChunker, error) {
	return &toyChunk{codec: o.codec, h: h, record: sealed}, nil
}

func (o *toyOpener) locate(_ context.Context, ranges map[hash.Hash]Range) error {
	o.codec.located.Add(int32(len(ranges)))
	return nil
}

type toySpans struct {
	codec *toyCodec
}

func (s *toySpans) openDict(_ context.Context, _ uint32, stored []byte) (*DecompBundle, error) {
	if s.codec.keyless {
		return nil, errToyKeyless
	}
	raw, err := toyOpen(toyKindDict, hash.Hash{}, stored)
	if err != nil {
		return nil, err
	}
	return NewDecompBundle(raw)
}

func (s *toySpans) chunkData(ctx context.Context, rc resolvedChunk, dict *DecompBundle, stored []byte) ([]byte, error) {
	c, err := s.toChunker(ctx, rc, dict, stored)
	if err != nil {
		return nil, err
	}
	chk, err := c.ToChunk()
	return chk.Data(), err
}

func (s *toySpans) toChunker(_ context.Context, rc resolvedChunk, dict *DecompBundle, stored []byte) (ToChunker, error) {
	return &toyChunk{codec: s.codec, h: rc.h, record: stored, dict: dict}, nil
}

func (s *toySpans) locate(_ context.Context, _ *archiveReader, resolved []resolvedChunk) error {
	s.codec.located.Add(int32(len(resolved)))
	return nil
}

// toyChunk is a sealed toy record: snappy data, or zstd data compressed with |dict|.
type toyChunk struct {
	codec  *toyCodec
	h      hash.Hash
	record []byte
	dict   *DecompBundle
}

func (tc *toyChunk) ToChunk() (chunks.Chunk, error) {
	if tc.codec.keyless {
		return chunks.EmptyChunk, errToyKeyless
	}
	payload, err := toyOpen(toyKindChunk, tc.h, tc.record)
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
	if hash.Of(data) != tc.h {
		return chunks.EmptyChunk, errToyTag
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

// toyJournal seals journal records with the toy key, and authenticates each index batch.
type toyJournal struct {
	codec   *toyCodec
	lookups uint32
	resets  int
}

func (j *toyJournal) replay(_ context.Context, _ *journalWriter, _ bool, off int64, r journalRec) error {
	switch r.kind {
	case encChunkJournalRecKind:
		c, err := j.chunker(r.address, r.payload)
		if err == nil {
			_, err = c.ToChunk()
		}
		return err
	case encRootHashJournalRecKind:
		_, err := toyOpen(toyKindRoot, r.address, r.payload)
		return err
	}
	return fmt.Errorf("%w: journal record kind %d at %d", ErrEncryptionMismatch, r.kind, off)
}

func (j *toyJournal) checkBatch(_ context.Context, m lookupMeta, batch []lookup) error {
	if !bytes.Equal(m.auth, j.batchAuth(m, uint32(len(batch)))) {
		return errToyTag
	}
	return nil
}

func (j *toyJournal) batchAuth(m lookupMeta, lookups uint32) []byte {
	b := binary.BigEndian.AppendUint64(nil, uint64(m.batchStart))
	b = binary.BigEndian.AppendUint64(b, uint64(m.batchEnd))
	return binary.BigEndian.AppendUint32(toyTag(toyKindRoot, m.latestHash, b), lookups)
}

func (j *toyJournal) sealChunk(_ context.Context, _ *journalWriter, _ dherrors.FatalBehavior, cc CompressedChunk) (sealedRecord, error) {
	if j.codec.keyless {
		return sealedRecord{}, errToyKeyless
	}
	return sealedRecord{kind: encChunkJournalRecKind, payload: toySeal(nil, toyKindChunk, cc.H, cc.CompressedData)}, nil
}

func (j *toyJournal) sealRoot(_ context.Context, _ *journalWriter, _ dherrors.FatalBehavior, root hash.Hash, _ uint64) (sealedRecord, error) {
	if j.codec.keyless {
		return sealedRecord{}, errToyKeyless
	}
	return sealedRecord{kind: encRootHashJournalRecKind, payload: toySeal(nil, toyKindRoot, root, nil)}, nil
}

func (j *toyJournal) indexedLookup(lookup) {
	j.lookups++
}

func (j *toyJournal) sealBatch(wr *journalWriter, m lookupMeta) (bool, error) {
	n := j.lookups
	j.lookups = 0
	return true, writeIndexBytes(wr.indexWriter, indexRecAuth, j.batchAuth(m, n))
}

func (j *toyJournal) chunker(h hash.Hash, payload []byte) (ToChunker, error) {
	return &toyChunk{codec: j.codec, h: h, record: payload}, nil
}

func (j *toyJournal) locate(_ context.Context, _ hash.Hash, r Range, _ journalReaderAt) (Range, error) {
	j.codec.located.Add(1)
	return r, nil
}

func (j *toyJournal) reset() {
	j.resets++
}
