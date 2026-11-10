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
	// addingOpens counts the files it opened as being added, and recompressing the archive sealers that recompress.
	addingOpens   atomic.Int32
	recompressing atomic.Int32
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

// toyKeyBlock is the key block of a toy file of |count| records, or of a toy journal's key record.
func toyKeyBlock(count uint32) []byte {
	return binary.BigEndian.AppendUint32([]byte("TOY"), count)
}

func checkToyKeyBlock(kb []byte, count uint32) error {
	if !bytes.Equal(kb, toyKeyBlock(count)) {
		return fmt.Errorf("toy key block %x is not for %d records", kb, count)
	}
	return nil
}

// toyTagSize makes a sealed record larger than its payload, as sealing does.
const toyTagSize = 16

func toyTag(kind byte, h hash.Hash, plain []byte) []byte {
	crc := crc32.Update(crc32.ChecksumIEEE([]byte{kind}), crc32.IEEETable, h[:])
	tag := binary.BigEndian.AppendUint32(nil, crc32.Update(crc, crc32.IEEETable, plain)^0x5a5a5a5a)
	return append(tag, h[:toyTagSize-4]...)
}

func toySeal(dst []byte, kind byte, h hash.Hash, plain []byte) []byte {
	for _, b := range plain {
		dst = append(dst, b^toyKey)
	}
	return append(dst, toyTag(kind, h, plain)...)
}

func toyOpen(kind byte, h hash.Hash, sealed []byte) ([]byte, error) {
	if len(sealed) < toyTagSize {
		return nil, errToyTag
	}
	plain := make([]byte, len(sealed)-toyTagSize, len(sealed))
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
	return &toySealer{}, nil
}

func (c *toyCodec) newSpanSealer(recompress bool) (spanSealer, error) {
	if c.keyless {
		return nil, errToyKeyless
	}
	if recompress {
		c.recompressing.Add(1)
	}
	return &toySpanSealer{}, nil
}

func (c *toyCodec) openTable(ti onHeapTableIndex, adding bool) (recordOpener, error) {
	if err := checkToyKeyBlock(ti.keyBlock(), ti.count); err != nil {
		return nil, err
	}
	c.notedOpen(adding)
	o := &toyOpener{codec: c, records: make(map[uint64]bool, ti.count)}
	for i := uint32(0); i < ti.count; i++ {
		o.records[ti.getIndexEntry(i).Offset()] = true
	}
	return o, nil
}

func (c *toyCodec) openArchive(keyBlock []byte, spans uint32, adding bool) (spanOpener, error) {
	if err := checkToyKeyBlock(keyBlock, spans); err != nil {
		return nil, err
	}
	c.notedOpen(adding)
	return &toySpans{codec: c, spans: spans}, nil
}

func (c *toyCodec) notedOpen(adding bool) {
	if adding {
		c.addingOpens.Add(1)
	}
}

// conjoinKeyBlock covers the records of every source.
func (c *toyCodec) conjoinKeyBlock(sources []sourceWithSize) ([]byte, error) {
	var count uint32
	for _, sws := range sources {
		var kb []byte
		var n uint32
		if acs, ok := sws.source.(*archiveChunkSource); ok {
			kb, n = acs.aRdr.keyBlock, acs.aRdr.footer.byteSpanCount
		} else {
			idx, err := sws.source.index()
			if err != nil {
				return nil, err
			}
			ti := idx.(onHeapTableIndex)
			kb, n = ti.keyBlock(), ti.count
		}
		if err := checkToyKeyBlock(kb, n); err != nil {
			return nil, err
		}
		count += n
	}
	return toyKeyBlock(count), nil
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

// toySealer checks that it seals a file's records in order.
type toySealer struct {
	next uint32
}

func (s *toySealer) record(ordinal uint32) error {
	if ordinal != s.next {
		return fmt.Errorf("toy record %d sealed as record %d", s.next, ordinal)
	}
	s.next++
	return nil
}

func (s *toySealer) seal(dst []byte, ordinal uint32, h hash.Hash, payload []byte) ([]byte, error) {
	if err := s.record(ordinal); err != nil {
		return nil, err
	}
	return toySeal(dst, toyKindChunk, h, payload), nil
}

// add seals |tc| again, where Dolt Enterprise may copy its record as it is.
func (s *toySealer) add(tw *CmpChunkTableWriter, tc ToChunker) (uint32, error) {
	if err := s.record(uint32(len(tw.prefixes))); err != nil {
		return 0, err
	}
	chk, err := tc.ToChunk()
	if err != nil {
		return 0, err
	}
	record := toySeal(nil, toyKindChunk, chk.Hash(), snappy.Encode(nil, chk.Data()))
	return tw.writeRecord(chk.Hash(), record, len(chk.Data()))
}

func (s *toySealer) keyBlock() ([]byte, error) {
	return toyKeyBlock(s.next), nil
}

type toySpanSealer struct {
	toySealer
}

func (s *toySpanSealer) seal(ordinal uint32, ref spanRef, b []byte) ([]byte, error) {
	if err := s.record(ordinal); err != nil {
		return nil, err
	}
	switch {
	case ref.isDict:
		return toySeal(nil, toyKindDict, hash.Hash{}, b), nil
	case ref.dict == 0:
		return toySeal(nil, toyKindChunk, ref.h, b[:len(b)-checksumSize]), nil
	default:
		return toySeal(nil, toyKindChunk, ref.h, b), nil
	}
}

func (s *toySpanSealer) add(asw *ArchiveStreamWriter, sc SealedChunk) (uint32, error) {
	chk, err := sc.ToChunk()
	if err != nil {
		return 0, err
	}
	return asw.writeCompressedChunk(ChunkToCompressedChunk(chk))
}

// toyOpener checks that it opens records at their positions in the file.
type toyOpener struct {
	codec   *toyCodec
	records map[uint64]bool
}

func (o *toyOpener) at(pos uint64) error {
	if !o.records[pos] {
		return fmt.Errorf("no toy record at %d", pos)
	}
	return nil
}

func (o *toyOpener) open(_ context.Context, pos uint64, h hash.Hash, sealed []byte) (CompressedChunk, error) {
	if o.codec.keyless {
		return CompressedChunk{}, errToyKeyless
	}
	if err := o.at(pos); err != nil {
		return CompressedChunk{}, err
	}
	payload, err := toyOpen(toyKindChunk, h, sealed)
	if err != nil {
		return CompressedChunk{}, err
	}
	full := binary.BigEndian.AppendUint32(payload, crc(payload))
	return CompressedChunk{H: h, FullCompressedChunk: full, CompressedData: full[:len(payload)], verify: true}, nil
}

func (o *toyOpener) chunker(_ context.Context, pos uint64, h hash.Hash, sealed []byte) (ToChunker, error) {
	if err := o.at(pos); err != nil {
		return nil, err
	}
	return &toyChunk{codec: o.codec, h: h, record: sealed}, nil
}

func (o *toyOpener) locate(_ context.Context, ranges map[hash.Hash]Range) error {
	o.codec.located.Add(int32(len(ranges)))
	return nil
}

// toySpans checks that it opens spans by their ids.
type toySpans struct {
	codec *toyCodec
	spans uint32
}

func (s *toySpans) span(id uint32) error {
	if id == 0 || id > s.spans {
		return fmt.Errorf("no toy span %d of %d", id, s.spans)
	}
	return nil
}

func (s *toySpans) openDict(_ context.Context, id uint32, stored []byte) (*DecompBundle, error) {
	if s.codec.keyless {
		return nil, errToyKeyless
	}
	if err := s.span(id); err != nil {
		return nil, err
	}
	raw, err := toyOpen(toyKindDict, hash.Hash{}, stored)
	if err != nil {
		return nil, err
	}
	return NewDecompBundle(raw)
}

func (s *toySpans) chunkData(ctx context.Context, rc resolvedChunk, dict *DecompBundle, stored []byte) ([]byte, error) {
	c, err := s.chunker(ctx, rc, dict, stored)
	if err != nil {
		return nil, err
	}
	chk, err := c.ToChunk()
	return chk.Data(), err
}

func (s *toySpans) chunker(_ context.Context, rc resolvedChunk, dict *DecompBundle, stored []byte) (ToChunker, error) {
	if err := s.span(rc.dataId); err != nil {
		return nil, err
	}
	if (rc.dictId == 0) != (dict == nil) || rc.dictId >= rc.dataId {
		return nil, fmt.Errorf("toy span %d has dictionary span %d", rc.dataId, rc.dictId)
	}
	return &toyChunk{codec: s.codec, h: rc.h, record: stored, dict: dict}, nil
}

func (s *toySpans) locate(_ context.Context, resolved []resolvedChunk, ranges map[hash.Hash]Range) error {
	for _, rc := range resolved {
		if _, ok := ranges[rc.h]; !ok {
			return fmt.Errorf("toy chunk %s was not located", rc.h.String())
		}
	}
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

// toyJournal seals journal records with the toy key, which it introduces with a key record that it also indexes, and
// authenticates each index batch.
type toyJournal struct {
	codec    *toyCodec
	lookups  uint32
	resets   int
	wroteKey bool
}

func (j *toyJournal) replay(_ context.Context, wr *journalWriter, canWrite bool, off int64, r journalRec) error {
	switch r.kind {
	case keyJournalRecKind:
		if err := checkToyKeyBlock(r.payload, 0); err != nil {
			return fmt.Errorf("toy key record at %d: %w", off, err)
		}
		j.wroteKey = true
		if canWrite {
			return wr.indexKey(r.payload)
		}
		return nil
	case encChunkJournalRecKind:
		c, err := j.chunker(r.address, r.payload)
		if err == nil {
			_, err = c.ToChunk()
		}
		return err
	case encRootHashJournalRecKind:
		ts, err := toyOpen(toyKindRoot, r.address, r.payload)
		if err == nil && (len(ts) != 8 || binary.BigEndian.Uint64(ts) != uint64(r.timestamp.Unix())) {
			err = fmt.Errorf("toy root record at %d has the wrong timestamp", off)
		}
		return err
	}
	return fmt.Errorf("%w: journal record kind %d at %d", ErrEncryptionMismatch, r.kind, off)
}

func (j *toyJournal) checkBatch(_ context.Context, m lookupMeta, batch []lookup) error {
	for _, k := range m.keys {
		if err := checkToyKeyBlock(k, 0); err != nil {
			return fmt.Errorf("toy key record in the index: %w", err)
		}
		j.wroteKey = true
	}
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

// writeKey introduces the toy key to |wr|'s journal before the first record sealed with it.
func (j *toyJournal) writeKey(ctx context.Context, wr *journalWriter, behavior dherrors.FatalBehavior) error {
	if j.codec.keyless {
		return errToyKeyless
	}
	if j.wroteKey {
		return nil
	}
	key := toyKeyBlock(0)
	n, _ := journalRecordSize(len(key))
	buf, err := wr.getBytes(ctx, behavior, int(n))
	if err != nil {
		return err
	}
	writeJournalRecord(buf, keyJournalRecKind, hash.Hash{}, key)
	j.wroteKey = true
	return wr.indexKey(key)
}

func (j *toyJournal) sealChunk(ctx context.Context, wr *journalWriter, behavior dherrors.FatalBehavior, cc CompressedChunk) (sealedRecord, error) {
	if err := j.writeKey(ctx, wr, behavior); err != nil {
		return sealedRecord{}, err
	}
	return sealedRecord{kind: encChunkJournalRecKind, payload: toySeal(nil, toyKindChunk, cc.H, cc.CompressedData)}, nil
}

func (j *toyJournal) sealRoot(ctx context.Context, wr *journalWriter, behavior dherrors.FatalBehavior, root hash.Hash, timestamp uint64) (sealedRecord, error) {
	if err := j.writeKey(ctx, wr, behavior); err != nil {
		return sealedRecord{}, err
	}
	ts := binary.BigEndian.AppendUint64(nil, timestamp)
	return sealedRecord{kind: encRootHashJournalRecKind, payload: toySeal(nil, toyKindRoot, root, ts)}, nil
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
