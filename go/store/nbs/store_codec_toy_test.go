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
	"sort"
	"sync"
	"sync/atomic"

	"github.com/dolthub/gozstd"
	"github.com/golang/snappy"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
)

// toyCodec is a store codec for tests of the encryption seams. It "seals" a record by XORing it with a one byte key
// and appending a tag that binds what the record holds, so a record read as plaintext, or as the wrong record, fails.
// It is not encryption.
type toyCodec struct {
	keyless bool
	nextKey *atomic.Uint32
	// added counts the commits of files added to the store.
	added *atomic.Int32
}

var errToyKeyless = errors.New("toy codec has no keys")

var errToyTag = errors.New("toy record does not authenticate")

func newToyCodec() *toyCodec {
	return &toyCodec{nextKey: new(atomic.Uint32), added: new(atomic.Int32)}
}

// withToy opens a store under |c|, creating it encrypted.
func withToy(c *toyCodec) StoreOption {
	return func(o *storeOptions) {
		o.newCodec = func(exists bool, format string) (string, storeCodec, error) {
			if !exists {
				format = constants.EncryptedFormatString
			}
			if format != constants.EncryptedFormatString {
				return format, plainCodec{}, nil
			}
			return format, c, nil
		}
	}
}

func (c *toyCodec) key() byte {
	return byte(c.nextKey.Add(1)%250) + 1
}

func toyTag(kind byte, h hash.Hash, plain []byte) []byte {
	crc := crc32.Update(crc32.ChecksumIEEE([]byte{kind}), crc32.IEEETable, h[:])
	return binary.BigEndian.AppendUint32(nil, crc32.Update(crc, crc32.IEEETable, plain)^0x5a5a5a5a)
}

func toySeal(dst []byte, key, kind byte, h hash.Hash, plain []byte) []byte {
	for _, b := range plain {
		dst = append(dst, b^key)
	}
	return append(dst, toyTag(kind, h, plain)...)
}

func toyOpen(key, kind byte, h hash.Hash, sealed []byte) ([]byte, error) {
	if len(sealed) < 4 {
		return nil, errToyTag
	}
	plain := make([]byte, len(sealed)-4, len(sealed))
	for i := range plain {
		plain[i] = sealed[i] ^ key
	}
	if !bytes.Equal(sealed[len(plain):], toyTag(kind, h, plain)) {
		return nil, errToyTag
	}
	return plain, nil
}

const (
	toyKindChunk = 'c'
	toyKindDict  = 'd'
	toyKindRoot  = 'r'
)

// toyKeys is a toy key block: runs of records, by ordinal, sealed with one key each.
type toyKeys struct {
	starts []uint32
	keys   []byte
}

func (k *toyKeys) add(ordinal uint32, key byte) {
	if n := len(k.keys); n == 0 || k.keys[n-1] != key {
		k.starts = append(k.starts, ordinal)
		k.keys = append(k.keys, key)
	}
}

func (k *toyKeys) encode() []byte {
	b := []byte("TOY")
	for i := range k.keys {
		b = binary.BigEndian.AppendUint32(b, k.starts[i])
		b = append(b, k.keys[i])
	}
	return b
}

func decodeToyKeys(b []byte) (*toyKeys, error) {
	if !bytes.HasPrefix(b, []byte("TOY")) || (len(b)-3)%5 != 0 {
		return nil, fmt.Errorf("bad toy key block %x", b)
	}
	k := &toyKeys{}
	for b = b[3:]; len(b) > 0; b = b[5:] {
		k.starts = append(k.starts, binary.BigEndian.Uint32(b))
		k.keys = append(k.keys, b[4])
	}
	return k, nil
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
	return &toySealer{key: c.key()}, nil
}

func (c *toyCodec) newSpanSealer(recompress bool) (spanSealer, error) {
	if c.keyless {
		return nil, errToyKeyless
	}
	return &toySpanSealer{toySealer: toySealer{key: c.key()}, recompress: recompress, copiedDicts: map[*toyDict]uint32{}}, nil
}

func (c *toyCodec) openTable(f fileKeys) (recordOpener, error) {
	return c.open(f)
}

func (c *toyCodec) openArchive(f fileKeys) (archiveSpans, error) {
	return c.open(f)
}

func (c *toyCodec) open(f fileKeys) (*toyOpener, error) {
	keys, err := decodeToyKeys(f.keyBlock)
	if err != nil {
		return nil, err
	}
	o := &toyOpener{codec: c, keys: keys.keys}
	for _, s := range keys.starts {
		if s >= f.count {
			return nil, fmt.Errorf("toy key run starts at %d of %d records", s, f.count)
		}
		o.starts = append(o.starts, f.position(s))
	}
	return o, nil
}

func (c *toyCodec) newJournalCodec() journalCodec {
	return &toyJournal{codec: c, keys: map[byte]bool{}}
}

func (c *toyCodec) mergeKeyBlocks(parts []keyBlockPart) ([]byte, error) {
	var merged toyKeys
	for _, p := range parts {
		keys, err := decodeToyKeys(p.keyBlock)
		if err != nil {
			return nil, err
		}
		for i := range keys.keys {
			merged.add(p.start+keys.starts[i], keys.keys[i])
		}
	}
	return merged.encode(), nil
}

func (c *toyCodec) checkOpened(context.Context, *NomsBlockStore, chunkSourceSet, hash.Hash) error {
	return nil
}

func (c *toyCodec) checkAdded(_ *NomsBlockStore, added chunkSourceSet) (func() error, error) {
	for _, cs := range added {
		if _, _, encrypted, err := sourceKeyBlock(cs); err != nil {
			return nil, err
		} else if !encrypted {
			if _, ok := cs.(journalChunkSource); !ok {
				return nil, ErrEncryptionMismatch
			}
		}
	}
	return func() error {
		c.added.Add(1)
		return nil
	}, nil
}

type toySealer struct {
	key  byte
	keys toyKeys
}

func (s *toySealer) seal(dst []byte, ordinal uint32, h hash.Hash, payload []byte) ([]byte, error) {
	s.keys.add(ordinal, s.key)
	return toySeal(dst, s.key, toyKindChunk, h, payload), nil
}

// adopt copies records sealed with up to two other keys, then re-seals the rest.
func (s *toySealer) adopt(ordinal uint32, sc SealedChunk) bool {
	tc, ok := sc.(*toyChunk)
	if !ok || tc.dict != nil || (len(s.keys.keys) >= 3 && s.keys.keys[len(s.keys.keys)-1] != tc.key) {
		return false
	}
	s.keys.add(ordinal, tc.key)
	return true
}

func (s *toySealer) keyBlock() ([]byte, error) {
	return s.keys.encode(), nil
}

type toySpanSealer struct {
	toySealer
	recompress  bool
	copiedDicts map[*toyDict]uint32
}

func (s *toySpanSealer) seal(ordinal uint32, ref spanRef, b []byte) ([]byte, error) {
	s.keys.add(ordinal, s.key)
	switch {
	case ref.isDict:
		return toySeal(nil, s.key, toyKindDict, hash.Hash{}, b), nil
	case ref.dict == 0:
		return toySeal(nil, s.key, toyKindChunk, ref.h, b[:len(b)-checksumSize]), nil
	default:
		return toySeal(nil, s.key, toyKindChunk, ref.h, b), nil
	}
}

func (s *toySpanSealer) addChunk(asw *ArchiveStreamWriter, c ToChunker) (uint32, error) {
	tc, ok := c.(*toyChunk)
	if !ok {
		return 0, fmt.Errorf("unknown chunk type %T", c)
	}
	if s.recompress && tc.dict == nil {
		chk, err := tc.ToChunk()
		if err != nil {
			return 0, err
		}
		return asw.writeCompressedChunk(ChunkToCompressedChunk(chk))
	}
	var dictId uint32
	if tc.dict != nil {
		d := tc.dict.sealed.(*toyDict)
		if dictId = s.copiedDicts[d]; dictId == 0 {
			var err error
			if dictId, err = asw.writer.writeByteSpan(d.stored); err != nil {
				return 0, err
			}
			s.keys.add(dictId-1, d.key)
			s.copiedDicts[d] = dictId
		}
	}
	dataId, err := asw.writer.writeByteSpan(tc.record)
	if err != nil {
		return 0, err
	}
	s.keys.add(dataId-1, tc.key)
	asw.chunkCount++
	if dictId == 0 {
		return uint32(len(tc.record)), asw.writer.stageSnappyChunk(tc.h, dataId)
	}
	return uint32(len(tc.record)), asw.writer.stageZStdChunk(tc.h, dictId, dataId)
}

type toyOpener struct {
	codec  *toyCodec
	starts []uint64
	keys   []byte
}

func (o *toyOpener) keyAt(pos uint64) (byte, error) {
	i := sort.Search(len(o.starts), func(i int) bool { return o.starts[i] > pos }) - 1
	if i < 0 {
		return 0, fmt.Errorf("no toy key run covers %d", pos)
	}
	return o.keys[i], nil
}

func (o *toyOpener) open(_ context.Context, pos uint64, h hash.Hash, sealed []byte) ([]byte, error) {
	if o.codec.keyless {
		return nil, errToyKeyless
	}
	key, err := o.keyAt(pos)
	if err != nil {
		return nil, err
	}
	return toyOpen(key, toyKindChunk, h, sealed)
}

func (o *toyOpener) sealedChunk(_ context.Context, pos uint64, h hash.Hash, sealed []byte) (SealedChunk, error) {
	key, err := o.keyAt(pos)
	if err != nil {
		return nil, err
	}
	return &toyChunk{codec: o.codec, h: h, key: key, record: sealed}, nil
}

func (o *toyOpener) storedKey(pos uint64) ([]byte, error) {
	key, err := o.keyAt(pos)
	return []byte{key}, err
}

type toyDict struct {
	stored []byte
	key    byte
}

func (o *toyOpener) openDict(ctx context.Context, id uint32, stored []byte) (*DecompBundle, error) {
	if o.codec.keyless {
		return nil, errToyKeyless
	}
	key, err := o.keyAt(uint64(id - 1))
	if err != nil {
		return nil, err
	}
	raw, err := toyOpen(key, toyKindDict, hash.Hash{}, stored)
	if err != nil {
		return nil, err
	}
	dict, err := NewDecompBundle(raw)
	if err != nil {
		return nil, err
	}
	dict.sealed = &toyDict{stored: bytes.Clone(stored), key: key}
	return dict, nil
}

func (o *toyOpener) chunkData(ctx context.Context, rc resolvedChunk, dict *DecompBundle, stored []byte) ([]byte, error) {
	c, err := o.toChunker(ctx, rc, dict, stored)
	if err != nil {
		return nil, err
	}
	chk, err := c.ToChunk()
	return chk.Data(), err
}

func (o *toyOpener) toChunker(_ context.Context, rc resolvedChunk, dict *DecompBundle, stored []byte) (ToChunker, error) {
	key, err := o.keyAt(uint64(rc.dataId - 1))
	if err != nil {
		return nil, err
	}
	return &toyChunk{codec: o.codec, h: rc.h, key: key, record: stored, dict: dict}, nil
}

// toyChunk is a sealed toy record: snappy data, or zstd data compressed with |dict|.
type toyChunk struct {
	codec  *toyCodec
	h      hash.Hash
	key    byte
	record []byte
	dict   *DecompBundle
}

func (tc *toyChunk) ToChunk() (chunks.Chunk, error) {
	if tc.codec.keyless {
		return chunks.EmptyChunk, errToyKeyless
	}
	payload, err := toyOpen(tc.key, toyKindChunk, tc.h, tc.record)
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

func (tc *toyChunk) sealedRecord() []byte {
	return tc.record
}

// toyJournal seals journal records as | key | sealed record |, so they copy as they are to table files and archives.
type toyJournal struct {
	codec   *toyCodec
	mu      sync.Mutex
	keys    map[byte]bool
	key     byte
	lookups uint32
	replays int
	resets  int
}

func (j *toyJournal) has(key byte) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.keys[key]
}

func (j *toyJournal) learn(key byte) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.keys[key] = true
}

func (j *toyJournal) replay(_ context.Context, off int64, r journalRec) error {
	j.replays++
	switch r.kind {
	case keyJournalRecKind:
		if len(r.payload) != 1 {
			return fmt.Errorf("bad toy key record at %d", off)
		}
		j.learn(r.payload[0])
		return nil
	case encChunkJournalRecKind:
		c, err := j.chunker(r.address, r.payload)
		if err == nil {
			_, err = c.ToChunk()
		}
		return err
	case encRootHashJournalRecKind:
		if len(r.payload) < 1 || !j.has(r.payload[0]) {
			return fmt.Errorf("toy root record at %d has no key", off)
		}
		_, err := toyOpen(r.payload[0], toyKindRoot, r.address, r.payload[1:])
		return err
	}
	return fmt.Errorf("%w: plaintext journal record at %d", ErrEncryptionMismatch, off)
}

func (j *toyJournal) checkBatch(_ context.Context, m lookupMeta, batch []lookup) error {
	for _, k := range m.keys {
		j.learn(k[0])
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

func (j *toyJournal) writerKey() (byte, []byte, error) {
	if j.codec.keyless {
		return 0, nil, errToyKeyless
	}
	if j.key == 0 {
		j.key = j.codec.key()
	}
	if j.has(j.key) {
		return j.key, nil, nil
	}
	return j.key, []byte{j.key}, nil
}

func (j *toyJournal) sealChunk(_ context.Context, cc CompressedChunk) (sealedRecord, error) {
	key, intro, err := j.writerKey()
	if err != nil {
		return sealedRecord{}, err
	}
	return sealedRecord{kind: encChunkJournalRecKind, payload: toySeal([]byte{key}, key, toyKindChunk, cc.H, cc.CompressedData), key: intro}, nil
}

func (j *toyJournal) sealRoot(_ context.Context, root hash.Hash, _ uint64) (sealedRecord, error) {
	key, intro, err := j.writerKey()
	if err != nil {
		return sealedRecord{}, err
	}
	return sealedRecord{kind: encRootHashJournalRecKind, payload: toySeal([]byte{key}, key, toyKindRoot, root, nil), key: intro}, nil
}

func (j *toyJournal) indexedLookup(lookup) {
	j.lookups++
}

func (j *toyJournal) indexedKey(key []byte) {
	j.learn(key[0])
}

func (j *toyJournal) sealBatch(m lookupMeta) ([]byte, bool, error) {
	n := j.lookups
	j.lookups = 0
	return j.batchAuth(m, n), true, nil
}

func (j *toyJournal) chunker(h hash.Hash, payload []byte) (ToChunker, error) {
	if len(payload) < 1 || !j.has(payload[0]) {
		return nil, fmt.Errorf("toy journal chunk %s has no key", h.String())
	}
	return &toyChunk{codec: j.codec, h: h, key: payload[0], record: payload[1:]}, nil
}

func (j *toyJournal) locate(_ hash.Hash, r Range, readAt func([]byte, int64) (int, error)) (Range, []byte, error) {
	key := make([]byte, 1)
	if _, err := readAt(key, int64(r.Offset)); err != nil {
		return Range{}, nil, err
	}
	return Range{Offset: r.Offset + 1, Length: r.Length - 1}, key, nil
}

func (j *toyJournal) reset() {
	j.resets++
	j.mu.Lock()
	defer j.mu.Unlock()
	j.keys = map[byte]bool{}
}
