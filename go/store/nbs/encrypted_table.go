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
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"slices"

	"github.com/golang/snappy"

	"github.com/dolthub/dolt/go/store/hash"
)

// An encrypted table file seals each record in place of its CRC, and has its own footer magic number:
//
//	| Records | Index | Key Block | (Uint32) Key Block Length | (Uint8) Version | Footer |
const (
	encMagicNumber   = "\xff\xb5\xd8\xc2ENC1"
	encTrailerSize   = uint32Size + 1
	tableEncVersion1 = 1
	maxKeyBlockLen   = 64 << 20
)

func isEncryptedFooter(footer []byte) bool {
	return len(footer) >= magicNumberSize && bytes.Equal(footer[len(footer)-magicNumberSize:], []byte(encMagicNumber))
}

// parseEncryptedExtra returns the length of the key block and trailer of the table ending in |tail|, 0 if it is plain.
func parseEncryptedExtra(tail []byte) (uint64, error) {
	if !isEncryptedFooter(tail) {
		return 0, nil
	}
	if len(tail) < footerSize+encTrailerSize {
		return 0, ErrInvalidTableFile
	}
	trailer := tail[len(tail)-footerSize-encTrailerSize:]
	if v := trailer[uint32Size]; v != tableEncVersion1 {
		return 0, fmt.Errorf("%w: encrypted table file version %d", ErrUnsupportedTableFileFormat, v)
	}
	kbLen := uint64(binary.BigEndian.Uint32(trailer))
	if kbLen > maxKeyBlockLen {
		return 0, fmt.Errorf("%w: key block of %d bytes", ErrInvalidTableFile, kbLen)
	}
	return kbLen + encTrailerSize, nil
}

// encryptedExtraSize is parseEncryptedExtra for a tail that a reader already checked with it.
func encryptedExtraSize(tail []byte) uint64 {
	extra, _ := parseEncryptedExtra(tail)
	return extra
}

func addEncryptedTailSize(rd io.ReadSeeker, idxSz int64) (int64, error) {
	end, err := rd.Seek(0, io.SeekEnd)
	if err != nil {
		return idxSz, err
	}
	n := min(end, footerSize+encTrailerSize)
	if _, err = rd.Seek(-n, io.SeekEnd); err != nil {
		return idxSz, err
	}
	tail := make([]byte, n)
	if _, err = io.ReadFull(rd, tail); err != nil {
		return idxSz, err
	}
	extra, err := parseEncryptedExtra(tail)
	return idxSz + int64(extra), err
}

// rereadEncryptedTail replaces |buf|, a plain-sized tail, with an encrypted table's whole tail read by |load|.
func rereadEncryptedTail(ctx context.Context, q MemoryQuotaProvider, buf []byte, tailSz int, fileSz uint64, load func([]byte) (uint64, error)) ([]byte, int, error) {
	extra, err := parseEncryptedExtra(buf[:tailSz])
	if err != nil || extra == 0 {
		if err != nil {
			q.ReleaseQuotaBytes(len(buf))
		}
		return buf, tailSz, err
	}
	full := uint64(tailSz) + extra
	q.ReleaseQuotaBytes(len(buf))
	if uint64(int(full)) != full || (fileSz != 0 && full > fileSz) {
		return nil, 0, fmt.Errorf("%w: table tail of %d bytes does not fit in the file", ErrInvalidTableFile, full)
	}
	nb, err := q.AcquireQuotaByteSlice(ctx, int(full)+len(buf)-tailSz)
	if err != nil {
		return nil, 0, err
	}
	if _, err = load(nb[:full]); err != nil {
		q.ReleaseQuotaBytes(len(nb))
		return nil, 0, err
	}
	return nb, int(full), nil
}

func (fra *fileReaderAt) readTail(p []byte) (uint64, error) {
	if int64(len(p)) > fra.sz {
		return 0, ErrInvalidTableFile
	}
	_, err := io.ReadFull(io.NewSectionReader(fra.f, fra.sz-int64(len(p)), int64(len(p))), p)
	return uint64(fra.sz), err
}

func (ti onHeapTableIndex) encrypted() bool {
	return isEncryptedFooter(ti.footer)
}

func (ti onHeapTableIndex) keyBlock() []byte {
	return ti.footer[:len(ti.footer)-footerSize-encTrailerSize]
}

// encryptedTailSize returns the size of an encrypted table's key block and trailer, 0 for a plain table.
func encryptedTailSize(idx tableIndex) uint64 {
	if ti, ok := idx.(onHeapTableIndex); ok && ti.encrypted() {
		return uint64(len(ti.footer)) - footerSize
	}
	return 0
}

func encryptedTail(keyBlock []byte) ([]byte, error) {
	if len(keyBlock) > maxKeyBlockLen {
		return nil, fmt.Errorf("%w: key block of %d bytes", ErrInvalidTableFile, len(keyBlock))
	}
	tail := binary.BigEndian.AppendUint32(slices.Clip(keyBlock), uint32(len(keyBlock)))
	return append(tail, tableEncVersion1), nil
}

func writeEncryptedFooter(dst []byte, chunkCount uint32, uncData uint64) uint64 {
	n := writeFooter(dst, chunkCount, uncData)
	copy(dst[n-magicNumberSize:], encMagicNumber)
	return n
}

// randomTableName names an encrypted table, so a rewritten file never collides with one holding the same chunks.
func randomTableName() hash.Hash {
	var h hash.Hash
	if _, err := rand.Read(h[:]); err != nil {
		panic(err)
	}
	return h
}

func (tw *tableWriter) addSealedChunk(h hash.Hash, data []byte) bool {
	if tw.sealErr != nil {
		return false
	}
	tw.scratch = tw.snapper.Encode(tw.scratch[:cap(tw.scratch)], data)
	ordinal := uint32(len(tw.prefixes))
	out, err := tw.sealer.seal(tw.buff[:tw.pos], ordinal, h, tw.scratch)
	if err != nil {
		tw.sealErr = err
		return false
	}
	n := uint64(len(out)) - tw.pos
	tw.buff = out[:cap(out)]
	tw.pos += n
	tw.totalCompressedData += n
	tw.totalUncompressedData += uint64(len(data))
	tw.prefixes = append(tw.prefixes, prefixIndexRec{h, ordinal, uint32(n)})
	return true
}

func (tw *tableWriter) finishSealed() (uint64, hash.Hash, error) {
	if tw.sealErr != nil {
		return 0, hash.Hash{}, tw.sealErr
	}
	kb, err := tw.sealer.keyBlock()
	if err != nil {
		return 0, hash.Hash{}, err
	}
	tail, err := encryptedTail(kb)
	if err != nil {
		return 0, hash.Hash{}, err
	}
	count := uint32(len(tw.prefixes))
	tw.buff = slices.Grow(tw.buff[:tw.pos], int(indexSize(count))+len(tail)+footerSize)
	tw.buff = tw.buff[:cap(tw.buff)]
	tw.recordsEnd = tw.pos
	if err = tw.writeIndex(); err != nil {
		return 0, hash.Hash{}, err
	}
	tw.pos += uint64(copy(tw.buff[tw.pos:], tail))
	tw.pos += writeEncryptedFooter(tw.buff[tw.pos:], count, tw.totalUncompressedData)
	return tw.pos, randomTableName(), nil
}

func (tw *CmpChunkTableWriter) addSealed(tc ToChunker) (uint32, error) {
	ordinal := uint32(len(tw.prefixes))
	var record []byte
	var uncLen int
	if sc, ok := tc.(SealedChunk); ok {
		// Authenticate the record before taking its key.
		chk, err := sc.ToChunk()
		if err != nil {
			return 0, err
		}
		if tw.sealer.adopt(ordinal, sc) {
			record, uncLen = sc.sealedRecord(), len(chk.Data())
		}
	}
	if record == nil {
		payload, n, err := snappyPayload(tc)
		if err != nil {
			return 0, err
		}
		if record, err = tw.sealer.seal(nil, ordinal, tc.Hash(), payload); err != nil {
			return 0, err
		}
		uncLen = n
	}
	if uint64(len(record)) > math.MaxUint32 {
		return 0, fmt.Errorf("chunk %s is too large", tc.Hash().String())
	}
	if _, err := tw.sink.Write(record); err != nil {
		return 0, err
	}
	tw.totalUncompressedData += uint64(uncLen)
	tw.prefixes = append(tw.prefixes, prefixIndexRec{tc.Hash(), ordinal, uint32(len(record))})
	return uint32(len(record)), nil
}

func snappyPayload(tc ToChunker) ([]byte, int, error) {
	if c, ok := tc.(CompressedChunk); ok && !c.verify {
		n, err := snappy.DecodedLen(c.CompressedData)
		return c.CompressedData, n, err
	}
	chk, err := tc.ToChunk()
	if err != nil {
		return nil, 0, err
	}
	return snappy.Encode(nil, chk.Data()), len(chk.Data()), nil
}

func (tw *CmpChunkTableWriter) writeEncryptedTail() error {
	kb, err := tw.sealer.keyBlock()
	if err != nil {
		return err
	}
	tail, err := encryptedTail(kb)
	if err != nil {
		return err
	}
	footer := make([]byte, footerSize)
	writeEncryptedFooter(footer, uint32(len(tw.prefixes)), tw.totalUncompressedData)
	_, err = tw.sink.Write(append(tail, footer...))
	return err
}

// newTableReaderFor is newTableReader for a table opened by a store, whose encryption it must match.
func newTableReaderFor(ctx context.Context, index tableIndex, r tableReaderAt, blockSize uint64, opts openOpts) (tableReader, error) {
	opener, err := tableOpener(index, opts)
	if err != nil {
		return tableReader{}, refusedFileError{err}
	}
	tr, err := newTableReader(ctx, index, r, blockSize)
	tr.opener = opener
	return tr, err
}

// refusedFileError is an error from a store's codec refusing a file that exists.
type refusedFileError struct {
	error
}

func (e refusedFileError) Unwrap() error {
	return e.error
}

func tableOpener(index tableIndex, opts openOpts) (recordOpener, error) {
	ti, ok := index.(onHeapTableIndex)
	encrypted := ok && ti.encrypted()
	codec, err := opts.fileCodec(encrypted)
	if err != nil {
		return nil, err
	}
	if encrypted != codec.encrypted() {
		return nil, ErrEncryptionMismatch
	} else if !encrypted {
		return nil, nil
	}
	return codec.openTable(fileKeys{
		keyBlock: ti.keyBlock(),
		count:    ti.count,
		position: func(ordinal uint32) uint64 { return ti.getIndexEntry(ordinal).Offset() },
		adding:   opts.adding,
	})
}

func (tr tableReader) chunkFromRecord(ctx context.Context, h hash.Hash, offset uint64, record []byte) (CompressedChunk, error) {
	if tr.opener == nil {
		return NewCompressedChunk(h, record)
	}
	payload, err := tr.opener.open(ctx, offset, h, record)
	if err != nil {
		return CompressedChunk{}, fmt.Errorf("chunk %s: %w", h.String(), err)
	}
	return openedSnappyChunk(h, payload), nil
}

// openedSnappyChunk is the chunk whose snappy |payload| was sealed without its CRC, which ToChunk checks by hash.
func openedSnappyChunk(h hash.Hash, payload []byte) CompressedChunk {
	full := binary.BigEndian.AppendUint32(payload, crc(payload))
	return CompressedChunk{H: h, FullCompressedChunk: full, CompressedData: full[:len(payload)], verify: true}
}

// extractChunkFromRead leaves an encrypted record sealed until ToChunk.
func (tr tableReader) extractChunkFromRead(ctx context.Context, rb readBatch, buff []byte, i int) (ToChunker, error) {
	if tr.opener == nil {
		return rb.ExtractChunkFromRead(buff, i)
	}
	rec := rb[i]
	start := rec.offset - rb.Start()
	return tr.opener.sealedChunk(ctx, rec.offset, *rec.a, buff[start:start+uint64(rec.length)])
}

func (tr tableReader) locateKeys(ctx context.Context, ranges map[hash.Hash]Range) error {
	c := locatedKeysFrom(ctx)
	if tr.opener == nil || c == nil {
		return nil
	}
	for h, r := range ranges {
		k, err := tr.opener.storedKey(r.Offset)
		if err != nil {
			return err
		}
		c.note(h, r, RangeKeys{Key: k})
	}
	return nil
}

func (cmp CompressedChunk) verifyData(data []byte) error {
	if cmp.verify {
		if actual := hash.Of(data); actual != cmp.H {
			return fmt.Errorf("%w: chunk %s decodes to data with hash %s", ErrInvalidTableFile, cmp.H.String(), actual.String())
		}
	}
	return nil
}

// sourceKeyBlock returns the key block of an encrypted table file or archive, and the number of records it covers.
func sourceKeyBlock(cs chunkSource) ([]byte, uint32, bool, error) {
	if acs, ok := cs.(*archiveChunkSource); ok {
		return acs.aRdr.keyBlock, acs.aRdr.footer.byteSpanCount, acs.aRdr.footer.encrypted(), nil
	}
	idx, err := cs.index()
	if err != nil {
		return nil, 0, false, err
	}
	if ti, ok := idx.(onHeapTableIndex); ok && ti.encrypted() {
		return ti.keyBlock(), ti.count, true, nil
	}
	return nil, idx.chunkCount(), false, nil
}

// conjoinedKeyBlock merges the key blocks of conjoin sources in copy order, or returns nil for plain sources.
func conjoinedKeyBlock(codec storeCodec, sources []sourceWithSize) ([]byte, bool, error) {
	var parts []keyBlockPart
	var start uint32
	for i, sws := range sources {
		kb, count, encrypted, err := sourceKeyBlock(sws.source)
		if err != nil {
			return nil, false, err
		}
		if i > 0 && encrypted != (parts != nil) {
			return nil, false, fmt.Errorf("%w: conjoining encrypted and plaintext files", ErrEncryptionMismatch)
		}
		if encrypted {
			parts = append(parts, keyBlockPart{keyBlock: kb, start: start, count: count})
		}
		start += count
	}
	if parts == nil {
		return nil, false, nil
	}
	kb, err := codec.mergeKeyBlocks(parts)
	return kb, true, err
}

func conjoinedTail(codec storeCodec, sources []sourceWithSize) ([]byte, error) {
	kb, encrypted, err := conjoinedKeyBlock(codec, sources)
	if err != nil || !encrypted {
		return nil, err
	}
	return encryptedTail(kb)
}

// encryptedRecordsEnd returns the end of an encrypted table's records, which its size does not give, or 0.
func encryptedRecordsEnd(cs chunkSource) uint64 {
	if idx, err := cs.index(); err == nil && encryptedTailSize(idx) != 0 {
		return calcChunkRangeSize(idx)
	}
	return 0
}
