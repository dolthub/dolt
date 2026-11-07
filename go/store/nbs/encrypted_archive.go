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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/store/hash"
)

// An encrypted archive has the version 3 layout with every byte span sealed, and a key block between the metadata
// and the footer. The key block's length is in the first 4 bytes of the unused data checksum. Its version is outside
// the plaintext versions' sequence, so no later plaintext version can be read as encrypted.
const archiveVersionEncrypted = uint8(0x80)

func (f *archiveFooter) encrypted() bool {
	return f.formatVersion == archiveVersionEncrypted
}

func (f *archiveFooter) keyBlockLen() uint64 {
	if !f.encrypted() {
		return 0
	}
	return uint64(binary.BigEndian.Uint32(f.dataCheckSum[:]))
}

// openSpans reads an encrypted archive's key block and opener, failing if the archive does not match its store.
func (ar *archiveReader) openSpans(ctx context.Context, opts openOpts, stats *Stats) error {
	f := ar.footer
	codec, err := opts.fileCodec(f.encrypted())
	if err != nil {
		return err
	}
	if f.encrypted() != codec.encrypted() {
		return fmt.Errorf("%w: archive %s", ErrEncryptionMismatch, f.hash.String())
	} else if !f.encrypted() {
		return nil
	}
	n := f.keyBlockLen()
	if n > maxKeyBlockLen || (n == 0) != (f.byteSpanCount == 0) {
		return fmt.Errorf("%w: invalid key block length %d for %d byte spans", ErrCorruptArchiveIndex, n, f.byteSpanCount)
	}
	ar.keyBlock = make([]byte, n)
	section := newSectionReader(ctx, ar.reader, int64(f.fileSize-f.actualFooterSize()), int64(n), stats)
	if _, err = io.ReadFull(section, ar.keyBlock); err != nil {
		return err
	}
	ar.spans, err = codec.openArchive(fileKeys{
		keyBlock: ar.keyBlock,
		count:    f.byteSpanCount,
		position: func(ordinal uint32) uint64 { return uint64(ordinal) },
		adding:   opts.adding,
	})
	if err == nil && ar.spans == nil {
		err = errors.New("runtime error: no opener for an encrypted archive")
	}
	return err
}

// openDict returns the dictionary stored in span |id|.
func (ar *archiveReader) openDict(ctx context.Context, id uint32, stored []byte) (*DecompBundle, error) {
	if ar.spans == nil {
		return NewDecompBundle(stored)
	}
	return ar.spans.openDict(ctx, id, stored)
}

// chunkData returns the data of chunk |rc| from its stored data span.
func (ar *archiveReader) chunkData(ctx context.Context, rc resolvedChunk, dict *DecompBundle, stored []byte) ([]byte, error) {
	if ar.spans == nil {
		return ar.decompress(rc.h, dict, stored)
	}
	return ar.spans.chunkData(ctx, rc, dict, stored)
}

// chunker returns chunk |rc| as stored, for writers that copy it.
func (ar *archiveReader) chunker(ctx context.Context, rc resolvedChunk, dict *DecompBundle, stored []byte) (ToChunker, error) {
	if ar.spans == nil {
		return ar.toChunker(rc.h, dict, stored)
	}
	return ar.spans.toChunker(ctx, rc, dict, stored)
}

func (ar *archiveReader) locateKeys(ctx context.Context, resolved []resolvedChunk) error {
	if ar.spans == nil {
		return nil
	}
	c := locatedKeysFrom(ctx)
	if c == nil {
		return nil
	}
	for _, rc := range resolved {
		var keys RangeKeys
		var err error
		if keys.Key, err = ar.spans.storedKey(uint64(rc.dataId - 1)); err != nil {
			return err
		}
		if rc.dictId != 0 {
			if keys.DictKey, err = ar.spans.storedKey(uint64(rc.dictId - 1)); err != nil {
				return err
			}
		}
		span := ar.getByteSpanByID(rc.dataId)
		c.note(rc.h, Range{Offset: span.offset, Length: uint32(span.length)}, keys)
	}
	return nil
}

// writeSpan writes |b|, sealed if the archive is encrypted, as a span holding |ref|.
func (aw *archiveWriter) writeSpan(ref spanRef, b []byte) (uint32, error) {
	if aw.sealer != nil {
		var err error
		if b, err = aw.sealer.seal(uint32(len(aw.stagedBytes)), ref, b); err != nil {
			return 0, err
		}
	}
	return aw.writeByteSpan(b)
}

// writeKeyBlock writes an encrypted archive's key block before its footer.
func (aw *archiveWriter) writeKeyBlock() error {
	if aw.sealer == nil {
		return nil
	}
	kb, err := aw.sealer.keyBlock()
	if err != nil {
		return err
	}
	if len(kb) > maxKeyBlockLen {
		return fmt.Errorf("%w: key block of %d bytes", ErrCorruptArchiveIndex, len(kb))
	}
	written, err := aw.output.Write(kb)
	if err != nil {
		return err
	}
	aw.bytesWritten += uint64(written)
	aw.keyBlockLen = uint32(written)
	return nil
}

func (aw *archiveWriter) formatVersion() byte {
	if aw.sealer != nil {
		return archiveVersionEncrypted
	}
	return archiveFormatVersionMax
}

// conjoinedKeys is the sealer of a conjoined archive, whose sources' spans are copied as they are stored.
type conjoinedKeys []byte

func (k conjoinedKeys) seal(uint32, spanRef, []byte) ([]byte, error) {
	return nil, errors.New("runtime error: conjoined archive spans are copied, not sealed")
}

func (k conjoinedKeys) adopt(uint32, SealedChunk, bool) bool {
	return false
}

func (k conjoinedKeys) keyBlock() ([]byte, error) {
	return k, nil
}

func conjoinSpanSealer(codec storeCodec, sources []sourceWithSize) (spanSealer, error) {
	kb, encrypted, err := conjoinedKeyBlock(codec, sources)
	if err != nil || !encrypted {
		return nil, err
	}
	return conjoinedKeys(kb), nil
}

// addSealed copies |sc| as it is stored, with its dictionary once, if the sealer takes its keys, and otherwise
// compresses and seals it again.
func (asw *ArchiveStreamWriter) addSealed(sc SealedChunk) (uint32, error) {
	dict := sc.sealedDict()
	dictKey := hash.Of(dict)
	dictId, copied := asw.sealedDicts[dictKey]
	ordinal := uint32(len(asw.writer.stagedBytes))
	if dict != nil && !copied {
		ordinal++
	}
	if !asw.writer.sealer.adopt(ordinal, sc, dict == nil || copied) {
		chk, err := sc.ToChunk()
		if err != nil {
			return 0, err
		}
		return asw.writeCompressedChunk(ChunkToCompressedChunk(chk))
	}
	var written uint32
	if dict != nil && !copied {
		var err error
		if dictId, err = asw.writer.writeByteSpan(dict); err != nil {
			return 0, err
		}
		if asw.sealedDicts == nil {
			asw.sealedDicts = make(map[hash.Hash]uint32)
		}
		asw.sealedDicts[dictKey] = dictId
		written += uint32(len(dict))
	}
	dataId, err := asw.writer.writeByteSpan(sc.sealedRecord())
	if err != nil {
		return written, err
	}
	written += uint32(len(sc.sealedRecord()))
	asw.chunkCount++
	if dict == nil {
		return written, asw.writer.stageSnappyChunk(sc.Hash(), dataId)
	}
	return written, asw.writer.stageZStdChunk(sc.Hash(), dictId, dataId)
}
