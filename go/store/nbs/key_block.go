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
	"slices"

	"github.com/dolthub/dolt/go/store/hash"
)

// An encrypted table file or archive carries a key block, which only its store's codec reads. A table file's records
// are sealed in place of their CRCs, and it has its own footer magic number:
//
//	| Records | Index | Key Block | (Uint32) Key Block Length | (Uint8) Version | Footer |
//
// An encrypted archive has the version 3 layout with every byte span sealed, and the key block between the metadata
// and the footer. The key block's length is in the first 4 bytes of the unused data checksum. Its version is outside
// the plaintext versions' sequence, so no later plaintext version can be read as encrypted.
const (
	encMagicNumber   = "\xff\xb5\xd8\xc2ENC1"
	encTrailerSize   = uint32Size + 1
	tableEncVersion1 = 1
	maxKeyBlockLen   = 64 << 20

	archiveVersionEncrypted = uint8(0x80)
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
	if err != nil {
		q.ReleaseQuotaBytes(len(buf))
		return buf, tailSz, err
	} else if extra == 0 {
		return buf, tailSz, nil
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

// encryptedRecordsEnd returns the end of an encrypted table's records, which its size does not give, or 0.
func encryptedRecordsEnd(cs chunkSource) uint64 {
	if idx, err := cs.index(); err == nil && encryptedTailSize(idx) != 0 {
		return calcChunkRangeSize(idx)
	}
	return 0
}

func (f *archiveFooter) encrypted() bool {
	return f.formatVersion == archiveVersionEncrypted
}

func (f *archiveFooter) keyBlockLen() uint64 {
	if !f.encrypted() {
		return 0
	}
	return uint64(binary.BigEndian.Uint32(f.dataCheckSum[:]))
}

// sourceEncrypted reports whether |cs| is an encrypted table file or archive.
func sourceEncrypted(cs chunkSource) (bool, error) {
	if acs, ok := cs.(*archiveChunkSource); ok {
		return acs.aRdr.footer.encrypted(), nil
	}
	idx, err := cs.index()
	if err != nil {
		return false, err
	}
	return encryptedTailSize(idx) != 0, nil
}

// conjoinedKeyBlock returns the key block of a file conjoining |sources| in order, or false if they are plaintext.
func conjoinedKeyBlock(codec storeCodec, sources []sourceWithSize) ([]byte, bool, error) {
	for _, sws := range sources {
		encrypted, err := sourceEncrypted(sws.source)
		if err != nil {
			return nil, false, err
		}
		if encrypted != codec.encrypted() {
			return nil, false, fmt.Errorf("%w: conjoining files of other encryption", ErrEncryptionMismatch)
		}
	}
	if !codec.encrypted() {
		return nil, false, nil
	}
	kb, err := codec.conjoinKeyBlock(sources)
	return kb, true, err
}

func conjoinedTail(codec storeCodec, sources []sourceWithSize) ([]byte, error) {
	kb, encrypted, err := conjoinedKeyBlock(codec, sources)
	if err != nil || !encrypted {
		return nil, err
	}
	return encryptedTail(kb)
}
