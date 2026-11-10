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

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

// An archiveCodec reads the byte spans of an archive that a spanSealer wrote. An archive without one stores them as
// they are.
type archiveCodec interface {
	// openDict returns the dictionary stored in span |id|.
	openDict(ctx context.Context, ar *archiveReader, id uint32, stored []byte) (*DecompBundle, error)
	// openData returns the compressed data of chunk |rc| from its stored span.
	openData(ctx context.Context, ar *archiveReader, rc resolvedChunk, dict *DecompBundle, stored []byte) ([]byte, error)
	// decompress returns the data of chunk |h| from its compressed data.
	decompress(h hash.Hash, dict *DecompBundle, data []byte) ([]byte, error)
	// getRaw returns the dictionary and compressed data of chunk |rc|.
	getRaw(ctx context.Context, ar *archiveReader, rc resolvedChunk, stats *Stats) (*DecompBundle, []byte, error)
	// chunker returns chunk |h| as it is stored, for writers that copy it, or an empty chunk if the archive lacks it.
	chunker(ctx context.Context, ar *archiveReader, h hash.Hash, stats *Stats) (ToChunker, error)
	// found passes chunk |rc|, as it is stored, to |found|.
	found(ctx context.Context, ar *archiveReader, rc resolvedChunk, dict *DecompBundle, stored []byte, found func(context.Context, ToChunker)) error
	// locateSpans is called with the chunks whose ranges a remote client is sent.
	locateSpans(ctx context.Context, ar *archiveReader, resolved []resolvedChunk) error
	iterate(ctx context.Context, ar *archiveReader, cb func(chunks.Chunk) error, stats *Stats) error
	tolerantIterate(ctx context.Context, ar *archiveReader, cb func(chunks.Chunk), errCb func(error), stats *Stats)
}

// A spanSealer writes the byte spans of an archive in place of the plain ones, and what an archiveCodec needs to read
// them back.
type spanSealer interface {
	writeDict(aw *archiveWriter, compressed []byte) (uint32, error)
	writeZStdData(aw *archiveWriter, h hash.Hash, dictId uint32, data []byte) (uint32, error)
	writeSnappyData(aw *archiveWriter, cc CompressedChunk) (uint32, error)
	// writeKeyBlock is called after the archive's metadata is written.
	writeKeyBlock(aw *archiveWriter) error
	// putKeyBlockLen is called with the footer's checksum bytes.
	putKeyBlockLen(checkSums []byte)
	formatVersion() uint8
}

// openArchiveCodec returns the codec of an archive with |footer|, or nil for a plain archive.
var openArchiveCodec = func(ctx context.Context, reader tableReaderAt, footer archiveFooter, opts openOpts, stats *Stats) (archiveCodec, error) {
	return nil, nil
}

// archiveFooterExtra returns the size of what an archive with footer |f| holds between its metadata and its footer.
var archiveFooterExtra = func(f *archiveFooter) uint64 {
	return 0
}

// codedArchiveVersion reports whether archive format |version| is one a codec reads.
var codedArchiveVersion = func(version uint8) bool {
	return false
}

// addCodedChunk adds |c| to |asw| if it is a chunk a codec stores, and returns false otherwise.
var addCodedChunk = func(asw *ArchiveStreamWriter, c ToChunker) (id uint32, ok bool, err error) {
	return 0, false, nil
}

// conjoinSpanSealer returns the sealer of the archive conjoining |sources|, or nil.
var conjoinSpanSealer = func(sources []sourceWithSize) (spanSealer, error) {
	return nil, nil
}

func (ar *archiveReader) openDict(ctx context.Context, id uint32, stored []byte) (*DecompBundle, error) {
	if ar.codec == nil {
		return NewDecompBundle(stored)
	}
	return ar.codec.openDict(ctx, ar, id, stored)
}

func (ar *archiveReader) openData(ctx context.Context, rc resolvedChunk, dict *DecompBundle, stored []byte) ([]byte, error) {
	if ar.codec == nil {
		return stored, nil
	}
	return ar.codec.openData(ctx, ar, rc, dict, stored)
}

func (ar *archiveReader) locateSpans(ctx context.Context, resolved []resolvedChunk) error {
	if ar.codec == nil {
		return nil
	}
	return ar.codec.locateSpans(ctx, ar, resolved)
}

func (aw *archiveWriter) writeKeyBlock() error {
	if aw.spans == nil {
		return nil
	}
	return aw.spans.writeKeyBlock(aw)
}

func (aw *archiveWriter) putKeyBlockLen(checkSums []byte) {
	if aw.spans != nil {
		aw.spans.putKeyBlockLen(checkSums)
	}
}

func (aw *archiveWriter) formatVersion() uint8 {
	if aw.spans == nil {
		return archiveFormatVersionMax
	}
	return aw.spans.formatVersion()
}

func (asw *ArchiveStreamWriter) writeDict(compressed []byte) (uint32, error) {
	if asw.writer.spans == nil {
		return asw.writer.writeByteSpan(compressed)
	}
	return asw.writer.spans.writeDict(asw.writer, compressed)
}

func (asw *ArchiveStreamWriter) writeZStdData(h hash.Hash, dictId uint32, data []byte) (uint32, error) {
	if asw.writer.spans == nil {
		return asw.writer.writeByteSpan(data)
	}
	return asw.writer.spans.writeZStdData(asw.writer, h, dictId, data)
}

func (asw *ArchiveStreamWriter) writeSnappyData(cc CompressedChunk) (uint32, error) {
	if asw.writer.spans == nil {
		return asw.writer.writeByteSpan(cc.FullCompressedChunk)
	}
	return asw.writer.spans.writeSnappyData(asw.writer, cc)
}
