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
	"os"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/hash"
)

// A journalCodec writes and reads the records of a journal in place of its plain chunk and root hash records. A writer
// calls it with its lock held, except as noted.
type journalCodec interface {
	// replay is called with each record bootstrapping reads, before the writer indexes it. It returns |skip| for a
	// record the writer does not index.
	replay(ctx context.Context, wr *journalWriter, canWrite bool, off int64, r journalRec) (skip bool, err error)
	// checkBatch checks a batch of lookups read from the journal's index. It runs on its own goroutine while the writer
	// indexes the batches.
	checkBatch(ctx context.Context, m lookupMeta, batch []lookup) error
	// reset forgets what bootstrapping replayed, when the index is rebuilt.
	reset()
	// openChunk returns chunk |h|, or an empty chunk if the journal lacks it. It takes the writer's read lock itself.
	openChunk(wr *journalWriter, h hash.Hash) (CompressedChunk, error)
	// openChunkAtRange returns chunk |h| from its record at |r|.
	openChunkAtRange(wr *journalWriter, r Range, h hash.Hash) (CompressedChunk, error)
	// chunkerAtRange returns chunk |h| from its record at |r|, as it is stored, for writers that copy it.
	chunkerAtRange(wr *journalWriter, r Range, h hash.Hash) (ToChunker, error)
	// chunker returns chunk |h| from |record|, which it may keep, as it is stored. It is called without the lock.
	chunker(h hash.Hash, record []byte) (ToChunker, error)
	// locate returns the range of chunk |h|, which the writer indexed at |r|, for a remote client to read.
	locate(ctx context.Context, wr *journalWriter, h hash.Hash, r Range) (Range, error)
	// writeChunk writes |cc| to the journal in place of a chunk record.
	writeChunk(ctx context.Context, wr *journalWriter, behavior dherrors.FatalBehavior, cc CompressedChunk) error
	// rootRecord returns the size of the record to commit |root| with, and what writes it.
	rootRecord(ctx context.Context, wr *journalWriter, behavior dherrors.FatalBehavior, root hash.Hash) (size int, write func(buf []byte) uint32, err error)
	// flushBatch is called before the writer flushes an index record ending at |end|, and returns |skip| to not.
	flushBatch(wr *journalWriter, root hash.Hash, end int64) (skip bool, err error)
}

// isChunkRecKind and isRootHashRecKind report whether journal records of a kind hold a chunk or a root hash. Builds
// whose journal codecs write other kinds extend them.
var isChunkRecKind = func(k journalRecKind) bool {
	return k == chunkJournalRecKind
}

var isRootHashRecKind = func(k journalRecKind) bool {
	return k == rootHashJournalRecKind
}

// maxRootHashRecordSize is the size of the largest root hash record a journal holds.
var maxRootHashRecordSize = rootHashRecordSize

// codedPayloadSize returns the uncompressed size of the payload of a chunk record a journal codec wrote.
var codedPayloadSize = func(r journalRec) uint64 {
	return 0
}

func (wr *journalWriter) checkIndexBatch(ctx context.Context, m lookupMeta, batch []lookup) error {
	if wr.codec == nil {
		return nil
	}
	return wr.codec.checkBatch(ctx, m, batch)
}

func (wr *journalWriter) chunkerAtRange(r Range, h hash.Hash) (ToChunker, error) {
	if wr.codec == nil {
		return wr.getCompressedChunkAtRange(r, h)
	}
	return wr.codec.chunkerAtRange(wr, r, h)
}

func readChunkerAt(codec journalCodec, f *os.File, r Range, h hash.Hash) (ToChunker, error) {
	if codec == nil {
		return readCompressedChunkAt(f, r, h)
	}
	buf := make([]byte, r.Length)
	if _, err := f.ReadAt(buf, int64(r.Offset)); err != nil {
		return nil, err
	}
	return codec.chunker(h, buf)
}
