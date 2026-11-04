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

package pull

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/nbs"
)

// tableFileFormat is the format of the table files that clone and pull write chunks to.
type tableFileFormat interface {
	// newWriter returns a writer of a new table file in |tmpDir|, an archive if |archive| is set.
	newWriter(tmpDir string, archive bool) (nbs.GenericTableWriter, error)
	// visitJournal calls |cb| with each chunk of the chunk journal that |r| reads from offset |off|.
	visitJournal(ctx context.Context, r io.Reader, off int64, cb func(nbs.ToChunker) error) error
	// fetched is called with each chunk that pull fetches from its source, before it is written.
	fetched(c nbs.ToChunker) error
}

// newTableFileFormat returns the format of the table files that copy chunks from |src| to |sink|, or an error if
// they cannot be copied. Dolt Enterprise replaces it.
var newTableFileFormat = func(src, sink chunks.ChunkStore) (tableFileFormat, error) {
	return plainTableFiles{}, nil
}

type plainTableFiles struct{}

func (plainTableFiles) newWriter(tmpDir string, archive bool) (nbs.GenericTableWriter, error) {
	if archive {
		return nbs.NewArchiveStreamWriter(tmpDir)
	}
	return nbs.NewCmpChunkTableWriter(tmpDir)
}

func (plainTableFiles) visitJournal(ctx context.Context, r io.Reader, off int64, cb func(nbs.ToChunker) error) error {
	return nbs.VisitJournalReaderChunks(ctx, r, off, func(c nbs.CompressedChunk) error {
		return cb(c)
	})
}

func (plainTableFiles) fetched(nbs.ToChunker) error {
	return nil
}

func (cfg *PullTableFileWriterConfig) tableFiles() tableFileFormat {
	if cfg.format == nil {
		return plainTableFiles{}
	}
	return cfg.format
}

// tableFileFormatFor is newTableFileFormat, which returns no format for a plain copy.
func tableFileFormatFor(src, sink chunks.ChunkStore) (tableFileFormat, error) {
	f, err := newTableFileFormat(src, sink)
	if f == nil && err == nil {
		f = plainTableFiles{}
	}
	return f, err
}
