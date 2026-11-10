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

// newJournalCopier returns what copies the journal of |src| into an archive for |sink| when cloning.
var newJournalCopier = func(ctx context.Context, src, sink chunks.ChunkStore) (journalCopier, error) {
	return archiveJournalCopier{}, nil
}

type journalCopier interface {
	// newWriter returns the writer of the archive.
	newWriter(ctx context.Context, tmpDir string) (*nbs.ArchiveStreamWriter, error)
	// visit passes each chunk of the journal records in |r|, which start at offset |off|, to |cb|.
	visit(ctx context.Context, r io.Reader, off int64, cb func(nbs.ToChunker) error) error
}

type archiveJournalCopier struct{}

func (archiveJournalCopier) newWriter(_ context.Context, tmpDir string) (*nbs.ArchiveStreamWriter, error) {
	return nbs.NewArchiveStreamWriter(tmpDir)
}

func (archiveJournalCopier) visit(ctx context.Context, r io.Reader, off int64, cb func(nbs.ToChunker) error) error {
	return nbs.VisitJournalReaderChunks(ctx, r, off, func(cc nbs.CompressedChunk) error {
		return cb(cc)
	})
}
