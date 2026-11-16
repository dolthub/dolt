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
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

// sinkTempDir points MovableTempFileProvider at a directory owned by the test
// so that leftover buffered_file_byte_sink_ files can be detected.
func sinkTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	old := tempfiles.MovableTempFileProvider
	tempfiles.MovableTempFileProvider = tempfiles.NewTempFileProviderAt(dir)
	t.Cleanup(func() {
		tempfiles.MovableTempFileProvider = old
	})
	return dir
}

func assertNoSinkFiles(t *testing.T, dir string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "buffered_file_byte_sink_*"))
	require.NoError(t, err)
	assert.Empty(t, matches, "leaked buffered file byte sink temp files")
}

// backingFileSink digs the temp-file-backed sink out from under however many
// layers of HashingByteSink a writer wrapped it in.
func backingFileSink(t *testing.T, s ByteSink) *BufferedFileByteSink {
	t.Helper()
	for {
		switch sink := s.(type) {
		case *BufferedFileByteSink:
			return sink
		case *HashingByteSink:
			s = sink.backingSink
		default:
			t.Fatalf("no BufferedFileByteSink behind %T", s)
			return nil
		}
	}
}

// TestBufferedFileByteSinkWritesExactBytes guards against the initial block
// being allocated with a non-zero length, which would prepend a block of zeros
// to sinks that are flushed without a full block of real data.
func TestBufferedFileByteSinkWritesExactBytes(t *testing.T) {
	sinkTempDir(t)

	t.Run("NoWrites", func(t *testing.T) {
		sink, err := NewBufferedFileByteSink("", 4096, 4)
		require.NoError(t, err)

		r, err := sink.Reader()
		require.NoError(t, err)
		defer r.Close()

		data, err := io.ReadAll(r)
		require.NoError(t, err)
		assert.Empty(t, data)
	})

	t.Run("ZeroLengthWrite", func(t *testing.T) {
		sink, err := NewBufferedFileByteSink("", 4096, 4)
		require.NoError(t, err)

		_, err = sink.Write(nil)
		require.NoError(t, err)
		_, err = sink.Write([]byte("hello"))
		require.NoError(t, err)

		r, err := sink.Reader()
		require.NoError(t, err)
		defer r.Close()

		data, err := io.ReadAll(r)
		require.NoError(t, err)
		assert.Equal(t, []byte("hello"), data)
		assert.Equal(t, uint64(5), sink.pos)
	})
}

// genericTableWriters returns one of each GenericTableWriter backed by a temp
// file, so the cleanup tests below cover both implementations.
func genericTableWriters(t *testing.T) map[string]func() GenericTableWriter {
	return map[string]func() GenericTableWriter{
		"CmpChunkTableWriter": func() GenericTableWriter {
			w, err := NewCmpChunkTableWriter("")
			require.NoError(t, err)
			return w
		},
		"ArchiveStreamWriter": func() GenericTableWriter {
			w, err := NewArchiveStreamWriter("")
			require.NoError(t, err)
			return w
		},
	}
}

func writerSink(t *testing.T, w GenericTableWriter) *BufferedFileByteSink {
	t.Helper()
	switch tw := w.(type) {
	case *CmpChunkTableWriter:
		return backingFileSink(t, tw.sink)
	case *ArchiveStreamWriter:
		return backingFileSink(t, tw.writer.output)
	default:
		t.Fatalf("unexpected writer type %T", w)
		return nil
	}
}

func TestTableWriterCancelRemovesTempFile(t *testing.T) {
	for name, newWriter := range genericTableWriters(t) {
		t.Run(name, func(t *testing.T) {
			t.Run("Clean", func(t *testing.T) {
				dir := sinkTempDir(t)
				w := newWriter()
				_, err := w.AddChunk(ChunkToCompressedChunk(chunks.NewChunk([]byte("some chunk data"))))
				require.NoError(t, err)

				require.NoError(t, w.Cancel())
				assertNoSinkFiles(t, dir)
			})

			// The sink remembers the first error its background writer saw and
			// returns it from every later call. Cancel must still delete the
			// temp file: a disk that is filling up is exactly when we cannot
			// afford to leak one.
			t.Run("AfterSinkError", func(t *testing.T) {
				dir := sinkTempDir(t)
				w := newWriter()
				_, err := w.AddChunk(ChunkToCompressedChunk(chunks.NewChunk([]byte("some chunk data"))))
				require.NoError(t, err)

				writerSink(t, w).ae.SetIfError(errors.New("simulated write failure"))

				err = w.Cancel()
				assert.Error(t, err, "Cancel should surface the sink error")
				assertNoSinkFiles(t, dir)
			})

			// Several call sites cancel defensively on paths that may already
			// have cancelled, so a second Cancel must not report a failure.
			t.Run("Twice", func(t *testing.T) {
				dir := sinkTempDir(t)
				w := newWriter()
				_, err := w.AddChunk(ChunkToCompressedChunk(chunks.NewChunk([]byte("some chunk data"))))
				require.NoError(t, err)

				require.NoError(t, w.Cancel())
				assert.NoError(t, w.Cancel())
				assertNoSinkFiles(t, dir)
			})

			// After FlushToFile the temp file has been renamed away. Cancelling
			// then is a no-op, not an error, so callers that unconditionally
			// clean up do not report a spurious failure --- gcCopier does
			// exactly this after a successful TryMoveCmpChunkTableWriter.
			t.Run("AfterFlushToFile", func(t *testing.T) {
				dir := sinkTempDir(t)
				w := newWriter()
				_, err := w.AddChunk(ChunkToCompressedChunk(chunks.NewChunk([]byte("some chunk data"))))
				require.NoError(t, err)

				_, name, err := w.Finish()
				require.NoError(t, err)

				dest := filepath.Join(t.TempDir(), name)
				require.NoError(t, w.FlushToFile(dest))

				_, err = os.Stat(dest)
				require.NoError(t, err)

				assert.NoError(t, w.Cancel())
				assert.NoError(t, w.Remove())
				assertNoSinkFiles(t, dir)
			})
		})
	}
}

// copyTablesToDir has to drop the writer's temp file even when Finish fails,
// because nothing else holds a reference to it once it returns --- for a
// rotating copier's child, the gcCopier it was handed is a throwaway copy.
func TestGCCopierCleansUpAfterFinishError(t *testing.T) {
	dir := sinkTempDir(t)
	ctx := context.Background()

	persister := newFSTablePersister(t.TempDir(), &UnlimitedQuotaProvider{}, false)
	gcc, err := newGarbageCollectionCopier(chunks.NoArchive, persister.(tableFilePersister))
	require.NoError(t, err)

	// Writing the same chunk twice makes Finish fail with
	// ErrDuplicateChunkWritten.
	c := ChunkToCompressedChunk(chunks.NewChunk([]byte("some chunk data")))
	_, err = gcc.writer.AddChunk(c)
	require.NoError(t, err)
	_, err = gcc.writer.AddChunk(c)
	require.NoError(t, err)

	_, _, err = gcc.copyTablesToDir(ctx)
	require.ErrorIs(t, err, ErrDuplicateChunkWritten)

	assertNoSinkFiles(t, dir)
}
