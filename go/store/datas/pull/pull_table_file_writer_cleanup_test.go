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
	"crypto/rand"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/nbs"
)

func sinkFiles(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "buffered_file_byte_sink_*"))
	require.NoError(t, err)
	return matches
}

// blockingTableFileDestStore stalls every upload until the test's context is
// cancelled, so that finished table files pile up in the writer's newWriterCh
// buffer.
type blockingTableFileDestStore struct {
	uploading chan struct{}
}

func (s *blockingTableFileDestStore) WriteTableFile(ctx context.Context, id string, splitOffset uint64, numChunks int, contentHash []byte, getRd func() (io.ReadCloser, uint64, error)) (io.Closer, error) {
	select {
	case s.uploading <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, context.Cause(ctx)
}

func (s *blockingTableFileDestStore) AddTableFilesToManifest(context.Context, map[string]int, chunks.InsertAddrsCurry) error {
	return nil
}

// A pull or push that dies partway through --- a cancelled context, a client
// disconnect, a failed upload --- must not leave its staging files behind. On a
// long lived sql-server nothing ever comes back to collect them.
func TestPullTableFileWriterCleansUpTempFiles(t *testing.T) {
	addChunks := func(ctx context.Context, wr *PullTableFileWriter, n, size int) {
		for i := 0; i < n; i++ {
			bs := make([]byte, size)
			if _, err := rand.Read(bs); err != nil {
				return
			}
			if err := wr.AddToChunker(ctx, nbs.ChunkToCompressedChunk(chunks.NewChunk(bs))); err != nil {
				return
			}
		}
	}

	// Covers both the in-progress writer and the finished writers stranded in
	// the newWriterCh buffer once the upload threads stop reading from it.
	t.Run("CancelledContext", func(t *testing.T) {
		tempDir := t.TempDir()
		s := &blockingTableFileDestStore{uploading: make(chan struct{}, 1)}
		wr := NewPullTableFileWriter(PullTableFileWriterConfig{
			ConcurrentUploads:    1,
			TargetFileSize:       1 << 16,
			MaximumBufferedFiles: 4,
			TempDir:              tempDir,
			DestStore:            s,
		})

		ctx, cancel := context.WithCancel(context.Background())
		eg, egCtx := errgroup.WithContext(ctx)
		eg.Go(func() error {
			return wr.Run(egCtx)
		})

		var adders sync.WaitGroup
		adders.Add(1)
		go func() {
			defer adders.Done()
			addChunks(egCtx, wr, 512, 1<<14)
		}()

		// Wait until an upload is stuck and several table files have been
		// staged, so cancelling strands writers in the buffer.
		select {
		case <-s.uploading:
		case <-time.After(30 * time.Second):
			t.Error("timed out waiting for an upload to start")
		}
		require.Eventually(t, func() bool {
			return len(sinkFiles(t, tempDir)) >= 3
		}, 30*time.Second, 10*time.Millisecond, "expected table files to pile up")

		cancel()
		adders.Wait()
		assert.Error(t, eg.Wait())

		assert.Empty(t, sinkFiles(t, tempDir), "leaked table file temp files")
	})

	// The upload thread owns a writer once it takes it off newWriterCh, so its
	// error paths have to clean up too.
	t.Run("UploadError", func(t *testing.T) {
		tempDir := t.TempDir()
		var s errTableFileDestStore
		wr := NewPullTableFileWriter(PullTableFileWriterConfig{
			ConcurrentUploads:    1,
			TargetFileSize:       1 << 16,
			MaximumBufferedFiles: 4,
			TempDir:              tempDir,
			DestStore:            &s,
		})

		eg, ctx := errgroup.WithContext(context.Background())
		eg.Go(func() error {
			return wr.Run(ctx)
		})

		addChunks(ctx, wr, 64, 1<<14)
		wr.Close()
		assert.Error(t, eg.Wait())

		assert.Empty(t, sinkFiles(t, tempDir), "leaked table file temp files")
	})
}
