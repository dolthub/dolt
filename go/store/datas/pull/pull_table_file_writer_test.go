// Copyright 2024 Dolthub, Inc.
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
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/nbs"
)

func TestPullTableFileWriter(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		var s noopTableFileDestStore
		wr := NewPullTableFileWriter(context.Background(), PullTableFileWriterConfig{
			ConcurrentUploads:    1,
			ChunksPerFile:        8,
			MaximumBufferedFiles: 1,
			TempDir:              t.TempDir(),
			DestStore:            &s,
		})
		assert.NoError(t, wr.Close())
		assert.Equal(t, s.writeCalled.Load(), uint32(0))
		assert.Equal(t, s.addCalled, 0)
	})

	t.Run("AddSomeChunks", func(t *testing.T) {
		t.Run("FinishOnFullWriter", func(t *testing.T) {
			var s noopTableFileDestStore
			wr := NewPullTableFileWriter(context.Background(), PullTableFileWriterConfig{
				ConcurrentUploads:    1,
				ChunksPerFile:        8,
				MaximumBufferedFiles: 1,
				TempDir:              t.TempDir(),
				DestStore:            &s,
			})

			for i := 0; i < 32; i++ {
				bs := make([]byte, 1024)
				_, err := rand.Read(bs)
				assert.NoError(t, err)
				chk := chunks.NewChunk(bs)
				cChk := nbs.ChunkToCompressedChunk(chk)
				err = wr.AddCompressedChunk(context.Background(), cChk)
				assert.NoError(t, err)
			}

			assert.NoError(t, wr.Close())
			assert.Equal(t, s.writeCalled.Load(), uint32(4))
			assert.Equal(t, s.addCalled, 1)
			assert.Len(t, s.manifest, 4)
		})

		t.Run("FinishOnPartialFile", func(t *testing.T) {
			var s noopTableFileDestStore
			wr := NewPullTableFileWriter(context.Background(), PullTableFileWriterConfig{
				ConcurrentUploads:    1,
				ChunksPerFile:        1024,
				MaximumBufferedFiles: 1,
				TempDir:              t.TempDir(),
				DestStore:            &s,
			})

			for i := 0; i < 32; i++ {
				bs := make([]byte, 1024)
				_, err := rand.Read(bs)
				assert.NoError(t, err)
				chk := chunks.NewChunk(bs)
				cChk := nbs.ChunkToCompressedChunk(chk)
				err = wr.AddCompressedChunk(context.Background(), cChk)
				assert.NoError(t, err)
			}

			assert.NoError(t, wr.Close())
			assert.Equal(t, s.writeCalled.Load(), uint32(1))
			assert.Equal(t, s.addCalled, 1)
			assert.Len(t, s.manifest, 1)
		})
	})

	t.Run("ConcurrentUpload", func(t *testing.T) {
		var s noopTableFileDestStore
		s.writeDelay = 50 * time.Millisecond
		wr := NewPullTableFileWriter(context.Background(), PullTableFileWriterConfig{
			ConcurrentUploads:    32,
			ChunksPerFile:        8,
			MaximumBufferedFiles: 1,
			TempDir:              t.TempDir(),
			DestStore:            &s,
		})

		start := time.Now()

		for i := 0; i < 8*32; i++ {
			bs := make([]byte, 1024)
			_, err := rand.Read(bs)
			assert.NoError(t, err)
			chk := chunks.NewChunk(bs)
			cChk := nbs.ChunkToCompressedChunk(chk)
			err = wr.AddCompressedChunk(context.Background(), cChk)
			assert.NoError(t, err)
		}

		assert.NoError(t, wr.Close())
		assert.Equal(t, s.writeCalled.Load(), uint32(32))
		assert.Equal(t, s.addCalled, 1)
		assert.Len(t, s.manifest, 32)
		assert.True(t, time.Since(start) < time.Second)
	})

	t.Run("ErrorOnUpload", func(t *testing.T) {
		t.Run("ErrAtClose", func(t *testing.T) {
			var s errTableFileDestStore
			wr := NewPullTableFileWriter(context.Background(), PullTableFileWriterConfig{
				ConcurrentUploads:    1,
				ChunksPerFile:        8,
				MaximumBufferedFiles: 0,
				TempDir:              t.TempDir(),
				DestStore:            &s,
			})

			for i := 0; i < 8; i++ {
				bs := make([]byte, 1024)
				_, err := rand.Read(bs)
				assert.NoError(t, err)
				chk := chunks.NewChunk(bs)
				cChk := nbs.ChunkToCompressedChunk(chk)
				err = wr.AddCompressedChunk(context.Background(), cChk)
				assert.NoError(t, err)
			}

			assert.EqualError(t, wr.Close(), "this dest store throws an error")
			assert.Equal(t, s.addCalled, 0)
		})

		t.Run("ErrAtAdd", func(t *testing.T) {
			var s errTableFileDestStore
			wr := NewPullTableFileWriter(context.Background(), PullTableFileWriterConfig{
				ConcurrentUploads:    1,
				ChunksPerFile:        8,
				MaximumBufferedFiles: 0,
				TempDir:              t.TempDir(),
				DestStore:            &s,
			})

			for i := 0; i < 8; i++ {
				bs := make([]byte, 1024)
				_, err := rand.Read(bs)
				assert.NoError(t, err)
				chk := chunks.NewChunk(bs)
				cChk := nbs.ChunkToCompressedChunk(chk)
				err = wr.AddCompressedChunk(context.Background(), cChk)
				assert.NoError(t, err)
			}

			// We should eventually see the upload error from AddCompressedChunk
			for i := 0; i < 1024; i++ {
				bs := make([]byte, 1024)
				_, err := rand.Read(bs)
				assert.NoError(t, err)
				chk := chunks.NewChunk(bs)
				cChk := nbs.ChunkToCompressedChunk(chk)
				err = wr.AddCompressedChunk(context.Background(), cChk)
				if err != nil {
					assert.EqualError(t, err, "this dest store throws an error")
					assert.EqualError(t, wr.Close(), "this dest store throws an error")
					assert.Equal(t, s.addCalled, 0)
					return
				}
			}

			t.Errorf("Did not see an error from AddCompressedChunk after concurrent upload failed")
		})
	})

	t.Run("ErrorOnAdd", func(t *testing.T) {
		var s errTableFileDestStore
		s.onAdd = true
		wr := NewPullTableFileWriter(context.Background(), PullTableFileWriterConfig{
			ConcurrentUploads:    1,
			ChunksPerFile:        8,
			MaximumBufferedFiles: 0,
			TempDir:              t.TempDir(),
			DestStore:            &s,
		})

		for i := 0; i < 8; i++ {
			bs := make([]byte, 1024)
			_, err := rand.Read(bs)
			assert.NoError(t, err)
			chk := chunks.NewChunk(bs)
			cChk := nbs.ChunkToCompressedChunk(chk)
			err = wr.AddCompressedChunk(context.Background(), cChk)
			assert.NoError(t, err)
		}

		assert.EqualError(t, wr.Close(), "this dest store throws an error")
		assert.Equal(t, s.addCalled, 1)
	})

	t.Run("SimpleStats", func(t *testing.T) {
		s := testDataTableFileDestStore{
			atWriteTableFile:   make(chan struct{}),
			doWriteTableFile:   make(chan struct{}),
			doneWriteTableFile: make(chan struct{}),
		}
		wr := NewPullTableFileWriter(context.Background(), PullTableFileWriterConfig{
			ConcurrentUploads:    1,
			ChunksPerFile:        8,
			MaximumBufferedFiles: 0,
			TempDir:              t.TempDir(),
			DestStore:            &s,
		})

		for i := 0; i < 8; i++ {
			bs := make([]byte, 1024)
			_, err := rand.Read(bs)
			assert.NoError(t, err)
			chk := chunks.NewChunk(bs)
			cChk := nbs.ChunkToCompressedChunk(chk)
			err = wr.AddCompressedChunk(context.Background(), cChk)
			assert.NoError(t, err)
		}

		<-s.atWriteTableFile

		wrStats := wr.GetStats()
		assert.Equal(t, wrStats.FinishedSendBytes, uint64(0))
		assert.Greater(t, wrStats.BufferedSendBytes, uint64(8*1024))

		close(s.doWriteTableFile)
		<-s.doneWriteTableFile

		wrStats = wr.GetStats()
		assert.Greater(t, wrStats.FinishedSendBytes, uint64(8*1024))
		assert.Equal(t, wrStats.FinishedSendBytes, wrStats.BufferedSendBytes)

		assert.NoError(t, wr.Close())
	})

	t.Run("UploadsAreParallel", func(t *testing.T) {
		s := testDataTableFileDestStore{
			atWriteTableFile:   make(chan struct{}),
			doWriteTableFile:   make(chan struct{}),
			doneWriteTableFile: make(chan struct{}),
		}
		wr := NewPullTableFileWriter(context.Background(), PullTableFileWriterConfig{
			ConcurrentUploads:    4,
			ChunksPerFile:        8,
			MaximumBufferedFiles: 0,
			TempDir:              t.TempDir(),
			DestStore:            &s,
		})

		for i := 0; i < 32; i++ {
			bs := make([]byte, 1024)
			_, err := rand.Read(bs)
			assert.NoError(t, err)
			chk := chunks.NewChunk(bs)
			cChk := nbs.ChunkToCompressedChunk(chk)
			err = wr.AddCompressedChunk(context.Background(), cChk)
			assert.NoError(t, err)
		}

		for i := 0; i < 4; i++ {
			<-s.atWriteTableFile
		}

		close(s.doWriteTableFile)

		for i := 0; i < 4; i++ {
			<-s.doneWriteTableFile
		}

		assert.NoError(t, wr.Close())
	})
}

type noopTableFileDestStore struct {
	writeDelay  time.Duration
	writeCalled atomic.Uint32
	addCalled   int
	manifest    map[string]int
}

func (s *noopTableFileDestStore) WriteTableFile(ctx context.Context, id string, numChunks int, contentHash []byte, getRd func() (io.ReadCloser, uint64, error)) error {
	if s.writeDelay > 0 {
		time.Sleep(s.writeDelay)
	}
	s.writeCalled.Add(1)
	rd, _, _ := getRd()
	if rd != nil {
		rd.Close()
	}
	return nil
}

func (s *noopTableFileDestStore) AddTableFilesToManifest(ctx context.Context, fileIdToNumChunks map[string]int, _ chunks.GetAddrsCurry) error {
	s.addCalled += 1
	s.manifest = fileIdToNumChunks
	return nil
}

type testDataTableFileDestStore struct {
	atWriteTableFile   chan struct{}
	doWriteTableFile   chan struct{}
	doneWriteTableFile chan struct{}
}

func (s *testDataTableFileDestStore) WriteTableFile(ctx context.Context, id string, numChunks int, contentHash []byte, getRd func() (io.ReadCloser, uint64, error)) error {
	s.atWriteTableFile <- struct{}{}
	<-s.doWriteTableFile
	defer func() {
		s.doneWriteTableFile <- struct{}{}
	}()
	rd, _, err := getRd()
	if err != nil {
		return err
	}
	defer rd.Close()
	_, err = io.ReadAll(rd)
	if err != nil {
		return err
	}
	return nil
}

func (s *testDataTableFileDestStore) AddTableFilesToManifest(context.Context, map[string]int, chunks.GetAddrsCurry) error {
	return nil
}

type errTableFileDestStore struct {
	onAdd     bool
	addCalled int
}

func (s *errTableFileDestStore) WriteTableFile(ctx context.Context, id string, numChunks int, contentHash []byte, getRd func() (io.ReadCloser, uint64, error)) error {
	rd, _, _ := getRd()
	if rd != nil {
		rd.Close()
	}
	if s.onAdd {
		return nil
	}
	return errors.New("this dest store throws an error")
}

func (s *errTableFileDestStore) AddTableFilesToManifest(ctx context.Context, fileIdToNumChunks map[string]int, _ chunks.GetAddrsCurry) error {
	s.addCalled += 1
	if s.onAdd {
		return errors.New("this dest store throws an error")
	}
	return nil
}
