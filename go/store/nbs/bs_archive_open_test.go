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
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/hash"
)

// getCountingBlobstore counts the Gets made of it.
type getCountingBlobstore struct {
	wholeBlobBlobstore
	mu   sync.Mutex
	gets int
}

func (b *getCountingBlobstore) Get(ctx context.Context, key string, br blobstore.BlobRange) (io.ReadCloser, uint64, string, error) {
	b.mu.Lock()
	b.gets++
	b.mu.Unlock()
	return b.wholeBlobBlobstore.Get(ctx, key, br)
}

// shortReadBlobstore returns one byte less than asked of every Get.
type shortReadBlobstore struct {
	*blobstore.InMemoryBlobstore
}

func (b shortReadBlobstore) Get(ctx context.Context, key string, br blobstore.BlobRange) (io.ReadCloser, uint64, string, error) {
	rc, sz, ver, err := b.InMemoryBlobstore.Get(ctx, key, br)
	if err != nil {
		return nil, 0, "", err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, 0, "", err
	}
	return io.NopCloser(bytes.NewReader(data[:len(data)-1])), sz, ver, nil
}

// buildArchiveBytes writes an archive of |chunkCount| snappy chunks, each with
// |chunkSize| bytes of data, followed by |metadata|.
func buildArchiveBytes(t *testing.T, chunkCount, chunkSize int, metadata []byte) ([]byte, hash.Hash, []hash.Hash) {
	writer := NewFixedBufferByteSink(make([]byte, chunkCount*(chunkSize+64)+len(metadata)+1<<16))
	aw := newArchiveWriterWithSink(writer)
	hashes := make([]hash.Hash, chunkCount)
	for i := range hashes {
		_, err := io.ReadFull(rand.Reader, hashes[i][:])
		require.NoError(t, err)
		data := make([]byte, chunkSize)
		_, err = io.ReadFull(rand.Reader, data)
		require.NoError(t, err)
		spanID, err := aw.writeByteSpan(data)
		require.NoError(t, err)
		require.NoError(t, aw.stageSnappyChunk(hashes[i], spanID))
	}
	require.NoError(t, aw.finalizeByteSpans())
	require.NoError(t, aw.writeIndex())
	require.NoError(t, aw.writeMetadata(metadata))
	require.NoError(t, aw.writeFooter())
	name, err := aw.getName()
	require.NoError(t, err)
	return writer.buff[:writer.pos], name, hashes
}

func TestOpenBSArchiveReadsTailOnce(t *testing.T) {
	const chunkCount = 64
	chunkCountIndexSize := int(archiveIndexSize(chunkCount, chunkCount))
	tests := []struct {
		name string
		// chunkCount passed to the open, which can disagree with the archive's
		openChunkCount uint32
		chunkSize      int
		metadataSize   int
		wantGets       int
	}{
		{name: "index within the estimate", openChunkCount: chunkCount, chunkSize: 1024, wantGets: 1},
		{name: "archive smaller than the estimate", openChunkCount: chunkCount, chunkSize: 1, wantGets: 1},
		{name: "metadata at the allowance", openChunkCount: chunkCount, chunkSize: 1024, metadataSize: archiveMetadataAllowance, wantGets: 1},
		{name: "metadata pushes part of the index out", openChunkCount: chunkCount, chunkSize: 1024, metadataSize: archiveMetadataAllowance + 2*chunkCount*uint64Size + chunkCountIndexSize/2, wantGets: 2},
		{name: "metadata pushes all of the index out", openChunkCount: chunkCount, chunkSize: 1024, metadataSize: 4 * chunkCountIndexSize, wantGets: 2},
		{name: "chunk count understated", openChunkCount: 0, chunkSize: 1024, wantGets: 2},
		{name: "chunk count overstated", openChunkCount: 100 * chunkCount, chunkSize: 1024, wantGets: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metadata := bytes.Repeat([]byte("m"), test.metadataSize)
			archive, name, hashes := buildArchiveBytes(t, chunkCount, test.chunkSize, metadata)
			for _, spool := range []bool{false, true} {
				t.Run(fmt.Sprintf("spool=%t", spool), func(t *testing.T) {
					ctx := context.Background()
					bs := &getCountingBlobstore{wholeBlobBlobstore: wholeBlobBlobstore{blobstore.NewInMemoryBlobstore(""), spool}}
					_, err := bs.Put(ctx, name.String()+ArchiveFileSuffix, int64(len(archive)), bytes.NewReader(archive))
					require.NoError(t, err)

					cs, err := newBSArchiveChunkSource(ctx, bs, name, test.openChunkCount, NewUnlimitedMemQuotaProvider(), openOpts{deepValidate: true}, &Stats{})
					require.NoError(t, err)
					defer cs.close()
					assert.Equal(t, test.wantGets, bs.gets)

					assert.Equal(t, uint32(chunkCount), cs.count())
					for _, h := range hashes {
						has, _, err := cs.has(h, nil)
						require.NoError(t, err)
						assert.True(t, has)
					}
				})
			}
		})
	}
}

func TestOpenBSArchiveErrors(t *testing.T) {
	ctx := context.Background()
	archive, name, _ := buildArchiveBytes(t, 8, 16, nil)
	key := name.String() + ArchiveFileSuffix

	t.Run("blob too small for a footer", func(t *testing.T) {
		bs := blobstore.NewInMemoryBlobstore("")
		_, err := bs.Put(ctx, key, 10, bytes.NewReader(archive[len(archive)-10:]))
		require.NoError(t, err)
		_, err = newBSArchiveChunkSource(ctx, bs, name, 8, NewUnlimitedMemQuotaProvider(), openOpts{}, &Stats{})
		require.ErrorIs(t, err, ErrCorruptArchiveIndex)
	})

	t.Run("short read", func(t *testing.T) {
		bs := shortReadBlobstore{blobstore.NewInMemoryBlobstore("")}
		_, err := bs.Put(ctx, key, int64(len(archive)), bytes.NewReader(archive))
		require.NoError(t, err)
		_, err = newBSArchiveChunkSource(ctx, bs, name, 8, NewUnlimitedMemQuotaProvider(), openOpts{}, &Stats{})
		require.ErrorContains(t, err, "expected")
	})

	t.Run("missing archive", func(t *testing.T) {
		bs := blobstore.NewInMemoryBlobstore("")
		_, err := newBSArchiveChunkSource(ctx, bs, name, 8, NewUnlimitedMemQuotaProvider(), openOpts{}, &Stats{})
		require.True(t, blobstore.IsNotFoundError(err))
	})
}
