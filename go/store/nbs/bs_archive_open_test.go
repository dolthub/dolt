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

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
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

// rangeIgnoringBlobstore answers every Get with the whole blob.
type rangeIgnoringBlobstore struct {
	*blobstore.InMemoryBlobstore
}

func (b rangeIgnoringBlobstore) Get(ctx context.Context, key string, _ blobstore.BlobRange) (io.ReadCloser, uint64, string, error) {
	return b.InMemoryBlobstore.Get(ctx, key, blobstore.AllRange)
}

// peakQuotaProvider records the most quota held at once, and the size of each
// byte slice acquired.
type peakQuotaProvider struct {
	UnlimitedQuotaProvider
	mu         sync.Mutex
	peak       uint64
	byteSlices []int
}

func (q *peakQuotaProvider) notePeak() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.peak = max(q.peak, q.Usage())
}

func (q *peakQuotaProvider) AcquireQuotaByteSlice(ctx context.Context, sz int) ([]byte, error) {
	defer q.notePeak()
	q.mu.Lock()
	q.byteSlices = append(q.byteSlices, sz)
	q.mu.Unlock()
	return q.UnlimitedQuotaProvider.AcquireQuotaByteSlice(ctx, sz)
}

func (q *peakQuotaProvider) AcquireQuotaUint32Slice(ctx context.Context, sz int) ([]uint32, error) {
	defer q.notePeak()
	return q.UnlimitedQuotaProvider.AcquireQuotaUint32Slice(ctx, sz)
}

func (q *peakQuotaProvider) AcquireQuotaUint64Slice(ctx context.Context, sz int) ([]uint64, error) {
	defer q.notePeak()
	return q.UnlimitedQuotaProvider.AcquireQuotaUint64Slice(ctx, sz)
}

func (q *peakQuotaProvider) AcquireQuotaBytes(ctx context.Context, sz int) error {
	defer q.notePeak()
	return q.UnlimitedQuotaProvider.AcquireQuotaBytes(ctx, sz)
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

					q := &peakQuotaProvider{}
					cs, err := newBSArchiveChunkSource(ctx, bs, name, test.openChunkCount, q, openOpts{deepValidate: true}, &Stats{})
					require.NoError(t, err)
					defer func() {
						require.NoError(t, cs.close())
						assert.Zero(t, q.Usage(), "closing the archive releases all of its quota")
					}()
					assert.Equal(t, test.wantGets, bs.gets)
					tailLen := min(archiveTailEstimate(test.openChunkCount), uint64(len(archive)))
					assert.GreaterOrEqual(t, q.peak, q.Usage()+tailLen, "the tail read is held against the quota while the index decodes")

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
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("range ignored", func(t *testing.T) {
		big, bigName, _ := buildArchiveBytes(t, 8, 4096, nil)
		bigKey := bigName.String() + ArchiveFileSuffix
		require.Greater(t, uint64(len(big)), archiveTailEstimate(8))
		bs := rangeIgnoringBlobstore{blobstore.NewInMemoryBlobstore("")}
		_, err := bs.Put(ctx, bigKey, int64(len(big)), bytes.NewReader(big))
		require.NoError(t, err)
		q := NewUnlimitedMemQuotaProvider()
		_, _, err = openBSArchiveIndex(ctx, bs, bigKey, bigName, 8, q, openOpts{}, &Stats{})
		require.ErrorContains(t, err, "read more than")
		assert.Zero(t, q.Usage())
	})

	t.Run("missing archive", func(t *testing.T) {
		bs := blobstore.NewInMemoryBlobstore("")
		_, err := newBSArchiveChunkSource(ctx, bs, name, 8, NewUnlimitedMemQuotaProvider(), openOpts{}, &Stats{})
		require.True(t, blobstore.IsNotFoundError(err))
	})
}

func singleBlobPersisters() map[string]func(blobstore.Blobstore) tablePersister {
	return map[string]func(blobstore.Blobstore) tablePersister{
		"single": func(bs blobstore.Blobstore) tablePersister {
			return &singleBlobBSPersister{bs: bs, q: NewUnlimitedMemQuotaProvider(), blockSize: s3BlockSize}
		},
		"no-conjoin": func(bs blobstore.Blobstore) tablePersister {
			return &noConjoinBlobstorePersister{bs: bs, q: NewUnlimitedMemQuotaProvider(), blockSize: s3BlockSize}
		},
	}
}

func TestSingleBlobPersistersPersistArchivesWhichOpenInOneRead(t *testing.T) {
	data := [][]byte{[]byte("root value"), []byte("commit")}
	for name, newPersister := range singleBlobPersisters() {
		for _, spool := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/spool=%t", name, spool), func(t *testing.T) {
				ctx := context.Background()
				bs := &getCountingBlobstore{wholeBlobBlobstore: wholeBlobBlobstore{blobstore.NewInMemoryBlobstore(""), spool}}
				p := newPersister(bs)

				persisted, err := persistTableData(p, data...)
				require.NoError(t, err)
				defer persisted.close()
				require.Equal(t, ArchiveFileSuffix, persisted.suffix())
				exists, err := bs.Exists(ctx, persisted.hash().String()+ArchiveFileSuffix)
				require.NoError(t, err)
				assert.True(t, exists)
				exists, err = bs.Exists(ctx, persisted.hash().String())
				require.NoError(t, err)
				assert.False(t, exists)
				for _, c := range data {
					got, _, err := persisted.get(ctx, computeAddr(c), nil, &Stats{})
					require.NoError(t, err)
					assert.Equal(t, c, got)
				}

				bs.gets = 0
				opened, err := p.Open(ctx, persisted.hash(), persisted.count(), openOpts{}, &Stats{})
				require.NoError(t, err)
				defer opened.close()
				assert.Equal(t, 1, bs.gets)
				for _, c := range data {
					got, _, err := opened.get(ctx, computeAddr(c), nil, &Stats{})
					require.NoError(t, err)
					assert.Equal(t, c, got)
				}
			})
		}
	}
}

func TestSingleBlobPersistersOpenTableFiles(t *testing.T) {
	data := [][]byte{[]byte("written"), []byte("by an older client")}
	for name, newPersister := range singleBlobPersisters() {
		for _, spool := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/spool=%t", name, spool), func(t *testing.T) {
				ctx := context.Background()
				bs := &getCountingBlobstore{wholeBlobBlobstore: wholeBlobBlobstore{blobstore.NewInMemoryBlobstore(""), spool}}
				mt := newMemTable(testMemTableSize)
				for _, c := range data {
					mt.addChunk(computeAddr(c), c)
				}
				tableName, table, _, chunkCount, _, err := mt.write(nil, nil, &Stats{})
				require.NoError(t, err)
				_, err = bs.Put(ctx, tableName.String(), int64(len(table)), bytes.NewReader(table))
				require.NoError(t, err)

				opened, err := newPersister(bs).Open(ctx, tableName, chunkCount, openOpts{}, &Stats{})
				require.NoError(t, err)
				defer opened.close()
				assert.Equal(t, "", opened.suffix())
				assert.Equal(t, 2, bs.gets, "a table file costs a failed archive read first")
				for _, c := range data {
					got, _, err := opened.get(ctx, computeAddr(c), nil, &Stats{})
					require.NoError(t, err)
					assert.Equal(t, c, got)
				}
			})
		}
	}
}

func TestSingleBlobPersistersPersistNothingNew(t *testing.T) {
	data := [][]byte{[]byte("already"), []byte("there")}
	for name, newPersister := range singleBlobPersisters() {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			bs := blobstore.NewInMemoryBlobstore("")
			p := newPersister(bs)
			existing, err := persistTableData(p, data...)
			require.NoError(t, err)
			defer existing.close()

			mt := newMemTable(testMemTableSize)
			for _, c := range data {
				mt.addChunk(computeAddr(c), c)
			}
			cs, gcb, err := p.Persist(ctx, dherrors.FatalBehaviorError, mt, existing, nil, &Stats{})
			require.NoError(t, err)
			assert.Equal(t, gcBehavior_Continue, gcb)
			assert.Equal(t, uint32(0), cs.count())
		})
	}
}

// The raw index and metadata read off a remote are held against the quota until
// they are decoded.
func TestArchiveReadBuffersUseQuota(t *testing.T) {
	ctx := context.Background()
	metadata := []byte(`{"dolt_version":"test"}`)
	archive, name, hashes := buildArchiveBytes(t, 64, 16, metadata)
	reader := tableReaderAtFromBytes(archive)
	footer, err := loadFooter(ctx, reader, name, uint64(len(archive)), &Stats{})
	require.NoError(t, err)

	t.Run("index", func(t *testing.T) {
		q := &peakQuotaProvider{}
		idx, err := newInMemoryArchiveIndexReader(ctx, reader, footer, q, &Stats{})
		require.NoError(t, err)
		decoded := q.Usage()
		assert.GreaterOrEqual(t, q.peak, decoded+footer.indexSize)
		ar := archiveReader{indexReader: idx, footer: footer}
		for _, h := range hashes {
			assert.True(t, ar.has(h))
		}
		require.NoError(t, idx.Close())
		assert.Zero(t, q.Usage())
	})

	t.Run("buffered index is not copied", func(t *testing.T) {
		q := &peakQuotaProvider{}
		indexSpan := footer.totalIndexSpan()
		buffered := &archiveIndexReaderAt{buf: archive[indexSpan.offset : indexSpan.offset+indexSpan.length], off: int64(indexSpan.offset)}
		idx, err := newInMemoryArchiveIndexReader(ctx, buffered, footer, q, &Stats{})
		require.NoError(t, err)
		assert.Equal(t, q.Usage(), q.peak)
		require.NoError(t, idx.Close())
	})

	t.Run("metadata", func(t *testing.T) {
		q := &peakQuotaProvider{}
		md, err := newArchiveMetadata(ctx, reader, name, uint64(len(archive)), q, &Stats{})
		require.NoError(t, err)
		assert.Equal(t, 64, md.snappyChunkCount)
		assert.Contains(t, q.byteSlices, len(metadata))
		assert.Zero(t, q.Usage())
	})
}
