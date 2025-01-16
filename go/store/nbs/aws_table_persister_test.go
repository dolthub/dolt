// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	crand "crypto/rand"
	"io"
	"math/rand"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

func randomChunks(t *testing.T, r *rand.Rand, sz int) [][]byte {
	buf := make([]byte, sz)
	_, err := io.ReadFull(crand.Reader, buf)
	require.NoError(t, err)

	var ret [][]byte
	var i int
	for i < len(buf) {
		j := int(r.NormFloat64()*1024 + 4096)
		if i+j >= len(buf) {
			ret = append(ret, buf[i:])
		} else {
			ret = append(ret, buf[i:i+j])
		}
		i += j
	}

	return ret
}

func TestRandomChunks(t *testing.T) {
	r := rand.New(rand.NewSource(1024))
	res := randomChunks(t, r, 10)
	assert.Len(t, res, 1)
	res = randomChunks(t, r, 4096+2048)
	assert.Len(t, res, 2)
	res = randomChunks(t, r, 4096+4096)
	assert.Len(t, res, 3)
}

func TestAWSTablePersisterPersist(t *testing.T) {
	ctx := context.Background()

	r := rand.New(rand.NewSource(1024))
	const sz15mb = 1 << 20 * 15
	mt := newMemTable(sz15mb)
	testChunks := randomChunks(t, r, 1<<20*12)
	for _, c := range testChunks {
		assert.Equal(t, mt.addChunk(computeAddr(c), c), chunkAdded)
	}

	var limits5mb = awsLimits{partTarget: 1 << 20 * 5}
	var limits64mb = awsLimits{partTarget: 1 << 20 * 64}

	t.Run("PersistToS3", func(t *testing.T) {
		testIt := func(t *testing.T, ns string) {
			t.Run("InMultipleParts", func(t *testing.T) {
				assert := assert.New(t)
				s3svc := makeFakeS3(t)
				s3p := awsTablePersister{s3: s3svc, bucket: "bucket", limits: limits5mb, ns: ns, q: &UnlimitedQuotaProvider{}}

				src, _, err := s3p.Persist(context.Background(), mt, nil, nil, &Stats{})
				require.NoError(t, err)
				defer src.close()

				if assert.True(mustUint32(src.count()) > 0) {
					if r, err := s3svc.readerForTableWithNamespace(ctx, ns, src.hash()); assert.NotNil(r) && assert.NoError(err) {
						assertChunksInReader(testChunks, r, assert)
						r.close()
					}
				}
			})

			t.Run("InSinglePart", func(t *testing.T) {
				assert := assert.New(t)

				s3svc := makeFakeS3(t)
				s3p := awsTablePersister{s3: s3svc, bucket: "bucket", limits: limits64mb, ns: ns, q: &UnlimitedQuotaProvider{}}

				src, _, err := s3p.Persist(context.Background(), mt, nil, nil, &Stats{})
				require.NoError(t, err)
				defer src.close()
				if assert.True(mustUint32(src.count()) > 0) {
					if r, err := s3svc.readerForTableWithNamespace(ctx, ns, src.hash()); assert.NotNil(r) && assert.NoError(err) {
						assertChunksInReader(testChunks, r, assert)
						r.close()
					}
				}
			})

			t.Run("NoNewChunks", func(t *testing.T) {
				assert := assert.New(t)

				mt := newMemTable(sz15mb)
				existingTable := newMemTable(sz15mb)

				for _, c := range testChunks {
					assert.Equal(mt.addChunk(computeAddr(c), c), chunkAdded)
					assert.Equal(existingTable.addChunk(computeAddr(c), c), chunkAdded)
				}

				s3svc := makeFakeS3(t)
				s3p := awsTablePersister{s3: s3svc, bucket: "bucket", limits: limits5mb, ns: ns, q: &UnlimitedQuotaProvider{}}

				src, _, err := s3p.Persist(context.Background(), mt, existingTable, nil, &Stats{})
				require.NoError(t, err)
				defer src.close()
				assert.True(mustUint32(src.count()) == 0)

				_, present := s3svc.data[src.hash().String()]
				assert.False(present)
			})

			t.Run("Abort", func(t *testing.T) {
				assert := assert.New(t)

				s3svc := &failingFakeS3{makeFakeS3(t), sync.Mutex{}, 1}
				s3p := awsTablePersister{s3: s3svc, bucket: "bucket", limits: limits5mb, ns: ns, q: &UnlimitedQuotaProvider{}}

				_, _, err := s3p.Persist(context.Background(), mt, nil, nil, &Stats{})
				assert.Error(err)
			})
		}
		t.Run("WithoutNamespace", func(t *testing.T) {
			testIt(t, "")
		})
		t.Run("WithNamespace", func(t *testing.T) {
			testIt(t, "a-namespace-here")
		})
	})
}

type waitOnStoreTableCache struct {
	readers map[hash.Hash]io.ReaderAt
	mu      sync.RWMutex
	storeWG sync.WaitGroup
}

func (mtc *waitOnStoreTableCache) checkout(h hash.Hash) (io.ReaderAt, error) {
	mtc.mu.RLock()
	defer mtc.mu.RUnlock()
	return mtc.readers[h], nil
}

func (mtc *waitOnStoreTableCache) checkin(h hash.Hash) error {
	return nil
}

func (mtc *waitOnStoreTableCache) store(h hash.Hash, data io.Reader, size uint64) error {
	defer mtc.storeWG.Done()
	mtc.mu.Lock()
	defer mtc.mu.Unlock()
	mtc.readers[h] = data.(io.ReaderAt)
	return nil
}

type failingFakeS3 struct {
	*fakeS3
	mu           sync.Mutex
	numSuccesses int
}

func (m *failingFakeS3) UploadPartWithContext(ctx aws.Context, input *s3.UploadPartInput, opts ...request.Option) (*s3.UploadPartOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.numSuccesses > 0 {
		m.numSuccesses--
		return m.fakeS3.UploadPartWithContext(ctx, input)
	}
	return nil, mockAWSError("MalformedXML")
}

func TestAWSTablePersisterDividePlan(t *testing.T) {
	assert := assert.New(t)
	minPartSize, maxPartSize := uint64(16), uint64(32)
	tooSmall := bytesToChunkSource(t, []byte("a"))
	justRight := bytesToChunkSource(t, []byte("123456789"), []byte("abcdefghi"))
	bigUns := [][]byte{make([]byte, maxPartSize-1), make([]byte, maxPartSize-1)}
	for _, b := range bigUns {
		rand.Read(b)
	}
	tooBig := bytesToChunkSource(t, bigUns...)

	sources := chunkSources{justRight, tooBig, tooSmall}
	defer func() {
		for _, s := range sources {
			s.close()
		}
	}()
	plan, err := planRangeCopyConjoin(sources, &Stats{})
	require.NoError(t, err)
	copies, manuals, _, err := dividePlan(context.Background(), plan, minPartSize, maxPartSize)
	require.NoError(t, err)

	perTableDataSize := map[string]int64{}
	for _, c := range copies {
		assert.True(minPartSize <= uint64(c.srcLen))
		assert.True(uint64(c.srcLen) <= maxPartSize)
		totalSize := perTableDataSize[c.name]
		totalSize += c.srcLen
		perTableDataSize[c.name] = totalSize
	}
	assert.Len(perTableDataSize, 2)
	assert.Contains(perTableDataSize, justRight.hash().String())
	assert.Contains(perTableDataSize, tooBig.hash().String())
	ti, err := justRight.index()
	require.NoError(t, err)
	assert.EqualValues(calcChunkRangeSize(ti), perTableDataSize[justRight.hash().String()])
	ti, err = tooBig.index()
	require.NoError(t, err)
	assert.EqualValues(calcChunkRangeSize(ti), perTableDataSize[tooBig.hash().String()])

	assert.Len(manuals, 1)
	ti, err = tooSmall.index()
	require.NoError(t, err)
	assert.EqualValues(calcChunkRangeSize(ti), manuals[0].end-manuals[0].start)
}

func TestAWSTablePersisterCalcPartSizes(t *testing.T) {
	assert := assert.New(t)
	min, max := uint64(8*1<<10), uint64(1+(16*1<<10))

	testPartSizes := func(dataLen uint64) {
		lengths := splitOnMaxSize(dataLen, max)
		var sum int64
		for _, l := range lengths {
			assert.True(uint64(l) >= min)
			assert.True(uint64(l) <= max)
			sum += l
		}
		assert.EqualValues(dataLen, sum)
	}

	testPartSizes(1 << 20)
	testPartSizes(max + 1)
	testPartSizes(10*max - 1)
	testPartSizes(max + max/2)
}

func TestAWSTablePersisterConjoinAll(t *testing.T) {
	ctx := context.Background()
	const sz5mb = 1 << 20 * 5
	targetPartSize := uint64(sz5mb)
	minPartSize, maxPartSize := targetPartSize, 5*targetPartSize

	rl := make(chan struct{}, 8)
	defer close(rl)

	newPersister := func(s3svc s3iface.S3API) awsTablePersister {
		return awsTablePersister{
			s3svc,
			"bucket",
			rl,
			awsLimits{targetPartSize, minPartSize, maxPartSize},
			"",
			&UnlimitedQuotaProvider{},
		}
	}

	var smallChunks [][]byte
	rnd := rand.New(rand.NewSource(0))
	for smallChunkTotal := uint64(0); smallChunkTotal <= uint64(minPartSize); {
		small := make([]byte, minPartSize/5)
		rnd.Read(small)
		src := bytesToChunkSource(t, small)
		smallChunks = append(smallChunks, small)
		ti, err := src.index()
		require.NoError(t, err)
		smallChunkTotal += calcChunkRangeSize(ti)
		ti.Close()
	}

	t.Run("Small", func(t *testing.T) {
		makeSources := func(s3p awsTablePersister, chunks [][]byte) (sources chunkSources) {
			for i := 0; i < len(chunks); i++ {
				mt := newMemTable(uint64(2 * targetPartSize))
				mt.addChunk(computeAddr(chunks[i]), chunks[i])
				cs, _, err := s3p.Persist(context.Background(), mt, nil, nil, &Stats{})
				require.NoError(t, err)
				sources = append(sources, cs)
			}
			return
		}

		t.Run("TotalUnderMinSize", func(t *testing.T) {
			assert := assert.New(t)
			s3svc := makeFakeS3(t)
			s3p := newPersister(s3svc)

			chunks := smallChunks[:len(smallChunks)-1]
			sources := makeSources(s3p, chunks)
			src, _, err := s3p.ConjoinAll(context.Background(), sources, &Stats{})
			require.NoError(t, err)
			defer src.close()
			for _, s := range sources {
				s.close()
			}

			if assert.True(mustUint32(src.count()) > 0) {
				if r, err := s3svc.readerForTable(ctx, src.hash()); assert.NotNil(r) && assert.NoError(err) {
					assertChunksInReader(chunks, r, assert)
					r.close()
				}
			}
		})

		t.Run("TotalOverMinSize", func(t *testing.T) {
			assert := assert.New(t)
			s3svc := makeFakeS3(t)
			s3p := newPersister(s3svc)

			sources := makeSources(s3p, smallChunks)
			src, _, err := s3p.ConjoinAll(context.Background(), sources, &Stats{})
			require.NoError(t, err)
			defer src.close()
			for _, s := range sources {
				s.close()
			}

			if assert.True(mustUint32(src.count()) > 0) {
				if r, err := s3svc.readerForTable(ctx, src.hash()); assert.NotNil(r) && assert.NoError(err) {
					assertChunksInReader(smallChunks, r, assert)
					r.close()
				}
			}
		})
	})

	bigUns1 := [][]byte{make([]byte, maxPartSize-1), make([]byte, maxPartSize-1)}
	bigUns2 := [][]byte{make([]byte, maxPartSize-1), make([]byte, maxPartSize-1)}
	for _, bu := range [][][]byte{bigUns1, bigUns2} {
		for _, b := range bu {
			rand.Read(b)
		}
	}

	t.Run("AllOverMax", func(t *testing.T) {
		assert := assert.New(t)
		s3svc := makeFakeS3(t)
		s3p := newPersister(s3svc)

		// Make 2 chunk sources that each have >maxPartSize chunk data
		sources := make(chunkSources, 2)
		for i, bu := range [][][]byte{bigUns1, bigUns2} {
			mt := newMemTable(uint64(2 * maxPartSize))
			for _, b := range bu {
				mt.addChunk(computeAddr(b), b)
			}

			var err error
			sources[i], _, err = s3p.Persist(context.Background(), mt, nil, nil, &Stats{})
			require.NoError(t, err)
		}
		src, _, err := s3p.ConjoinAll(context.Background(), sources, &Stats{})
		require.NoError(t, err)
		defer src.close()
		for _, s := range sources {
			s.close()
		}

		if assert.True(mustUint32(src.count()) > 0) {
			if r, err := s3svc.readerForTable(ctx, src.hash()); assert.NotNil(r) && assert.NoError(err) {
				assertChunksInReader(bigUns1, r, assert)
				assertChunksInReader(bigUns2, r, assert)
				r.close()
			}
		}
	})

	t.Run("SomeOverMax", func(t *testing.T) {
		assert := assert.New(t)
		s3svc := makeFakeS3(t)
		s3p := newPersister(s3svc)

		// Add one chunk source that has >maxPartSize data
		mtb := newMemTable(uint64(2 * maxPartSize))
		for _, b := range bigUns1 {
			mtb.addChunk(computeAddr(b), b)
		}

		// Follow up with a chunk source where minPartSize < data size < maxPartSize
		medChunks := make([][]byte, 2)
		mt := newMemTable(uint64(2 * maxPartSize))
		for i := range medChunks {
			medChunks[i] = make([]byte, minPartSize+1)
			rand.Read(medChunks[i])
			mt.addChunk(computeAddr(medChunks[i]), medChunks[i])
		}
		cs1, _, err := s3p.Persist(context.Background(), mt, nil, nil, &Stats{})
		require.NoError(t, err)
		cs2, _, err := s3p.Persist(context.Background(), mtb, nil, nil, &Stats{})
		require.NoError(t, err)
		sources := chunkSources{cs1, cs2}

		src, _, err := s3p.ConjoinAll(context.Background(), sources, &Stats{})
		require.NoError(t, err)
		defer src.close()
		for _, s := range sources {
			s.close()
		}

		if assert.True(mustUint32(src.count()) > 0) {
			if r, err := s3svc.readerForTable(ctx, src.hash()); assert.NotNil(r) && assert.NoError(err) {
				assertChunksInReader(bigUns1, r, assert)
				assertChunksInReader(medChunks, r, assert)
				r.close()
			}
		}
	})

	t.Run("Mix", func(t *testing.T) {
		assert := assert.New(t)
		s3svc := makeFakeS3(t)
		s3p := newPersister(s3svc)

		// Start with small tables. Since total > minPartSize, will require more than one part to upload.
		sources := make(chunkSources, len(smallChunks))
		for i := 0; i < len(smallChunks); i++ {
			mt := newMemTable(uint64(2 * targetPartSize))
			mt.addChunk(computeAddr(smallChunks[i]), smallChunks[i])
			var err error
			sources[i], _, err = s3p.Persist(context.Background(), mt, nil, nil, &Stats{})
			require.NoError(t, err)
		}

		// Now, add a table with big chunks that will require more than one upload copy part.
		mt := newMemTable(uint64(2 * maxPartSize))
		for _, b := range bigUns1 {
			mt.addChunk(computeAddr(b), b)
		}

		var err error
		cs, _, err := s3p.Persist(context.Background(), mt, nil, nil, &Stats{})
		require.NoError(t, err)
		sources = append(sources, cs)

		// Last, some tables that should be directly upload-copyable
		medChunks := make([][]byte, 2)
		mt = newMemTable(uint64(2 * maxPartSize))
		for i := range medChunks {
			medChunks[i] = make([]byte, minPartSize+1)
			rand.Read(medChunks[i])
			mt.addChunk(computeAddr(medChunks[i]), medChunks[i])
		}

		cs, _, err = s3p.Persist(context.Background(), mt, nil, nil, &Stats{})
		require.NoError(t, err)
		sources = append(sources, cs)

		src, _, err := s3p.ConjoinAll(context.Background(), sources, &Stats{})
		require.NoError(t, err)
		defer src.close()
		for _, s := range sources {
			s.close()
		}

		if assert.True(mustUint32(src.count()) > 0) {
			if r, err := s3svc.readerForTable(ctx, src.hash()); assert.NotNil(r) && assert.NoError(err) {
				assertChunksInReader(smallChunks, r, assert)
				assertChunksInReader(bigUns1, r, assert)
				assertChunksInReader(medChunks, r, assert)
				r.close()
			}
		}
	})
}

func bytesToChunkSource(t *testing.T, bs ...[]byte) chunkSource {
	ctx := context.Background()
	sum := 0
	for _, b := range bs {
		sum += len(b)
	}
	maxSize := maxTableSize(uint64(len(bs)), uint64(sum))
	buff := make([]byte, maxSize)
	tw := newTableWriter(buff, nil)
	for _, b := range bs {
		tw.addChunk(computeAddr(b), b)
	}
	tableSize, name, err := tw.finish()
	require.NoError(t, err)
	data := buff[:tableSize]
	ti, err := parseTableIndexByCopy(ctx, data, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	rdr, err := newTableReader(ti, tableReaderAtFromBytes(data), fileBlockSize)
	require.NoError(t, err)
	return chunkSourceAdapter{rdr, name}
}
