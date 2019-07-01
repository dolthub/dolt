// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"io"
	"math/rand"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/liquidata-inc/ld/dolt/go/store/util/sizecache"
	"github.com/stretchr/testify/assert"
)

func TestAWSTablePersisterPersist(t *testing.T) {
	calcPartSize := func(rdr chunkReader, maxPartNum uint64) uint64 {
		return maxTableSize(uint64(rdr.count()), rdr.uncompressedLen()) / maxPartNum
	}

	mt := newMemTable(testMemTableSize)
	for _, c := range testChunks {
		assert.True(t, mt.addChunk(computeAddr(c), c))
	}

	t.Run("PersistToS3", func(t *testing.T) {
		testIt := func(t *testing.T, ns string) {
			t.Run("InMultipleParts", func(t *testing.T) {
				assert := assert.New(t)
				s3svc, ddb := makeFakeS3(t), makeFakeDTS(makeFakeDDB(t), nil)
				ic := newIndexCache(1024)
				limits := awsLimits{partTarget: calcPartSize(mt, 3)}
				s3p := awsTablePersister{s3: s3svc, bucket: "bucket", ddb: ddb, limits: limits, indexCache: ic, ns: ns}

				src, err := s3p.Persist(context.Background(), mt, nil, &Stats{})
				assert.NoError(err)
				assert.NotNil(ic.get(src.hash()))

				if assert.True(src.count() > 0) {
					if r, err := s3svc.readerForTableWithNamespace(ns, src.hash()); assert.NotNil(r) && assert.NoError(err) {
						assertChunksInReader(testChunks, r, assert)
					}
				}
			})

			t.Run("CacheTable", func(t *testing.T) {
				s3svc, ddb := makeFakeS3(t), makeFakeDTS(makeFakeDDB(t), nil)
				limits := awsLimits{partTarget: calcPartSize(mt, 3)}
				tc := &waitOnStoreTableCache{readers: map[addr]io.ReaderAt{}}
				s3p := awsTablePersister{s3: s3svc, bucket: "bucket", ddb: ddb, limits: limits, tc: tc, ns: ns}

				// Persist and wait until tc.store() has completed
				tc.storeWG.Add(1)
				src, err := s3p.Persist(context.Background(), mt, nil, &Stats{})
				assert.NoError(t, err)
				tc.storeWG.Wait()

				// Now, open the table that should have been cached by the above Persist() and read out all the chunks. All the reads should be serviced from tc.
				rdr, err := s3p.Open(context.Background(), src.hash(), src.count(), &Stats{})
				assert.NoError(t, err)
				baseline := s3svc.getCount
				ch := make(chan extractRecord)
				go func() {
					defer close(ch)
					err := rdr.extract(context.Background(), ch)
					assert.NoError(t, err)
				}()
				for range ch {
				}
				assert.Zero(t, s3svc.getCount-baseline)
			})

			t.Run("InSinglePart", func(t *testing.T) {
				assert := assert.New(t)

				s3svc, ddb := makeFakeS3(t), makeFakeDTS(makeFakeDDB(t), nil)
				limits := awsLimits{partTarget: calcPartSize(mt, 1)}
				s3p := awsTablePersister{s3: s3svc, bucket: "bucket", ddb: ddb, limits: limits, ns: ns}

				src, err := s3p.Persist(context.Background(), mt, nil, &Stats{})
				assert.NoError(err)
				if assert.True(src.count() > 0) {
					if r, err := s3svc.readerForTableWithNamespace(ns, src.hash()); assert.NotNil(r) && assert.NoError(err) {
						assertChunksInReader(testChunks, r, assert)
					}
				}
			})

			t.Run("NoNewChunks", func(t *testing.T) {
				assert := assert.New(t)

				mt := newMemTable(testMemTableSize)
				existingTable := newMemTable(testMemTableSize)

				for _, c := range testChunks {
					assert.True(mt.addChunk(computeAddr(c), c))
					assert.True(existingTable.addChunk(computeAddr(c), c))
				}

				s3svc, ddb := makeFakeS3(t), makeFakeDTS(makeFakeDDB(t), nil)
				limits := awsLimits{partTarget: 1 << 10}
				s3p := awsTablePersister{s3: s3svc, bucket: "bucket", ddb: ddb, limits: limits, ns: ns}

				src, err := s3p.Persist(context.Background(), mt, existingTable, &Stats{})
				assert.NoError(err)
				assert.True(src.count() == 0)

				_, present := s3svc.data[src.hash().String()]
				assert.False(present)
			})

			t.Run("Abort", func(t *testing.T) {
				assert := assert.New(t)

				s3svc := &failingFakeS3{makeFakeS3(t), sync.Mutex{}, 1}
				ddb := makeFakeDTS(makeFakeDDB(t), nil)
				limits := awsLimits{partTarget: calcPartSize(mt, 4)}
				s3p := awsTablePersister{s3: s3svc, bucket: "bucket", ddb: ddb, limits: limits, ns: ns}

				_, err := s3p.Persist(context.Background(), mt, nil, &Stats{})
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

	t.Run("PersistToDynamo", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			assert := assert.New(t)

			ddb := makeFakeDDB(t)
			s3svc, dts := makeFakeS3(t), makeFakeDTS(ddb, nil)
			limits := awsLimits{itemMax: maxDynamoItemSize, chunkMax: 2 * mt.count()}
			s3p := awsTablePersister{s3: s3svc, bucket: "bucket", ddb: dts, limits: limits, ns: ""}

			src, err := s3p.Persist(context.Background(), mt, nil, &Stats{})
			assert.NoError(err)
			if assert.True(src.count() > 0) {
				if r, err := ddb.readerForTable(src.hash()); assert.NotNil(r) && assert.NoError(err) {
					assertChunksInReader(testChunks, r, assert)
				}
			}
		})

		t.Run("CacheOnOpen", func(t *testing.T) {
			assert := assert.New(t)

			tc := sizecache.New(maxDynamoItemSize)
			ddb := makeFakeDDB(t)
			s3svc, dts := makeFakeS3(t), makeFakeDTS(ddb, tc)
			limits := awsLimits{itemMax: maxDynamoItemSize, chunkMax: 2 * mt.count()}

			s3p := awsTablePersister{s3: s3svc, bucket: "bucket", ddb: dts, limits: limits, ns: ""}

			tableData, name := buildTable(testChunks)
			ddb.putData(fmtTableName(name), tableData)

			src, err := s3p.Open(context.Background(), name, uint32(len(testChunks)), &Stats{})
			assert.NoError(err)
			if assert.True(src.count() > 0) {
				if r, err := ddb.readerForTable(src.hash()); assert.NotNil(r) && assert.NoError(err) {
					assertChunksInReader(testChunks, r, assert)
				}
				if data, present := tc.Get(name); assert.True(present) {
					assert.Equal(tableData, data.([]byte))
				}
			}
		})

		t.Run("FailTooManyChunks", func(t *testing.T) {
			assert := assert.New(t)

			ddb := makeFakeDDB(t)
			s3svc, dts := makeFakeS3(t), makeFakeDTS(ddb, nil)
			limits := awsLimits{itemMax: maxDynamoItemSize, chunkMax: 1, partTarget: calcPartSize(mt, 1)}
			s3p := awsTablePersister{s3: s3svc, bucket: "bucket", ddb: dts, limits: limits, ns: ""}

			src, err := s3p.Persist(context.Background(), mt, nil, &Stats{})
			assert.NoError(err)
			if assert.True(src.count() > 0) {
				if r, err := ddb.readerForTable(src.hash()); assert.Nil(r) && assert.NoError(err) {
					if r, err := s3svc.readerForTable(src.hash()); assert.NotNil(r) && assert.NoError(err) {
						assertChunksInReader(testChunks, r, assert)
					}
				}
			}
		})

		t.Run("FailItemTooBig", func(t *testing.T) {
			assert := assert.New(t)

			ddb := makeFakeDDB(t)
			s3svc, dts := makeFakeS3(t), makeFakeDTS(ddb, nil)
			limits := awsLimits{itemMax: 0, chunkMax: 2 * mt.count(), partTarget: calcPartSize(mt, 1)}
			s3p := awsTablePersister{s3: s3svc, bucket: "bucket", ddb: dts, limits: limits, ns: ""}

			src, err := s3p.Persist(context.Background(), mt, nil, &Stats{})
			assert.NoError(err)
			if assert.True(src.count() > 0) {
				if r, err := ddb.readerForTable(src.hash()); assert.Nil(r) && assert.NoError(err) {
					if r, err := s3svc.readerForTable(src.hash()); assert.NotNil(r) && assert.NoError(err) {
						assertChunksInReader(testChunks, r, assert)
					}
				}
			}
		})
	})
}
func makeFakeDTS(ddb ddbsvc, tc *sizecache.SizeCache) *ddbTableStore {
	return &ddbTableStore{ddb, "table", nil, tc}
}

type waitOnStoreTableCache struct {
	readers map[addr]io.ReaderAt
	mu      sync.RWMutex
	storeWG sync.WaitGroup
}

func (mtc *waitOnStoreTableCache) checkout(h addr) (io.ReaderAt, error) {
	mtc.mu.RLock()
	defer mtc.mu.RUnlock()
	return mtc.readers[h], nil
}

func (mtc *waitOnStoreTableCache) checkin(h addr) error {
	return nil
}

func (mtc *waitOnStoreTableCache) store(h addr, data io.Reader, size uint64) error {
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
	plan := planConjoin(sources, &Stats{})
	copies, manuals, _ := dividePlan(context.Background(), plan, minPartSize, maxPartSize)

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
	assert.EqualValues(calcChunkDataLen(justRight.index()), perTableDataSize[justRight.hash().String()])
	assert.EqualValues(calcChunkDataLen(tooBig.index()), perTableDataSize[tooBig.hash().String()])

	assert.Len(manuals, 1)
	assert.EqualValues(calcChunkDataLen(tooSmall.index()), manuals[0].dstEnd-manuals[0].dstStart)
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
	targetPartSize := uint64(1024)
	minPartSize, maxPartSize := targetPartSize, 5*targetPartSize
	maxItemSize, maxChunkCount := int(targetPartSize/2), uint32(4)

	ic := newIndexCache(1024)
	rl := make(chan struct{}, 8)
	defer close(rl)

	newPersister := func(s3svc s3svc, ddb *ddbTableStore) awsTablePersister {
		return awsTablePersister{s3svc, "bucket", rl, nil, ddb, awsLimits{targetPartSize, minPartSize, maxPartSize, maxItemSize, maxChunkCount}, ic, ""}
	}

	smallChunks := [][]byte{}
	rnd := rand.New(rand.NewSource(0))
	for smallChunkTotal := uint64(0); smallChunkTotal <= uint64(minPartSize); {
		small := make([]byte, minPartSize/5)
		rnd.Read(small)
		src := bytesToChunkSource(t, small)
		smallChunks = append(smallChunks, small)
		smallChunkTotal += calcChunkDataLen(src.index())
	}

	t.Run("Small", func(t *testing.T) {
		makeSources := func(s3p awsTablePersister, chunks [][]byte) (sources chunkSources) {
			for i := 0; i < len(chunks); i++ {
				mt := newMemTable(uint64(2 * targetPartSize))
				mt.addChunk(computeAddr(chunks[i]), chunks[i])
				cs, err := s3p.Persist(context.Background(), mt, nil, &Stats{})
				assert.NoError(t, err)
				sources = append(sources, cs)
			}
			return
		}

		t.Run("TotalUnderMinSize", func(t *testing.T) {
			assert := assert.New(t)
			s3svc, ddb := makeFakeS3(t), makeFakeDTS(makeFakeDDB(t), nil)
			s3p := newPersister(s3svc, ddb)

			chunks := smallChunks[:len(smallChunks)-1]
			sources := makeSources(s3p, chunks)
			src, err := s3p.ConjoinAll(context.Background(), sources, &Stats{})
			assert.NoError(err)
			assert.NotNil(ic.get(src.hash()))

			if assert.True(src.count() > 0) {
				if r, err := s3svc.readerForTable(src.hash()); assert.NotNil(r) && assert.NoError(err) {
					assertChunksInReader(chunks, r, assert)
				}
			}
		})

		t.Run("TotalOverMinSize", func(t *testing.T) {
			assert := assert.New(t)
			s3svc, ddb := makeFakeS3(t), makeFakeDTS(makeFakeDDB(t), nil)
			s3p := newPersister(s3svc, ddb)

			sources := makeSources(s3p, smallChunks)
			src, err := s3p.ConjoinAll(context.Background(), sources, &Stats{})
			assert.NoError(err)
			assert.NotNil(ic.get(src.hash()))

			if assert.True(src.count() > 0) {
				if r, err := s3svc.readerForTable(src.hash()); assert.NotNil(r) && assert.NoError(err) {
					assertChunksInReader(smallChunks, r, assert)
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
		s3svc, ddb := makeFakeS3(t), makeFakeDTS(makeFakeDDB(t), nil)
		s3p := newPersister(s3svc, ddb)

		// Make 2 chunk sources that each have >maxPartSize chunk data
		sources := make(chunkSources, 2)
		for i, bu := range [][][]byte{bigUns1, bigUns2} {
			mt := newMemTable(uint64(2 * maxPartSize))
			for _, b := range bu {
				mt.addChunk(computeAddr(b), b)
			}

			var err error
			sources[i], err = s3p.Persist(context.Background(), mt, nil, &Stats{})

			assert.NoError(err)
		}
		src, err := s3p.ConjoinAll(context.Background(), sources, &Stats{})
		assert.NoError(err)
		assert.NotNil(ic.get(src.hash()))

		if assert.True(src.count() > 0) {
			if r, err := s3svc.readerForTable(src.hash()); assert.NotNil(r) && assert.NoError(err) {
				assertChunksInReader(bigUns1, r, assert)
				assertChunksInReader(bigUns2, r, assert)
			}
		}
	})

	t.Run("SomeOverMax", func(t *testing.T) {
		assert := assert.New(t)
		s3svc, ddb := makeFakeS3(t), makeFakeDTS(makeFakeDDB(t), nil)
		s3p := newPersister(s3svc, ddb)

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
		cs1, err := s3p.Persist(context.Background(), mt, nil, &Stats{})
		assert.NoError(err)
		cs2, err := s3p.Persist(context.Background(), mtb, nil, &Stats{})
		assert.NoError(err)
		sources := chunkSources{cs1, cs2}

		src, err := s3p.ConjoinAll(context.Background(), sources, &Stats{})
		assert.NoError(err)
		assert.NotNil(ic.get(src.hash()))

		if assert.True(src.count() > 0) {
			if r, err := s3svc.readerForTable(src.hash()); assert.NotNil(r) && assert.NoError(err) {
				assertChunksInReader(bigUns1, r, assert)
				assertChunksInReader(medChunks, r, assert)
			}
		}
	})

	t.Run("Mix", func(t *testing.T) {
		assert := assert.New(t)
		s3svc, ddb := makeFakeS3(t), makeFakeDTS(makeFakeDDB(t), nil)
		s3p := newPersister(s3svc, ddb)

		// Start with small tables. Since total > minPartSize, will require more than one part to upload.
		sources := make(chunkSources, len(smallChunks))
		for i := 0; i < len(smallChunks); i++ {
			mt := newMemTable(uint64(2 * targetPartSize))
			mt.addChunk(computeAddr(smallChunks[i]), smallChunks[i])
			var err error
			sources[i], err = s3p.Persist(context.Background(), mt, nil, &Stats{})
			assert.NoError(err)
		}

		// Now, add a table with big chunks that will require more than one upload copy part.
		mt := newMemTable(uint64(2 * maxPartSize))
		for _, b := range bigUns1 {
			mt.addChunk(computeAddr(b), b)
		}

		var err error
		cs, err := s3p.Persist(context.Background(), mt, nil, &Stats{})
		assert.NoError(err)
		sources = append(sources, cs)

		// Last, some tables that should be directly upload-copyable
		medChunks := make([][]byte, 2)
		mt = newMemTable(uint64(2 * maxPartSize))
		for i := range medChunks {
			medChunks[i] = make([]byte, minPartSize+1)
			rand.Read(medChunks[i])
			mt.addChunk(computeAddr(medChunks[i]), medChunks[i])
		}

		cs, err = s3p.Persist(context.Background(), mt, nil, &Stats{})
		assert.NoError(err)
		sources = append(sources, cs)

		src, err := s3p.ConjoinAll(context.Background(), sources, &Stats{})
		assert.NoError(err)
		assert.NotNil(ic.get(src.hash()))

		if assert.True(src.count() > 0) {
			if r, err := s3svc.readerForTable(src.hash()); assert.NotNil(r) && assert.NoError(err) {
				assertChunksInReader(smallChunks, r, assert)
				assertChunksInReader(bigUns1, r, assert)
				assertChunksInReader(medChunks, r, assert)
			}
		}
	})
}

func bytesToChunkSource(t *testing.T, bs ...[]byte) chunkSource {
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
	tableSize, name := tw.finish()
	data := buff[:tableSize]
	ti, err := parseTableIndex(data)
	assert.NoError(t, err)
	rdr := newTableReader(ti, tableReaderAtFromBytes(data), fileBlockSize)
	return chunkSourceAdapter{rdr, name}
}
