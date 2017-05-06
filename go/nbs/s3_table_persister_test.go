// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"math/rand"
	"sync"
	"testing"

	"github.com/attic-labs/testify/assert"
	"github.com/aws/aws-sdk-go/service/s3"
)

func TestS3TablePersisterPersist(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(testMemTableSize)

	for _, c := range testChunks {
		assert.True(mt.addChunk(computeAddr(c), c))
	}

	s3svc := makeFakeS3(assert)
	cache := newIndexCache(1024)
	sz := calcPartSize(mt, 3)
	s3p := s3TablePersister{s3: s3svc, bucket: "bucket", targetPartSize: sz, indexCache: cache}

	src := s3p.Persist(mt, nil, &Stats{})
	assert.NotNil(cache.get(src.hash()))

	if assert.True(src.count() > 0) {
		if r := s3svc.readerForTable(src.hash()); assert.NotNil(r) {
			assertChunksInReader(testChunks, r, assert)
		}
	}
}

func calcPartSize(rdr chunkReader, maxPartNum uint64) uint64 {
	return maxTableSize(uint64(rdr.count()), rdr.uncompressedLen()) / maxPartNum
}

func TestS3TablePersisterPersistSinglePart(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(testMemTableSize)

	for _, c := range testChunks {
		assert.True(mt.addChunk(computeAddr(c), c))
	}

	s3svc := makeFakeS3(assert)
	s3p := s3TablePersister{s3: s3svc, bucket: "bucket", targetPartSize: calcPartSize(mt, 1)}

	src := s3p.Persist(mt, nil, &Stats{})
	if assert.True(src.count() > 0) {
		if r := s3svc.readerForTable(src.hash()); assert.NotNil(r) {
			assertChunksInReader(testChunks, r, assert)
		}
	}
}

func TestS3TablePersisterPersistAbort(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(testMemTableSize)

	for _, c := range testChunks {
		assert.True(mt.addChunk(computeAddr(c), c))
	}

	s3svc := &failingFakeS3{makeFakeS3(assert), sync.Mutex{}, 1}
	s3p := s3TablePersister{s3: s3svc, bucket: "bucket", targetPartSize: calcPartSize(mt, 4)}

	assert.Panics(func() { s3p.Persist(mt, nil, &Stats{}) })
}

type failingFakeS3 struct {
	*fakeS3
	mu           sync.Mutex
	numSuccesses int
}

func (m *failingFakeS3) UploadPart(input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.numSuccesses > 0 {
		m.numSuccesses--
		return m.fakeS3.UploadPart(input)
	}
	return nil, mockAWSError("MalformedXML")
}

func TestS3TablePersisterCompactNoData(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(testMemTableSize)
	existingTable := newMemTable(testMemTableSize)

	for _, c := range testChunks {
		assert.True(mt.addChunk(computeAddr(c), c))
		assert.True(existingTable.addChunk(computeAddr(c), c))
	}

	s3svc := makeFakeS3(assert)
	s3p := s3TablePersister{s3: s3svc, bucket: "bucket", targetPartSize: 1 << 10}

	src := s3p.Persist(mt, existingTable, &Stats{})
	assert.True(src.count() == 0)

	_, present := s3svc.data[src.hash().String()]
	assert.False(present)
}

func TestS3TablePersisterDividePlan(t *testing.T) {
	assert := assert.New(t)
	minPartSize, maxPartSize := uint64(16), uint64(32)
	tooSmall := bytesToChunkSource([]byte("a"))
	justRight := bytesToChunkSource([]byte("123456789"), []byte("abcdefghi"))
	bigUns := [][]byte{make([]byte, maxPartSize-1), make([]byte, maxPartSize-1)}
	for _, b := range bigUns {
		rand.Read(b)
	}
	tooBig := bytesToChunkSource(bigUns...)

	sources := chunkSources{justRight, tooBig, tooSmall}
	plan := planCompaction(sources, &Stats{})
	copies, manuals, _ := dividePlan(plan, minPartSize, maxPartSize)

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

func TestS3TablePersisterCalcPartSizes(t *testing.T) {
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

func TestS3TablePersisterCompactAll(t *testing.T) {
	targetPartSize := uint64(1024)
	minPartSize, maxPartSize := targetPartSize, 5*targetPartSize

	cache := newIndexCache(1024)
	rl := make(chan struct{}, 8)
	defer close(rl)

	smallChunks := [][]byte{}
	rnd := rand.New(rand.NewSource(0))
	for smallChunkTotal := uint64(0); smallChunkTotal <= uint64(minPartSize); {
		small := make([]byte, minPartSize/5)
		rnd.Read(small)
		src := bytesToChunkSource(small)
		smallChunks = append(smallChunks, small)
		smallChunkTotal += calcChunkDataLen(src.index())
	}

	t.Run("Small", func(t *testing.T) {
		makeSources := func(s3p s3TablePersister, chunks [][]byte) (sources chunkSources) {
			for i := 0; i < len(chunks); i++ {
				mt := newMemTable(uint64(2 * targetPartSize))
				mt.addChunk(computeAddr(chunks[i]), chunks[i])
				sources = append(sources, s3p.Persist(mt, nil, &Stats{}))
			}
			return
		}

		t.Run("TotalUnderMinSize", func(t *testing.T) {
			assert := assert.New(t)
			s3svc := makeFakeS3(assert)
			s3p := s3TablePersister{s3svc, "bucket", targetPartSize, minPartSize, maxPartSize, cache, rl}

			chunks := smallChunks[:len(smallChunks)-1]
			sources := makeSources(s3p, chunks)
			src := s3p.CompactAll(sources, &Stats{})
			assert.NotNil(cache.get(src.hash()))

			if assert.True(src.count() > 0) {
				if r := s3svc.readerForTable(src.hash()); assert.NotNil(r) {
					assertChunksInReader(chunks, r, assert)
				}
			}
		})

		t.Run("TotalOverMinSize", func(t *testing.T) {
			assert := assert.New(t)
			s3svc := makeFakeS3(assert)
			s3p := s3TablePersister{s3svc, "bucket", targetPartSize, minPartSize, maxPartSize, cache, rl}

			sources := makeSources(s3p, smallChunks)
			src := s3p.CompactAll(sources, &Stats{})
			assert.NotNil(cache.get(src.hash()))

			if assert.True(src.count() > 0) {
				if r := s3svc.readerForTable(src.hash()); assert.NotNil(r) {
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
		s3svc := makeFakeS3(assert)
		s3p := s3TablePersister{s3svc, "bucket", targetPartSize, minPartSize, maxPartSize, cache, rl}

		// Make 2 chunk sources that each have >maxPartSize chunk data
		sources := make(chunkSources, 2)
		for i, bu := range [][][]byte{bigUns1, bigUns2} {
			mt := newMemTable(uint64(2 * maxPartSize))
			for _, b := range bu {
				mt.addChunk(computeAddr(b), b)
			}
			sources[i] = s3p.Persist(mt, nil, &Stats{})
		}
		src := s3p.CompactAll(sources, &Stats{})
		assert.NotNil(cache.get(src.hash()))

		if assert.True(src.count() > 0) {
			if r := s3svc.readerForTable(src.hash()); assert.NotNil(r) {
				assertChunksInReader(bigUns1, r, assert)
				assertChunksInReader(bigUns2, r, assert)
			}
		}
	})

	t.Run("SomeOverMax", func(t *testing.T) {
		assert := assert.New(t)
		s3svc := makeFakeS3(assert)
		s3p := s3TablePersister{s3svc, "bucket", targetPartSize, minPartSize, maxPartSize, cache, rl}

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
		sources := chunkSources{s3p.Persist(mt, nil, &Stats{}), s3p.Persist(mtb, nil, &Stats{})}

		src := s3p.CompactAll(sources, &Stats{})
		assert.NotNil(cache.get(src.hash()))

		if assert.True(src.count() > 0) {
			if r := s3svc.readerForTable(src.hash()); assert.NotNil(r) {
				assertChunksInReader(bigUns1, r, assert)
				assertChunksInReader(medChunks, r, assert)
			}
		}
	})

	t.Run("Mix", func(t *testing.T) {
		assert := assert.New(t)
		s3svc := makeFakeS3(assert)
		s3p := s3TablePersister{s3svc, "bucket", targetPartSize, minPartSize, maxPartSize, cache, rl}

		// Start with small tables. Since total > minPartSize, will require more than one part to upload.
		sources := make(chunkSources, len(smallChunks))
		for i := 0; i < len(smallChunks); i++ {
			mt := newMemTable(uint64(2 * targetPartSize))
			mt.addChunk(computeAddr(smallChunks[i]), smallChunks[i])
			sources[i] = s3p.Persist(mt, nil, &Stats{})
		}

		// Now, add a table with big chunks that will require more than one upload copy part.
		mt := newMemTable(uint64(2 * maxPartSize))
		for _, b := range bigUns1 {
			mt.addChunk(computeAddr(b), b)
		}
		sources = append(sources, s3p.Persist(mt, nil, &Stats{}))

		// Last, some tables that should be directly upload-copyable
		medChunks := make([][]byte, 2)
		mt = newMemTable(uint64(2 * maxPartSize))
		for i := range medChunks {
			medChunks[i] = make([]byte, minPartSize+1)
			rand.Read(medChunks[i])
			mt.addChunk(computeAddr(medChunks[i]), medChunks[i])
		}
		sources = append(sources, s3p.Persist(mt, nil, &Stats{}))

		src := s3p.CompactAll(sources, &Stats{})
		assert.NotNil(cache.get(src.hash()))

		if assert.True(src.count() > 0) {
			if r := s3svc.readerForTable(src.hash()); assert.NotNil(r) {
				assertChunksInReader(smallChunks, r, assert)
				assertChunksInReader(bigUns1, r, assert)
				assertChunksInReader(medChunks, r, assert)
			}
		}
	})
}

func bytesToChunkSource(bs ...[]byte) chunkSource {
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
	rdr := newTableReader(parseTableIndex(data), bytes.NewReader(data), fileBlockSize)
	return chunkSourceAdapter{rdr, name}
}
