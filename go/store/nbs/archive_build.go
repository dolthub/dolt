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

package nbs

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/dolthub/gozstd"
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

const defaultDictionarySize = 1 << 12 // NM4 - maybe just select the largest chunk. TBD.
const maxSamples = 1000
const fourMb = 1 << 22

// indexAndFinalizeArchive writes the index, metadata, and footer to the archive file. It also flushes the archive writer
// to the directory provided. The name is calculated from the footer, and can be obtained by calling getName on the archive.
func indexFinalizeFlushArchive(arcW *archiveWriter, archivePath string, originTableFile hash.Hash) error {
	err := arcW.finalizeByteSpans()
	if err != nil {
		return err
	}
	err = arcW.indexFinalize(archiveOrigin{
		ConvertedTableFileName: originTableFile,
		ConversionTime:         time.Now(),
	})
	if err != nil {
		return err
	}

	fileName, err := arcW.genFileName(archivePath)
	if err != nil {
		return err
	}

	return arcW.flushToFile(fileName)
}

// chunkGroup is a collection of chunks that are compressed together using the same Dictionary. It also contains
// calculated statistics about the group, specifically the total compression ratio and the average raw chunk size.
type chunkGroup struct {
	dict  []byte
	cDict *gozstd.CDict
	// Sorted list of chunks and their compression score. Higher is better. The score doesn't include the dictionary size.
	chks []chunkCmpScore
	// The total ratio _includes_ the dictionary size.
	totalRatioWDict            float64
	totalBytesSavedWDict       int
	totalRatioDefaultDict      float64
	totalBytesSavedDefaultDict int
	avgRawChunkSize            int
}

// chunkCmpScore wraps a chunk and its compression score for use by the chunkGroup. This score is calculated by comparing
// the size of the compressed chunk using the group's dictionary to the size of the compressed chunk using the default
// dictionary. Closer to 1.0 is better.

// This score if for an individual chunk, and does not include the dictionary size. The group may not be used when the
// full set is considered with the inclusion of the default dictionary.
type chunkCmpScore struct {
	chunkId hash.Hash
	// The compression score. Higher is better. This is the ratio of the compressed size to the raw size, using the group's
	// dictionary. IE, this number only has meaning within the group
	score float64
	// The size of the compressed chunk using the group's dictionary.
	dictCmpSize int
	// The size of the compressed chunk using the default dictionary. If this is smaller than dictCmpSize, then this chunk
	// is not a good candidate for the group.
	defaultDictCmpSize int
}

// newChunkGroup creates a new chunkGroup from a set of chunks.
func newChunkGroup(
	ctx context.Context,
	chunkCache *simpleChunkSourceCache,
	chks hash.HashSet,
	defaultDict *gozstd.CDict,
	stats *Stats,
) (*chunkGroup, error) {
	scored := make([]chunkCmpScore, len(chks))
	i := 0
	for h := range chks {
		scored[i] = chunkCmpScore{h, 0.0, 0, 0}
		i++
	}

	result := chunkGroup{dict: nil, cDict: nil, chks: scored}
	err := result.rebuild(ctx, chunkCache, defaultDict, stats)
	if err != nil {
		return nil, err
	}
	return &result, err
}

// For whatever reason, we need 7 or more samples to build a dictionary. But in principle we only need 1. So duplicate
// the samples until we have enough. Note that we need to add each chunk the same number of times do we don't end up
// with bias in the dictionary.
func padSamples(chks []*chunks.Chunk) []*chunks.Chunk {
	samples := []*chunks.Chunk{}
	for len(samples) < 7 {
		for _, c := range chks {
			samples = append(samples, c)
		}
	}
	return samples
}

// Add this chunk into the group. It does not attempt to determine if this is a good addition. The caller should
// use testChunk to determine if this chunk should be added.
//
// The chunkGroup will be recalculated after this chunk is added.
func (cg *chunkGroup) addChunk(
	ctx context.Context,
	chunkChache *simpleChunkSourceCache,
	c *chunks.Chunk,
	defaultDict *gozstd.CDict,
	stats *Stats,
) error {
	scored := chunkCmpScore{
		chunkId:     c.Hash(),
		score:       0.0,
		dictCmpSize: 0,
	}

	cg.chks = append(cg.chks, scored)
	return cg.rebuild(ctx, chunkChache, defaultDict, stats)
}

// worstZScore returns the z-score of the worst chunk in the group. The z-score is a measure of how many standard
// deviations from the mean the value is. This value will always be negative (If something has a positive
// z-score, it would make it better than the mean, and we don't care about that).
func (cg *chunkGroup) worstZScore() float64 {
	// Calculate the mean
	var sum float64
	for _, v := range cg.chks {
		sum += v.score
	}
	mean := sum / float64(len(cg.chks))

	// Calculate the sum of squares of differences from the mean
	var sumSquares float64
	for _, v := range cg.chks {
		diff := v.score - mean
		sumSquares += diff * diff
	}

	// Calculate the standard deviation
	stdDev := math.Sqrt(sumSquares / float64(len(cg.chks)))

	// Calculate z-score for the given value
	zScore := (cg.chks[len(cg.chks)-1].score - mean) / stdDev

	return zScore
}

// rebuild - recalculate the entire group's compression ratio. Dictionary and total compression ratio are updated as well.
// This method is called after a new chunk is added to the group. Ensures that stats about the group are up-to-date.
func (cg *chunkGroup) rebuild(ctx context.Context, chunkCache *simpleChunkSourceCache, defaultDict *gozstd.CDict, stats *Stats) error {
	chks := make([]*chunks.Chunk, 0, len(cg.chks))

	for _, cs := range cg.chks {
		chk, err := chunkCache.get(ctx, cs.chunkId, stats)
		if err != nil {
			return err
		}
		if chk == nil {
			continue
		}
		chks = append(chks, chk)
	}

	totalBytes := 0
	for _, c := range chks {
		totalBytes += len(c.Data())
	}

	dct := buildDictionary(padSamples(chks))

	var cDict *gozstd.CDict
	cDict, err := gozstd.NewCDict(dct)
	if err != nil {
		return err
	}

	cmpDct := gozstd.Compress(nil, dct)

	raw := 0
	dictCmpSize := len(cmpDct)
	noDictCmpSize := 0
	scored := make([]chunkCmpScore, len(chks))
	for i, c := range chks {
		d := c.Data()
		comp := gozstd.CompressDict(nil, d, cDict)
		defaultDictComp := gozstd.CompressDict(nil, d, defaultDict)

		ccs := chunkCmpScore{
			chunkId:            c.Hash(),
			score:              float64(len(d)-len(comp)) / float64(len(d)),
			dictCmpSize:        len(comp),
			defaultDictCmpSize: len(defaultDictComp),
		}
		scored[i] = ccs
		raw += len(c.Data())
		dictCmpSize += ccs.dictCmpSize
		noDictCmpSize += ccs.defaultDictCmpSize
	}
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score > scored[j].score
	})

	cg.dict = dct
	cg.cDict = cDict
	cg.chks = scored

	cg.totalRatioWDict = float64(raw-dictCmpSize) / float64(raw)
	cg.totalBytesSavedWDict = raw - dictCmpSize

	cg.totalRatioDefaultDict = float64(raw-noDictCmpSize) / float64(raw)
	cg.totalBytesSavedDefaultDict = raw - noDictCmpSize

	cg.avgRawChunkSize = raw / len(chks)
	return nil
}

// Helper method to build new dictionary objects from a set of chunks.
func buildDictionary(chks []*chunks.Chunk) (ans []byte) {
	samples := make([][]uint8, 0, len(chks))
	for _, c := range chks {
		samples = append(samples, c.Data())
	}
	return gozstd.BuildDict(samples, defaultDictionarySize)
}

// Returns true if the chunk's compression ratio (using the existing dictionary) is better than the group's worst chunk.
func (cg *chunkGroup) testChunk(c *chunks.Chunk) (bool, error) {
	comp := gozstd.CompressDict(nil, c.Data(), cg.cDict)

	ratio := float64(len(c.Data())-len(comp)) / float64(len(c.Data()))

	if ratio > cg.chks[len(cg.chks)-1].score {
		return true, nil
	}
	return false, nil
}

func NewChunkRelations() ChunkRelations {
	m := make(map[hash.Hash]*hash.HashSet)
	return ChunkRelations{m}
}

// ChunkRelations is holds chunks that are related to each other. Currently the only relationship type is for chunks
// which we know are related based on modifications of a Prolly Tree, and relationships are fully transitive:
// A is related to B, and B is related to C, then A is related to C.
type ChunkRelations struct {
	// Fully transitive relationships between chunks. The key is a chunk hash, and the value is a set of chunk hashes
	// it is related to. The value is a pointer to the set, so that we can update the set in place, and many keys
	// can map to it.
	manyToGroup map[hash.Hash]*hash.HashSet
}

func (cr *ChunkRelations) Count() int {
	return len(cr.manyToGroup)
}

func (cr *ChunkRelations) groups() []hash.HashSet {
	seen := map[*hash.HashSet]struct{}{}
	groups := make([]hash.HashSet, 0, len(cr.manyToGroup))
	for _, v := range cr.manyToGroup {
		if _, ok := seen[v]; !ok {
			groups = append(groups, *v)
			seen[v] = struct{}{}
		}
	}
	return groups
}

func (cr *ChunkRelations) Contains(h hash.Hash) bool {
	_, ok := cr.manyToGroup[h]
	return ok
}

// Add a pair of hashes to the relations. If either chunk is already in a group, the other chunk will be added to that.
// This method has no access to the chunks themselves, so it cannot determine if the chunks are similar. This method
// is used if you know from other sources that the chunks are similar.
func (cr *ChunkRelations) Add(a, b hash.Hash) {
	_, haveA := cr.manyToGroup[a]
	_, haveB := cr.manyToGroup[b]

	if !haveA && !haveB {
		newGroup := hash.NewHashSet(a, b)
		cr.manyToGroup[a] = &newGroup
		cr.manyToGroup[b] = &newGroup
		return
	}

	if haveA && !haveB {
		cr.manyToGroup[a].Insert(b)
		cr.manyToGroup[b] = cr.manyToGroup[a]
		return
	}

	if !haveA { // haveB must be true
		cr.manyToGroup[b].Insert(a)
		cr.manyToGroup[a] = cr.manyToGroup[b]
		return
	}

	// Both are not new, and they are already in the same group.
	if cr.manyToGroup[a] == cr.manyToGroup[b] {
		return
	}

	// Both are not new, and they are in different groups. Merge the groups.
	merged := hash.NewHashSet()
	for h := range *cr.manyToGroup[a] {
		merged.Insert(h)
	}
	for h := range *cr.manyToGroup[b] {
		merged.Insert(h)
	}
	for h := range merged {
		cr.manyToGroup[h] = &merged
	}
}

// simpleChunkSourceCache is a simple cache for chunks. For the purposes of building archives, we want to avoid storing
// all chunks in memory.
type simpleChunkSourceCache struct {
	cache *lru.TwoQueueCache[hash.Hash, *chunks.Chunk]
	cs    chunkSource
}

// newSimpleChunkSourceCache creates a new simpleChunkSourceCache. The cache size is fixed, should use approximately
// 12Gb of memory.
func newSimpleChunkSourceCache(cs chunkSource) (*simpleChunkSourceCache, error) {
	// Chunks are 4K on average, so 3M chunks should be about 12Gb.
	lruCache, err := lru.New2Q[hash.Hash, *chunks.Chunk](3000000)
	if err != nil {
		return nil, err
	}
	return &simpleChunkSourceCache{lruCache, cs}, nil
}

// get a chunk from the cache. If the chunk is not in the cache, it will be fetched from the ChunkSource.
// If the ChunkSource doesn't have the chunk, return nil - this is a valid case.
func (csc *simpleChunkSourceCache) get(ctx context.Context, h hash.Hash, stats *Stats) (*chunks.Chunk, error) {
	if chk, ok := csc.cache.Get(h); ok {
		return chk, nil
	}

	bytes, _, err := csc.cs.get(ctx, h, nil, stats)
	if bytes == nil || err != nil {
		return nil, err
	}

	chk := chunks.NewChunk(bytes)
	csc.cache.Add(h, &chk)
	return &chk, nil
}

// has returns true if the chunk is in the ChunkSource. This is not related to what is cached, just a helper.
func (csc *simpleChunkSourceCache) has(h hash.Hash) (bool, error) {
	res, _, err := csc.cs.has(h, nil)
	return res, err
}

// addresses get all chunk addresses of the ChunkSource as a hash.HashSet.
// This is not related to what is cached, just a helper since the cache has a reference to the ChunkSource.
func (csc *simpleChunkSourceCache) addresses() (hash.HashSet, error) {
	idx, err := csc.cs.index()
	if err != nil {
		return nil, err
	}

	result := hash.NewHashSet()
	for i := uint32(0); i < idx.chunkCount(); i++ {
		var h hash.Hash
		_, err := idx.indexEntry(i, &h)
		if err != nil {
			return nil, err
		}
		result.Insert(h)
	}
	return result, nil
}
