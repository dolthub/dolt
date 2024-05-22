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
	"math"
	"sort"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/gozstd"
)

const defaultDictionarySize = 1 << 12 // NM4 - maybe just select the largest chunk. TBD.

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

// chunkCmpScore wraps a chunk and its compression score for use by the chunkGroup.
type chunkCmpScore struct {
	chunk              *chunks.Chunk
	score              float64
	dictCmpSize        int
	defaultDictCmpSize int
}

// newChunkGroup creates a new chunkGroup from a set of chunks.
func newChunkGroup(cmpBuff []byte, chks []*chunks.Chunk, defaultDict *gozstd.CDict) *chunkGroup {
	scored := make([]chunkCmpScore, len(chks))
	for i, c := range chks {
		scored[i] = chunkCmpScore{c, 0.0, 0, 0}
	}
	result := chunkGroup{dict: nil, cDict: nil, chks: scored}
	result.rebuild(cmpBuff, defaultDict)
	return &result
}

func (cg *chunkGroup) getLeader() *chunks.Chunk {
	return cg.chks[0].chunk
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
func (cg *chunkGroup) addChunk(cmpBuff []byte, c *chunks.Chunk, defaultDict *gozstd.CDict) error {
	scored := chunkCmpScore{
		chunk:       c,
		score:       0.0,
		dictCmpSize: 0,
	}

	cg.chks = append(cg.chks, scored)
	return cg.rebuild(cmpBuff, defaultDict)
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
func (cg *chunkGroup) rebuild(cmpBuff []byte, defaultDict *gozstd.CDict) error {
	chks := make([]*chunks.Chunk, len(cg.chks))
	for i, cs := range cg.chks {
		chks[i] = cs.chunk
	}

	dct := buildDictionary(padSamples(chks))

	var cDict *gozstd.CDict
	cDict, err := gozstd.NewCDict(dct)
	if err != nil {
		return err
	}

	cmpDct := gozstd.Compress(nil, dct)

	scored := make([]chunkCmpScore, len(chks))
	for i, c := range chks {
		d := c.Data()
		comp := gozstd.CompressDict(cmpBuff, d, cDict)
		defaultDictComp := gozstd.CompressDict(cmpBuff, d, defaultDict)

		scored[i] = chunkCmpScore{
			chunk:              c,
			score:              float64(len(d)-len(comp)) / float64(len(d)),
			dictCmpSize:        len(comp),
			defaultDictCmpSize: len(defaultDictComp),
		}
	}
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score > scored[j].score
	})

	cg.dict = dct
	cg.cDict = cDict
	cg.chks = scored

	raw := 0
	dictCmpSize := 0
	noDictCmpSize := 0
	for _, cs := range cg.chks {
		c := cs.chunk
		raw += len(c.Data())
		dictCmpSize += cs.dictCmpSize
		noDictCmpSize += cs.defaultDictCmpSize
	}
	dictCmpSize += len(cmpDct)

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

func (cr *ChunkRelations) convertToChunkGroups(chks map[hash.Hash]*chunks.Chunk, defaultDict *gozstd.CDict) []*chunkGroup {
	result := make([]*chunkGroup, 0, cr.Count())
	buff := make([]byte, 0, maxChunkSize)

	// For each group, look up the addresses and build a chunk group.
	for _, v := range cr.groups() {
		var c []*chunks.Chunk
		for h := range v {
			// There will be chunks referenced in the chunk group which are not in the chks map. This is because
			// we walk the history to get the chunk groups, but the map comes from a specific table file only - which
			// may not contain all the chunks of the DB.
			chk := chks[h]
			if chk != nil {
				c = append(c, chk)
			}
		}

		if len(c) > 1 {
			result = append(result, newChunkGroup(buff, c, defaultDict))
		}
	}
	return result
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
