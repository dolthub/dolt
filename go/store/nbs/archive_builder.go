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
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"

	"github.com/valyala/gozstd"
	"golang.org/x/sync/errgroup"
)

const defaultDictionarySize = 1 << 12
const levensteinThreshold = 0.9
const dictCompressionThreshold = 0.775

type PrintfFunc func(format string, args ...interface{})

func RunExperiment(cs chunks.ChunkStore, dagGroups *ChunkRelations, p PrintfFunc) (err error) {
	if gs, ok := cs.(*GenerationalNBS); ok {
		oldgen := gs.oldGen.tables.upstream

		for tf, ogcs := range oldgen {
			p("Copying table file: %s\n", tf.String())

			outPath := experimentOutputFile(tf)

			idx, err := ogcs.index()
			if err != nil {
				return err
			}

			//err = copyAllChunks(ogcs, idx, outPath)
			// err = groupAllChunks(ogcs, idx, dagGroups, outPath, p)
			if err != nil {
				return err
			}

			start := time.Now()
			err = verifyAllChunks(idx, outPath)

			if err != nil {
				return err
			}
			duration := time.Since(start)

			p("Verified table file: %s\n", outPath)
			p("Execution time: %v\n", duration)
		}

	} else {
		return errors.New("Modern DB Expected")
	}
	return nil
}

func groupAllChunks(cs chunkSource, idx tableIndex, dagGroups *ChunkRelations, archivePath string, p PrintfFunc) (err error) {
	records := make([]getRecord, idx.chunkCount())
	for i := uint32(0); i < idx.chunkCount(); i++ {
		var h hash.Hash
		_, err := idx.indexEntry(i, &h)
		if err != nil {
			return err
		}

		records = append(records, getRecord{&h, h.Prefix(), false})
	}

	file, err := os.Create(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := io.Writer(file)

	arcW := newArchiveWriter(writer)

	group, ctx := errgroup.WithContext(context.TODO())

	lengthGroups := map[uint64][]*chunks.Chunk{}

	ungroupedChunks := map[hash.Hash]*chunks.Chunk{}
	groupedChunks := map[hash.Hash]*chunks.Chunk{}

	totalChunkCount := 0
	// Allocate buffer used to compress chunks.
	//	cmpBuff := make([]byte, 0, maxChunkSize)
	bottleneck := sync.Mutex{} // This code doesn't cope with parallelism yet.
	var stats Stats
	allFound, err := cs.getMany(ctx, group, records, func(_ context.Context, c *chunks.Chunk) {
		bottleneck.Lock()
		defer bottleneck.Unlock()

		totalChunkCount++

		h := c.Hash()
		if dagGroups.Contains(h) {
			groupedChunks[h] = c
		} else {
			ungroupedChunks[h] = c
		}

		if len(c.Data()) > 10 {
			b := c.Data()
			lengthGroups[uint64(len(b))] = append(lengthGroups[uint64(len(b))], c)
		} else {
			p("skipping chunk with len < 10\n") // Apparently there are no chunks with len < 10.
		}
	}, &stats)
	if err != nil {
		panic(err)
	}
	if !allFound { // Unlikely to happen, given we got the list of chunks from this index.
		panic("not all chunks found")
	}
	err = group.Wait()
	if err != nil {
		panic(err)
	}

	// We have every chunk in memory. noice
	for _, lenG := range lengthGroups {
		if len(lenG) >= 2 {
			for i, trailing := range lenG[:len(lenG)-1] {
				leading := lenG[i+1]

				if !dagGroups.Contains(trailing.Hash()) && !dagGroups.Contains(leading.Hash()) {
					if similarityScore(trailing.Data(), leading.Data()) > levensteinThreshold {
						p("adding to lenGroups: %s, %s\n", trailing.Hash().String(), leading.Hash().String())
						dagGroups.Add(trailing.Hash(), leading.Hash())

						delete(ungroupedChunks, trailing.Hash())
						delete(ungroupedChunks, leading.Hash())
						groupedChunks[trailing.Hash()] = trailing
						groupedChunks[leading.Hash()] = leading
					}
				} else if dagGroups.Contains(trailing.Hash()) && !dagGroups.Contains(leading.Hash()) {
					yes, leader, err := dagGroups.testAdd(leading, trailing.Hash(), groupedChunks)
					if err != nil {
						panic(err)
					}
					if yes {
						p("Adding %s to existing group (leader: %s)\n", leading.Hash().String(), leader.String()[:8])
						dagGroups.Add(leading.Hash(), trailing.Hash())
						delete(ungroupedChunks, leading.Hash())
						groupedChunks[leading.Hash()] = leading
					}
				} else if !dagGroups.Contains(trailing.Hash()) && dagGroups.Contains(leading.Hash()) {
					yes, leader, err := dagGroups.testAdd(trailing, leading.Hash(), groupedChunks)
					if err != nil {
						panic(err)
					}
					if yes {
						p("Adding %s to existing group (leader: %s)\n", leading.Hash().String(), leader.String()[:8])
						dagGroups.Add(trailing.Hash(), leading.Hash())
						delete(ungroupedChunks, trailing.Hash())
						groupedChunks[trailing.Hash()] = trailing
					}
				} else {
					//p("Both chunks are in groups. Skipping.\n")
				}
			}
		}
	}

	cgList := dagGroups.convertToChunkGroups(groupedChunks)

	sort.Slice(cgList, func(i, j int) bool {
		return cgList[i].totalBytesSavedWDict > cgList[j].totalBytesSavedWDict
	})

	// For informational purposes.... Look at each chunk and determine if any group is a good fit. This
	// is prohibitively expensive for moderate numbers of chunks.
	/*
		for h, c := range ungroupedChunks {
			bestSimScore := 0.0
			var bestGroup *chunkGroup

			for _, cg := range cgList {
				if cg.totalBytesSavedWDict > cg.totalBytesSavedNoDict {
					simScore := similarityScore(c.Data(), cg.getLeader().Data())
					if simScore > bestSimScore {
						bestSimScore = simScore
						bestGroup = cg
					}
				}
			}

			if bestGroup != nil {
				p("You're a Brute: Adding %s to existing Group %s (sim score: %f)\n", h.String(), bestGroup.getLeader().Hash().String()[:8], bestSimScore)
				bestGroup.addChunk(c)
				delete(ungroupedChunks, h)
				groupedChunks[h] = c
				break
			}
		}
	*/
	for n, cg := range cgList {
		cg.print(n, p)
	}

	unGroupCount := 0
	groupCount := 0

	for h, c := range ungroupedChunks {
		compressed := gozstd.Compress(nil, c.Data())
		id, err := arcW.writeByteSpan(compressed)
		if err != nil {
			panic(err)
		}
		err = arcW.stageChunk(h, 0, id)
		if err != nil {
			panic(err)
		}
		unGroupCount++
	}

	for _, cg := range cgList {
		if cg.totalBytesSavedWDict < cg.totalBytesSavedNoDict {
			// Not a good group, just write the chunks out individually.
			for _, cs := range cg.chks {
				c := cs.chunk
				if !arcW.chunkSeen(c.Hash()) {
					compressed := gozstd.Compress(nil, c.Data())
					id, err := arcW.writeByteSpan(compressed)
					if err != nil {
						panic(err)
					}
					err = arcW.stageChunk(c.Hash(), 0, id)
					if err != nil {
						panic(err)
					}

					unGroupCount++
				} else {
					p("WARN: chunk already written: %s\n", c.Hash().String())
				}
			}
		} else {
			dictId, err := arcW.writeByteSpan(cg.dict)
			if err != nil {
				panic(err)
			}

			for _, cs := range cg.chks {
				c := cs.chunk
				if !arcW.chunkSeen(c.Hash()) {
					compressed := gozstd.CompressDict(nil, c.Data(), cg.cDict)

					dataId, err := arcW.writeByteSpan(compressed)
					if err != nil {
						panic(err)
					}
					err = arcW.stageChunk(c.Hash(), dictId, dataId)
					if err != nil {
						panic(err)
					}
					groupCount++
				} else {
					p("WARN: chunk already written: %s\n", c.Hash().String())
				}
			}
		}
	}

	n, err := arcW.writeIndex()
	if err != nil {
		panic(err)
	}

	p("index size: %d\n", n)

	err = arcW.writeFooter(n)
	if err != nil {
		panic(err)
	}

	err = file.Close()
	if err != nil {
		panic(err)
	}

	p("grouped: %d\n", groupCount)
	p("ungrouped: %d\n", unGroupCount)

	if groupCount+unGroupCount != totalChunkCount {
		missing := totalChunkCount - (groupCount + unGroupCount)
		panic(fmt.Sprintf("chunk count mismatch. Missing: %d", missing))
	}

	return nil
}

func experimentOutputFile(tf hash.Hash) string {
	// For the purposes of the experiment, write to the CWD.
	return fmt.Sprintf("%s.darc", tf.String())
}

// copyAllChunks copies all chunks from the given chunkSource to the given archive file. No grouping is currently done.
func copyAllChunks(cs chunkSource, idx tableIndex, archivePath string) error {
	records := make([]getRecord, idx.chunkCount())
	for i := uint32(0); i < idx.chunkCount(); i++ {
		var h hash.Hash
		_, err := idx.indexEntry(i, &h)
		if err != nil {
			return err
		}

		records = append(records, getRecord{&h, h.Prefix(), false})
	}

	file, err := os.Create(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := io.Writer(file)

	arcW := newArchiveWriter(writer)

	group, ctx := errgroup.WithContext(context.TODO())

	// Allocate buffer used to compress chunks.
	cmpBuff := make([]byte, 0, maxChunkSize)
	var innerErr error
	bottleneck := sync.Mutex{} // This code doesn't cope with parallelism yet.
	var stats Stats
	allFound, err := cs.getMany(ctx, group, records, func(_ context.Context, c *chunks.Chunk) {
		bottleneck.Lock()
		defer bottleneck.Unlock()

		// For the first pass, don't group any chunks. Simply write chunks to the archive.
		// compressed, err := zCompress(cmpBuff, c.Data())
		compressed := gozstd.Compress(cmpBuff, c.Data())
		if err != nil {
			innerErr = err
			return
		}
		id, err := arcW.writeByteSpan(compressed)
		if err != nil {
			innerErr = err
		}
		err = arcW.stageChunk(c.Hash(), 0, id)
		if err != nil {
			innerErr = err
		}
	}, &stats)
	if err != nil {
		return err
	}
	if innerErr != nil {
		return innerErr
	}

	if !allFound { // Unlikely to happen, given we got the list of chunks from this index.
		return errors.New("not all chunks found")
	}
	err = group.Wait()
	if err != nil {
		return err
	}

	n, err := arcW.writeIndex()
	if err != nil {
		return err
	}
	return arcW.writeFooter(n)
}

func verifyAllChunks(idx tableIndex, archiveFile string) error {
	file, err := os.Open(archiveFile)
	if err != nil {
		return err
	}

	stat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()

	index, err := newArchiveIndex(file, uint64(fileSize))
	if err != nil {
		return err
	}

	buff := make([]byte, 0, maxChunkSize*2)

	for i := uint32(0); i < idx.chunkCount(); i++ {
		var h hash.Hash
		_, err := idx.indexEntry(i, &h)
		if err != nil {
			return err
		}

		if !index.has(h) {
			msg := fmt.Sprintf("chunk not found in archive: %s", h.String())
			return errors.New(msg)
		}

		data, err := index.get(buff, h)
		if err != nil {
			return err
		}
		if data == nil {
			msg := fmt.Sprintf("nil data returned from archive for expected chunk: %s", h.String())
			return errors.New(msg)
		}

		chk := chunks.NewChunk(data)

		// Verify the hash of the chunk. This is the best sanity check that our data is being stored and retrieved
		// without any errors.
		if chk.Hash() != h {
			msg := fmt.Sprintf("hash mismatch for chunk: %s", h.String())
			return errors.New(msg)
		}
	}
	return nil
}

// Compress input to output.
func zCompress(dst, data []byte) ([]byte, error) {
	if dst == nil {
		return nil, errors.New("nil destination buffer")
	}

	// Create a bytes.Buffer to write compressed data into
	buf := bytes.NewBuffer(dst)
	opt1 := zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(3))

	enc, err := zstd.NewWriter(buf, opt1)
	if err != nil {
		return nil, err
	}
	defer enc.Close()

	dataBuf := bytes.NewBuffer(data)
	written, err := io.Copy(enc, dataBuf)
	if err != nil {
		return nil, err
	}
	return dst[:written], nil
}

type chunkCmpScore struct {
	chunk         *chunks.Chunk
	score         float64
	dictCmpSize   int
	noDictCmpSize int
}

type chunkGroup struct {
	dict  []byte
	cDict *gozstd.CDict
	// Sorted list of chunks and their compression score. Higher is better. The score doesn't include the dictionary size.
	chks []chunkCmpScore
	// The total ration _includes_ the dictionary size.
	totalRatioWDict       float64
	totalBytesSavedWDict  int
	totalRatioNoDict      float64
	totalBytesSavedNoDict int
	avgRawChunkSize       int
}

func newChunkGroup(chks []*chunks.Chunk) *chunkGroup {
	scored := make([]chunkCmpScore, len(chks))
	for i, c := range chks {
		scored[i] = chunkCmpScore{c, 0.0, 0, 0}
	}
	result := chunkGroup{dict: nil, cDict: nil, chks: scored}
	result.recalculate()
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
func (cg *chunkGroup) addChunk(c *chunks.Chunk) {
	scored := chunkCmpScore{
		chunk:       c,
		score:       0.0,
		dictCmpSize: 0,
	}

	cg.chks = append(cg.chks, scored)
	cg.recalculate()
}

/*
// Remove the worst chunk from the group. The group will be recalculated after the chunk is removed. If the group
func (cg *chunkGroup) trimWorstChunk() (*chunks.Chunk, error) {
	worst := cg.chks[len(cg.chks)-1]
	cg.chks = cg.chks[:len(cg.chks)-1]
	cg.recalculate()
	return worst.chunk
}
*/

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

// recalculates the group's compression ratio and re-sorts the chunks. Dictionary and total compression ratio are updated as well.
func (cg *chunkGroup) recalculate() {
	chks := make([]*chunks.Chunk, len(cg.chks))
	for i, cs := range cg.chks {
		chks[i] = cs.chunk
	}

	var cDict *gozstd.CDict
	samples := padSamples(chks)

	dict, cDict := buildDictionary(samples)

	scored := make([]chunkCmpScore, len(chks))
	for i, c := range chks {
		d := c.Data()
		comp := gozstd.CompressDict(nil, d, cDict)
		noDictComp := gozstd.Compress(nil, d)
		scored[i] = chunkCmpScore{
			chunk:         c,
			score:         float64(len(d)-len(comp)) / float64(len(d)),
			dictCmpSize:   len(comp),
			noDictCmpSize: len(noDictComp),
		}
	}
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score > scored[j].score
	})

	cg.dict = dict
	cg.cDict = cDict
	cg.chks = scored

	raw := 0
	dictCmpSize := 0
	noDictCmpSize := 0
	for _, cs := range cg.chks {
		c := cs.chunk
		raw += len(c.Data())
		dictCmpSize += cs.dictCmpSize
		noDictCmpSize += cs.noDictCmpSize
	}
	dictCmpSize += len(dict)

	cg.totalRatioWDict = float64(raw-dictCmpSize) / float64(raw)
	cg.totalBytesSavedWDict = raw - dictCmpSize

	cg.totalRatioNoDict = float64(raw-noDictCmpSize) / float64(raw)
	cg.totalBytesSavedNoDict = raw - noDictCmpSize

	cg.avgRawChunkSize = raw / len(chks)

}

// Helper method to build new dictionary objects from a set of chunks.
func buildDictionary(chks []*chunks.Chunk) (dict []byte, cDict *gozstd.CDict) {
	samples := [][]byte{}
	for _, c := range chks {
		samples = append(samples, c.Data())
	}

	dict = gozstd.BuildDict(samples, defaultDictionarySize)
	if dict != nil && len(dict) != 0 {
		var err error
		cDict, err = gozstd.NewCDict(dict)
		if err != nil {
			panic(err)
		}
	} else {
		panic("dict is nil, but sample count is > 7.")
	}
	return
}

// Returns true if the chunk's compression ratio (using the existing dictionary) is better than the group's worst chunk.
func (cg *chunkGroup) testChunk(c *chunks.Chunk) bool {
	comp := gozstd.CompressDict(nil, c.Data(), cg.cDict)
	ratio := float64(len(c.Data())-len(comp)) / float64(len(c.Data()))

	if ratio > cg.chks[len(cg.chks)-1].score {
		return true
	}
	return false
}

func (cg *chunkGroup) print(n int, p PrintfFunc) {

	var scores []string
	for _, c := range cg.chks {
		scores = append(scores, fmt.Sprintf("%.3f", c.score))
	}

	p("-- GROUP %d -- %s -- \n", n, cg.getLeader().Hash().String()[:8])
	p("totalRatioWDict: %f (bytes saved: %d)\n", cg.totalRatioWDict, cg.totalBytesSavedWDict)
	p("totalRatioNoDict: %f (bytes saved: %d)\n", cg.totalRatioNoDict, cg.totalBytesSavedNoDict)
	p("Worst z-score: %f\n", cg.worstZScore())
	p("  Scores: %s\n", strings.Join(scores, ", "))
	p("chunks: %d (avg size: %d)\n", len(cg.chks), cg.avgRawChunkSize)
	p("Dict Size: %d\n", len(cg.dict))

}

func NewChunkRelations() ChunkRelations {
	m := make(map[hash.Hash]*hash.HashSet)
	return ChunkRelations{m}
}

type ChunkRelations struct {
	manyToGroup map[hash.Hash]*hash.HashSet
}

func (cr *ChunkRelations) Count() int {
	return len(cr.manyToGroup)
}

func (cr *ChunkRelations) convertToChunkGroups(chks map[hash.Hash]*chunks.Chunk) []*chunkGroup {
	result := make([]*chunkGroup, 0, cr.Count())
	// For each group, look up the addresses and build a chunk group.
	for _, v := range cr.groups() {
		var c []*chunks.Chunk
		for h := range v {
			c = append(c, chks[h])
		}

		result = append(result, newChunkGroup(c))
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

// testAdd will take two hashes and a map of chunks, and determine if adding |chk| to the group containing |to| will
// result in a better group. This requires building a temporary chunkGroup, which is why the map of chunks is required
//
// The only error that can be returned is if the |to| hash is not in a group.
func (cr *ChunkRelations) testAdd(chk *chunks.Chunk, to hash.Hash, chks map[hash.Hash]*chunks.Chunk) (bool, hash.Hash, error) {
	if hs, ok := cr.manyToGroup[to]; ok {
		chunkList := []*chunks.Chunk{}
		for h := range *hs {
			chunkList = append(chunkList, chks[h])
		}

		cg := newChunkGroup(chunkList)
		return cg.testChunk(chk), cg.getLeader().Hash(), nil
	} else {
		return false, hash.Hash{}, fmt.Errorf("to hash not in group")
	}
}

// Add a pair of hashes to the relations. If either chunk is already in a group, the other chunk will be added to that.
// This method has no access to the chunks themselves, so it cannot determine if the chunks are similar. This method
// is used if you know from other sources that the chunks are similar.
func (cr *ChunkRelations) Add(a, b hash.Hash) {
	aNew := true
	bNew := true
	if _, ok := cr.manyToGroup[a]; ok {
		aNew = false
	}
	if _, ok := cr.manyToGroup[b]; ok {
		bNew = false
	}

	if aNew && bNew {
		newGroup := hash.NewHashSet(a, b)

		cr.manyToGroup[a] = &newGroup
		cr.manyToGroup[b] = &newGroup
		return
	}

	if !aNew && bNew {
		cr.manyToGroup[a].Insert(b)
		cr.manyToGroup[b] = cr.manyToGroup[a]
		return
	}

	if aNew && !bNew {
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

/*
func levensteinGrouping(sims []*chunks.Chunk, scoreThreshold float64) []*chunkGroup {
	type highScore struct {
		chunk *chunks.Chunk
		score float64
	}
	scoreBoard := map[*chunks.Chunk]highScore{}

	for i := 0; i < len(sims); i++ {
		for j := i + 1; j < len(sims); j++ {
			score := similarityScore(sims[i].Data(), sims[j].Data())

			if scoreBoard[sims[i]] == (highScore{}) || score > scoreBoard[sims[i]].score {
				scoreBoard[sims[i]] = highScore{sims[j], score}
			}
			if scoreBoard[sims[j]] == (highScore{}) || score > scoreBoard[sims[j]].score {
				scoreBoard[sims[j]] = highScore{sims[i], score}
			}
		}
	}

	// gather scores, then sort
	scores := map[*chunks.Chunk][]*chunks.Chunk{}
	for k, v := range scoreBoard {
		scores[v.chunk] = append(scores[v.chunk], k)
	}
	sort.Slice(sims, func(i, j int) bool {
		return len(scores[sims[i]]) > len(scores[sims[j]])
	})

	groupSeq := 1

	leaders := map[int]*chunks.Chunk{}
	groups := map[int][]*chunks.Chunk{}
	similarityGroups := map[*chunks.Chunk]int{}
	for _, c := range sims {
		// loop over scores, and ensure no member of the set is already in a group.
		leader := c
		followers := scores[c]

		// If the leader is not in a group, then we can assign a new group.
		if similarityGroups[leader] == 0 {
			leaders[groupSeq] = leader
			similarityGroups[leader] = groupSeq
			groups[groupSeq] = append(groups[groupSeq], leader)
			for _, f := range followers {
				similarityGroups[f] = groupSeq
				groups[groupSeq] = append(groups[groupSeq], f)
			}
			groupSeq++
		} else {
			for _, f := range followers {
				similarityGroups[f] = similarityGroups[leader]
				groups[similarityGroups[leader]] = append(groups[similarityGroups[leader]], f)
			}
		}
	}

	// Now see if any group leaders are close enough to join groups
	for targetGrp := 1; targetGrp < groupSeq; targetGrp++ {
		if leader, ok := leaders[targetGrp]; ok {
			for merge := targetGrp + 1; merge < groupSeq; merge++ {
				if otherLeader, ok := leaders[merge]; ok {
					if similarityScore(leader.Data(), otherLeader.Data()) > scoreThreshold {
						// merge the groups
						for _, c := range groups[merge] {
							similarityGroups[c] = targetGrp
							groups[targetGrp] = append(groups[targetGrp], c)
						}
						delete(groups, merge)
						delete(leaders, merge)
					}
				}
			}
		}
	}

	var result []*chunkGroup
	for i := 1; i < groupSeq; i++ {
		if leader, ok := leaders[i]; ok {
			if len(groups[i]) >= 7 {
				cg := buildChunkGroup(leader, groups[i])
				result = append(result, cg)
			}
		}
	}
	return result
}
*/

// Given two byte slices, return a similarity score between 0 and 1. 1 means the slices are identical, 0 means they are
// completely different.
func similarityScore(a, b []byte) float64 {

	maxLen := max(len(a), len(b))

	lev := levenshteinDistance(a, b)

	levScore := float64(maxLen-lev) / float64(maxLen)
	return levScore
}

func levenshteinDistance(a, b []byte) int {
	m, n := len(a), len(b)
	if m == 0 {
		return n
	}
	if n == 0 {
		return m
	}

	lev := 0

	if m == n {
		// If the lengths are the same, we can just compare the bytes. Saves allocation, and turns out to be pretty common.
		for i := 0; i < m; i++ {
			if a[i] != b[i] {
				lev++
			}
		}
	} else {
		matrix := make([][]int, m+1)
		for i := range matrix {
			matrix[i] = make([]int, n+1)
			matrix[i][0] = i
		}
		for j := 0; j <= n; j++ {
			matrix[0][j] = j
		}

		for i := 1; i <= m; i++ {
			for j := 1; j <= n; j++ {
				cost := 0
				if a[i-1] != b[j-1] {
					cost = 1
				}
				matrix[i][j] = min(matrix[i-1][j]+1, matrix[i][j-1]+1, matrix[i-1][j-1]+cost)
			}
		}

		lev = matrix[m][n]
	}

	return lev
}
