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
	"os"
	"sort"
	"sync"

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

			err = copyAllChunks(ogcs, idx, outPath)
			if err != nil {
				return err
			}

			err = verifyAllChunks(idx, outPath)
			if err != nil {
				return err
			}

			p("Verified table file: %s\n", outPath)
		}

	} else {
		return errors.New("Modern DB Expected")
	}
	return nil
}

/*
	//////////////////////////

			p("table file: %s\n", tf.String())

			idx, err := ogcs.index()
			if err != nil {
				panic(err)
			}

			records := make([]getRecord, idx.chunkCount())
			for i := uint32(0); i < idx.chunkCount(); i++ {
				var h hash.Hash
				_, err := idx.indexEntry(i, &h)
				if err != nil {
					panic(err)
				}

				records = append(records, getRecord{&h, h.Prefix(), false})
			}

			group, ctx := errgroup.WithContext(context.TODO())

			lengthGroups := map[uint64][]*chunks.Chunk{}

			ungroupedChunks := map[hash.Hash]*chunks.Chunk{}
			preGroupedChunks := map[hash.Hash]*chunks.Chunk{}

			totalChunkCount := 0

			bottleneck := sync.Mutex{}
			var stats Stats
			allFound, err := ogcs.getMany(ctx, group, records, func(_ context.Context, c *chunks.Chunk) {
				bottleneck.Lock()
				defer bottleneck.Unlock()

				totalChunkCount++

				h := c.Hash()
				if dagGroups.Contains(h) {
					preGroupedChunks[h] = c
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

			cgList := dagGroups.convertToChunkGroups(preGroupedChunks)

			/*
				type groupStat struct {
					id    uint64
					count int
				}


					lenStat := []groupStat{}
					// Loop over lengths, and build an array of the chunk counts then sort them.
					for k, v := range lengthGroups {
						lenStat = append(lenStat, groupStat{k, len(v)})
					}
					sort.Slice(lenStat, func(i, j int) bool { return lenStat[i].count > lenStat[j].count })

					cgList := []*chunkGroup{}
					for _, v := range lenStat {
						similarGroups := levensteinGrouping(lengthGroups[v.id], levensteinThreshold)
						for _, cg := range similarGroups {
							if cg.cmpRatio > 0.70 {
								for _, c := range cg.rawChks {
									delete(ungroupedChunks, c)
									groupedChunks[c] = cg
								}
								cgList = append(cgList, cg)
							}
						}
					}

					prefixGroups := map[uint64][]*chunks.Chunk{}
					for c, _ := range ungroupedChunks {
						prefixGroups[c.Hash().Prefix()] = append(prefixGroups[c.Hash().Prefix()], c)
					}
					perfStats := []groupStat{}
					for k, v := range prefixGroups {
						perfStats = append(perfStats, groupStat{k, len(v)})
					}
					sort.Slice(perfStats, func(i, j int) bool { return perfStats[i].count > perfStats[j].count })
					for _, v := range perfStats {
						similarGroups := levensteinGrouping(prefixGroups[v.id], levensteinThreshold)
						for _, cg := range similarGroups {
							if cg.cmpRatio > 0.703 {
								for _, c := range cg.rawChks {
									delete(ungroupedChunks, c)
									groupedChunks[c] = cg
								}
								cgList = append(cgList, cg)
							}
						}
					}
			*/

			sort.Slice(cgList, func(i, j int) bool { return cgList[i].cmpRatio > cgList[j].cmpRatio })
			for _, cg := range cgList {
				cg.print(p)
			}

			unGroupCount := 0
			groupCount := 0

			////////////////////// NM4 FILE WRITE..............

			outName := fmt.Sprintf("%s.darc", tf.String())
			file, err := os.Create(outName)
			if err != nil {
				panic(err)
			}

			// Create an io.Writer that writes to the file
			writer := io.Writer(file)
			arcW := newArchiveWriter(writer)

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
				if cg.cmpRatio < dictCompressionThreshold {
					// Not a good group, just write the chunks out individually.
					for _, c := range cg.rawChks {
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
						}
					}
				} else {
					dictId, err := arcW.writeByteSpan(cg.dict)
					if err != nil {
						panic(err)
					}

					for _, c := range cg.rawChks {
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
						}
					}
				}
			}

			n, err := arcW.writeIndex()
			if err != nil {
				panic(err)
			}
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
		}
	}

	return
}

*/

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
		compressed, err := zCompress(cmpBuff, c.Data())
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

		data, err := index.get(h)
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

type chunkGroup struct {
	dict  []byte
	cDict *gozstd.CDict

	// NM4 The chunk which has the highest similarity to the rest of the group. Used to quickly determine if other chunks
	// could be added to this group. DEPRECATED.
	leader  *chunks.Chunk
	rawChks []*chunks.Chunk

	cmpRatio float64
}

func buildChunkGroup(leader *chunks.Chunk, chks []*chunks.Chunk) *chunkGroup {
	var cDict *gozstd.CDict
	samples := packSamples(chks)

	dict, cDict := buildDictionary(samples)

	result := chunkGroup{dict: dict, leader: leader, rawChks: chks, cmpRatio: -1.0, cDict: cDict}
	result.calcRatio()
	return &result
}

// For whatever reason, we need 7 or more samples to build a dictionary. But in principle we only need 1. So duplicate
// the samples until we have enough. Note that we need to add each chunk the same number of times do we don't end up
// with bias in the dictionary.
func packSamples(chks []*chunks.Chunk) []*chunks.Chunk {
	samples := []*chunks.Chunk{}
	for len(samples) < 7 {
		for _, c := range chks {
			samples = append(samples, c)
		}
	}
	return samples
}

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

func (cg *chunkGroup) calcRatio() {
	raw := 0
	cmpSize := 0
	for _, c := range cg.rawChks {
		d := c.Data()

		raw += len(d)

		comp := gozstd.CompressDict(nil, d, cg.cDict)
		cmpSize += len(comp)
	}

	cmpSize += len(cg.dict)
	cg.cmpRatio = float64(raw-cmpSize) / float64(raw)
}

func (cg *chunkGroup) addChunk(c *chunks.Chunk) {
	cg.rawChks = append(cg.rawChks, c)
	cg.dict, cg.cDict = buildDictionary(cg.rawChks)
	cg.calcRatio()
}

func (cg *chunkGroup) print(p PrintfFunc) {
	if cg.cmpRatio > dictCompressionThreshold {
		p("------------ GROUP ------------------\n")
		p("leader: %s\n", cg.leader.Hash().String())
		p("dict: %d\n", len(cg.dict))
		p("cmpRatio: %f\n", cg.cmpRatio)
		p("chunks: %d\n", len(cg.rawChks))
	}
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

		result = append(result, buildChunkGroup(c[0], c))
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
			score := diffScore(sims[i].Data(), sims[j].Data())

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
					if diffScore(leader.Data(), otherLeader.Data()) > scoreThreshold {
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

func diffScore(a, b []byte) float64 {

	maxLen := max(len(a), len(b))

	lev := levenshteinDistance(a, b)

	levScore := float64(maxLen-lev) / float64(maxLen)
	return levScore
}

func levenshteinDistance(a, b []byte) int {
	m, n := len(a), len(b)
	if m == 0 {
		panic("a is empty")
	}
	if n == 0 {
		panic("b is empty")
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
*/
