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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/dolthub/gozstd"
	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/cmd/dolt/doltversion"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

const defaultDictionarySize = 1 << 12 // NM4 - maybe just select the largest chunk. TBD.
const maxSamples = 1000
const minSamples = 25

func UnArchive(ctx context.Context, cs chunks.ChunkStore, smd StorageMetadata, progress chan interface{}) error {
	if gs, ok := cs.(*GenerationalNBS); ok {
		outPath, _ := gs.oldGen.Path()
		oldgen := gs.oldGen.tables.upstream

		swapMap := make(map[hash.Hash]hash.Hash)

		revertMap := smd.RevertMap()

		for id, ogcs := range oldgen {
			if arc, ok := ogcs.(archiveChunkSource); ok {
				orginTfId := revertMap[id]
				exists, err := smd.oldGenTableExists(orginTfId)
				if err != nil {
					return err
				}
				if exists {
					// We have a fast path to follow because oritinal table file is still on disk.
					swapMap[arc.hash()] = orginTfId
				} else {
					// We don't have the original table file id, so we have to create a new one.
					classicTable, err := NewCmpChunkTableWriter("")
					if err != nil {
						return err
					}

					err = arc.iterate(ctx, func(chk chunks.Chunk) error {
						cmpChk := ChunkToCompressedChunk(chk)
						err := classicTable.AddCmpChunk(cmpChk)
						if err != nil {
							return err
						}

						progress <- fmt.Sprintf("Unarchiving %s (bytes: %d)", chk.Hash().String(), len(chk.Data()))
						return nil
					})
					if err != nil {
						return err
					}

					id, err := classicTable.Finish()
					if err != nil {
						return err
					}
					err = classicTable.FlushToFile(filepath.Join(outPath, id))
					if err != nil {
						return err
					}

					swapMap[arc.hash()] = hash.Parse(id)
				}
			}
		}

		if len(swapMap) > 0 {
			//NM4 TODO: This code path must only be run on an offline database. We should add a check for that.
			specs, err := gs.oldGen.tables.toSpecs()
			newSpecs := make([]tableSpec, 0, len(specs))
			for _, spec := range specs {
				if newSpec, exists := swapMap[spec.name]; exists {
					newSpecs = append(newSpecs, tableSpec{newSpec, spec.chunkCount})
				} else {
					newSpecs = append(newSpecs, spec)
				}
			}
			err = gs.oldGen.swapTables(ctx, newSpecs, chunks.GCMode_Default)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func BuildArchive(ctx context.Context, cs chunks.ChunkStore, dagGroups *ChunkRelations, progress chan interface{}) (err error) {
	// Currently, we don't have any stats to report. Required for calls to the lower layers tho.
	var stats Stats

	if gs, ok := cs.(*GenerationalNBS); ok {
		outPath, _ := gs.oldGen.Path()
		oldgen := gs.oldGen.tables.upstream

		swapMap := make(map[hash.Hash]hash.Hash)

		for tf, ogcs := range oldgen {
			if _, ok := ogcs.(archiveChunkSource); ok {
				continue
			}

			idx, err := ogcs.index()
			if err != nil {
				return err
			}

			originalSize := idx.tableFileSize()

			archivePath := ""
			archiveName := hash.Hash{}
			archivePath, archiveName, err = convertTableFileToArchive(ctx, ogcs, idx, dagGroups, outPath, progress, &stats)
			if err != nil {
				return err
			}

			fileInfo, err := os.Stat(archivePath)
			if err != nil {
				progress <- "Failed to stat archive file"
				return err
			}
			archiveSize := fileInfo.Size()

			err = verifyAllChunks(idx, archivePath, progress)
			if err != nil {
				return err
			}

			percentReduction := -100.0 * (float64(archiveSize)/float64(originalSize) - 1.0)
			progress <- fmt.Sprintf("Archived %s (%d -> %d bytes, %.2f%% reduction)", archiveName, originalSize, archiveSize, percentReduction)

			swapMap[tf] = archiveName
		}

		if len(swapMap) == 0 {
			return fmt.Errorf("No tables found to archive. Run 'dolt gc' first")
		}

		//NM4 TODO: This code path must only be run on an offline database. We should add a check for that.
		specs, err := gs.oldGen.tables.toSpecs()
		newSpecs := make([]tableSpec, 0, len(specs))
		for _, spec := range specs {
			if newSpec, exists := swapMap[spec.name]; exists {
				newSpecs = append(newSpecs, tableSpec{newSpec, spec.chunkCount})
			} else {
				newSpecs = append(newSpecs, spec)
			}
		}
		err = gs.oldGen.swapTables(ctx, newSpecs, chunks.GCMode_Default)
		if err != nil {
			return err
		}
	} else {
		return errors.New("Modern DB Expected")
	}
	return nil
}

func convertTableFileToArchive(
	ctx context.Context,
	cs chunkSource,
	idx tableIndex,
	dagGroups *ChunkRelations,
	archivePath string,
	progress chan interface{},
	stats *Stats,
) (string, hash.Hash, error) {
	allChunks, defaultSamples, err := gatherAllChunks(ctx, cs, idx, stats)
	if err != nil {
		return "", hash.Hash{}, err
	}

	var defaultDict []byte
	if len(defaultSamples) >= minSamples {
		defaultDict = buildDictionary(defaultSamples)
	} else {
		return "", hash.Hash{}, errors.New("Not enough samples to build default dictionary")
	}
	defaultSamples = nil

	defaultCDict, err := gozstd.NewCDict(defaultDict)
	if err != nil {
		return "", hash.Hash{}, err
	}

	cgList, err := dagGroups.convertToChunkGroups(ctx, allChunks, defaultCDict, progress, stats)
	if err != nil {
		return "", hash.Hash{}, err
	}
	sort.Slice(cgList, func(i, j int) bool {
		return cgList[i].totalBytesSavedWDict > cgList[j].totalBytesSavedWDict
	})

	// NM4 - this is helpful info to ensure that the groups are being built correctly. Maybe write to a log, or a channel.
	// for n, cg := range cgList {
	//	cg.print(n, p)
	//}

	const fourMb = 1 << 22

	// Allocate buffer used to compress chunks.
	cmpBuff := make([]byte, 0, fourMb)

	cmpBuff = gozstd.Compress(cmpBuff[:0], defaultDict)
	// p("Default Dict Raw vs Compressed: %d , %d\n", len(defaultDict), len(cmpDefDict))

	arcW, err := newArchiveWriter()
	if err != nil {
		return "", hash.Hash{}, err
	}
	var defaultDictByteSpanId uint32
	defaultDictByteSpanId, err = arcW.writeByteSpan(cmpBuff)
	if err != nil {
		return "", hash.Hash{}, err
	}

	_, grouped, singles, err := writeDataToArchive(ctx, cmpBuff[:0], allChunks, cgList, defaultDictByteSpanId, defaultCDict, arcW, progress, stats)
	if err != nil {
		return "", hash.Hash{}, err
	}

	err = indexAndFinalizeArchive(arcW, archivePath, cs.hash())
	if err != nil {
		return "", hash.Hash{}, err
	}

	if grouped+singles != idx.chunkCount() {
		// Leaving as a panic. This should never happen.
		missing := idx.chunkCount() - (grouped + singles)
		panic(fmt.Sprintf("chunk count mismatch. Missing: %d", missing))
	}

	name, err := arcW.getName()
	if err != nil {
		return "", hash.Hash{}, err
	}

	return arcW.finalPath, name, err
}

// indexAndFinalizeArchive writes the index, metadata, and footer to the archive file. It also flushes the archive writer
// to the directory provided. The name is calculated from the footer, and can be obtained by calling getName on the archive.
func indexAndFinalizeArchive(arcW *archiveWriter, archivePath string, originTableFile hash.Hash) error {
	err := arcW.finalizeByteSpans()
	if err != nil {
		return err
	}

	err = arcW.writeIndex()
	if err != nil {
		return err
	}

	meta := map[string]string{
		amdkDoltVersion:     doltversion.Version,
		amdkOriginTableFile: originTableFile.String(),
		amdkConversionTime:  time.Now().UTC().Format(time.RFC3339),
	}
	jsonData, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	err = arcW.writeMetadata(jsonData)
	if err != nil {
		return err
	}

	err = arcW.writeFooter()
	if err != nil {
		return err
	}

	fileName, err := arcW.genFileName(archivePath)
	if err != nil {
		return err
	}

	return arcW.flushToFile(fileName)
}

func writeDataToArchive(
	ctx context.Context,
	cmpBuff []byte,
	chunkCache *simpleChunkSourceCache,
	cgList []*chunkGroup,
	defaultSpanId uint32,
	defaultDict *gozstd.CDict,
	arcW *archiveWriter,
	progress chan interface{},
	stats *Stats,
) (groupCount, groupedChunkCount, individualChunkCount uint32, err error) {
	var allChunks hash.HashSet
	allChunks, err = chunkCache.addresses()
	if err != nil {
		return 0, 0, 0, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	possibleGroupCount := len(cgList)
	groupsCompleted := int32(0)

	for _, cg := range cgList {
		select {
		case <-ctx.Done():
			return 0, 0, 0, ctx.Err()
		default:
			if cg.totalBytesSavedWDict > cg.totalBytesSavedDefaultDict {
				groupCount++

				cmpBuff = gozstd.Compress(cmpBuff[:0], cg.dict)

				dictId, err := arcW.writeByteSpan(cmpBuff)
				if err != nil {
					return 0, 0, 0, err
				}

				for _, cs := range cg.chks {
					c, e2 := chunkCache.get(ctx, cs.chunkId, stats)
					if e2 != nil {
						return 0, 0, 0, e2
					}

					if !arcW.chunkSeen(cs.chunkId) {
						cmpBuff = gozstd.CompressDict(cmpBuff[:0], c.Data(), cg.cDict)

						dataId, err := arcW.writeByteSpan(cmpBuff)
						if err != nil {
							return 0, 0, 0, err
						}
						err = arcW.stageChunk(cs.chunkId, dictId, dataId)
						if err != nil {
							return 0, 0, 0, err
						}
						groupedChunkCount++
					}
					allChunks.Remove(cs.chunkId)
				}
			}
			groupsCompleted++
			progress <- ArchiveBuildProgressMsg{Stage: "Materializing Chunk Groups", Total: int32(possibleGroupCount), Completed: groupsCompleted}
		}
	}

	ungroupedChunkCount := int32(len(allChunks))
	ungroupedChunkProgress := int32(0)

	// Any chunks remaining will be written out individually.
	for h := range allChunks {
		select {
		case <-ctx.Done():
			return 0, 0, 0, ctx.Err()
		default:
			dictId := uint32(0)

			c, e2 := chunkCache.get(ctx, h, stats)
			if e2 != nil {
				return 0, 0, 0, e2
			}

			cmpBuff = gozstd.CompressDict(cmpBuff[:0], c.Data(), defaultDict)
			dictId = defaultSpanId

			id, err := arcW.writeByteSpan(cmpBuff)
			if err != nil {
				return 0, 0, 0, err
			}
			err = arcW.stageChunk(h, dictId, id)
			if err != nil {
				return 0, 0, 0, err
			}

			ungroupedChunkProgress++
			progress <- ArchiveBuildProgressMsg{Stage: "Writing Ungrouped Chunks", Total: ungroupedChunkCount, Completed: ungroupedChunkProgress}
		}
	}

	individualChunkCount = uint32(len(allChunks))

	return
}

// gatherAllChunks reads all the chunks from the chunk source and returns them in a map. The map is keyed by the hash of
// the chunk. This is going to take up a bunch of memory.
// It also returns a list of default samples, which are the first 1000 chunks that are read. These are used to build the default dictionary.
func gatherAllChunks(ctx context.Context, cs chunkSource, idx tableIndex, stats *Stats) (*simpleChunkSourceCache, []*chunks.Chunk, error) {
	sampleCount := min(idx.chunkCount(), maxSamples)

	defaultSamples := make([]*chunks.Chunk, 0, sampleCount)
	for i := uint32(0); i < sampleCount; i++ {
		var h hash.Hash
		_, err := idx.indexEntry(i, &h)
		if err != nil {
			return nil, nil, err
		}

		bytes, err := cs.get(ctx, h, stats)
		if err != nil {
			return nil, nil, err
		}

		chk := chunks.NewChunk(bytes)
		defaultSamples = append(defaultSamples, &chk)
	}

	chkCache, err := newSimpleChunkSourceCache(cs)
	if err != nil {
		return nil, nil, err
	}

	return chkCache, defaultSamples, nil
}
func verifyAllChunks(idx tableIndex, archiveFile string, progress chan interface{}) error {
	file, err := os.Open(archiveFile)
	if err != nil {
		return err
	}

	stat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()

	index, err := newArchiveReader(file, uint64(fileSize))
	if err != nil {
		return err
	}

	hashList := make([]hash.Hash, 0, idx.chunkCount())

	for i := uint32(0); i < idx.chunkCount(); i++ {
		var h hash.Hash
		_, err := idx.indexEntry(i, &h)
		if err != nil {
			return err
		}

		hashList = append(hashList, h)
	}

	rand.Shuffle(len(hashList), func(i, j int) {
		hashList[i], hashList[j] = hashList[j], hashList[i]
	})

	chunkCount := int32(idx.chunkCount())
	chunkProgress := int32(0)

	for _, h := range hashList {
		if !index.has(h) {
			msg := fmt.Sprintf("chunk not found in archive: %s", h.String())
			return errors.New(msg)
		}

		data, err := index.get(h)
		if err != nil {
			return fmt.Errorf("error reading chunk: %s (err: %w)", h.String(), err)
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

		chunkProgress++
		progress <- ArchiveBuildProgressMsg{Stage: "Verifying Chunks", Total: chunkCount, Completed: chunkProgress}
	}

	return nil
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

type ArchiveBuildProgressMsg struct {
	Stage     string
	Total     int32
	Completed int32
}

func (cr *ChunkRelations) convertToChunkGroups(
	ctx context.Context,
	chks *simpleChunkSourceCache,
	defaultDict *gozstd.CDict,
	progress chan interface{},
	stats *Stats,
) ([]*chunkGroup, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	result := make([]*chunkGroup, 0, cr.Count())

	groups := cr.groups()

	if len(groups) > 0 {
		groupCount := int32(len(groups))
		completedGroupCount := int32(0)

		groupChannel := make(chan hash.HashSet, len(groups))
		resultChannel := make(chan *chunkGroup, len(groups))

		// Start worker goroutines
		numThreads := 1 // TODO: Fix CGO zstd lib to handle multiple dictionary build threads.
		g, egCtx := errgroup.WithContext(ctx)
		for i := 0; i < numThreads; i++ {
			g.Go(func() error {
				for {
					select {
					case hs, ok := <-groupChannel:
						if !ok {
							return nil
						}
						if len(hs) > 1 {
							chkGrp, err := newChunkGroup(egCtx, chks, hs, defaultDict, stats)
							if err != nil {
								return err
							}
							atomic.AddInt32(&completedGroupCount, 1)
							progress <- ArchiveBuildProgressMsg{Stage: "Building Chunk Group Dictionaries", Total: groupCount, Completed: completedGroupCount}
							resultChannel <- chkGrp
						}
					case <-egCtx.Done():
						return egCtx.Err()
					}
				}
			})
		}

		// Send groups to process
		for _, v := range groups {
			groupChannel <- v
		}
		close(groupChannel)

		err := g.Wait()
		if err != nil {
			return nil, err
		}

		close(resultChannel)

		pullResult := true
		for pullResult {
			select {
			case group, ok := <-resultChannel:
				if !ok {
					pullResult = false
					break
				}
				result = append(result, group)
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	} else {
		progress <- ArchiveBuildProgressMsg{Stage: "Chunk Grouping Skipped", Total: -1, Completed: -1}
	}

	return result, nil
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

	bytes, err := csc.cs.get(ctx, h, stats)
	if bytes == nil || err != nil {
		return nil, err
	}

	chk := chunks.NewChunk(bytes)
	csc.cache.Add(h, &chk)
	return &chk, nil
}

// has returns true if the chunk is in the ChunkSource. This is not related to what is cached, just a helper.
func (csc *simpleChunkSourceCache) has(h hash.Hash) (bool, error) {
	return csc.cs.has(h)
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
