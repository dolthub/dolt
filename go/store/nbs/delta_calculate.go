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
	"fmt"

	"github.com/balacode/go-delta"
	"github.com/dolthub/dolt/go/store/chunks"
)

const deltaPrefixLen = 16

type bprefix [deltaPrefixLen]byte

type addrBytes struct {
	a addr
	b []byte
}

type PrintfFunc func(format string, args ...interface{})

func RunExperiment(cs chunks.ChunkStore, p PrintfFunc) error {
	if gs, ok := cs.(*GenerationalNBS); ok {
		oldgen := gs.oldGen.tables.upstream

		prefixMap := make(map[bprefix][]addrBytes)

		uncompressedTotal := uint64(0)
		for tf, cs := range oldgen {
			p("table file: %s\n", tf.String())

			idx, err := cs.index()
			if err != nil {
				panic(err)
			}

			largeChunks := 0
			for i := uint32(0); i < idx.chunkCount(); i++ {
				var a addr
				_, err := idx.indexEntry(i, &a)
				if err != nil {
					panic(err)
				}
				var stat Stats
				bytes, err := cs.get(context.TODO(), a, &stat)
				if err != nil {
					panic(err)
				}

				if len(bytes) > 512 { // NM4 - try other values? Smaller? Bigger?
					prefix := bprefix(bytes[2 : deltaPrefixLen+2])
					prefixMap[prefix] = append(prefixMap[prefix], addrBytes{a, bytes})
					largeChunks++
				} else {
					uncompressedTotal += uint64(len(bytes))
				}
			}

			progress := 0
			sumSaved := uint64(0)
			for pfx, group := range prefixMap {
				total, saved := biteSizeEvaluate(group, .75)
				progress += len(group)
				p("Group: %v (%d/%d chunks) saved: %d of %d\n", pfx, progress, largeChunks, saved, total)
				sumSaved += saved
				uncompressedTotal += total
			}

			savings := 100.0 * (1.0 - float64(uncompressedTotal-sumSaved)/float64(uncompressedTotal))
			p("Total saved: %d of %d (%.2f%% reduction)\n", sumSaved, uncompressedTotal, savings)
		}

	} else {
		panic(fmt.Sprintf("Use a modern db brah"))
	}

	return nil
}

func biteSizeEvaluate(group []addrBytes, threshold float64) (uncompressedTotal, saved uint64) {
	if len(group) < 100 {
		return bruteForceGroup(group, threshold)
	} else {
		mid := len(group) / 2

		uncompressedTotal, saved = biteSizeEvaluate(group[:mid], threshold)

		uncompressedTotal2, saved2 := biteSizeEvaluate(group[mid:], threshold)

		uncompressedTotal += uncompressedTotal2
		saved += saved2

		return
	}
}

type savings struct {
	src        addr
	dst        addr
	bytesSaved uint64
	ratio      float64
}

func bruteForceGroup(group []addrBytes, threshold float64) (uncompressedTotal, saved uint64) {
	bestSavings := make(map[int]savings)
	for srcIndx, src := range group {
		uncompressedTotal += uint64(len(src.b))
		for dstIndx, dst := range group[srcIndx+1:] {
			dif := delta.Make(src.b, dst.b)
			difBytes := dif.Bytes()

			// NM4 - 64 is arbitrary. Based on the diff, it's probably not worth the compute time to store as a delta.
			if len(difBytes)+64 < len(dst.b) {
				deltaRatio := float64(len(difBytes)) / float64(len(dst.b))

				if deltaRatio <= threshold {
					// NM4 - 32 is a guess of the bytes for the address + other serialization overhead
					s := savings{src.a, dst.a, uint64(len(dst.b) - len(difBytes) - 32), deltaRatio}

					if best, ok := bestSavings[srcIndx]; !ok || s.bytesSaved > best.bytesSaved {
						bestSavings[srcIndx+dstIndx] = s
					}

					/*
						p("Source: %d bytes\n", len(src.b))
						p("Destination: %d bytes\n", len(dst.b))
						p("Delta: %d bytes\n", len(difBytes))
						p("Total: %d bytes raw vs %d bytes delta compressed (%f)\n", totalBytes, compressedBytes, deltaRatio)
					*/
				}
			}

		}

		srcSavings, ok := bestSavings[srcIndx]
		if ok {
			saved += srcSavings.bytesSaved
		}
	}
	return
}
