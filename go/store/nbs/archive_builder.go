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
	"io"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/valyala/gozstd"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

type PrintfFunc func(format string, args ...interface{})

func RunExperiment(cs chunks.ChunkStore, p PrintfFunc) (err error) {
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
		panic(fmt.Sprintf("Modern DB Expected"))
	}

	return
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
		compressed := gozstd.Compress(cmpBuff, c.Data())
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
