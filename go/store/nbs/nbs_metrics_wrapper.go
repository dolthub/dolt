// Copyright 2020 Liquidata, Inc.
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
	"io"
	"sync/atomic"

	"github.com/dolthub/dolt/go/store/chunks"

	"github.com/dolthub/dolt/go/store/hash"
)

// NBSMetricWrapper is a ChunkStore implementation that wraps a ChunkStore, and collects metrics on the calls.
type NBSMetricWrapper struct {
	*chunks.CSMetricWrapper
	nbs *NomsBlockStore
}

// NewCSMetricWrapper returns a new NBSMetricWrapper
func NewNBSMetricWrapper(nbs *NomsBlockStore) *NBSMetricWrapper {
	csMW := chunks.NewCSMetricWrapper(nbs)
	return &NBSMetricWrapper{
		csMW,
		nbs,
	}
}

var _ TableFileStore = &NBSMetricWrapper{}
var _ chunks.ChunkStoreGarbageCollector = &NBSMetricWrapper{}

// Sources retrieves the current root hash, and a list of all the table files
func (nbsMW *NBSMetricWrapper) Sources(ctx context.Context) (hash.Hash, []TableFile, error) {
	return nbsMW.nbs.Sources(ctx)
}

func (nbsMW *NBSMetricWrapper) Size(ctx context.Context) (uint64, error) {
	return nbsMW.nbs.Size(ctx)
}

// WriteTableFile will read a table file from the provided reader and write it to the TableFileStore
func (nbsMW *NBSMetricWrapper) WriteTableFile(ctx context.Context, fileId string, numChunks int, rd io.Reader, contentLength uint64, contentHash []byte) error {
	return nbsMW.nbs.WriteTableFile(ctx, fileId, numChunks, rd, contentLength, contentHash)
}

// SetRootChunk changes the root chunk hash from the previous value to the new root.
func (nbsMW *NBSMetricWrapper) SetRootChunk(ctx context.Context, root, previous hash.Hash) error {
	return nbsMW.nbs.SetRootChunk(ctx, root, previous)
}

// Forwards SupportedOperations to wrapped block store.
func (nbsMW *NBSMetricWrapper) SupportedOperations() TableFileStoreOps {
	return nbsMW.nbs.SupportedOperations()
}

func (nbsMW *NBSMetricWrapper) MarkAndSweepChunks(ctx context.Context, last hash.Hash, keepChunks <-chan hash.Hash, errChan chan<- error) error {
	return nbsMW.nbs.MarkAndSweepChunks(ctx, last, keepChunks, errChan)
}

// PruneTableFiles deletes old table files that are no longer referenced in the manifest.
func (nbsMW *NBSMetricWrapper) PruneTableFiles(ctx context.Context) error {
	return nbsMW.nbs.PruneTableFiles(ctx)
}

// GetManyCompressed gets the compressed Chunks with |hashes| from the store. On return,
// |foundChunks| will have been fully sent all chunks which have been
// found. Any non-present chunks will silently be ignored.
func (nbsMW *NBSMetricWrapper) GetManyCompressed(ctx context.Context, hashes hash.HashSet, cmpChChan chan<- CompressedChunk) error {
	atomic.AddInt32(&nbsMW.TotalChunkGets, int32(len(hashes)))
	return nbsMW.nbs.GetManyCompressed(ctx, hashes, cmpChChan)
}
