// Copyright 2026 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

// StoreOption configures a NomsBlockStore as it is created.
type StoreOption func(*storeOptions)

type storeOptions struct{}

func (nbs *NomsBlockStore) applyOptions(opts []StoreOption) {
	for _, opt := range opts {
		opt(&nbs.opts)
	}
}

// The store calls the functions below where its options take effect. Dolt defines no options, so they change nothing.

func (nbs *NomsBlockStore) journalFormat(dir, nbfVers string) (string, error)          { return nbfVers, nil }
func (nbs *NomsBlockStore) configurePersister(p tablePersister)                        {}
func (nbs *NomsBlockStore) checkManifest(exists bool, contents manifestContents) error { return nil }
func (nbs *NomsBlockStore) checkWrite() error                                          { return nil }

// checkAddedFiles runs under the store lock; |commit| runs as the files' manifest update returns.
func (nbs *NomsBlockStore) checkAddedFiles(added chunkSourceSet) (commit func(*error, *bool), err error) {
	return func(*error, *bool) {}, nil
}

func (nbs *NomsBlockStore) openAddedChunkSources(ctx context.Context, files map[hash.Hash]uint32, existing chunkSourceSet) (openChunkSourcesResult, error) {
	return nbs.openChunkSourcesForManifestUpdate(ctx, files, existing)
}

func (nbs *NomsBlockStore) checkGC(dest *NomsBlockStore) error { return nil }

func (nbs *NomsBlockStore) newGCCopier(level chunks.GCArchiveLevel, tfp tableFilePersister) (*gcCopier, error) {
	return newGarbageCollectionCopier(level, tfp)
}

func (nbs *NomsBlockStore) encrypted() bool                     { return false }
func (nbs *NomsBlockStore) canReadChunks() bool                 { return true }
func addedFileOpts(opts openOpts) openOpts                      { return opts }
func tableFileSplitOffset(cs chunkSource, offset uint64) uint64 { return offset }
