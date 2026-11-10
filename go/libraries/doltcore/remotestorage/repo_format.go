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

package remotestorage

import (
	"context"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/nbs"
)

// repoFormat is how a DoltChunkStore reads and writes the records of a remote repository. A store's format is shared
// by every ChunkFetcher of the store: wrap runs on every download thread at once, while the key hooks of one fetcher
// run on a single goroutine.
type repoFormat interface {
	// clientFormat returns |f|, the format that the store sends a server, which creates a missing repository in it.
	clientFormat(f *remotesapi.ClientRepoFormat) *remotesapi.ClientRepoFormat
	// opened returns the format of the repository |md| describes, or an error if the store cannot use it. The store
	// calls opened on the format it returned to check the metadata of each refresh.
	opened(md *remotesapi.GetRepoMetadataResponse) (repoFormat, error)
	// writeChunks returns the table file of |chnks| to upload.
	writeChunks(ctx context.Context, chnks []chunks.Chunk) (name string, data []byte, splitOffset uint64, err error)
	// newStreamKeys returns what adds record keys to the downloads of one StreamChunkLocations stream.
	newStreamKeys() streamKeys
	// rangeKeys returns the keys that open the record and dictionary of each of |hgr|'s ranges, or nil if none.
	rangeKeys(hgr *remotesapi.HttpGetRange) ([]rangeKeys, error)
	// wrap returns the callback that delivers the ranges |resp| fetches, which |cb| delivers if they need no key.
	wrap(cb func(context.Context, []byte, *Range) error, resp fetchResp, chunkCh chan nbs.ToChunker) func(context.Context, []byte, *Range) error
}

// streamKeys adds to |locs| the keys that |resp|, the answer to |req|, locates their records with.
type streamKeys func(locs []*remotesapi.DownloadLoc, req *remotesapi.StreamChunkLocationsRequest, resp *remotesapi.StreamChunkLocationsResponse, tfByID map[uint32]*tableFileRec) error

// rangeKeys holds the keys of a range's record and its dictionary.
type rangeKeys struct {
	key, dict *[]byte
}

// plainFormat is the format of repositories whose records are stored as they are.
type plainFormat struct{}

func (plainFormat) clientFormat(f *remotesapi.ClientRepoFormat) *remotesapi.ClientRepoFormat {
	return f
}

func (plainFormat) opened(*remotesapi.GetRepoMetadataResponse) (repoFormat, error) {
	return plainFormat{}, nil
}

func (plainFormat) writeChunks(_ context.Context, chnks []chunks.Chunk) (string, []byte, uint64, error) {
	return nbs.WriteChunks(chnks)
}

func (plainFormat) newStreamKeys() streamKeys {
	return func([]*remotesapi.DownloadLoc, *remotesapi.StreamChunkLocationsRequest, *remotesapi.StreamChunkLocationsResponse, map[uint32]*tableFileRec) error {
		return nil
	}
}

func (plainFormat) rangeKeys(*remotesapi.HttpGetRange) ([]rangeKeys, error) {
	return nil, nil
}

func (plainFormat) wrap(cb func(context.Context, []byte, *Range) error, _ fetchResp, _ chan nbs.ToChunker) func(context.Context, []byte, *Range) error {
	return cb
}
