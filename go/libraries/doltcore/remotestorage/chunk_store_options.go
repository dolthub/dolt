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

// ChunkStoreOption configures a DoltChunkStore that NewDoltChunkStoreFromPath opens.
type ChunkStoreOption func(*chunkStoreOptions)

type chunkStoreOptions struct {
	format repoFormat
}

func chunkStoreOptionsOf(opts []ChunkStoreOption) chunkStoreOptions {
	o := chunkStoreOptions{format: storedFormat{}}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// repoFormat is how a DoltChunkStore reads and writes the records of a remote repository.
type repoFormat interface {
	// clientFormat returns |f|, the format that the store sends a server, which creates a missing repository in it.
	clientFormat(f *remotesapi.ClientRepoFormat) *remotesapi.ClientRepoFormat
	// opened returns the format of the repository |md| describes, or an error if the store cannot use it. The store
	// calls opened on the format it returned to check the metadata of each refresh.
	opened(md *remotesapi.GetRepoMetadataResponse) (repoFormat, error)
	// writeChunks returns the table file of |chnks| to upload.
	writeChunks(chnks []chunks.Chunk) (name string, data []byte, splitOffset uint64, err error)
	// addLocationKeys adds to the downloads |locs| the keys that |resp| locates their records with. |keysByID| holds
	// the keys of the stream that |resp| is part of.
	addLocationKeys(locs []*remotesapi.DownloadLoc, req *remotesapi.StreamChunkLocationsRequest, resp *remotesapi.StreamChunkLocationsResponse, tfByID map[uint32]*tableFileRec, keysByID map[uint32][]byte) error
	// addRangeKeys sets the keys of the ranges that |loc| added to |d|.
	addRangeKeys(d downloads, loc *remotesapi.DownloadLoc) error
	// wrap returns the callback that delivers the ranges |resp| fetches, which |cb| delivers if they need no key.
	wrap(cb func(context.Context, []byte, *Range) error, resp fetchResp, chunkCh chan nbs.ToChunker) func(context.Context, []byte, *Range) error
}

// storedFormat is the format of repositories whose records are stored as they are.
type storedFormat struct{}

func (storedFormat) clientFormat(f *remotesapi.ClientRepoFormat) *remotesapi.ClientRepoFormat {
	return f
}

func (storedFormat) opened(*remotesapi.GetRepoMetadataResponse) (repoFormat, error) {
	return storedFormat{}, nil
}

func (storedFormat) writeChunks(chnks []chunks.Chunk) (string, []byte, uint64, error) {
	return nbs.WriteChunks(chnks)
}

func (storedFormat) addLocationKeys([]*remotesapi.DownloadLoc, *remotesapi.StreamChunkLocationsRequest, *remotesapi.StreamChunkLocationsResponse, map[uint32]*tableFileRec, map[uint32][]byte) error {
	return nil
}

func (storedFormat) addRangeKeys(downloads, *remotesapi.DownloadLoc) error {
	return nil
}

func (storedFormat) wrap(cb func(context.Context, []byte, *Range) error, _ fetchResp, _ chan nbs.ToChunker) func(context.Context, []byte, *Range) error {
	return cb
}
