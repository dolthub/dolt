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

package remotesrv

import (
	"context"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

// checkClient refuses a client with |caps| that cannot read the records of |cs|. Dolt refuses none.
var checkClient = func(cs RemoteSrvStore, caps []remotesapi.ClientCapability) error {
	return nil
}

// checkClientRepoFormat validates the format a client sends with GetRepoMetadata beyond what Dolt checks.
var checkClientRepoFormat = func(f *remotesapi.ClientRepoFormat) error {
	return nil
}

// withClientRepoFormat returns |ctx| for opening or creating the repository of a client that sent format |f|.
var withClientRepoFormat = func(ctx context.Context, f *remotesapi.ClientRepoFormat) context.Context {
	return ctx
}

// addRefusal returns the error to send a client for table files a store refused to add with |err|, or nil to send
// an internal error.
var addRefusal = func(err error) error {
	return nil
}

// newChunkLocator returns the chunk locator of one download request or stream.
var newChunkLocator = func() chunkLocator {
	return storeLocator{}
}

// chunkLocator locates the records of chunks, and adds the keys that opening them takes, which Dolt's records lack.
type chunkLocator interface {
	// locate returns the locations of |hashes| in |cs|, deleting from |hashes| those it finds.
	locate(ctx context.Context, cs RemoteSrvStore, hashes hash.HashSet) (map[string]map[hash.Hash]nbs.Range, error)
	// addRangeKeys adds to |hgr| the keys of the records it locates, which the last locate found.
	addRangeKeys(hgr *remotesapi.HttpGetRange) error
	// addLocationKeys adds to |resp| the keys of the records it locates for |chunkHashes|, which the last locate
	// found, numbered across the locator's stream.
	addLocationKeys(resp *remotesapi.StreamChunkLocationsResponse, chunkHashes []byte) error
}

type storeLocator struct{}

func (storeLocator) locate(ctx context.Context, cs RemoteSrvStore, hashes hash.HashSet) (map[string]map[hash.Hash]nbs.Range, error) {
	return cs.GetChunkLocationsWithPaths(ctx, hashes)
}

func (storeLocator) addRangeKeys(*remotesapi.HttpGetRange) error {
	return nil
}

func (storeLocator) addLocationKeys(*remotesapi.StreamChunkLocationsResponse, []byte) error {
	return nil
}
