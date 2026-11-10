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

// storeFormats is how the server treats the formats of the stores it serves. Dolt Enterprise replaces it.
var storeFormats serverFormats = plainFormats{}

type serverFormats interface {
	// refuseClient returns the error to send a client with |caps| that cannot read a store of format |nbfVersion|.
	refuseClient(nbfVersion string, caps []remotesapi.ClientCapability) error
	// refusal returns the error to send a client for files a store refused to add with |err|, or nil to send |err|.
	refusal(err error) error
	// locator returns the chunkLocator of one download request or stream.
	locator() chunkLocator
}

// chunkLocator locates chunks and sets the keys of their records, which plaintext records lack, in the messages that
// carry their locations.
type chunkLocator interface {
	// locate returns the locations of |hashes| in |cs|, deleting those it finds from |hashes|.
	locate(ctx context.Context, cs RemoteSrvStore, hashes hash.HashSet) (map[string]map[hash.Hash]nbs.Range, error)
	// setRangeKeys sets the keys of the ranges of |hgr|, which were located by the last call to locate.
	setRangeKeys(hgr *remotesapi.HttpGetRange)
	// setLocationKeys sets the keys of |loc|, the location of chunk |h| found by the last call to locate.
	setLocationKeys(loc *remotesapi.StreamChunkLocationsResponse_ChunkLocation, h hash.Hash)
	// newKeys returns the keys that setLocationKeys referenced for the first time since its last call.
	newKeys() []*remotesapi.StreamChunkLocationsResponse_DataKey
}

type plainFormats struct{}

func (plainFormats) refuseClient(string, []remotesapi.ClientCapability) error {
	return nil
}

func (plainFormats) refusal(error) error {
	return nil
}

func (plainFormats) locator() chunkLocator {
	return plainLocator{}
}

type plainLocator struct{}

func (plainLocator) locate(ctx context.Context, cs RemoteSrvStore, hashes hash.HashSet) (map[string]map[hash.Hash]nbs.Range, error) {
	return cs.GetChunkLocationsWithPaths(ctx, hashes)
}

func (plainLocator) setRangeKeys(*remotesapi.HttpGetRange) {}

func (plainLocator) setLocationKeys(*remotesapi.StreamChunkLocationsResponse_ChunkLocation, hash.Hash) {
}

func (plainLocator) newKeys() []*remotesapi.StreamChunkLocationsResponse_DataKey {
	return nil
}

// checkClientCapability refuses to locate the records of |cs| for a client that cannot read them.
func checkClientCapability(cs RemoteSrvStore, caps []remotesapi.ClientCapability) error {
	return storeFormats.refuseClient(cs.Version(), caps)
}

type repoFormatKey struct{}

// WithRepoFormat records in |ctx| the format a client sent with GetRepoMetadata.
func WithRepoFormat(ctx context.Context, f *remotesapi.ClientRepoFormat) context.Context {
	return context.WithValue(ctx, repoFormatKey{}, f)
}

// RequestedRepoFormat returns the format a client sent with GetRepoMetadata, which a repository it creates takes.
func RequestedRepoFormat(ctx context.Context) (*remotesapi.ClientRepoFormat, bool) {
	f, ok := ctx.Value(repoFormatKey{}).(*remotesapi.ClientRepoFormat)
	return f, ok && f != nil
}
