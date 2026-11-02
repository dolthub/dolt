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
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

// storeFormats is how the server treats the formats of the stores it serves. Dolt Enterprise replaces it.
var storeFormats serverFormats = plainFormats{}

type serverFormats interface {
	// requiredCapability returns the capability a client needs to read a store of format |nbfVersion|, or
	// CLIENT_CAPABILITY_UNSPECIFIED if it needs none.
	requiredCapability(nbfVersion string) remotesapi.ClientCapability
	// refusal returns the error to send a client for files a store refused to add with |err|, or nil to send |err|.
	refusal(err error) error
}

type plainFormats struct{}

func (plainFormats) requiredCapability(string) remotesapi.ClientCapability {
	return remotesapi.ClientCapability_CLIENT_CAPABILITY_UNSPECIFIED
}

func (plainFormats) refusal(error) error {
	return nil
}

// checkClientCapability refuses to locate the records of |cs| for a client without the capability its format needs.
func checkClientCapability(cs RemoteSrvStore, caps []remotesapi.ClientCapability) error {
	c := storeFormats.requiredCapability(cs.Version())
	if c != remotesapi.ClientCapability_CLIENT_CAPABILITY_UNSPECIFIED && !slices.Contains(caps, c) {
		return status.Errorf(codes.FailedPrecondition, "upgrade your dolt client; this repository requires %v", c)
	}
	return nil
}

type repoFormatKey struct{}

// WithRepoFormat records in |ctx| the format a client sent with GetRepoMetadata.
func WithRepoFormat(ctx context.Context, f *remotesapi.ClientRepoFormat) context.Context {
	return context.WithValue(ctx, repoFormatKey{}, f)
}

// RequestedRepoFormat returns the format a client sent with GetRepoMetadata, the request that creates repositories.
func RequestedRepoFormat(ctx context.Context) (*remotesapi.ClientRepoFormat, bool) {
	f, ok := ctx.Value(repoFormatKey{}).(*remotesapi.ClientRepoFormat)
	return f, ok && f != nil
}

// locatedKeys returns the keys of located records; see nbs.LocatedKeys.
type locatedKeys interface {
	At(h hash.Hash, offset uint64, length uint32) nbs.RangeKeys
}

// locateChunks locates |hashes| in |cs| with the keys of their records. Tests replace it.
var locateChunks = func(ctx context.Context, cs RemoteSrvStore, hashes hash.HashSet) (map[string]map[hash.Hash]nbs.Range, locatedKeys, error) {
	return nbs.GetChunkLocationsWithPathsAndKeys(ctx, cs, hashes)
}

// keyNumbering numbers the record keys of one HttpGetRange or StreamChunkLocations stream from one, in order of
// first use.
type keyNumbering struct {
	ids   map[string]uint32
	added [][]byte
}

func (kn *keyNumbering) ref(key []byte) uint32 {
	if key == nil {
		return 0
	}
	if id, ok := kn.ids[string(key)]; ok {
		return id
	}
	if kn.ids == nil {
		kn.ids = make(map[string]uint32)
	}
	kn.added = append(kn.added, key)
	id := uint32(len(kn.ids) + 1)
	kn.ids[string(key)] = id
	return id
}

// dataKeys returns the keys numbered since its last call.
func (kn *keyNumbering) dataKeys() []*remotesapi.StreamChunkLocationsResponse_DataKey {
	var ret []*remotesapi.StreamChunkLocationsResponse_DataKey
	first := len(kn.ids) - len(kn.added) + 1
	for i, k := range kn.added {
		ret = append(ret, &remotesapi.StreamChunkLocationsResponse_DataKey{KeyId: uint32(first + i), WrappedKey: k})
	}
	kn.added = nil
	return ret
}

func addRangeKeys(hgr *remotesapi.HttpGetRange, located locatedKeys) {
	var kn keyNumbering
	for _, rc := range hgr.Ranges {
		k := located.At(hash.New(rc.Hash), rc.Offset, rc.Length)
		rc.KeyIndex, rc.DictionaryKeyIndex = kn.ref(k.Key), kn.ref(k.DictKey)
	}
	hgr.Keys = kn.added
}
