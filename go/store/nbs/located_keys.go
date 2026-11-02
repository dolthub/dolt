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
	"sync"

	"github.com/dolthub/dolt/go/store/hash"
)

// RangeKeys are the stored keys that sealed a located chunk record and its dictionary; nil for plaintext records.
type RangeKeys struct {
	Key     []byte
	DictKey []byte
}

type rangeLoc struct {
	h      hash.Hash
	offset uint64
	length uint32
}

// LocatedKeys holds the keys of the records getRecordRanges locates, by location, since a chunk can be in more
// than one file. A lookup retried after GC notes again, so the last note for a location wins.
type LocatedKeys struct {
	mu   sync.Mutex
	keys map[rangeLoc]RangeKeys
}

type locatedKeysKey struct{}

func locatedKeysFrom(ctx context.Context) *LocatedKeys {
	c, _ := ctx.Value(locatedKeysKey{}).(*LocatedKeys)
	return c
}

func (c *LocatedKeys) note(h hash.Hash, r Range, k RangeKeys) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keys[rangeLoc{h, r.Offset, r.Length}] = k
}

// At returns the keys of the record of |h| located at |offset| and |length|.
func (c *LocatedKeys) At(h hash.Hash, offset uint64, length uint32) RangeKeys {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.keys[rangeLoc{h, offset, length}]
}

type chunkLocator interface {
	GetChunkLocationsWithPaths(ctx context.Context, hashes hash.HashSet) (map[string]map[hash.Hash]Range, error)
}

// GetChunkLocationsWithPathsAndKeys also returns the keys of encrypted records; |cs| must pass |ctx| to the store.
func GetChunkLocationsWithPathsAndKeys(ctx context.Context, cs chunkLocator, hashes hash.HashSet) (map[string]map[hash.Hash]Range, *LocatedKeys, error) {
	c := &LocatedKeys{keys: make(map[rangeLoc]RangeKeys)}
	locs, err := cs.GetChunkLocationsWithPaths(context.WithValue(ctx, locatedKeysKey{}, c), hashes)
	if err != nil {
		return nil, nil, err
	}
	return locs, c, nil
}
