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

package blobstore

import (
	"context"
	"errors"
	"fmt"
	"io"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
)

func (gbs *GitBlobstore) objectCache() OIDCache {
	gbs.oidCacheMu.Lock()
	defer gbs.oidCacheMu.Unlock()
	if gbs.oidCache == nil {
		// Bare GitBlobstore literals used by tests have no disk cache options.
		gbs.oidCache = NewMemoryOIDCache()
	}
	return gbs.oidCache
}

func (gbs *GitBlobstore) cachedBlobSize(ctx context.Context, oid git.OID) (int64, error) {
	gbs.oidSizeMu.Lock()
	defer gbs.oidSizeMu.Unlock()
	cache := gbs.objectCache()
	if size, ok := cache.LookupSize(oid.String()); ok {
		return size, nil
	}
	size, err := gbs.api.BlobSize(ctx, oid)
	if err == nil && size >= 0 {
		cache.RecordSize(oid.String(), size)
	} else if err == nil {
		err = fmt.Errorf("invalid blob size %d for %s", size, oid)
	}
	return size, err
}

func (gbs *GitBlobstore) cachedBlobSizes(ctx context.Context, oids []git.OID) ([]int64, error) {
	gbs.oidSizeMu.Lock()
	defer gbs.oidSizeMu.Unlock()
	cache := gbs.objectCache()
	sizes := make([]int64, len(oids))
	known := make(map[git.OID]int64)
	var missing []git.OID
	seen := make(map[git.OID]bool)
	for _, oid := range oids {
		if size, ok := cache.LookupSize(oid.String()); ok {
			known[oid] = size
		} else if !seen[oid] {
			missing = append(missing, oid)
			seen[oid] = true
		}
	}
	if len(missing) > 0 {
		found, err := gbs.api.BlobSizes(ctx, missing)
		if err != nil {
			return nil, err
		}
		if len(found) != len(missing) {
			return nil, fmt.Errorf("gitblobstore: expected %d blob sizes, got %d", len(missing), len(found))
		}
		for i, size := range found {
			if size < 0 {
				return nil, fmt.Errorf("invalid blob size %d for %s", size, missing[i])
			}
		}
		for i, oid := range missing {
			known[oid] = found[i]
			cache.RecordSize(oid.String(), found[i])
		}
	}
	for i, oid := range oids {
		sizes[i] = known[oid]
	}
	return sizes, nil
}

func (gbs *GitBlobstore) openBlobRange(ctx context.Context, oid git.OID, br BlobRange) (io.ReadCloser, error) {
	size, err := gbs.cachedBlobSize(ctx, oid)
	if err != nil {
		return nil, err
	}
	start, end, err := normalizeRange(size, br.offset, br.length)
	if err != nil {
		return nil, err
	}
	obj, err := gbs.objectCache().GetOrLoad(ctx, oid.String(), func(ctx context.Context) (io.ReadCloser, int64, error) {
		rc, err := gbs.api.BlobReader(ctx, oid)
		return rc, size, err
	})
	if errors.Is(err, errOIDCacheStorage) {
		// A read-only or full cache filesystem must not prevent remote reads.
		rc, err := gbs.api.BlobReader(ctx, oid)
		if err != nil {
			return nil, err
		}
		sliced, _, _, err := sliceInlineBlob(rc, size, br, oid.String())
		return sliced, err
	}
	if err != nil {
		return nil, err
	}
	return &limitReadCloser{
		r: &contextOIDReader{ctx: ctx, r: io.NewSectionReader(obj, start, end-start)},
		c: obj,
	}, nil
}
