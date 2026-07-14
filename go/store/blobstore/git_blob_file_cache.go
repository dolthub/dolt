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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"github.com/dolthub/dolt/go/store/blobstore/internal/git"
)

// blobFileCache materializes git blobs into local files so that ranged and
// repeated reads are served by ordinary file I/O instead of spawning a
// `git cat-file` subprocess per read and streaming the blob from byte zero.
//
// Without it, every BlobRange read of an inline or chunked-part blob costs one
// `git cat-file -s` plus one `git cat-file blob` subprocess, and a read at
// offset N first streams and discards N bytes. Table-file reads during a pull
// are many small ranged reads, so on stores with large table files the
// aggregate cost is quadratic and a pull can appear to hang (see the runner
// TODO this replaces, and gastownhall/beads#4770 for a field report).
//
// Cache entries are keyed by blob OID. Git blobs are content-addressed and
// immutable, so entries can never go stale. Materialization writes to a temp
// file in the same directory and renames it into place; the loser of a
// concurrent race simply uses the winner's file.
//
// The cache directory lives inside the git dir (git tools ignore unknown
// directories there). Set DOLT_GIT_BLOB_FILE_CACHE=0 to disable the cache and
// fall back to streaming reads. DOLT_GIT_BLOB_FILE_CACHE_MAX_BYTES overrides
// the default size cap; the cap is enforced best-effort by evicting the
// least-recently-modified entries after a materialization pushes the total
// over the cap.
type blobFileCache struct {
	dir      string
	maxBytes int64

	mu       sync.Mutex
	inFlight map[string]chan struct{}
}

const (
	blobFileCacheDirName         = "dolt-blob-cache"
	blobFileCacheEnvDisable      = "DOLT_GIT_BLOB_FILE_CACHE"
	blobFileCacheEnvMaxBytes     = "DOLT_GIT_BLOB_FILE_CACHE_MAX_BYTES"
	blobFileCacheDefaultMaxBytes = int64(4) << 30 // 4 GiB
)

// newBlobFileCache returns a cache rooted under |gitDir|, or nil if the cache
// is disabled via DOLT_GIT_BLOB_FILE_CACHE=0. A nil *blobFileCache is valid;
// callers fall back to streaming reads.
func newBlobFileCache(gitDir string) *blobFileCache {
	if v, ok := os.LookupEnv(blobFileCacheEnvDisable); ok {
		if enabled, err := strconv.ParseBool(v); err == nil && !enabled {
			return nil
		}
	}
	maxBytes := blobFileCacheDefaultMaxBytes
	if v := os.Getenv(blobFileCacheEnvMaxBytes); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			maxBytes = n
		}
	}
	return &blobFileCache{
		dir:      filepath.Join(gitDir, blobFileCacheDirName),
		maxBytes: maxBytes,
		inFlight: make(map[string]chan struct{}),
	}
}

func (c *blobFileCache) path(oid git.OID) string {
	s := oid.String()
	return filepath.Join(c.dir, s[:2], s)
}

// open returns an open file handle positioned at byte zero of the fully
// materialized blob, along with the blob's size. The caller owns the handle.
func (c *blobFileCache) open(ctx context.Context, api git.GitAPI, oid git.OID) (*os.File, int64, error) {
	p := c.path(oid)
	for attempt := 0; ; attempt++ {
		f, err := os.Open(p)
		if err == nil {
			info, err := f.Stat()
			if err != nil {
				_ = f.Close()
				return nil, 0, err
			}
			return f, info.Size(), nil
		}
		if !os.IsNotExist(err) {
			return nil, 0, err
		}
		if attempt > 0 {
			return nil, 0, fmt.Errorf("gitblobstore: blob cache file %s missing after materialization", p)
		}
		if err := c.materialize(ctx, api, oid, p); err != nil {
			return nil, 0, err
		}
	}
}

// sizeOf reports the blob's size from the cache without materializing it.
func (c *blobFileCache) sizeOf(oid git.OID) (int64, bool) {
	info, err := os.Stat(c.path(oid))
	if err != nil {
		return 0, false
	}
	return info.Size(), true
}

// materialize streams the blob once via `git cat-file blob` into the cache.
// Concurrent callers for the same OID wait for the first materialization to
// finish rather than each spawning their own subprocess.
func (c *blobFileCache) materialize(ctx context.Context, api git.GitAPI, oid git.OID, dst string) error {
	key := oid.String()
	c.mu.Lock()
	if ch, ok := c.inFlight[key]; ok {
		c.mu.Unlock()
		select {
		case <-ch:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ch := make(chan struct{})
	c.inFlight[key] = ch
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.inFlight, key)
		c.mu.Unlock()
		close(ch)
	}()

	// Another process may have won a cross-process race while we waited.
	if _, err := os.Stat(dst); err == nil {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), "."+key+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	written, err := c.copyBlob(ctx, api, oid, tmp)
	cerr := tmp.Close()
	if err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, dst); err != nil {
		// On Windows, renaming over an existing file fails. If another
		// process materialized the blob first, its copy is equivalent.
		if _, serr := os.Stat(dst); serr == nil {
			_ = os.Remove(tmpName)
		} else {
			_ = os.Remove(tmpName)
			return err
		}
	}
	c.trim(written)
	return nil
}

func (c *blobFileCache) copyBlob(ctx context.Context, api git.GitAPI, oid git.OID, dst io.Writer) (int64, error) {
	rc, err := api.BlobReader(ctx, oid)
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(dst, rc)
	cerr := rc.Close()
	if err == nil {
		err = cerr
	}
	return n, err
}

// trim enforces the size cap best-effort: if the cache exceeds maxBytes it
// deletes least-recently-modified entries until under the cap. Files that are
// currently open (e.g. on Windows, where open files cannot be removed) are
// skipped. Called after each materialization that wrote |justWrote| bytes.
func (c *blobFileCache) trim(justWrote int64) {
	if c.maxBytes <= 0 || justWrote <= 0 {
		return
	}
	type entry struct {
		path    string
		size    int64
		modTime int64
	}
	var entries []entry
	var total int64
	_ = filepath.Walk(c.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		entries = append(entries, entry{path: path, size: info.Size(), modTime: info.ModTime().UnixNano()})
		total += info.Size()
		return nil
	})
	if total <= c.maxBytes {
		return
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].modTime < entries[j].modTime })
	for _, e := range entries {
		if total <= c.maxBytes {
			return
		}
		if err := os.Remove(e.path); err == nil {
			total -= e.size
		}
	}
}
