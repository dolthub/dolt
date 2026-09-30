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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// CachedObject is an independent handle to immutable, decompressed object data.
// Close releases the handle; other handles remain readable.
type CachedObject interface {
	io.ReaderAt
	Size() int64
	io.Closer
}

// OIDCache caches immutable object contents and sizes by Git object ID.
// GetOrLoad coalesces concurrent loads. Failed loads are not cached.
// Clear releases cached data once outstanding readers and loads finish. The
// cache remains usable, including after GitBlobstore teardown or close.
type OIDCache interface {
	GetOrLoad(context.Context, string, func(context.Context) (io.ReadCloser, int64, error)) (CachedObject, error)
	LookupSize(string) (int64, bool)
	RecordSize(string, int64)
	Clear() error
}

// MemoryOIDCache stores decompressed objects in memory, primarily for tests.
type MemoryOIDCache struct{ *oidCache }

func NewMemoryOIDCache() *MemoryOIDCache {
	return &MemoryOIDCache{newOIDCache("", false)}
}

// DiskOIDCache stores decompressed objects in an instance-owned temporary
// directory under root. No directory is created until an object is loaded.
type DiskOIDCache struct{ *oidCache }

func NewDiskOIDCache(root string) *DiskOIDCache {
	return &DiskOIDCache{newOIDCache(root, true)}
}

var _ OIDCache = (*MemoryOIDCache)(nil)
var _ OIDCache = (*DiskOIDCache)(nil)

type oidCacheEntry struct {
	io.ReaderAt
	size int64
	path string
}

type oidCacheState struct {
	mu      sync.Mutex
	objects map[string]*oidCacheEntry
	sizes   map[string]int64
	loading map[string]chan struct{}
	dir     string
	// refs and retired are protected by oidCache.mu. A load holds a reference
	// that is transferred to its returned reader on success.
	refs    int
	retired bool
}

type oidCache struct {
	mu    sync.Mutex
	state *oidCacheState
	root  string
	disk  bool
}

func newOIDCache(root string, disk bool) *oidCache {
	return &oidCache{root: root, disk: disk, state: newOIDCacheState()}
}

func newOIDCacheState() *oidCacheState {
	return &oidCacheState{
		objects: make(map[string]*oidCacheEntry),
		sizes:   make(map[string]int64),
		loading: make(map[string]chan struct{}),
	}
}

func (c *oidCache) acquire() *oidCacheState {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.state
	s.refs++
	return s
}

func (c *oidCache) release(s *oidCacheState) error {
	c.mu.Lock()
	s.refs--
	cleanup := s.retired && s.refs == 0
	c.mu.Unlock()
	if cleanup {
		return s.cleanup()
	}
	return nil
}

func (c *oidCache) Clear() error {
	c.mu.Lock()
	s := c.state
	s.retired = true
	c.state = newOIDCacheState()
	cleanup := s.refs == 0
	c.mu.Unlock()
	if cleanup {
		return s.cleanup()
	}
	return nil
}

func (s *oidCacheState) cleanup() error {
	s.objects = nil
	s.sizes = nil
	s.loading = nil
	if s.dir != "" {
		return os.RemoveAll(s.dir)
	}
	return nil
}

func (c *oidCache) LookupSize(oid string) (int64, bool) {
	s := c.acquire()
	defer c.release(s)
	s.mu.Lock()
	defer s.mu.Unlock()
	size, ok := s.sizes[oid]
	return size, ok
}

func (c *oidCache) RecordSize(oid string, size int64) {
	if size < 0 {
		return
	}
	s := c.acquire()
	defer c.release(s)
	s.mu.Lock()
	s.sizes[oid] = size
	s.mu.Unlock()
}

func (c *oidCache) GetOrLoad(ctx context.Context, oid string, load func(context.Context) (io.ReadCloser, int64, error)) (CachedObject, error) {
	s := c.acquire()
	for {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, c.release(s))
		}
		s.mu.Lock()
		if obj, ok := s.objects[oid]; ok {
			s.mu.Unlock()
			return c.open(s, obj)
		}
		if done, ok := s.loading[oid]; ok {
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, errors.Join(ctx.Err(), c.release(s))
			case <-done:
				continue
			}
		}
		done := make(chan struct{})
		s.loading[oid] = done
		s.mu.Unlock()

		obj, err := c.load(ctx, s, load)
		s.mu.Lock()
		if err == nil {
			s.objects[oid] = obj
			s.sizes[oid] = obj.size
		}
		delete(s.loading, oid)
		close(done)
		s.mu.Unlock()
		if err != nil {
			return nil, errors.Join(err, c.release(s))
		}
		return c.open(s, obj)
	}
}

func (c *oidCache) open(s *oidCacheState, obj *oidCacheEntry) (CachedObject, error) {
	r := &cachedOIDReader{oidCacheEntry: obj, cache: c, state: s}
	if obj.path != "" {
		var err error
		r.file, err = os.Open(obj.path)
		if err != nil {
			return nil, errors.Join(errOIDCacheStorage, err, c.release(s))
		}
	}
	return r, nil
}

// errOIDCacheStorage lets GitBlobstore fall back to streaming if local cache
// storage is unavailable. Object read errors and corruption must still surface.
var errOIDCacheStorage = errors.New("OID cache storage unavailable")

func (c *oidCache) load(ctx context.Context, s *oidCacheState, load func(context.Context) (io.ReadCloser, int64, error)) (*oidCacheEntry, error) {
	var dst io.Writer
	var buf bytes.Buffer
	var f *os.File
	if c.disk {
		s.mu.Lock()
		if s.dir == "" {
			err := os.MkdirAll(c.root, 0700)
			if err == nil {
				s.dir, err = os.MkdirTemp(c.root, "objects-")
			}
			if err != nil {
				s.mu.Unlock()
				return nil, errors.Join(errOIDCacheStorage, err)
			}
		}
		dir := s.dir
		s.mu.Unlock()
		var err error
		f, err = os.CreateTemp(dir, "object-")
		if err != nil {
			return nil, errors.Join(errOIDCacheStorage, err)
		}
		dst = f
	} else {
		dst = &buf
	}
	keep := false
	defer func() {
		if f != nil && !keep {
			_ = f.Close()
			_ = os.Remove(f.Name())
		}
	}()

	rc, size, err := load(ctx)
	if err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, errors.Join(fmt.Errorf("invalid object size %d", size), rc.Close())
	}
	n, err := io.Copy(dst, &contextOIDReader{ctx: ctx, r: rc})
	err = errors.Join(err, rc.Close(), ctx.Err())
	if err != nil {
		var pathErr *os.PathError
		if errors.As(err, &pathErr) && pathErr.Op == "write" {
			return nil, errors.Join(errOIDCacheStorage, err)
		}
		return nil, err
	}
	if n < size {
		return nil, io.ErrUnexpectedEOF
	}
	if n != size {
		return nil, fmt.Errorf("object size mismatch: expected %d, read %d", size, n)
	}
	obj := &oidCacheEntry{size: size}
	if f != nil {
		// Keep only the path between reads, avoiding one open descriptor per
		// cached OID. Outstanding reader handles pin this generation's files.
		if err := f.Close(); err != nil {
			return nil, errors.Join(errOIDCacheStorage, err)
		}
		obj.path = f.Name()
	} else {
		obj.ReaderAt = bytes.NewReader(buf.Bytes())
	}
	keep = true
	return obj, nil
}

type contextOIDReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextOIDReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

type cachedOIDReader struct {
	*oidCacheEntry
	file  *os.File
	cache *oidCache
	state *oidCacheState
	once  sync.Once
	err   error
}

func (r *cachedOIDReader) Size() int64 { return r.size }

func (r *cachedOIDReader) ReadAt(p []byte, off int64) (int, error) {
	if r.file != nil {
		return r.file.ReadAt(p, off)
	}
	return r.oidCacheEntry.ReadAt(p, off)
}

func (r *cachedOIDReader) Close() error {
	r.once.Do(func() {
		if r.file != nil {
			r.err = r.file.Close()
		}
		r.err = errors.Join(r.err, r.cache.release(r.state))
	})
	return r.err
}
