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
	"container/list"
	"net/http"
	"net/url"
	"sync"
)

// PerURLFetcher is an HTTPFetcher that routes each request to an
// *http.Client dedicated to the request's URL path. Because Go's
// http.Transport pools connections by scheme+host (not by URL), the
// default fetcher mixes many different :path values onto each HTTP/2
// connection — and if those :path values are long presigned URLs that
// don't fit in HPACK's dynamic table (~4KB default), each request
// re-sends the full path as a literal. PerURLFetcher gives every
// URL-path its own connection pool, so every connection sees exactly
// one :path value, which HPACK can index after the first request.
//
// Clients are keyed by scheme+host+path so that presigned URL refreshes
// (new signed query params for the same underlying object) share an
// entry and keep HPACK state warm.
//
// An LRU cap bounds the number of live Transports; on eviction, the
// underlying Transport's CloseIdleConnections is called to release any
// pooled connections.
type PerURLFetcher struct {
	newClient func() *http.Client
	cap       int

	mu    sync.Mutex
	cache map[string]*list.Element
	lru   *list.List
}

type perURLEntry struct {
	key    string
	client *http.Client
}

// NewPerURLFetcher returns a fetcher that caches up to |cap| *http.Client
// instances, each minted by |newClient|. |newClient| must return a client
// with a fresh connection pool (typically a fresh *http.Transport) on
// each call, otherwise the per-URL connection isolation is defeated.
// If |cap| is <= 0, a default of 512 is used.
func NewPerURLFetcher(newClient func() *http.Client, cap int) *PerURLFetcher {
	if cap <= 0 {
		cap = 512
	}
	return &PerURLFetcher{
		newClient: newClient,
		cap:       cap,
		cache:     make(map[string]*list.Element),
		lru:       list.New(),
	}
}

// Do dispatches req through the cached client for its URL path.
func (f *PerURLFetcher) Do(req *http.Request) (*http.Response, error) {
	return f.clientFor(req.URL).Do(req)
}

func (f *PerURLFetcher) clientFor(u *url.URL) *http.Client {
	key := perURLKey(u)
	f.mu.Lock()
	defer f.mu.Unlock()
	if el, ok := f.cache[key]; ok {
		f.lru.MoveToFront(el)
		return el.Value.(*perURLEntry).client
	}
	client := f.newClient()
	entry := &perURLEntry{key: key, client: client}
	el := f.lru.PushFront(entry)
	f.cache[key] = el
	for f.lru.Len() > f.cap {
		oldest := f.lru.Back()
		if oldest == nil {
			break
		}
		old := oldest.Value.(*perURLEntry)
		if ci, ok := old.client.Transport.(interface{ CloseIdleConnections() }); ok {
			ci.CloseIdleConnections()
		}
		f.lru.Remove(oldest)
		delete(f.cache, old.key)
	}
	return client
}

func perURLKey(u *url.URL) string {
	if u == nil {
		return ""
	}
	return u.Scheme + "://" + u.Host + u.Path
}
