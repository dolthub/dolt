// Copyright 2022 Dolthub, Inc.
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

package jwtauth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	jose "gopkg.in/go-jose/go-jose.v2"
	"gopkg.in/go-jose/go-jose.v2/json"
)

type cachedJWKS struct {
	value   *jose.JSONWebKeySet
	expires time.Time
	mutex   *sync.Mutex
}

func newCachedJWKS() *cachedJWKS {
	return &cachedJWKS{value: nil, expires: time.Now(), mutex: &sync.Mutex{}}
}

type fetchedJWKS struct {
	URL           string
	HTTPTransport *http.Transport
	cache         *cachedJWKS
}

func newJWKS(provider JWTProvider) (*fetchedJWKS, error) {
	return newFetchedJWKS(provider.URL)
}

func newFetchedJWKS(url string) (*fetchedJWKS, error) {
	ret := &fetchedJWKS{
		URL:   url,
		cache: newCachedJWKS(),
	}

	pwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	// Allows use of file:// for jwks location  url for tests
	tr := &http.Transport{}
	tr.RegisterProtocol("file", http.NewFileTransport(http.Dir(pwd)))
	ret.HTTPTransport = tr

	return ret, nil
}

func (f *fetchedJWKS) needsRefresh() bool {
	return f.cache.value == nil || time.Now().After(f.cache.expires)
}

func (f *fetchedJWKS) GetJWKS() (*jose.JSONWebKeySet, error) {
	f.cache.mutex.Lock()
	defer f.cache.mutex.Unlock()
	if f.needsRefresh() {
		client := &http.Client{Transport: f.HTTPTransport}

		request, err := http.NewRequest("GET", f.URL, nil)
		if err != nil {
			return nil, err
		}

		response, err := client.Do(request)
		if err != nil {
			return nil, err
		} else if response.StatusCode/100 != 2 {
			return nil, errors.New("FetchedJWKS: Non-2xx status code from JWKS fetch")
		} else {
			defer response.Body.Close()
			contents, err := io.ReadAll(response.Body)
			if err != nil {
				return nil, err
			}

			jwks := jose.JSONWebKeySet{}
			err = json.Unmarshal(contents, &jwks)
			if err != nil {
				return nil, err
			}
			f.cache.value = &jwks
		}
	}
	return f.cache.value, nil
}

func (f *fetchedJWKS) GetKey(kid string) ([]jose.JSONWebKey, error) {
	jwks, err := f.GetJWKS()
	if err != nil {
		return nil, err
	}
	return jwks.Key(kid), nil
}

// The MultiJWKS will source JWKS from multiple URLs and will make them all
// available through GetKey(). It's GetKey() cannot error, but it can return no
// results.
//
// The URLs in the refresh list are static. Each URL will be periodically
// refreshed and the results will be aggregated into the JWKS view. If a key no
// longer appears at the URL, it may eventually be removed from the set of keys
// available through GetKey(). Requesting a key which is not currently in the
// key set asks every URL to be refetched. GetKey() then waits until the key
// shows up or until every URL has finished a fetch which started after the
// request. Each fetch is bounded by a timeout, so an unresponsive URL delays
// only lookups of keys which no other URL serves.
//
// GracefulStop() will shutdown any ongoing fetching work and will return when
// everything is cleanly shutdown.
type MultiJWKS struct {
	client       *http.Client
	lgr          *logrus.Entry
	urls         []string
	fetchTimeout time.Duration

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
	// pokes[i] asks the fetch loop of urls[i] to fetch now. A poke which
	// arrives during a fetch causes another fetch right after it.
	pokes []chan struct{}

	mu   sync.RWMutex
	sets []jose.JSONWebKeySet
	agg  jose.JSONWebKeySet
	// started[i] and done[i] count the fetches of urls[i] which have started
	// and which have finished, successfully or not.
	started []uint64
	done    []uint64
	// Closed and replaced whenever a fetch finishes and on stop.
	changed chan struct{}
	stopped bool
}

const defaultJWKSFetchTimeout = 5 * time.Second

func NewMultiJWKS(lgr *logrus.Entry, urls []string, client *http.Client) *MultiJWKS {
	res := &MultiJWKS{
		client:       client,
		lgr:          lgr,
		urls:         urls,
		fetchTimeout: defaultJWKSFetchTimeout,
		pokes:        make([]chan struct{}, len(urls)),
		sets:         make([]jose.JSONWebKeySet, len(urls)),
		started:      make([]uint64, len(urls)),
		done:         make([]uint64, len(urls)),
		changed:      make(chan struct{}),
	}
	res.ctx, res.cancel = context.WithCancel(context.Background())
	for i := range res.pokes {
		res.pokes[i] = make(chan struct{}, 1)
	}
	return res
}

func (t *MultiJWKS) Run() {
	t.wg.Add(len(t.urls))
	for i := 0; i < len(t.urls); i++ {
		go t.thread(i)
	}
	t.wg.Wait()
}

func (t *MultiJWKS) GracefulStop() {
	t.mu.Lock()
	t.stopped = true
	t.broadcast()
	t.mu.Unlock()
	t.cancel()
	t.wg.Wait()
}

// broadcast wakes every GetKey() waiting on |t.changed|. Callers hold |t.mu|.
func (t *MultiJWKS) broadcast() {
	close(t.changed)
	t.changed = make(chan struct{})
}

func (t *MultiJWKS) GetKey(kid string) ([]jose.JSONWebKey, error) {
	t.mu.RLock()
	res := t.agg.Key(kid)
	if len(res) > 0 || t.stopped {
		t.mu.RUnlock()
		return res, nil
	}
	// Wait on fetches which start after now, since earlier ones may predate
	// the key.
	want := make([]uint64, len(t.started))
	for i, n := range t.started {
		want[i] = n + 1
	}
	t.mu.RUnlock()

	t.lgr.Infof("fetched key %s, found no key, signaling refresh", kid)
	for _, p := range t.pokes {
		select {
		case p <- struct{}{}:
		default:
		}
	}
	for {
		t.mu.RLock()
		res = t.agg.Key(kid)
		refreshed := t.stopped || t.fetchedSince(want)
		changed := t.changed
		t.mu.RUnlock()
		// Return as soon as any URL serves the key; don't wait on the others.
		if len(res) > 0 || refreshed {
			t.lgr.Infof("refresh for key %s done, found %d keys", kid, len(res))
			return res, nil
		}
		<-changed
	}
}

// fetchedSince returns true when every URL has finished its |want|th fetch.
// Callers hold |t.mu|.
func (t *MultiJWKS) fetchedSince(want []uint64) bool {
	for i, n := range want {
		if t.done[i] < n {
			return false
		}
	}
	return true
}

// fetch fetches urls[i], stores its keys on success, and wakes any GetKey()
// waiting on it either way.
func (t *MultiJWKS) fetch(i int) error {
	t.mu.Lock()
	t.started[i]++
	n := t.started[i]
	t.mu.Unlock()

	jwks, err := t.get(i)

	t.mu.Lock()
	defer t.mu.Unlock()
	if err == nil {
		t.sets[i] = jwks
		sum := 0
		for _, s := range t.sets {
			sum += len(s.Keys)
		}
		t.agg.Keys = make([]jose.JSONWebKey, 0, sum)
		for _, s := range t.sets {
			t.agg.Keys = append(t.agg.Keys, s.Keys...)
		}
	}
	t.done[i] = n
	t.broadcast()
	return err
}

func (t *MultiJWKS) get(i int) (jose.JSONWebKeySet, error) {
	var jwks jose.JSONWebKeySet
	ctx, cancel := context.WithTimeout(t.ctx, t.fetchTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, "GET", t.urls[i], nil)
	if err != nil {
		return jwks, err
	}
	response, err := t.client.Do(request)
	if err != nil {
		return jwks, err
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return jwks, fmt.Errorf("http request failed: StatusCode: %d", response.StatusCode)
	}
	contents, err := io.ReadAll(response.Body)
	if err != nil {
		return jwks, err
	}
	err = json.Unmarshal(contents, &jwks)
	return jwks, err
}

func (t *MultiJWKS) thread(i int) {
	defer t.wg.Done()
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		nextRefresh := 30 * time.Second
		if err := t.fetch(i); err != nil {
			t.lgr.Warnf("error fetching %s: %v", t.urls[i], err)
			nextRefresh = 1 * time.Second
		}
		timer.Reset(nextRefresh)
		select {
		case <-t.ctx.Done():
			return
		case <-t.pokes[i]:
		case <-timer.C:
		}
	}
}
