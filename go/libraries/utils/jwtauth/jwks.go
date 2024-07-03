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
// key set will generally hint that the URLs should be more aggressively
// refreshed, but there is no blocking on refreshing the URLs.
//
// GracefulStop() will shutdown any ongoing fetching work and will return when
// everything is cleanly shutdown.
type MultiJWKS struct {
	client  *http.Client
	wg      sync.WaitGroup
	stop    chan struct{}
	refresh []chan *sync.WaitGroup
	urls    []string
	sets    []jose.JSONWebKeySet
	agg     jose.JSONWebKeySet
	mu      sync.RWMutex
	lgr     *logrus.Entry
	stopped bool
}

func NewMultiJWKS(lgr *logrus.Entry, urls []string, client *http.Client) *MultiJWKS {
	res := new(MultiJWKS)
	res.lgr = lgr
	res.client = client
	res.urls = urls
	res.stop = make(chan struct{})
	res.refresh = make([]chan *sync.WaitGroup, len(urls))
	for i := range res.refresh {
		res.refresh[i] = make(chan *sync.WaitGroup, 3)
	}
	res.sets = make([]jose.JSONWebKeySet, len(urls))
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
	t.mu.Unlock()
	close(t.stop)
	t.wg.Wait()
	// TODO: Potentially clear t.refresh channels, ensure nothing else can call GetKey()...
}

func (t *MultiJWKS) needsRefresh() *sync.WaitGroup {
	wg := new(sync.WaitGroup)
	if t.stopped {
		return wg
	}
	wg.Add(len(t.refresh))
	for _, c := range t.refresh {
		select {
		case c <- wg:
		default:
			wg.Done()
		}
	}
	return wg
}

func (t *MultiJWKS) store(i int, jwks jose.JSONWebKeySet) {
	t.mu.Lock()
	defer t.mu.Unlock()
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

func (t *MultiJWKS) GetKey(kid string) ([]jose.JSONWebKey, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	res := t.agg.Key(kid)
	if len(res) == 0 {
		t.lgr.Infof("fetched key %s, found no key, signaling refresh", kid)
		refresh := t.needsRefresh()
		t.mu.RUnlock()
		refresh.Wait()
		t.mu.RLock()
		res = t.agg.Key(kid)
		t.lgr.Infof("refresh for key %s done, found %d keys", kid, len(res))
	}
	return res, nil
}

func (t *MultiJWKS) fetch(i int) error {
	request, err := http.NewRequest("GET", t.urls[i], nil)
	if err != nil {
		return err
	}
	response, err := t.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return fmt.Errorf("http request failed: StatusCode: %d", response.StatusCode)
	}
	contents, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	var jwks jose.JSONWebKeySet
	err = json.Unmarshal(contents, &jwks)
	if err != nil {
		return err
	}
	t.store(i, jwks)
	return nil
}

func (t *MultiJWKS) thread(i int) {
	defer t.wg.Done()
	timer := time.NewTimer(30 * time.Second)
	var refresh *sync.WaitGroup
	for {
		nextRefresh := 30 * time.Second
		err := t.fetch(i)
		if err != nil {
			// Something bad...
			t.lgr.Warnf("error fetching %s: %v", t.urls[i], err)
			nextRefresh = 1 * time.Second
		}
		timer.Reset(nextRefresh)
		if refresh != nil {
			refresh.Done()
		}
		refresh = nil
		select {
		case <-t.stop:
			if !timer.Stop() {
				<-timer.C
			}
			for {
				select {
				case refresh = <-t.refresh[i]:
					refresh.Done()
				default:
					return
				}
			}
		case refresh = <-t.refresh[i]:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}
}
