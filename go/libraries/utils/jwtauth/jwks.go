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
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	jose "gopkg.in/square/go-jose.v2"
	"gopkg.in/square/go-jose.v2/json"
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
			contents, err := ioutil.ReadAll(response.Body)
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
