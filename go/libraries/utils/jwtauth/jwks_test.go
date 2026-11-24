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

package jwtauth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	jose "gopkg.in/go-jose/go-jose.v2"
)

// jwksServer serves whatever JWKS is currently stored in |keys| and signals
// |fetched| on every request.
type jwksServer struct {
	*httptest.Server
	keys    atomic.Pointer[[]byte]
	fetched chan struct{}
}

func newJWKSServer(t *testing.T) *jwksServer {
	s := &jwksServer{fetched: make(chan struct{}, 16)}
	s.setKeys(t)
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(*s.keys.Load())
		select {
		case s.fetched <- struct{}{}:
		default:
		}
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *jwksServer) setKeys(t *testing.T, keys ...jose.JSONWebKey) {
	bs, err := json.Marshal(jose.JSONWebKeySet{Keys: keys})
	require.NoError(t, err)
	s.keys.Store(&bs)
}

// newSilentServer returns a server which accepts requests and never
// responds, like a peer whose network has gone away without closing the
// connection. Requests are held until |release| is called.
func newSilentServer(t *testing.T) (s *httptest.Server, release func()) {
	ch := make(chan struct{})
	release = sync.OnceFunc(func() { close(ch) })
	s = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-ch:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() {
		release()
		s.Close()
	})
	return s, release
}

func newEd25519JWK(t *testing.T, kid string) jose.JSONWebKey {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return jose.JSONWebKey{Key: pub, KeyID: kid, Algorithm: "EdDSA", Use: "sig"}
}

// runMultiJWKS uses the default fetch timeout when |fetchTimeout| is 0.
func runMultiJWKS(t *testing.T, urls []string, fetchTimeout time.Duration) *MultiJWKS {
	lgr := logrus.New()
	lgr.SetLevel(logrus.WarnLevel)
	// Like the cluster controller's client: no timeouts.
	client := &http.Client{Transport: &http.Transport{}}
	jwks := NewMultiJWKS(logrus.NewEntry(lgr), urls, client)
	if fetchTimeout != 0 {
		jwks.fetchTimeout = fetchTimeout
	}
	var wg sync.WaitGroup
	wg.Go(jwks.Run)
	t.Cleanup(func() {
		jwks.GracefulStop()
		wg.Wait()
	})
	return jwks
}

func getKeyAsync(jwks *MultiJWKS, kid string) <-chan []jose.JSONWebKey {
	ch := make(chan []jose.JSONWebKey, 1)
	go func() {
		res, _ := jwks.GetKey(kid)
		ch <- res
	}()
	return ch
}

// A restarted peer publishes a new key. Looking that key up should not wait
// on a fetch from some other, unresponsive, peer.
func TestMultiJWKSGetKeyDoesNotWaitOnSilentPeer(t *testing.T) {
	restarted := newJWKSServer(t)
	silent, release := newSilentServer(t)
	jwks := runMultiJWKS(t, []string{restarted.URL, silent.URL}, 0)
	// Cleanups run LIFO; let the old code's GracefulStop complete.
	t.Cleanup(release)

	select {
	case <-restarted.fetched:
	case <-time.After(5 * time.Second):
		t.Fatal("initial fetch did not happen")
	}

	restarted.setKeys(t, newEd25519JWK(t, "new"))

	select {
	case res := <-getKeyAsync(jwks, "new"):
		require.Len(t, res, 1)
		assert.Equal(t, "new", res[0].KeyID)
	case <-time.After(2 * time.Second):
		t.Fatal("GetKey for a key served by a responsive peer blocked on an unresponsive peer")
	}
}

// Fetches from an unresponsive peer time out, even when the http.Client has no
// timeout of its own, so a lookup of an unknown key does not block forever.
func TestMultiJWKSFetchTimesOut(t *testing.T) {
	silent, release := newSilentServer(t)
	jwks := runMultiJWKS(t, []string{silent.URL}, 100*time.Millisecond)
	t.Cleanup(release)

	select {
	case res := <-getKeyAsync(jwks, "missing"):
		require.Empty(t, res)
	case <-time.After(5 * time.Second):
		t.Fatal("GetKey blocked on a fetch from an unresponsive peer")
	}
}
