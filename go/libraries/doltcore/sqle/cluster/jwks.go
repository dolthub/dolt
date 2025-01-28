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

package cluster

import (
	"crypto/ed25519"
	"encoding/json"
	"net/http"

	"gopkg.in/go-jose/go-jose.v2"
	"gopkg.in/go-jose/go-jose.v2/jwt"

	"github.com/dolthub/dolt/go/libraries/doltcore/creds"
)

type JWKSHandler struct {
	KeyID     string
	PublicKey ed25519.PublicKey
}

func (h JWKSHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, err := json.Marshal(jose.JSONWebKeySet{
		Keys: []jose.JSONWebKey{
			{
				Key:   h.PublicKey,
				KeyID: h.KeyID,
			},
		},
	})
	if err != nil {
		http.Error(w, "error marshaling json", http.StatusInternalServerError)
		return
	}
	w.Write(b)
}

func JWKSHandlerInterceptor(existing func(http.Handler) http.Handler, keyID string, pub ed25519.PublicKey) func(http.Handler) http.Handler {
	jh := JWKSHandler{KeyID: keyID, PublicKey: pub}
	return func(h http.Handler) http.Handler {
		this := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.EscapedPath() == "/.well-known/jwks.json" {
				jh.ServeHTTP(w, r)
				return
			}
			h.ServeHTTP(w, r)
		})
		if existing != nil {
			return existing(this)
		} else {
			return this
		}
	}
}

func JWTExpectations() jwt.Expected {
	return jwt.Expected{Issuer: creds.ClientIssuer, Audience: jwt.Audience{DoltClusterRemoteApiAudience}}
}
