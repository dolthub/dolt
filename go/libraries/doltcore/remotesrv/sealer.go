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

package remotesrv

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Interface to seal requests to the HTTP server so that they cannot be forged.
// The gRPC server seals URLs and the HTTP server unseals them.
type Sealer interface {
	Seal(*url.URL) (*url.URL, error)
	Unseal(*url.URL) (*url.URL, error)
}

var _ Sealer = identitySealer{}

type identitySealer struct {
}

func (identitySealer) Seal(u *url.URL) (*url.URL, error) {
	return u, nil
}

func (identitySealer) Unseal(u *url.URL) (*url.URL, error) {
	return u, nil
}

// Seals a URL by encrypting its Path and Query components and passing those in
// a base64 encoded query parameter. Adds a not before timestamp (nbf) and an
// expiration timestamp (exp) as query parameters. Encrypts the URL with
// AES-256 GCM and adds the nbf and exp parameters as authenticated data.
type singleSymmetricKeySealer struct {
	privateKeyBytes []byte
}

func NewSingleSymmetricKeySealer() (Sealer, error) {
	var key [32]byte
	_, err := rand.Read(key[:])
	if err != nil {
		return nil, err
	}
	return singleSymmetricKeySealer{privateKeyBytes: key[:]}, nil
}

func (s singleSymmetricKeySealer) Seal(u *url.URL) (*url.URL, error) {
	requestURI := (&url.URL{
		Path:     u.EscapedPath(),
		RawQuery: u.RawQuery,
	}).String()
	nbf := time.Now().Add(-10 * time.Second)
	exp := time.Now().Add(15 * time.Minute)
	nbfStr := strconv.FormatInt(nbf.UnixMilli(), 10)
	expStr := strconv.FormatInt(exp.UnixMilli(), 10)
	var nonceBytes [12]byte
	_, err := rand.Read(nonceBytes[:])
	if err != nil {
		return nil, err
	}
	nonceStr := base64.RawURLEncoding.EncodeToString(nonceBytes[:])

	block, err := aes.NewCipher(s.privateKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("internal error: error making aes cipher with key: %w", err)
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("internal error: error making gcm mode opener with key: %w", err)
	}

	reqBytes := aesgcm.Seal(nil, nonceBytes[:], []byte(requestURI), []byte(nbfStr+":"+expStr))
	reqStr := base64.RawURLEncoding.EncodeToString(reqBytes)

	ret := *u
	ret.Path = "/single_symmetric_key_sealed_request/" + u.EscapedPath()
	ret.RawQuery = url.Values(map[string][]string{
		"req":   []string{reqStr},
		"nbf":   []string{strconv.FormatInt(nbf.UnixMilli(), 10)},
		"exp":   []string{strconv.FormatInt(exp.UnixMilli(), 10)},
		"nonce": []string{nonceStr},
	}).Encode()
	return &ret, nil
}

func (s singleSymmetricKeySealer) Unseal(u *url.URL) (*url.URL, error) {
	if !strings.HasPrefix(u.Path, "/single_symmetric_key_sealed_request/") {
		return nil, errors.New("bad request: cannot unseal URL whose path does not start with /single_symmetric_key_sealed_request/")
	}
	q := u.Query()
	if !q.Has("nbf") {
		return nil, errors.New("bad request: cannot unseal URL which does not include an nbf")
	}
	if !q.Has("exp") {
		return nil, errors.New("bad request: cannot unseal URL which does not include an exp")
	}
	if !q.Has("nonce") {
		return nil, errors.New("bad request: cannot unseal URL which does not include a nonce")
	}
	if !q.Has("req") {
		return nil, errors.New("bad request: cannot unseal URL which does not include a req")
	}
	nbfStr := q.Get("nbf")
	expStr := q.Get("exp")
	nonceStr := q.Get("nonce")

	nbf, err := strconv.ParseInt(nbfStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("bad request: error parsing nbf as int64: %w", err)
	}
	exp, err := strconv.ParseInt(expStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("bad request: error parsing exp as int64: %w", err)
	}
	nonce, err := base64.RawURLEncoding.DecodeString(nonceStr)
	if err != nil {
		return nil, fmt.Errorf("bad request: error parsing nonce as base64 URL encoded: %w", err)
	}

	if time.Now().Before(time.UnixMilli(nbf)) {
		return nil, fmt.Errorf("bad request: nbf is invalid")
	}
	if time.Now().After(time.UnixMilli(exp)) {
		return nil, fmt.Errorf("bad request: exp is invalid")
	}

	block, err := aes.NewCipher(s.privateKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("internal error: error making aes cipher with key: %w", err)
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("internal error: error making gcm mode opener with key: %w", err)
	}

	reqStr := q.Get("req")
	reqBytes, err := base64.RawURLEncoding.DecodeString(reqStr)
	if err != nil {
		return nil, fmt.Errorf("bad request: error parsing req as base64 URL encoded: %w", err)
	}

	requestURI, err := aesgcm.Open(nil, nonce, reqBytes, []byte(nbfStr+":"+expStr))
	if err != nil {
		return nil, fmt.Errorf("bad request: error opening sealed url: %w", err)
	}
	requestURL, err := url.Parse(string(requestURI))
	if err != nil {
		return nil, fmt.Errorf("bad request: error parsing unsealed request uri: %w", err)
	}

	if strings.TrimPrefix(u.Path, "/single_symmetric_key_sealed_request/") != requestURL.EscapedPath() {
		return nil, fmt.Errorf("bad request: unsealed request path did not equal request path in sealed request")
	}

	ret := *u
	ret.Path = requestURL.Path
	ret.RawQuery = requestURL.RawQuery
	return &ret, nil
}
