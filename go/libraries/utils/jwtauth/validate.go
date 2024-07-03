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
	"time"

	jose "gopkg.in/go-jose/go-jose.v2"
	"gopkg.in/go-jose/go-jose.v2/jwt"
)

type KeyProvider interface {
	GetKey(kid string) ([]jose.JSONWebKey, error)
}

var ErrKeyNotFound = errors.New("Key not found")

func ValidateJWT(unparsed string, reqTime time.Time, keyProvider KeyProvider, expectedClaims jwt.Expected) (*Claims, error) {
	parsed, err := jwt.ParseSigned(unparsed)
	if err != nil {
		return nil, err
	}

	if len(parsed.Headers) != 1 {
		return nil, fmt.Errorf("ValidateJWT: Unexpected JWT headers length %v.", len(parsed.Headers))
	}

	if parsed.Headers[0].Algorithm != "RS512" &&
		parsed.Headers[0].Algorithm != "RS256" &&
		parsed.Headers[0].Algorithm != "EdDSA" {
		return nil, fmt.Errorf("ValidateJWT: Currently only support RS256, RS512 and EdDSA signatures. Unexpected algorithm: %v", parsed.Headers[0].Algorithm)
	}

	keyID := parsed.Headers[0].KeyID

	keys, err := keyProvider.GetKey(keyID)
	if err != nil {
		return nil, err
	}

	var claims Claims
	claimsError := fmt.Errorf("ValidateJWT: KeyID: %v. Err: %w", keyID, ErrKeyNotFound)
	for _, key := range keys {
		claimsError = parsed.Claims(key.Key, &claims)
		if claimsError == nil {
			break
		}
	}
	if claimsError != nil {
		return nil, claimsError
	}

	if err := claims.Validate(expectedClaims.WithTime(reqTime)); err != nil {
		return nil, err
	}

	return &claims, nil
}
