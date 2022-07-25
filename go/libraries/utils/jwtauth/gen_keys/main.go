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

package main

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/google/uuid"
	"gopkg.in/square/go-jose.v2"
	"gopkg.in/square/go-jose.v2/jwt"
)

var sub = "test_user"
var iss = "dolthub.com"
var aud = "my_resource"
var onBehalfOf = "my_user"

func main() {
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		fmt.Println(err)
	}
	pubKey := privKey.Public()

	kid, err := uuid.NewRandom()
	if err != nil {
		fmt.Println(err)
	}

	err = writeJWKSToFile(pubKey, kid.String())
	if err != nil {
		fmt.Println(err)
	}

	jwt, err := generateJWT(privKey, kid.String())
	if err != nil {
		fmt.Println(err)
	}

	err = ioutil.WriteFile("token.jwt", []byte(jwt), 0644)
	if err != nil {
		fmt.Println(err)
	}
}

func writeJWKSToFile(pubKey crypto.PublicKey, kid string) error {
	jwk := jose.JSONWebKey{
		KeyID:     kid,
		Key:       pubKey,
		Use:       "sig",
		Algorithm: "RS256",
	}
	jwks := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{jwk}}

	jwksjson, err := json.Marshal(jwks)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile("test_jwks.json", jwksjson, 0644)
	if err != nil {
		return err
	}

	return nil
}

func generateJWT(privKey *rsa.PrivateKey, kid string) (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}

	now := time.Now()
	claims := jwt.Claims{
		ID:       id.String(),
		Audience: []string{aud},
		Issuer:   iss,
		Subject:  sub,
		IssuedAt: jwt.NewNumericDate(now),
		Expiry:   jwt.NewNumericDate(now.Add(30 * time.Second)),
	}
	privClaims := struct {
		OnBehalfOf string `json:"on_behalf_of"`
	}{
		onBehalfOf,
	}

	sig := jose.SigningKey{Algorithm: jose.RS256, Key: privKey}
	opts := (&jose.SignerOptions{ExtraHeaders: map[jose.HeaderKey]interface{}{
		"kid": kid,
	}}).WithType("JWT")

	signer, err := jose.NewSigner(sig, opts)
	if err != nil {
		return "", err
	}

	jwtBuilder := jwt.Signed(signer)
	jwtBuilder = jwtBuilder.Claims(claims)
	jwtBuilder = jwtBuilder.Claims(privClaims)

	com, err := jwtBuilder.CompactSerialize()
	if err != nil {
		return "", err
	}

	return com, nil
}
