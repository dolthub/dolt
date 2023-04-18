// Copyright 2019 Dolthub, Inc.
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

package creds

import (
	"context"
	"crypto/sha512"
	"encoding/base32"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"golang.org/x/crypto/ed25519"
	"gopkg.in/square/go-jose.v2"
	"gopkg.in/square/go-jose.v2/jwt"

	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/util/datetime"
)

const (
	pubKeySize  = 32
	privKeySize = 64
)

const (
	B32CharEncoding = "0123456789abcdefghijklmnopqrstuv"

	B32EncodedPubKeyLen = 52
	B32EncodedKeyIdLen  = 45

	JWTKIDHeader           = "kid"
	JWTAlgHeader           = "alg"
	DoltTokenVersionHeader = "dolt_token_version"
)

var B32CredsByteSet = set.NewByteSet([]byte(B32CharEncoding))
var B32CredsEncoding = base32.NewEncoding(B32CharEncoding).WithPadding(base32.NoPadding)
var EmptyCreds = DoltCreds{}

var ErrBadB32CredsEncoding = errors.New("bad base32 credentials encoding")
var ErrCredsNotFound = errors.New("credentials not found")

type DoltCreds struct {
	PubKey  []byte
	PrivKey []byte
	KeyID   []byte
}

func PubKeyStrToKIDStr(pub string) (string, error) {
	data, err := B32CredsEncoding.DecodeString(pub)

	if err != nil {
		return "", err
	}

	return PubKeyToKIDStr(data), nil
}

func PubKeyToKID(pub []byte) []byte {
	kidBytes := sha512.Sum512_224(pub)
	return kidBytes[:]
}

func PubKeyToKIDStr(pub []byte) string {
	kidBytes := PubKeyToKID(pub)
	kid := B32CredsEncoding.EncodeToString(kidBytes)
	return kid
}

func GenerateCredentials() (DoltCreds, error) {
	pub, priv, err := ed25519.GenerateKey(nil)

	if err == nil {
		kid := PubKeyToKID(pub)
		return DoltCreds{pub, priv, kid}, nil
	}

	return DoltCreds{}, err
}

func (dc DoltCreds) HasPrivKey() bool {
	return len(dc.PrivKey) > 0
}

func (dc DoltCreds) IsPrivKeyValid() bool {
	return len(dc.PrivKey) == privKeySize
}

func (dc DoltCreds) IsPubKeyValid() bool {
	return len(dc.PubKey) == pubKeySize
}

func (dc DoltCreds) PubKeyBase32Str() string {
	return B32CredsEncoding.EncodeToString(dc.PubKey)
}

func (dc DoltCreds) PrivKeyBase32Str() string {
	return B32CredsEncoding.EncodeToString(dc.PubKey)
}

func (dc DoltCreds) KeyIDBase32Str() string {
	return B32CredsEncoding.EncodeToString(dc.KeyID)
}

func (dc DoltCreds) Sign(data []byte) []byte {
	return ed25519.Sign(dc.PrivKey, data)
}

type RPCCreds struct {
	PrivKey    ed25519.PrivateKey
	KeyID      string
	Audience   string
	Issuer     string
	Subject    string
	RequireTLS bool
}

func (c *RPCCreds) toBearerToken() (string, error) {
	key := jose.SigningKey{Algorithm: jose.EdDSA, Key: c.PrivKey}
	opts := &jose.SignerOptions{ExtraHeaders: map[jose.HeaderKey]interface{}{
		JWTKIDHeader:           c.KeyID,
		DoltTokenVersionHeader: "2023.01",
	}}

	signer, err := jose.NewSigner(key, opts)
	if err != nil {
		return "", err
	}

	jwtBuilder := jwt.Signed(signer)
	jwtBuilder = jwtBuilder.Claims(jwt.Claims{
		Audience: []string{c.Audience},
		Issuer:   c.Issuer,
		Subject:  c.Subject,
		Expiry:   jwt.NewNumericDate(datetime.Now().Add(30 * time.Second)),
	})

	return jwtBuilder.CompactSerialize()
}

func (c *RPCCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	t, err := c.toBearerToken()
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"authorization": "Bearer " + t,
	}, nil
}

func (c *RPCCreds) RequireTransportSecurity() bool {
	return c.RequireTLS
}

const ClientIssuer = "dolt-client.dolthub.com"

func (dc DoltCreds) RPCCreds(audience string) *RPCCreds {
	b32KIDStr := dc.KeyIDBase32Str()
	return &RPCCreds{
		PrivKey:    ed25519.PrivateKey(dc.PrivKey),
		KeyID:      b32KIDStr,
		Audience:   audience,
		Issuer:     ClientIssuer,
		Subject:    "doltClientCredentials/" + b32KIDStr,
		RequireTLS: false,
	}
}

type DoltCredsForPass struct {
	Username string
	Password string
}

type RPCCredsForPass struct {
	RequireTLS       bool
	UserPassContents string
}

func (dcp DoltCredsForPass) ToBase64Str() string {
	bStr := []byte(fmt.Sprintf("%s:%s", dcp.Username, dcp.Password))
	return base64.StdEncoding.EncodeToString(bStr)
}

func (dc DoltCredsForPass) RPCCreds() *RPCCredsForPass {
	return &RPCCredsForPass{
		RequireTLS:       false,
		UserPassContents: dc.ToBase64Str(),
	}
}

func (c *RPCCredsForPass) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Basic " + c.UserPassContents,
	}, nil
}

func (c *RPCCredsForPass) RequireTransportSecurity() bool {
	return c.RequireTLS
}
