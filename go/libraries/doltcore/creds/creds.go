package creds

import (
	"context"
	"crypto/sha512"
	"encoding/base32"
	"errors"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/set"
	"github.com/liquidata-inc/ld/dolt/go/store/go/util/datetime"
	"golang.org/x/crypto/ed25519"
	"gopkg.in/square/go-jose.v2"
	"gopkg.in/square/go-jose.v2/jwt"
	"time"
)

const (
	pubKeySize  = 32
	privKeySize = 64
)

const (
	B32CharEncoding = "0123456789abcdefghijklmnopqrstuv"

	B32EncodedPubKeyLen = 52
	B32EncodedKeyIdLen  = 45

	JWTKIDHeader = "kid"
	JWTAlgHeader = "alg"
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

func (dc DoltCreds) toBearerToken() (string, error) {
	b32KIDStr := dc.KeyIDBase32Str()
	key := jose.SigningKey{Algorithm: jose.EdDSA, Key: ed25519.PrivateKey(dc.PrivKey)}
	opts := &jose.SignerOptions{ExtraHeaders: map[jose.HeaderKey]interface{}{
		JWTKIDHeader: b32KIDStr,
	}}

	signer, err := jose.NewSigner(key, opts)

	if err != nil {
		return "", err
	}

	// Shouldn't be hard coded
	jwtBuilder := jwt.Signed(signer)
	jwtBuilder = jwtBuilder.Claims(jwt.Claims{
		Audience: []string{"dolthub-remote-api.liquidata.co"},
		Issuer:   "dolt-client.liquidata.co",
		Subject:  "doltClientCredentials/" + b32KIDStr,
		Expiry:   jwt.NewNumericDate(datetime.Now().Add(30 * time.Second)),
	})

	return jwtBuilder.CompactSerialize()
}

func (dc DoltCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	t, err := dc.toBearerToken()

	if err != nil {
		return nil, err
	}

	return map[string]string{
		"authorization": "Bearer " + t,
	}, nil
}

func (dc DoltCreds) RequireTransportSecurity() bool {
	return false
}
