package creds

import (
	"crypto/sha512"
	"encoding/base32"
	"golang.org/x/crypto/ed25519"
)

const (
	pubKeySize  = 32
	privKeySize = 64
)

var encoding = base32.NewEncoding("0123456789abcdefghijklmnopqrstuv").WithPadding(base32.NoPadding)

type DoltCreds struct {
	PubKey  []byte
	PrivKey []byte
	KeyID   string
}

func PubKeyStrToKID(pub string) (string, error) {
	data, err := encoding.DecodeString(pub)

	if err != nil {
		return "", err
	}

	return PubKeyToKID(data), nil
}

func PubKeyToKID(pub []byte) string {
	kidBytes := sha512.Sum512_224(pub)
	kid := encoding.EncodeToString(kidBytes[:])
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
	return encoding.EncodeToString(dc.PubKey)
}

func (dc DoltCreds) PrivKeyBase32Str() string {
	return encoding.EncodeToString(dc.PubKey)
}
