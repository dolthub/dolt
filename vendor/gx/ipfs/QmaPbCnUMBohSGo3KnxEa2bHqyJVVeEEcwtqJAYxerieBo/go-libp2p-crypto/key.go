// package crypto implements various cryptographic utilities used by ipfs.
// This includes a Public and Private key interface and an RSA key implementation
// that satisfies it.
package crypto

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"

	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"hash"

	pb "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto/pb"

	proto "gx/ipfs/QmZ4Qi3GaRbjcx28Sme5eMH7RQjGkt8wHxt2a65oLaeFEV/gogo-protobuf/proto"
)

var ErrBadKeyType = errors.New("invalid or unsupported key type")

const (
	RSA = iota
	Ed25519
	Secp256k1
)

// Key represents a crypto key that can be compared to another key
type Key interface {
	// Bytes returns a serialized, storeable representation of this key
	Bytes() ([]byte, error)

	// Equals checks whether two PubKeys are the same
	Equals(Key) bool
}

// PrivKey represents a private key that can be used to generate a public key,
// sign data, and decrypt data that was encrypted with a public key
type PrivKey interface {
	Key

	// Cryptographically sign the given bytes
	Sign([]byte) ([]byte, error)

	// Return a public key paired with this private key
	GetPublic() PubKey
}

type PubKey interface {
	Key

	// Verify that 'sig' is the signed hash of 'data'
	Verify(data []byte, sig []byte) (bool, error)
}

// Given a public key, generates the shared key.
type GenSharedKey func([]byte) ([]byte, error)

func GenerateKeyPair(typ, bits int) (PrivKey, PubKey, error) {
	return GenerateKeyPairWithReader(typ, bits, rand.Reader)
}

// Generates a keypair of the given type and bitsize
func GenerateKeyPairWithReader(typ, bits int, src io.Reader) (PrivKey, PubKey, error) {
	switch typ {
	case RSA:
		priv, err := rsa.GenerateKey(src, bits)
		if err != nil {
			return nil, nil, err
		}
		pk := &priv.PublicKey
		return &RsaPrivateKey{sk: priv}, &RsaPublicKey{pk}, nil
	case Ed25519:
		return GenerateEd25519Key(src)
	case Secp256k1:
		return GenerateSecp256k1Key(src)
	default:
		return nil, nil, ErrBadKeyType
	}
}

// Generates an ephemeral public key and returns a function that will compute
// the shared secret key.  Used in the identify module.
//
// Focuses only on ECDH now, but can be made more general in the future.
func GenerateEKeyPair(curveName string) ([]byte, GenSharedKey, error) {
	var curve elliptic.Curve

	switch curveName {
	case "P-256":
		curve = elliptic.P256()
	case "P-384":
		curve = elliptic.P384()
	case "P-521":
		curve = elliptic.P521()
	}

	priv, x, y, err := elliptic.GenerateKey(curve, rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	pubKey := elliptic.Marshal(curve, x, y)

	done := func(theirPub []byte) ([]byte, error) {
		// Verify and unpack node's public key.
		x, y := elliptic.Unmarshal(curve, theirPub)
		if x == nil {
			return nil, fmt.Errorf("Malformed public key: %d %v", len(theirPub), theirPub)
		}

		if !curve.IsOnCurve(x, y) {
			return nil, errors.New("Invalid public key.")
		}

		// Generate shared secret.
		secret, _ := curve.ScalarMult(x, y, priv)

		return secret.Bytes(), nil
	}

	return pubKey, done, nil
}

type StretchedKeys struct {
	IV        []byte
	MacKey    []byte
	CipherKey []byte
}

// Generates a set of keys for each party by stretching the shared key.
// (myIV, theirIV, myCipherKey, theirCipherKey, myMACKey, theirMACKey)
func KeyStretcher(cipherType string, hashType string, secret []byte) (StretchedKeys, StretchedKeys) {
	var cipherKeySize int
	var ivSize int
	switch cipherType {
	case "AES-128":
		ivSize = 16
		cipherKeySize = 16
	case "AES-256":
		ivSize = 16
		cipherKeySize = 32
	case "Blowfish":
		ivSize = 8
		// Note: 24 arbitrarily selected, needs more thought
		cipherKeySize = 32
	}

	hmacKeySize := 20

	seed := []byte("key expansion")

	result := make([]byte, 2*(ivSize+cipherKeySize+hmacKeySize))

	var h func() hash.Hash

	switch hashType {
	case "SHA1":
		h = sha1.New
	case "SHA256":
		h = sha256.New
	case "SHA512":
		h = sha512.New
	default:
		panic("Unrecognized hash function, programmer error?")
	}

	m := hmac.New(h, secret)
	m.Write(seed)

	a := m.Sum(nil)

	j := 0
	for j < len(result) {
		m.Reset()
		m.Write(a)
		m.Write(seed)
		b := m.Sum(nil)

		todo := len(b)

		if j+todo > len(result) {
			todo = len(result) - j
		}

		copy(result[j:j+todo], b)

		j += todo

		m.Reset()
		m.Write(a)
		a = m.Sum(nil)
	}

	half := len(result) / 2
	r1 := result[:half]
	r2 := result[half:]

	var k1 StretchedKeys
	var k2 StretchedKeys

	k1.IV = r1[0:ivSize]
	k1.CipherKey = r1[ivSize : ivSize+cipherKeySize]
	k1.MacKey = r1[ivSize+cipherKeySize:]

	k2.IV = r2[0:ivSize]
	k2.CipherKey = r2[ivSize : ivSize+cipherKeySize]
	k2.MacKey = r2[ivSize+cipherKeySize:]

	return k1, k2
}

// UnmarshalPublicKey converts a protobuf serialized public key into its
// representative object
func UnmarshalPublicKey(data []byte) (PubKey, error) {
	pmes := new(pb.PublicKey)
	err := proto.Unmarshal(data, pmes)
	if err != nil {
		return nil, err
	}

	switch pmes.GetType() {
	case pb.KeyType_RSA:
		return UnmarshalRsaPublicKey(pmes.GetData())
	case pb.KeyType_Ed25519:
		var pubk [32]byte
		copy(pubk[:], pmes.Data)
		return &Ed25519PublicKey{&pubk}, nil
	case pb.KeyType_Secp256k1:
		return UnmarshalSecp256k1PublicKey(pmes.GetData())
	default:
		return nil, ErrBadKeyType
	}
}

// MarshalPublicKey converts a public key object into a protobuf serialized
// public key
func MarshalPublicKey(k PubKey) ([]byte, error) {
	return k.Bytes()
}

// UnmarshalPrivateKey converts a protobuf serialized private key into its
// representative object
func UnmarshalPrivateKey(data []byte) (PrivKey, error) {
	pmes := new(pb.PrivateKey)
	err := proto.Unmarshal(data, pmes)
	if err != nil {
		return nil, err
	}

	switch pmes.GetType() {
	case pb.KeyType_RSA:
		return UnmarshalRsaPrivateKey(pmes.GetData())
	case pb.KeyType_Ed25519:
		return UnmarshalEd25519PrivateKey(pmes.GetData())
	case pb.KeyType_Secp256k1:
		return UnmarshalSecp256k1PrivateKey(pmes.GetData())
	default:
		return nil, ErrBadKeyType
	}
}

// MarshalPrivateKey converts a key object into its protobuf serialized form.
func MarshalPrivateKey(k PrivKey) ([]byte, error) {

	switch k.(type) {
	case *Ed25519PrivateKey:
		return k.Bytes()
	case *RsaPrivateKey:
		return k.Bytes()
	default:
		return nil, ErrBadKeyType
	}
}

// ConfigDecodeKey decodes from b64 (for config file), and unmarshals.
func ConfigDecodeKey(b string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(b)
}

// ConfigEncodeKey encodes to b64 (for config file), and marshals.
func ConfigEncodeKey(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

// KeyEqual checks whether two
func KeyEqual(k1, k2 Key) bool {
	if k1 == k2 {
		return true
	}

	b1, err1 := k1.Bytes()
	b2, err2 := k2.Bytes()
	return bytes.Equal(b1, b2) && err1 == err2
}
