package multibase

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"

	b58 "gx/ipfs/QmT8rehPR3F6bmwL6zjUN8XpiDBFFpMP2myPdC6ApsWfJf/go-base58"
	b32 "gx/ipfs/QmfVj3x4D6Jkq9SEoi5n2NmoUomLwoeiwnYz2KQa15wRw6/base32"
)

// Encoding identifies the type of base-encoding that a multibase is carrying.
type Encoding int

// These are the supported encodings
const (
	Identity          = 0x00
	Base1             = '1'
	Base2             = '0'
	Base8             = '7'
	Base10            = '9'
	Base16            = 'f'
	Base16Upper       = 'F'
	Base32            = 'b'
	Base32Upper       = 'B'
	Base32pad         = 'c'
	Base32padUpper    = 'C'
	Base32hex         = 'v'
	Base32hexUpper    = 'V'
	Base32hexPad      = 't'
	Base32hexPadUpper = 'T'
	Base58Flickr      = 'Z'
	Base58BTC         = 'z'
	Base64            = 'm'
	Base64url         = 'u'
	Base64pad         = 'M'
	Base64urlPad      = 'U'
)

// ErrUnsupportedEncoding is returned when the selected encoding is not known or
// implemented.
var ErrUnsupportedEncoding = fmt.Errorf("selected encoding not supported")

// Encode encodes a given byte slice with the selected encoding and returns a
// multibase string (<encoding><base-encoded-string>). It will return
// an error if the selected base is not known.
func Encode(base Encoding, data []byte) (string, error) {
	switch base {
	case Identity:
		// 0x00 inside a string is OK in golang and causes no problems with the length calculation.
		return string(Identity) + string(data), nil
	case Base16:
		return string(Base16) + hex.EncodeToString(data), nil
	case Base16Upper:
		return string(Base16Upper) + hexEncodeToStringUpper(data), nil
	case Base32:
		return string(Base32) + base32StdLowerNoPad.EncodeToString(data), nil
	case Base32Upper:
		return string(Base32Upper) + base32StdUpperNoPad.EncodeToString(data), nil
	case Base32hex:
		return string(Base32hex) + base32HexLowerNoPad.EncodeToString(data), nil
	case Base32hexUpper:
		return string(Base32hexUpper) + base32HexUpperNoPad.EncodeToString(data), nil
	case Base32pad:
		return string(Base32pad) + base32StdLowerPad.EncodeToString(data), nil
	case Base32padUpper:
		return string(Base32padUpper) + base32StdUpperPad.EncodeToString(data), nil
	case Base32hexPad:
		return string(Base32hexPad) + base32HexLowerPad.EncodeToString(data), nil
	case Base32hexPadUpper:
		return string(Base32hexPadUpper) + base32HexUpperPad.EncodeToString(data), nil
	case Base58BTC:
		return string(Base58BTC) + b58.EncodeAlphabet(data, b58.BTCAlphabet), nil
	case Base58Flickr:
		return string(Base58Flickr) + b58.EncodeAlphabet(data, b58.FlickrAlphabet), nil
	case Base64pad:
		return string(Base64pad) + base64.StdEncoding.EncodeToString(data), nil
	case Base64urlPad:
		return string(Base64urlPad) + base64.URLEncoding.EncodeToString(data), nil
	case Base64url:
		return string(Base64url) + base64.RawURLEncoding.EncodeToString(data), nil
	case Base64:
		return string(Base64) + base64.RawStdEncoding.EncodeToString(data), nil
	default:
		return "", ErrUnsupportedEncoding
	}
}

// Decode takes a multibase string and decodes into a bytes buffer.
// It will return an error if the selected base is not known.
func Decode(data string) (Encoding, []byte, error) {
	if len(data) == 0 {
		return 0, nil, fmt.Errorf("cannot decode multibase for zero length string")
	}

	enc := Encoding(data[0])

	switch enc {
	case Identity:
		return Identity, []byte(data[1:]), nil
	case Base16, Base16Upper:
		bytes, err := hex.DecodeString(data[1:])
		return enc, bytes, err
	case Base32, Base32Upper:
		bytes, err := b32.RawStdEncoding.DecodeString(data[1:])
		return enc, bytes, err
	case Base32hex, Base32hexUpper:
		bytes, err := b32.RawHexEncoding.DecodeString(data[1:])
		return enc, bytes, err
	case Base32pad, Base32padUpper:
		bytes, err := b32.StdEncoding.DecodeString(data[1:])
		return enc, bytes, err
	case Base32hexPad, Base32hexPadUpper:
		bytes, err := b32.HexEncoding.DecodeString(data[1:])
		return enc, bytes, err
	case Base58BTC:
		return Base58BTC, b58.DecodeAlphabet(data[1:], b58.BTCAlphabet), nil
	case Base58Flickr:
		return Base58Flickr, b58.DecodeAlphabet(data[1:], b58.FlickrAlphabet), nil
	case Base64pad:
		bytes, err := base64.StdEncoding.DecodeString(data[1:])
		return Base64pad, bytes, err
	case Base64urlPad:
		bytes, err := base64.URLEncoding.DecodeString(data[1:])
		return Base64urlPad, bytes, err
	case Base64:
		bytes, err := base64.RawStdEncoding.DecodeString(data[1:])
		return Base64, bytes, err
	case Base64url:
		bytes, err := base64.RawURLEncoding.DecodeString(data[1:])
		return Base64url, bytes, err
	default:
		return -1, nil, ErrUnsupportedEncoding
	}
}
