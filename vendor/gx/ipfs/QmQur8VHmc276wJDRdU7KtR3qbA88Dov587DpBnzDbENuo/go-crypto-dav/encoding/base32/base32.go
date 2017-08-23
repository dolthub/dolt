package base32

import (
	"encoding/base32"
	"errors"
	"strings"
)

// drop the following letters:
// i - can be confused with 1,j
// l - can be confused with 1
// o - can be confused with 0
// u - can be confused with v
const encodeCompact = "0123456789abcdefghjkmnpqrstvwxyz"

// CompactEncoding is a lowercase base32 encoding using Crockford's alphabet
var CompactEncoding = base32.NewEncoding(encodeCompact)

// EncodeToString encodes src using CompactEncoding without padding
func EncodeToString(src []byte) string {
	s := CompactEncoding.EncodeToString(src)
	return strings.TrimRight(s, "=")
}

func DecodeString(s string) ([]byte, error) {
	pad := strings.Repeat("=", (8-len(s)%8)%8)
	src := s + pad
	dst, err := CompactEncoding.DecodeString(src)
	if err != nil {
		return nil, err
	}
	if CompactEncoding.EncodeToString(dst) != src {
		return dst, errors.New("uncanonical base32 input")
	}
	return dst, nil
}
