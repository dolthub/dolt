package base32

import (
	"bytes"
	"crypto/rand"
	"math/big"
	"testing"
)

var tests = []struct {
	decoded, encoded string
}{
	{"", ""},
	{"f", "cr"},
	{"fo", "csqg"},
	{"foo", "csqpy"},
	{"foob", "csqpyrg"},
	{"fooba", "csqpyrk1"},
	{"foobar", "csqpyrk1e8"},
	{"Twas brillig, and the slithy toves", "ahvp2ws0c9s6jv3cd5kjr831dsj20x38cmg76v39ehm7j83mdxv6awr"},
}

func TestEncode(t *testing.T) {
	for _, test := range tests {
		got := EncodeToString([]byte(test.decoded))
		if got != test.encoded {
			t.Fatalf("EncodeToString(%q) = %q, want %q", test.decoded, got, test.encoded)
		}
	}
}

func TestDecode(t *testing.T) {
	for _, test := range tests {
		got, err := DecodeString(test.encoded)
		if err != nil {
			t.Fatalf("DecodeString(%q) error: %s", test.encoded, err)
		}
		if !bytes.Equal(got, []byte(test.decoded)) {
			t.Fatalf("DecodeString(%q) = %q, want %q", test.encoded, got, test.decoded)
		}
	}
}

func TestRandom(t *testing.T) {
	for i := 0; i < 10000; i++ {
		b := make([]byte, randInt(1024))
		rand.Read(b)
		enc := EncodeToString(b)
		dec, err := DecodeString(enc)
		if err != nil {
			t.Fatalf("DecodeString(%q) error: %s", enc, err)
		}
		if !bytes.Equal(b, dec) {
			t.Fatalf("DecodeString(%q) = %q, want %q\n", enc, dec, b)
		}
	}
}

func TestCanonical(t *testing.T) {
	tests := []string{"csqpyrk1e9", "cq"}
	for _, test := range tests {
		if _, err := DecodeString(test); err == nil {
			t.Fatalf("DecodeString(%q): expecting uncanonical error", test)
		}
	}
}

func randInt(max int) int {
	m := big.NewInt(int64(max) + 1)
	n, err := rand.Int(rand.Reader, m)
	if err != nil {
		panic(err)
	}
	return int(n.Int64())
}
