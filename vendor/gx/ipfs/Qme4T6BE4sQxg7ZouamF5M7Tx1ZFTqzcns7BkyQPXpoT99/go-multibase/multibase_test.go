package multibase

import (
	"bytes"
	"math/rand"
	"testing"
)

var sampleBytes = []byte("Decentralize everything!!")
var encodedSamples = map[Encoding]string{
	Identity:     string(0x00) + "Decentralize everything!!",
	Base16:       "f446563656e7472616c697a652065766572797468696e672121",
	Base58BTC:    "zUXE7GvtEk8XTXs1GF8HSGbVA9FCX9SEBPe",
	Base64pad:    "MRGVjZW50cmFsaXplIGV2ZXJ5dGhpbmchIQ==",
	Base64urlPad: "URGVjZW50cmFsaXplIGV2ZXJ5dGhpbmchIQ==",
}

func testEncode(t *testing.T, encoding Encoding, bytes []byte, expected string) {
	actual, err := Encode(encoding, bytes)
	if err != nil {
		t.Error(err)
		return
	}
	if actual != expected {
		t.Errorf("encoding failed for %c (%d), expected: %s, got: %s", encoding, encoding, expected, actual)
	}
}

func testDecode(t *testing.T, expectedEncoding Encoding, expectedBytes []byte, data string) {
	actualEncoding, actualBytes, err := Decode(data)
	if err != nil {
		t.Error(err)
		return
	}
	if actualEncoding != expectedEncoding {
		t.Errorf("wrong encoding code, expected: %c (%d), got %c (%d)", expectedEncoding, expectedEncoding, actualEncoding, actualEncoding)
	}
	if !bytes.Equal(actualBytes, expectedBytes) {
		t.Errorf("decoding failed for %c (%d), expected: %v, got %v", actualEncoding, actualEncoding, expectedBytes, actualBytes)
	}
}

func TestEncode(t *testing.T) {
	for encoding, data := range encodedSamples {
		testEncode(t, encoding, sampleBytes, data)
	}
}

func TestDecode(t *testing.T) {
	for encoding, data := range encodedSamples {
		testDecode(t, encoding, sampleBytes, data)
	}
}

func TestRoundTrip(t *testing.T) {
	buf := make([]byte, 17)
	rand.Read(buf)

	baseList := []Encoding{Identity, Base16, Base32, Base32hex, Base32pad, Base32hexPad, Base58BTC, Base58Flickr, Base64pad, Base64urlPad}

	for _, base := range baseList {
		enc, err := Encode(base, buf)
		if err != nil {
			t.Fatal(err)
		}

		e, out, err := Decode(enc)
		if err != nil {
			t.Fatal(err)
		}

		if e != base {
			t.Fatal("got wrong encoding out")
		}

		if !bytes.Equal(buf, out) {
			t.Fatal("input wasnt the same as output", buf, out)
		}
	}

	_, _, err := Decode("")
	if err == nil {
		t.Fatal("shouldnt be able to decode empty string")
	}
}
