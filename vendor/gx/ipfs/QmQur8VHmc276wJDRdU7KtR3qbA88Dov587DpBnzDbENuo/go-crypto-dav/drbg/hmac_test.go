package drbg

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// Test vectors from drbgtestvectors.zip: http://csrc.nist.gov/groups/STM/cavp/
// Prediction Resistance (PR) enabled.
func TestHmacSha512Pr(t *testing.T) {
	for _, m := range HmacSha512PrTests {
		nonce := readhex(t, m["Nonce"])
		personal := readhex(t, m["PersonalizationString"])
		entropy0 := readhex(t, m["EntropyInput"])
		entropy1 := readhex(t, m["EntropyInputPR"])
		entropy2 := readhex(t, m["EntropyInputPR2"])
		exinput1 := readhex(t, m["AdditionalInput"])
		exinput2 := readhex(t, m["AdditionalInput2"])
		expected := readhex(t, m["ReturnedBits"])

		actual := make([]byte, len(expected))

		seed0 := concat(entropy0, nonce, personal)
		rng := New(seed0)

		seed1 := concat(entropy1, exinput1)
		rng.Reseed(seed1)
		rng.Read(actual)

		seed2 := concat(entropy2, exinput2)
		rng.Reseed(seed2)
		rng.Read(actual)

		if !bytes.Equal(actual, expected) {
			t.Error(expected)
		}
	}
}

func readhex(t *testing.T, s string) []byte {
	v, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func concat(xs ...[]byte) []byte {
	return bytes.Join(xs, []byte{})
}
