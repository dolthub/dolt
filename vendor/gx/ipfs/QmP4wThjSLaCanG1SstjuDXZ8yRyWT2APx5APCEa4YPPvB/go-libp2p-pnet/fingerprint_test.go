package pnet

import (
	"bytes"
	"testing"
)

var tpsk *[32]byte = &[32]byte{}

func TestFingerprintGen(t *testing.T) {
	f := fingerprint(tpsk)
	exp := []byte{0x70, 0x8a, 0x75, 0xaf, 0xd0, 0x5a, 0xff, 0xb0, 0x87, 0x36, 0xcb, 0xf1, 0x7c, 0x73, 0x77, 0x3e}

	if !bytes.Equal(f, exp) {
		t.Fatal("fingerprint different than expected")
	}

}
