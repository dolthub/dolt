package pnet

import (
	"bytes"
	"io/ioutil"
	"testing"
)

func TestGeneratedPSKCanBeUsed(t *testing.T) {
	psk, err := GenerateV1PSK()
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewProtector(psk)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGeneratedKeysAreDifferent(t *testing.T) {
	psk1, err := GenerateV1PSK()
	if err != nil {
		t.Fatal(err)
	}
	psk2, err := GenerateV1PSK()
	if err != nil {
		t.Fatal(err)
	}
	bpsk1, err := ioutil.ReadAll(psk1)
	if err != nil {
		t.Fatal(err)
	}
	bpsk2, err := ioutil.ReadAll(psk2)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(bpsk1, bpsk2) {
		t.Fatal("generated keys are the same")
	}
}

func TestGeneratedV1BytesAreDifferent(t *testing.T) {
	b1, err := GenerateV1Bytes()
	if err != nil {
		t.Fatal(err)
	}
	b2, err := GenerateV1Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(b1[:], b2[:]) {
		t.Fatal("generated keys are the same")
	}
}
