package pnet

import (
	"bytes"
	"io/ioutil"
	"testing"
)

func TestGeneratedPSKCanBeUsed(t *testing.T) {
	psk := GenerateV1PSK()

	_, err := NewProtector(psk)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGeneratedKeysAreDifferent(t *testing.T) {
	psk1 := GenerateV1PSK()
	psk2 := GenerateV1PSK()
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
