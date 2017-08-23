package crypto

import (
	"crypto/rand"
	"testing"
)

func TestSecp256k1BasicSignAndVerify(t *testing.T) {
	priv, pub, err := GenerateSecp256k1Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("hello! and welcome to some awesome crypto primitives")

	sig, err := priv.Sign(data)
	if err != nil {
		t.Fatal(err)
	}

	ok, err := pub.Verify(data, sig)
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatal("signature didnt match")
	}

	// change data
	data[0] = ^data[0]
	ok, err = pub.Verify(data, sig)
	if err != nil {
		t.Fatal(err)
	}

	if ok {
		t.Fatal("signature matched and shouldn't")
	}
}

func TestSecp256k1SignZero(t *testing.T) {
	priv, pub, err := GenerateSecp256k1Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 0)
	sig, err := priv.Sign(data)
	if err != nil {
		t.Fatal(err)
	}

	ok, err := pub.Verify(data, sig)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("signature didn't match")
	}
}

func TestSecp256k1MarshalLoop(t *testing.T) {
	priv, pub, err := GenerateSecp256k1Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	privB, err := priv.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	privNew, err := UnmarshalPrivateKey(privB)
	if err != nil {
		t.Fatal(err)
	}

	if !priv.Equals(privNew) || !privNew.Equals(priv) {
		t.Fatal("keys are not equal")
	}

	pubB, err := pub.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	pubNew, err := UnmarshalPublicKey(pubB)
	if err != nil {
		t.Fatal(err)
	}

	if !pub.Equals(pubNew) || !pubNew.Equals(pub) {
		t.Fatal("keys are not equal")
	}

}
