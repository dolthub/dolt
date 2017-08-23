package pnet

import (
	"bytes"
	"context"
	"math/rand"
	"testing"

	tconn "gx/ipfs/QmQVm7pWYKPStMeMrXNRpvAJE5rSm9ThtQoNmjNHC7sh3k/go-libp2p-transport"
	dconn "gx/ipfs/QmfVFvii3oxJDs162R7t6nM1tZt7b1Hkk3T535vruAZv2d/go-libp2p-dummy-conn"
)

var testPSK = [32]byte{} // null bytes are as good test key as any other key

func setupPSKConns(ctx context.Context, t *testing.T) (tconn.Conn, tconn.Conn) {
	conn1, conn2, err := dconn.NewDummyConnPair()
	if err != nil {
		t.Fatal(err)
	}

	psk1, err := newPSKConn(&testPSK, conn1)
	if err != nil {
		t.Fatal(err)
	}
	psk2, err := newPSKConn(&testPSK, conn2)
	if err != nil {
		t.Fatal(err)
	}
	return psk1, psk2
}

func TestPSKSimpelMessges(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	psk1, psk2 := setupPSKConns(ctx, t)
	msg1 := []byte("hello world")
	out1 := make([]byte, len(msg1))

	_, err := psk1.Write(msg1)
	if err != nil {
		t.Fatal(err)
	}
	n, err := psk2.Read(out1)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(out1) {
		t.Fatalf("expected to read %d bytes, read: %d", len(out1), n)
	}

	if !bytes.Equal(msg1, out1) {
		t.Fatalf("input and output are not the same")
	}
}

func TestPSKFragmentation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	psk1, psk2 := setupPSKConns(ctx, t)

	in := make([]byte, 1000)
	_, err := rand.Read(in)
	if err != nil {
		t.Fatal(err)
	}

	out := make([]byte, 100)

	_, err = psk1.Write(in)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		_, err = psk2.Read(out)
		if !bytes.Equal(in[:100], out) {
			t.Fatalf("input and output are not the same")
		}
		in = in[100:]
	}
}
