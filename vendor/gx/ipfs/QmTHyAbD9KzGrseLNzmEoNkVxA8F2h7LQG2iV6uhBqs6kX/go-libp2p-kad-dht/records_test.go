package dht

import (
	"context"
	"crypto/rand"
	"testing"

	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	ci "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
)

func TestPubkeyExtract(t *testing.T) {
	_, pk, err := ci.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	pid, err := peer.IDFromEd25519PublicKey(pk)
	if err != nil {
		t.Fatal(err)
	}

	// no need to actually construct one
	d := new(IpfsDHT)

	pk_out, err := d.GetPublicKey(context.Background(), pid)
	if err != nil {
		t.Fatal(err)
	}

	if !pk_out.Equals(pk) {
		t.Fatal("got incorrect public key out")
	}
}
