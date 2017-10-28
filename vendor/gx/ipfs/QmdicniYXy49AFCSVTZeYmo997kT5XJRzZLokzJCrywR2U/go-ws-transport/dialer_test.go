package websocket

import (
	"testing"

	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
)

func TestDialerMatches(t *testing.T) {
	addrWs, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/5555/ws")
	if err != nil {
		t.Fatal(err)
	}

	addrTcp, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/5555")
	if err != nil {
		t.Fatal(err)
	}

	d := &dialer{}
	matchTrue := d.Matches(addrWs)
	matchFalse := d.Matches(addrTcp)

	if !matchTrue {
		t.Fatal("expected to match websocket maddr, but did not")
	}

	if matchFalse {
		t.Fatal("expected to not match tcp maddr, but did")
	}
}
