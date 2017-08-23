package tcp

import (
	"testing"

	tpt "gx/ipfs/QmQVm7pWYKPStMeMrXNRpvAJE5rSm9ThtQoNmjNHC7sh3k/go-libp2p-transport"
	utils "gx/ipfs/QmQVm7pWYKPStMeMrXNRpvAJE5rSm9ThtQoNmjNHC7sh3k/go-libp2p-transport/test"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
)

func TestTcpTransport(t *testing.T) {
	ta := NewTCPTransport()
	tb := NewTCPTransport()

	zero := "/ip4/127.0.0.1/tcp/0"
	utils.SubtestTransport(t, ta, tb, zero)
}

func TestTcpTransportCantListenUtp(t *testing.T) {
	utpa, err := ma.NewMultiaddr("/ip4/127.0.0.1/udp/0/utp")
	if err != nil {
		t.Fatal(err)
	}

	tpt := NewTCPTransport()
	_, err = tpt.Listen(utpa)
	if err == nil {
		t.Fatal("shouldnt be able to listen on utp addr with tcp transport")
	}
}

func TestCorrectIPVersionMatching(t *testing.T) {
	ta := NewTCPTransport()

	addr4, err := ma.NewMultiaddr("/ip4/0.0.0.0/tcp/0")
	if err != nil {
		t.Fatal(err)
	}
	addr6, err := ma.NewMultiaddr("/ip6/::1/tcp/0")
	if err != nil {
		t.Fatal(err)
	}

	d4, err := ta.Dialer(addr4, tpt.ReuseportOpt(true))
	if err != nil {
		t.Fatal(err)
	}

	d6, err := ta.Dialer(addr6, tpt.ReuseportOpt(true))
	if err != nil {
		t.Fatal(err)
	}

	if d4.Matches(addr6) {
		t.Fatal("tcp4 dialer should not match ipv6 address")
	}

	if d6.Matches(addr4) {
		t.Fatal("tcp4 dialer should not match ipv6 address")
	}
}
