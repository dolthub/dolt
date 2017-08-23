package manet

import (
	"net"
	"testing"

	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
)

type GenFunc func() (ma.Multiaddr, error)

func testConvert(t *testing.T, s string, gen GenFunc) {
	m, err := gen()
	if err != nil {
		t.Fatal("failed to generate.")
	}

	if s2 := m.String(); err != nil || s2 != s {
		t.Fatal("failed to convert: " + s + " != " + s2)
	}
}

func testToNetAddr(t *testing.T, maddr, ntwk, addr string) {
	m, err := ma.NewMultiaddr(maddr)
	if err != nil {
		t.Fatal("failed to generate.")
	}

	naddr, err := ToNetAddr(m)
	if addr == "" { // should fail
		if err == nil {
			t.Fatalf("failed to error: %s", m)
		}
		return
	}

	// shouldn't fail
	if err != nil {
		t.Fatalf("failed to convert to net addr: %s", m)
	}

	if naddr.String() != addr {
		t.Fatalf("naddr.Address() == %s != %s", naddr, addr)
	}

	if naddr.Network() != ntwk {
		t.Fatalf("naddr.Network() == %s != %s", naddr.Network(), ntwk)
	}

	// should convert properly
	switch ntwk {
	case "tcp":
		_ = naddr.(*net.TCPAddr)
	case "udp":
		_ = naddr.(*net.UDPAddr)
	case "ip":
		_ = naddr.(*net.IPAddr)
	}
}

func TestFromIP4(t *testing.T) {
	testConvert(t, "/ip4/10.20.30.40", func() (ma.Multiaddr, error) {
		return FromIP(net.ParseIP("10.20.30.40"))
	})
}

func TestFromIP6(t *testing.T) {
	testConvert(t, "/ip6/2001:4860:0:2001::68", func() (ma.Multiaddr, error) {
		return FromIP(net.ParseIP("2001:4860:0:2001::68"))
	})
}

func TestFromTCP(t *testing.T) {
	testConvert(t, "/ip4/10.20.30.40/tcp/1234", func() (ma.Multiaddr, error) {
		return FromNetAddr(&net.TCPAddr{
			IP:   net.ParseIP("10.20.30.40"),
			Port: 1234,
		})
	})
}

func TestFromUDP(t *testing.T) {
	testConvert(t, "/ip4/10.20.30.40/udp/1234", func() (ma.Multiaddr, error) {
		return FromNetAddr(&net.UDPAddr{
			IP:   net.ParseIP("10.20.30.40"),
			Port: 1234,
		})
	})
}

func TestThinWaist(t *testing.T) {
	addrs := map[string]bool{
		"/ip4/127.0.0.1/udp/1234":              true,
		"/ip4/127.0.0.1/tcp/1234":              true,
		"/ip4/127.0.0.1/udp/1234/tcp/1234":     true,
		"/ip4/127.0.0.1/tcp/12345/ip4/1.2.3.4": true,
		"/ip6/::1/tcp/80":                      true,
		"/ip6/::1/udp/80":                      true,
		"/ip6/::1":                             true,
		"/tcp/1234/ip4/1.2.3.4":                false,
		"/tcp/1234":                            false,
		"/tcp/1234/udp/1234":                   false,
		"/ip4/1.2.3.4/ip4/2.3.4.5":             true,
		"/ip6/::1/ip4/2.3.4.5":                 true,
	}

	for a, res := range addrs {
		m, err := ma.NewMultiaddr(a)
		if err != nil {
			t.Fatalf("failed to construct Multiaddr: %s", a)
		}

		if IsThinWaist(m) != res {
			t.Fatalf("IsThinWaist(%s) != %v", a, res)
		}
	}
}

func TestDialArgs(t *testing.T) {
	test := func(e_maddr, e_nw, e_host string) {
		m, err := ma.NewMultiaddr(e_maddr)
		if err != nil {
			t.Fatal("failed to construct", "/ip4/127.0.0.1/udp/1234", e_maddr)
		}

		nw, host, err := DialArgs(m)
		if err != nil {
			t.Fatal("failed to get dial args", e_maddr, m, err)
		}

		if nw != e_nw {
			t.Error("failed to get udp network Dial Arg", e_nw, nw)
		}

		if host != e_host {
			t.Error("failed to get host:port Dial Arg", e_host, host)
		}
	}

	test("/ip4/127.0.0.1/udp/1234", "udp4", "127.0.0.1:1234")
	test("/ip4/127.0.0.1/tcp/4321", "tcp4", "127.0.0.1:4321")
	test("/ip6/::1/udp/1234", "udp6", "[::1]:1234")
	test("/ip6/::1/tcp/4321", "tcp6", "[::1]:4321")
}
