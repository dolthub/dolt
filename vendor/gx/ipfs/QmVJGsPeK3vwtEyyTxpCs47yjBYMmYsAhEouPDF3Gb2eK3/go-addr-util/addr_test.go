package addrutil

import (
	"testing"

	manet "gx/ipfs/QmX3U3YXCQ6UYBxq2LVWF8dARS1hPUTEYLrSx654Qyxyw6/go-multiaddr-net"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
)

func newMultiaddr(t *testing.T, s string) ma.Multiaddr {
	maddr, err := ma.NewMultiaddr(s)
	if err != nil {
		t.Fatal(err)
	}
	return maddr
}

func TestFilterAddrs(t *testing.T) {

	bad := []ma.Multiaddr{
		newMultiaddr(t, "/ip4/1.2.3.4/udp/1234"),           // unreliable
		newMultiaddr(t, "/ip4/1.2.3.4/udp/1234/sctp/1234"), // not in manet
		newMultiaddr(t, "/ip4/1.2.3.4/udp/1234/udt"),       // udt is broken on arm
		newMultiaddr(t, "/ip6/fe80::1/tcp/1234"),           // link local
		newMultiaddr(t, "/ip6/fe80::100/tcp/1234"),         // link local
	}

	good := []ma.Multiaddr{
		newMultiaddr(t, "/ip4/127.0.0.1/tcp/1234"),
		newMultiaddr(t, "/ip6/::1/tcp/1234"),
		newMultiaddr(t, "/ip4/1.2.3.4/udp/1234/utp"),
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/1234/ws"),
	}

	goodAndBad := append(good, bad...)

	// test filters

	for _, a := range bad {
		if AddrUsable(a, false) {
			t.Errorf("addr %s should be unusable", a)
		}
	}

	for _, a := range good {
		if !AddrUsable(a, false) {
			t.Errorf("addr %s should be usable", a)
		}
	}

	subtestAddrsEqual(t, FilterUsableAddrs(bad), []ma.Multiaddr{})
	subtestAddrsEqual(t, FilterUsableAddrs(good), good)
	subtestAddrsEqual(t, FilterUsableAddrs(goodAndBad), good)
}

func subtestAddrsEqual(t *testing.T, a, b []ma.Multiaddr) {
	if len(a) != len(b) {
		t.Error(t)
	}

	in := func(addr ma.Multiaddr, l []ma.Multiaddr) bool {
		for _, addr2 := range l {
			if addr.Equal(addr2) {
				return true
			}
		}
		return false
	}

	for _, aa := range a {
		if !in(aa, b) {
			t.Errorf("%s not in %s", aa, b)
		}
	}
}

func TestInterfaceAddrs(t *testing.T) {
	addrs, err := InterfaceAddresses()
	if err != nil {
		t.Fatal(err)
	}

	if len(addrs) < 1 {
		t.Error("no addresses")
	}

	for _, a := range addrs {
		if manet.IsIP6LinkLocal(a) {
			t.Error("should not return ip link local addresses", a)
		}
	}

	if len(addrs) < 1 {
		t.Error("no good interface addrs")
	}
}

func TestResolvingAddrs(t *testing.T) {

	unspec := []ma.Multiaddr{
		newMultiaddr(t, "/ip4/0.0.0.0/tcp/1234"),
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/1234"),
		newMultiaddr(t, "/ip6/::/tcp/1234"),
		newMultiaddr(t, "/ip6/::100/tcp/1234"),
	}

	iface := []ma.Multiaddr{
		newMultiaddr(t, "/ip4/127.0.0.1"),
		newMultiaddr(t, "/ip4/10.20.30.40"),
		newMultiaddr(t, "/ip6/::1"),
		newMultiaddr(t, "/ip6/::f"),
	}

	spec := []ma.Multiaddr{
		newMultiaddr(t, "/ip4/127.0.0.1/tcp/1234"),
		newMultiaddr(t, "/ip4/10.20.30.40/tcp/1234"),
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/1234"),
		newMultiaddr(t, "/ip6/::1/tcp/1234"),
		newMultiaddr(t, "/ip6/::f/tcp/1234"),
		newMultiaddr(t, "/ip6/::100/tcp/1234"),
	}

	actual, err := ResolveUnspecifiedAddresses(unspec, iface)
	if err != nil {
		t.Fatal(err)
	}

	for i, a := range actual {
		if !a.Equal(spec[i]) {
			t.Error(a, " != ", spec[i])
		}
	}

	ip4u := []ma.Multiaddr{newMultiaddr(t, "/ip4/0.0.0.0")}
	ip4i := []ma.Multiaddr{newMultiaddr(t, "/ip4/1.2.3.4")}

	ip6u := []ma.Multiaddr{newMultiaddr(t, "/ip6/::")}
	ip6i := []ma.Multiaddr{newMultiaddr(t, "/ip6/::1")}

	if _, err := ResolveUnspecifiedAddress(ip4u[0], ip6i); err == nil {
		t.Fatal("should have failed")
	}
	if _, err := ResolveUnspecifiedAddress(ip6u[0], ip4i); err == nil {
		t.Fatal("should have failed")
	}

	if _, err := ResolveUnspecifiedAddresses(ip6u, ip4i); err == nil {
		t.Fatal("should have failed")
	}
	if _, err := ResolveUnspecifiedAddresses(ip4u, ip6i); err == nil {
		t.Fatal("should have failed")
	}

}

func TestWANShareable(t *testing.T) {

	wanok := []ma.Multiaddr{
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/1234"),
		newMultiaddr(t, "/ip6/abcd::1/tcp/1234"),
	}

	wanbad := []ma.Multiaddr{
		newMultiaddr(t, "/ip4/127.0.0.1/tcp/1234"),
		newMultiaddr(t, "/ip4/0.0.0.0/tcp/1234"),
		newMultiaddr(t, "/ip6/::1/tcp/1234"),
		newMultiaddr(t, "/ip6/::/tcp/1234"),
		newMultiaddr(t, "/ip6/fe80::1/tcp/1234"),
		newMultiaddr(t, "/ip6/fe80::/tcp/1234"),
	}

	for _, a := range wanok {
		if !AddrIsShareableOnWAN(a) {
			t.Error("should be true", a)
		}
	}

	for _, a := range wanbad {
		if AddrIsShareableOnWAN(a) {
			t.Error("should be false", a)
		}
	}

	wanok2 := WANShareableAddrs(wanok)
	if len(wanok) != len(wanok2) {
		t.Error("should be the same")
	}

	wanbad2 := WANShareableAddrs(wanbad)
	if len(wanbad2) != 0 {
		t.Error("should be zero")
	}
}

func TestSubtract(t *testing.T) {

	a := []ma.Multiaddr{
		newMultiaddr(t, "/ip4/127.0.0.1/tcp/1234"),
		newMultiaddr(t, "/ip4/0.0.0.0/tcp/1234"),
		newMultiaddr(t, "/ip6/::1/tcp/1234"),
		newMultiaddr(t, "/ip6/::/tcp/1234"),
		newMultiaddr(t, "/ip6/fe80::1/tcp/1234"),
		newMultiaddr(t, "/ip6/fe80::/tcp/1234"),
	}

	b := []ma.Multiaddr{
		newMultiaddr(t, "/ip4/127.0.0.1/tcp/1234"),
		newMultiaddr(t, "/ip6/::1/tcp/1234"),
		newMultiaddr(t, "/ip6/fe80::1/tcp/1234"),
	}

	c1 := []ma.Multiaddr{
		newMultiaddr(t, "/ip4/0.0.0.0/tcp/1234"),
		newMultiaddr(t, "/ip6/::/tcp/1234"),
		newMultiaddr(t, "/ip6/fe80::/tcp/1234"),
	}

	c2 := Subtract(a, b)
	if len(c1) != len(c2) {
		t.Error("should be the same")
	}
	for i, ca := range c1 {
		if !c2[i].Equal(ca) {
			t.Error("should be the same", ca, c2[i])
		}
	}
}
