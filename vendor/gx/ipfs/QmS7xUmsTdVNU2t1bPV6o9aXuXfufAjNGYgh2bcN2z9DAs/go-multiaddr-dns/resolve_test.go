package madns

import (
	"context"
	"net"
	"testing"

	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
)

var ip4a = net.IPAddr{IP: net.ParseIP("192.0.2.1")}
var ip4b = net.IPAddr{IP: net.ParseIP("192.0.2.2")}
var ip6a = net.IPAddr{IP: net.ParseIP("2001:db8::a3")}
var ip6b = net.IPAddr{IP: net.ParseIP("2001:db8::a4")}

var ip4ma = ma.StringCast("/ip4/" + ip4a.IP.String())
var ip4mb = ma.StringCast("/ip4/" + ip4b.IP.String())
var ip6ma = ma.StringCast("/ip6/" + ip6a.IP.String())
var ip6mb = ma.StringCast("/ip6/" + ip6b.IP.String())

var txtmc = ma.Join(ip4ma, ma.StringCast("/tcp/123/http"))
var txtmd = ma.Join(ip4ma, ma.StringCast("/tcp/123"))
var txtme = ma.Join(ip4ma, ma.StringCast("/tcp/789/http"))

var txta = "dnsaddr=" + ip4ma.String()
var txtb = "dnsaddr=" + ip6ma.String()
var txtc = "dnsaddr=" + txtmc.String()
var txtd = "dnsaddr=" + txtmd.String()
var txte = "dnsaddr=" + txtme.String()

func makeResolver() *Resolver {
	mock := &MockBackend{
		IP: map[string][]net.IPAddr{
			"example.com": []net.IPAddr{ip4a, ip4b, ip6a, ip6b},
		},
		TXT: map[string][]string{
			"_dnsaddr.example.com":  []string{txta, txtb},
			"_dnsaddr.matching.com": []string{txtc, txtd, txte},
		},
	}
	resolver := &Resolver{Backend: mock}
	return resolver
}

func TestMatches(t *testing.T) {
	if !Matches(ma.StringCast("/dns4/example.com")) {
		t.Fatalf("expected match, didn't: /dns4/example.com")
	}
	if !Matches(ma.StringCast("/dns6/example.com")) {
		t.Fatalf("expected match, didn't: /dns6/example.com")
	}
	if !Matches(ma.StringCast("/dnsaddr/example.com")) {
		t.Fatalf("expected match, didn't: /dnsaddr/example.com")
	}
	if Matches(ip4ma) {
		t.Fatalf("expected no-match, but did: %s", ip4ma.String())
	}
}

func TestSimpleIPResolve(t *testing.T) {
	ctx := context.Background()
	resolver := makeResolver()

	addrs4, err := resolver.Resolve(ctx, ma.StringCast("/dns4/example.com"))
	if err != nil {
		t.Error(err)
	}
	if len(addrs4) != 2 || !addrs4[0].Equal(ip4ma) || addrs4[0].Equal(ip4mb) {
		t.Fatalf("expected [%s %s], got %+v", ip4ma, ip4mb, addrs4)
	}

	addrs6, err := resolver.Resolve(ctx, ma.StringCast("/dns6/example.com"))
	if err != nil {
		t.Error(err)
	}
	if len(addrs6) != 2 || !addrs6[0].Equal(ip6ma) || addrs6[0].Equal(ip6mb) {
		t.Fatalf("expected [%s %s], got %+v", ip6ma, ip6mb, addrs6)
	}
}

func TestSimpleTXTResolve(t *testing.T) {
	ctx := context.Background()
	resolver := makeResolver()

	addrs, err := resolver.Resolve(ctx, ma.StringCast("/dnsaddr/example.com"))
	if err != nil {
		t.Error(err)
	}
	if len(addrs) != 2 || !addrs[0].Equal(ip4ma) || addrs[0].Equal(ip6ma) {
		t.Fatalf("expected [%s %s], got %+v", ip4ma, ip6ma, addrs)
	}
}

func TestNonResolvable(t *testing.T) {
	ctx := context.Background()
	resolver := makeResolver()

	addrs, err := resolver.Resolve(ctx, ip4ma)
	if err != nil {
		t.Error(err)
	}
	if len(addrs) != 1 || !addrs[0].Equal(ip4ma) {
		t.Fatalf("expected [%s], got %+v", ip4ma, addrs)
	}
}

func TestEmptyResult(t *testing.T) {
	ctx := context.Background()
	resolver := makeResolver()

	addrs, err := resolver.Resolve(ctx, ma.StringCast("/dnsaddr/none.com"))
	if err != nil {
		t.Error(err)
	}
	if len(addrs) > 0 {
		t.Fatalf("expected [], got %+v", addrs)
	}
}

func TestDnsaddrMatching(t *testing.T) {
	ctx := context.Background()
	resolver := makeResolver()

	addrs, err := resolver.Resolve(ctx, ma.StringCast("/dnsaddr/matching.com/tcp/123/http"))
	if err != nil {
		t.Error(err)
	}
	if len(addrs) != 1 || !addrs[0].Equal(txtmc) {
		t.Fatalf("expected [%s], got %+v", txtmc, addrs)
	}

	addrs, err = resolver.Resolve(ctx, ma.StringCast("/dnsaddr/matching.com/tcp/123"))
	if err != nil {
		t.Error(err)
	}
	if len(addrs) != 1 || !addrs[0].Equal(txtmd) {
		t.Fatalf("expected [%s], got %+v", txtmd, addrs)
	}
}
