package mafmt

import (
	"testing"

	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
)

func assertMatches(t *testing.T, p Pattern, args ...[]string) {
	t.Logf("testing assertions for %q", p)
	for _, argset := range args {
		for _, s := range argset {
			addr, err := ma.NewMultiaddr(s)
			if err != nil {
				t.Fatal(err)
			}

			if !p.Matches(addr) {
				t.Fatal("mismatch!", s, p)
			}
		}
	}
}

func assertMismatches(t *testing.T, p Pattern, args ...[]string) {
	for _, argset := range args {
		for _, s := range argset {
			addr, err := ma.NewMultiaddr(s)
			if err != nil {
				t.Fatal(err)
			}

			if p.Matches(addr) {
				t.Fatal("incorrect match!", s, p)
			}
		}
	}
}

func TestBasicMatching(t *testing.T) {
	good_ip := []string{
		"/ip4/0.0.0.0",
		"/ip6/fc00::",
	}

	bad_ip := []string{
		"/ip4/0.0.0.0/tcp/555",
		"/udp/789/ip6/fc00::",
	}

	good_tcp := []string{
		"/ip4/0.0.7.6/tcp/1234",
		"/ip6/::/tcp/0",
	}

	bad_tcp := []string{
		"/tcp/12345",
		"/ip6/fc00::/udp/5523/tcp/9543",
	}

	good_udp := []string{
		"/ip4/0.0.7.6/udp/1234",
		"/ip6/::/udp/0",
	}

	bad_udp := []string{
		"/udp/12345",
		"/ip6/fc00::/tcp/5523/udp/9543",
	}

	good_utp := []string{
		"/ip4/1.2.3.4/udp/3456/utp",
		"/ip6/::/udp/0/utp",
	}

	bad_utp := []string{
		"/ip4/0.0.0.0/tcp/12345/utp",
		"/ip6/1.2.3.4/ip4/0.0.0.0/udp/1234/utp",
		"/utp",
	}

	good_quic := []string{
		"/ip4/1.2.3.4/udp/1234/quic",
		"/ip6/::/udp/1234/quic",
	}

	bad_quic := []string{
		"/ip4/0.0.0.0/tcp/12345/quic",
		"/ip6/1.2.3.4/ip4/0.0.0.0/udp/1234/quic",
		"/quic",
	}

	good_ipfs := []string{
		"/ip4/1.2.3.4/tcp/1234/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
		"/ip6/::/tcp/1234/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
		"/ip6/::/udp/1234/utp/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
		"/ip4/0.0.0.0/udp/1234/utp/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
	}

	bad_ipfs := []string{
		"/ip4/1.2.3.4/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
		"/ip6/::/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
		"/tcp/123/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
		"/ip6/::/udp/1234/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
		"/ip6/::/utp/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
		"/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
	}

	assertMatches(t, IP, good_ip)
	assertMismatches(t, IP, bad_ip, good_tcp)

	assertMatches(t, TCP, good_tcp)
	assertMismatches(t, TCP, bad_tcp, good_ip)

	assertMatches(t, UDP, good_udp)
	assertMismatches(t, UDP, bad_udp, good_ip, good_tcp, good_ipfs, good_utp, good_quic)

	assertMatches(t, UTP, good_utp)
	assertMismatches(t, UTP, bad_utp, good_ip, good_tcp, good_udp, good_quic)

	assertMatches(t, QUIC, good_quic)
	assertMismatches(t, QUIC, bad_quic, good_ip, good_tcp, good_udp, good_utp)

	assertMatches(t, Reliable, good_utp, good_tcp, good_quic)
	assertMismatches(t, Reliable, good_ip, good_udp, good_ipfs)

	assertMatches(t, Unreliable, good_udp)
	assertMismatches(t, Unreliable, good_ip, good_tcp, good_utp, good_ipfs, good_quic)

	assertMatches(t, IPFS, good_ipfs)
	assertMismatches(t, IPFS, bad_ipfs, good_ip, good_tcp, good_utp, good_udp, good_quic)
}
