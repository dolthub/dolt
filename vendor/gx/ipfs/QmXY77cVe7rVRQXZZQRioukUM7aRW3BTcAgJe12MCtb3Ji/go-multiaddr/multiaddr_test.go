package multiaddr

import (
	"bytes"
	"encoding/hex"
	"math/rand"
	"strings"
	"testing"
	"time"
)

func newMultiaddr(t *testing.T, a string) Multiaddr {
	m, err := NewMultiaddr(a)
	if err != nil {
		t.Error(err)
	}
	return m
}

func TestConstructFails(t *testing.T) {
	cases := []string{
		"/ip4",
		"/ip4/::1",
		"/ip4/fdpsofodsajfdoisa",
		"/ip6",
		"/udp",
		"/tcp",
		"/sctp",
		"/udp/65536",
		"/tcp/65536",
		"/quic/65536",
		"/onion/9imaq4ygg2iegci7:80",
		"/onion/aaimaq4ygg2iegci7:80",
		"/onion/timaq4ygg2iegci7:0",
		"/onion/timaq4ygg2iegci7:-1",
		"/onion/timaq4ygg2iegci7",
		"/onion/timaq4ygg2iegci@:666",
		"/udp/1234/sctp",
		"/udp/1234/udt/1234",
		"/udp/1234/utp/1234",
		"/ip4/127.0.0.1/udp/jfodsajfidosajfoidsa",
		"/ip4/127.0.0.1/udp",
		"/ip4/127.0.0.1/tcp/jfodsajfidosajfoidsa",
		"/ip4/127.0.0.1/tcp",
		"/ip4/127.0.0.1/quic/1234",
		"/ip4/127.0.0.1/ipfs",
		"/ip4/127.0.0.1/ipfs/tcp",
		"/unix",
		"/ip4/1.2.3.4/tcp/80/unix",
	}

	for _, a := range cases {
		if _, err := NewMultiaddr(a); err == nil {
			t.Errorf("should have failed: %s - %s", a, err)
		}
	}
}

func TestConstructSucceeds(t *testing.T) {
	cases := []string{
		"/ip4/1.2.3.4",
		"/ip4/0.0.0.0",
		"/ip6/::1",
		"/ip6/2601:9:4f81:9700:803e:ca65:66e8:c21",
		"/ip6/2601:9:4f81:9700:803e:ca65:66e8:c21/udp/1234/quic",
		"/onion/timaq4ygg2iegci7:1234",
		"/onion/timaq4ygg2iegci7:80/http",
		"/udp/0",
		"/tcp/0",
		"/sctp/0",
		"/udp/1234",
		"/tcp/1234",
		"/sctp/1234",
		"/udp/65535",
		"/tcp/65535",
		"/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
		"/udp/1234/sctp/1234",
		"/udp/1234/udt",
		"/udp/1234/utp",
		"/tcp/1234/http",
		"/tcp/1234/https",
		"/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC/tcp/1234",
		"/ip4/127.0.0.1/udp/1234",
		"/ip4/127.0.0.1/udp/0",
		"/ip4/127.0.0.1/tcp/1234",
		"/ip4/127.0.0.1/tcp/1234/",
		"/ip4/127.0.0.1/udp/1234/quic",
		"/ip4/127.0.0.1/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
		"/ip4/127.0.0.1/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC/tcp/1234",
		"/unix/a/b/c/d/e",
		"/unix/stdio",
		"/ip4/1.2.3.4/tcp/80/unix/a/b/c/d/e/f",
		"/ip4/127.0.0.1/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC/tcp/1234/unix/stdio",
	}

	for _, a := range cases {
		if _, err := NewMultiaddr(a); err != nil {
			t.Errorf("should have succeeded: %s -- %s", a, err)
		}
	}
}

func TestEqual(t *testing.T) {
	m1 := newMultiaddr(t, "/ip4/127.0.0.1/udp/1234")
	m2 := newMultiaddr(t, "/ip4/127.0.0.1/tcp/1234")
	m3 := newMultiaddr(t, "/ip4/127.0.0.1/tcp/1234")
	m4 := newMultiaddr(t, "/ip4/127.0.0.1/tcp/1234/")

	if m1.Equal(m2) {
		t.Error("should not be equal")
	}

	if m2.Equal(m1) {
		t.Error("should not be equal")
	}

	if !m2.Equal(m3) {
		t.Error("should be equal")
	}

	if !m3.Equal(m2) {
		t.Error("should be equal")
	}

	if !m1.Equal(m1) {
		t.Error("should be equal")
	}

	if !m2.Equal(m4) {
		t.Error("should be equal")
	}

	if !m4.Equal(m3) {
		t.Error("should be equal")
	}
}

func TestStringToBytes(t *testing.T) {

	testString := func(s string, h string) {
		b1, err := hex.DecodeString(h)
		if err != nil {
			t.Error("failed to decode hex", h)
		}

		b2, err := stringToBytes(s)
		if err != nil {
			t.Error("failed to convert", s)
		}

		if !bytes.Equal(b1, b2) {
			t.Error("failed to convert", s, "to", b1, "got", b2)
		}

		if err := validateBytes(b2); err != nil {
			t.Error(err)
		}
	}

	testString("/ip4/127.0.0.1/udp/1234", "047f0000011104d2")
	testString("/ip4/127.0.0.1/tcp/4321", "047f0000010610e1")
	testString("/ip4/127.0.0.1/udp/1234/ip4/127.0.0.1/tcp/4321", "047f0000011104d2047f0000010610e1")
}

func TestBytesToString(t *testing.T) {

	testString := func(s1 string, h string) {
		b, err := hex.DecodeString(h)
		if err != nil {
			t.Error("failed to decode hex", h)
		}

		if err := validateBytes(b); err != nil {
			t.Error(err)
		}

		s2, err := bytesToString(b)
		if err != nil {
			t.Error("failed to convert", b, err)
		}

		if s1 != s2 {
			t.Error("failed to convert", b, "to", s1, "got", s2)
		}
	}

	testString("/ip4/127.0.0.1/udp/1234", "047f0000011104d2")
	testString("/ip4/127.0.0.1/tcp/4321", "047f0000010610e1")
	testString("/ip4/127.0.0.1/udp/1234/ip4/127.0.0.1/tcp/4321", "047f0000011104d2047f0000010610e1")
	testString("/onion/aaimaq4ygg2iegci:80", "bc030010c0439831b48218480050")
}

func TestBytesSplitAndJoin(t *testing.T) {

	testString := func(s string, res []string) {
		m, err := NewMultiaddr(s)
		if err != nil {
			t.Fatal("failed to convert", s, err)
		}

		split := Split(m)
		if len(split) != len(res) {
			t.Error("not enough split components", split)
			return
		}

		for i, a := range split {
			if a.String() != res[i] {
				t.Errorf("split component failed: %s != %s", a, res[i])
			}
		}

		joined := Join(split...)
		if !m.Equal(joined) {
			t.Errorf("joined components failed: %s != %s", m, joined)
		}

		// modifying underlying bytes is fine.
		m2 := m.(*multiaddr)
		for i := range m2.bytes {
			m2.bytes[i] = 0
		}

		for i, a := range split {
			if a.String() != res[i] {
				t.Errorf("split component failed: %s != %s", a, res[i])
			}
		}
	}

	testString("/ip4/1.2.3.4/udp/1234", []string{"/ip4/1.2.3.4", "/udp/1234"})
	testString("/ip4/1.2.3.4/tcp/1/ip4/2.3.4.5/udp/2",
		[]string{"/ip4/1.2.3.4", "/tcp/1", "/ip4/2.3.4.5", "/udp/2"})
	testString("/ip4/1.2.3.4/utp/ip4/2.3.4.5/udp/2/udt",
		[]string{"/ip4/1.2.3.4", "/utp", "/ip4/2.3.4.5", "/udp/2", "/udt"})
}

func TestProtocols(t *testing.T) {
	m, err := NewMultiaddr("/ip4/127.0.0.1/udp/1234")
	if err != nil {
		t.Error("failed to construct", "/ip4/127.0.0.1/udp/1234")
	}

	ps := m.Protocols()
	if ps[0].Code != ProtocolWithName("ip4").Code {
		t.Error(ps[0], ProtocolWithName("ip4"))
		t.Error("failed to get ip4 protocol")
	}

	if ps[1].Code != ProtocolWithName("udp").Code {
		t.Error(ps[1], ProtocolWithName("udp"))
		t.Error("failed to get udp protocol")
	}

}

func TestProtocolsWithString(t *testing.T) {
	pwn := ProtocolWithName
	good := map[string][]Protocol{
		"/ip4":                    []Protocol{pwn("ip4")},
		"/ip4/tcp":                []Protocol{pwn("ip4"), pwn("tcp")},
		"ip4/tcp/udp/ip6":         []Protocol{pwn("ip4"), pwn("tcp"), pwn("udp"), pwn("ip6")},
		"////////ip4/tcp":         []Protocol{pwn("ip4"), pwn("tcp")},
		"ip4/udp/////////":        []Protocol{pwn("ip4"), pwn("udp")},
		"////////ip4/tcp////////": []Protocol{pwn("ip4"), pwn("tcp")},
	}

	for s, ps1 := range good {
		ps2, err := ProtocolsWithString(s)
		if err != nil {
			t.Errorf("ProtocolsWithString(%s) should have succeeded", s)
		}

		for i, ps1p := range ps1 {
			ps2p := ps2[i]
			if ps1p.Code != ps2p.Code {
				t.Errorf("mismatch: %s != %s, %s", ps1p.Name, ps2p.Name, s)
			}
		}
	}

	bad := []string{
		"dsijafd",                           // bogus proto
		"/ip4/tcp/fidosafoidsa",             // bogus proto
		"////////ip4/tcp/21432141/////////", // bogus proto
		"////////ip4///////tcp/////////",    // empty protos in between
	}

	for _, s := range bad {
		if _, err := ProtocolsWithString(s); err == nil {
			t.Errorf("ProtocolsWithString(%s) should have failed", s)
		}
	}

}

func TestEncapsulate(t *testing.T) {
	m, err := NewMultiaddr("/ip4/127.0.0.1/udp/1234")
	if err != nil {
		t.Error(err)
	}

	m2, err := NewMultiaddr("/udp/5678")
	if err != nil {
		t.Error(err)
	}

	b := m.Encapsulate(m2)
	if s := b.String(); s != "/ip4/127.0.0.1/udp/1234/udp/5678" {
		t.Error("encapsulate /ip4/127.0.0.1/udp/1234/udp/5678 failed.", s)
	}

	m3, _ := NewMultiaddr("/udp/5678")
	c := b.Decapsulate(m3)
	if s := c.String(); s != "/ip4/127.0.0.1/udp/1234" {
		t.Error("decapsulate /udp failed.", "/ip4/127.0.0.1/udp/1234", s)
	}

	m4, _ := NewMultiaddr("/ip4/127.0.0.1")
	d := c.Decapsulate(m4)
	if s := d.String(); s != "" {
		t.Error("decapsulate /ip4 failed.", "/", s)
	}
}

func assertValueForProto(t *testing.T, a Multiaddr, p int, exp string) {
	t.Logf("checking for %s in %s", ProtocolWithCode(p).Name, a)
	fv, err := a.ValueForProtocol(p)
	if err != nil {
		t.Fatal(err)
	}

	if fv != exp {
		t.Fatalf("expected %q for %d in %s, but got %q instead", exp, p, a, fv)
	}
}

func TestGetValue(t *testing.T) {
	a := newMultiaddr(t, "/ip4/127.0.0.1/utp/tcp/5555/udp/1234/utp/ipfs/QmbHVEEepCi7rn7VL7Exxpd2Ci9NNB6ifvqwhsrbRMgQFP")
	assertValueForProto(t, a, P_IP4, "127.0.0.1")
	assertValueForProto(t, a, P_UTP, "")
	assertValueForProto(t, a, P_TCP, "5555")
	assertValueForProto(t, a, P_UDP, "1234")
	assertValueForProto(t, a, P_IPFS, "QmbHVEEepCi7rn7VL7Exxpd2Ci9NNB6ifvqwhsrbRMgQFP")

	_, err := a.ValueForProtocol(P_IP6)
	switch err {
	case ErrProtocolNotFound:
		break
	case nil:
		t.Fatal("expected value lookup to fail")
	default:
		t.Fatalf("expected ErrProtocolNotFound but got: %s", err)
	}

	a = newMultiaddr(t, "/ip4/0.0.0.0") // only one addr
	assertValueForProto(t, a, P_IP4, "0.0.0.0")

	a = newMultiaddr(t, "/ip4/0.0.0.0/ip4/0.0.0.0/ip4/0.0.0.0") // same sub-addr
	assertValueForProto(t, a, P_IP4, "0.0.0.0")

	a = newMultiaddr(t, "/ip4/0.0.0.0/udp/12345/utp") // ending in a no-value one.
	assertValueForProto(t, a, P_IP4, "0.0.0.0")
	assertValueForProto(t, a, P_UDP, "12345")
	assertValueForProto(t, a, P_UTP, "")

	a = newMultiaddr(t, "/ip4/0.0.0.0/unix/a/b/c/d") // ending in a path one.
	assertValueForProto(t, a, P_IP4, "0.0.0.0")
	assertValueForProto(t, a, P_UNIX, "a/b/c/d")
}

func TestFuzzBytes(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	// Bump up these numbers if you want to stress this
	buf := make([]byte, 256)
	for i := 0; i < 2000; i++ {
		l := rand.Intn(len(buf))
		rand.Read(buf[:l])

		// just checking that it doesnt panic
		ma, err := NewMultiaddrBytes(buf[:l])
		if err == nil {
			// for any valid multiaddrs, make sure these calls don't panic
			_ = ma.String()
			ma.Protocols()
		}
	}
}

func randMaddrString() string {
	good_corpus := []string{"tcp", "ip", "udp", "ipfs", "0.0.0.0", "127.0.0.1", "12345", "QmbHVEEepCi7rn7VL7Exxpd2Ci9NNB6ifvqwhsrbRMgQFP"}

	size := rand.Intn(256)
	parts := make([]string, 0, size)
	for i := 0; i < size; i++ {
		switch rand.Intn(5) {
		case 0, 1, 2:
			parts = append(parts, good_corpus[rand.Intn(len(good_corpus))])
		default:
			badbuf := make([]byte, rand.Intn(256))
			rand.Read(badbuf)
			parts = append(parts, string(badbuf))
		}
	}

	return "/" + strings.Join(parts, "/")
}

func TestFuzzString(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	// Bump up these numbers if you want to stress this
	for i := 0; i < 2000; i++ {

		// just checking that it doesnt panic
		ma, err := NewMultiaddr(randMaddrString())
		if err == nil {
			// for any valid multiaddrs, make sure these calls don't panic
			_ = ma.String()
			ma.Protocols()
		}
	}
}
