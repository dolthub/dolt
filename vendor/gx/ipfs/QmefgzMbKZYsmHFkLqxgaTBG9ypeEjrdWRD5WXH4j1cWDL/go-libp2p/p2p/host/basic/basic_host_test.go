package basichost

import (
	"bytes"
	"context"
	"io"
	"sort"
	"testing"
	"time"

	inet "gx/ipfs/QmNa31VPzC561NWwRsJLE7nGYZYuuD2QfpK2b1q9BK54J1/go-libp2p-net"
	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	testutil "gx/ipfs/QmQGX417WoxKxDJeHqouMEmmH4G1RCENNSzkZYHrXy3Xb3/go-libp2p-netutil"
	madns "gx/ipfs/QmS7xUmsTdVNU2t1bPV6o9aXuXfufAjNGYgh2bcN2z9DAs/go-multiaddr-dns"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	protocol "gx/ipfs/QmZNkThpqfVXs9GNbexPrfBbXSLNYeKrE7jwFM2oqHbyqN/go-libp2p-protocol"
	host "gx/ipfs/Qmc1XhrFEiSeBNn3mpfg6gEuYCt5im2gYmNVmncsvmpeAk/go-libp2p-host"
)

func TestHostSimple(t *testing.T) {

	ctx := context.Background()
	h1 := New(testutil.GenSwarmNetwork(t, ctx))
	h2 := New(testutil.GenSwarmNetwork(t, ctx))
	defer h1.Close()
	defer h2.Close()

	h2pi := h2.Peerstore().PeerInfo(h2.ID())
	if err := h1.Connect(ctx, h2pi); err != nil {
		t.Fatal(err)
	}

	piper, pipew := io.Pipe()
	h2.SetStreamHandler(protocol.TestingID, func(s inet.Stream) {
		defer s.Close()
		w := io.MultiWriter(s, pipew)
		io.Copy(w, s) // mirror everything
	})

	s, err := h1.NewStream(ctx, h2pi.ID, protocol.TestingID)
	if err != nil {
		t.Fatal(err)
	}

	// write to the stream
	buf1 := []byte("abcdefghijkl")
	if _, err := s.Write(buf1); err != nil {
		t.Fatal(err)
	}

	// get it from the stream (echoed)
	buf2 := make([]byte, len(buf1))
	if _, err := io.ReadFull(s, buf2); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf1, buf2) {
		t.Fatal("buf1 != buf2 -- %x != %x", buf1, buf2)
	}

	// get it from the pipe (tee)
	buf3 := make([]byte, len(buf1))
	if _, err := io.ReadFull(piper, buf3); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf1, buf3) {
		t.Fatal("buf1 != buf3 -- %x != %x", buf1, buf3)
	}
}

func TestHostAddrsFactory(t *testing.T) {
	maddr := ma.StringCast("/ip4/1.2.3.4/tcp/1234")
	addrsFactory := func(addrs []ma.Multiaddr) []ma.Multiaddr {
		return []ma.Multiaddr{maddr}
	}

	ctx := context.Background()
	h := New(testutil.GenSwarmNetwork(t, ctx), AddrsFactory(addrsFactory))
	defer h.Close()

	addrs := h.Addrs()
	if len(addrs) != 1 {
		t.Fatalf("expected 1 addr, got %d", len(addrs))
	}
	if addrs[0] != maddr {
		t.Fatalf("expected %s, got %s", maddr.String(), addrs[0].String())
	}
}

func getHostPair(ctx context.Context, t *testing.T) (host.Host, host.Host) {
	h1 := New(testutil.GenSwarmNetwork(t, ctx))
	h2 := New(testutil.GenSwarmNetwork(t, ctx))

	h2pi := h2.Peerstore().PeerInfo(h2.ID())
	if err := h1.Connect(ctx, h2pi); err != nil {
		t.Fatal(err)
	}

	return h1, h2
}

func assertWait(t *testing.T, c chan protocol.ID, exp protocol.ID) {
	select {
	case proto := <-c:
		if proto != exp {
			t.Fatal("should have connected on ", exp)
		}
	case <-time.After(time.Second * 5):
		t.Fatal("timeout waiting for stream")
	}
}

func TestHostProtoPreference(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1, h2 := getHostPair(ctx, t)
	defer h1.Close()
	defer h2.Close()

	protoOld := protocol.ID("/testing")
	protoNew := protocol.ID("/testing/1.1.0")
	protoMinor := protocol.ID("/testing/1.2.0")

	connectedOn := make(chan protocol.ID)

	handler := func(s inet.Stream) {
		connectedOn <- s.Protocol()
		s.Close()
	}

	h1.SetStreamHandler(protoOld, handler)

	s, err := h2.NewStream(ctx, h1.ID(), protoMinor, protoNew, protoOld)
	if err != nil {
		t.Fatal(err)
	}

	assertWait(t, connectedOn, protoOld)
	s.Close()

	mfunc, err := host.MultistreamSemverMatcher(protoMinor)
	if err != nil {
		t.Fatal(err)
	}

	h1.SetStreamHandlerMatch(protoMinor, mfunc, handler)

	// remembered preference will be chosen first, even when the other side newly supports it
	s2, err := h2.NewStream(ctx, h1.ID(), protoMinor, protoNew, protoOld)
	if err != nil {
		t.Fatal(err)
	}

	// required to force 'lazy' handshake
	_, err = s2.Write([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	assertWait(t, connectedOn, protoOld)

	s2.Close()

	s3, err := h2.NewStream(ctx, h1.ID(), protoMinor)
	if err != nil {
		t.Fatal(err)
	}

	assertWait(t, connectedOn, protoMinor)
	s3.Close()
}

func TestHostProtoMismatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1, h2 := getHostPair(ctx, t)
	defer h1.Close()
	defer h2.Close()

	h1.SetStreamHandler("/super", func(s inet.Stream) {
		t.Error("shouldnt get here")
		s.Reset()
	})

	_, err := h2.NewStream(ctx, h1.ID(), "/foo", "/bar", "/baz/1.0.0")
	if err == nil {
		t.Fatal("expected new stream to fail")
	}
}

func TestHostProtoPreknowledge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1 := New(testutil.GenSwarmNetwork(t, ctx))
	h2 := New(testutil.GenSwarmNetwork(t, ctx))

	conn := make(chan protocol.ID)
	handler := func(s inet.Stream) {
		conn <- s.Protocol()
		s.Close()
	}

	h1.SetStreamHandler("/super", handler)

	h2pi := h2.Peerstore().PeerInfo(h2.ID())
	if err := h1.Connect(ctx, h2pi); err != nil {
		t.Fatal(err)
	}
	defer h1.Close()
	defer h2.Close()

	// wait for identify handshake to finish completely
	select {
	case <-h1.ids.IdentifyWait(h1.Network().ConnsToPeer(h2.ID())[0]):
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for identify")
	}

	select {
	case <-h2.ids.IdentifyWait(h2.Network().ConnsToPeer(h1.ID())[0]):
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for identify")
	}

	h1.SetStreamHandler("/foo", handler)

	s, err := h2.NewStream(ctx, h1.ID(), "/foo", "/bar", "/super")
	if err != nil {
		t.Fatal(err)
	}

	select {
	case p := <-conn:
		t.Fatal("shouldnt have gotten connection yet, we should have a lazy stream: ", p)
	case <-time.After(time.Millisecond * 50):
	}

	_, err = s.Read(nil)
	if err != nil {
		t.Fatal(err)
	}

	assertWait(t, conn, "/super")

	s.Close()
}

func TestNewDialOld(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1, h2 := getHostPair(ctx, t)
	defer h1.Close()
	defer h2.Close()

	connectedOn := make(chan protocol.ID)
	h1.SetStreamHandler("/testing", func(s inet.Stream) {
		connectedOn <- s.Protocol()
		s.Close()
	})

	s, err := h2.NewStream(ctx, h1.ID(), "/testing/1.0.0", "/testing")
	if err != nil {
		t.Fatal(err)
	}

	assertWait(t, connectedOn, "/testing")

	if s.Protocol() != "/testing" {
		t.Fatal("shoould have gotten /testing")
	}

	s.Close()
}

func TestProtoDowngrade(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1, h2 := getHostPair(ctx, t)
	defer h1.Close()
	defer h2.Close()

	connectedOn := make(chan protocol.ID)
	h1.SetStreamHandler("/testing/1.0.0", func(s inet.Stream) {
		connectedOn <- s.Protocol()
		s.Close()
	})

	s, err := h2.NewStream(ctx, h1.ID(), "/testing/1.0.0", "/testing")
	if err != nil {
		t.Fatal(err)
	}

	assertWait(t, connectedOn, "/testing/1.0.0")

	if s.Protocol() != "/testing/1.0.0" {
		t.Fatal("shoould have gotten /testing")
	}
	s.Close()

	h1.Network().ConnsToPeer(h2.ID())[0].Close()

	time.Sleep(time.Millisecond * 50) // allow notifications to propogate
	h1.RemoveStreamHandler("/testing/1.0.0")
	h1.SetStreamHandler("/testing", func(s inet.Stream) {
		connectedOn <- s.Protocol()
		s.Close()
	})

	h2pi := h2.Peerstore().PeerInfo(h2.ID())
	if err := h1.Connect(ctx, h2pi); err != nil {
		t.Fatal(err)
	}

	s2, err := h2.NewStream(ctx, h1.ID(), "/testing/1.0.0", "/testing")
	if err != nil {
		t.Fatal(err)
	}

	_, err = s2.Write(nil)
	if err != nil {
		t.Fatal(err)
	}

	assertWait(t, connectedOn, "/testing")

	if s2.Protocol() != "/testing" {
		t.Fatal("shoould have gotten /testing")
	}
	s2.Close()

}

func TestAddrResolution(t *testing.T) {
	ctx := context.Background()

	p1, err := testutil.RandPeerID()
	if err != nil {
		t.Error(err)
	}
	p2, err := testutil.RandPeerID()
	if err != nil {
		t.Error(err)
	}
	addr1 := ma.StringCast("/dnsaddr/example.com")
	addr2 := ma.StringCast("/ip4/192.0.2.1/tcp/123")
	p2paddr1 := ma.StringCast("/dnsaddr/example.com/ipfs/" + p1.Pretty())
	p2paddr2 := ma.StringCast("/ip4/192.0.2.1/tcp/123/ipfs/" + p1.Pretty())
	p2paddr3 := ma.StringCast("/ip4/192.0.2.1/tcp/123/ipfs/" + p2.Pretty())

	backend := &madns.MockBackend{
		TXT: map[string][]string{"_dnsaddr.example.com": []string{
			"dnsaddr=" + p2paddr2.String(), "dnsaddr=" + p2paddr3.String(),
		}},
	}
	resolver := &madns.Resolver{Backend: backend}

	h := New(testutil.GenSwarmNetwork(t, ctx), resolver)
	defer h.Close()

	pi, err := pstore.InfoFromP2pAddr(p2paddr1)
	if err != nil {
		t.Error(err)
	}

	tctx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
	defer cancel()
	_ = h.Connect(tctx, *pi)

	addrs := h.Peerstore().Addrs(pi.ID)
	sort.Sort(sortedMultiaddrs(addrs))

	if len(addrs) != 2 || !addrs[0].Equal(addr1) || !addrs[1].Equal(addr2) {
		t.Fatalf("expected [%s %s], got %+v", addr1, addr2, addrs)
	}
}

type sortedMultiaddrs []ma.Multiaddr

func (sma sortedMultiaddrs) Len() int      { return len(sma) }
func (sma sortedMultiaddrs) Swap(i, j int) { sma[i], sma[j] = sma[j], sma[i] }
func (sma sortedMultiaddrs) Less(i, j int) bool {
	return bytes.Compare(sma[i].Bytes(), sma[j].Bytes()) == 1
}
