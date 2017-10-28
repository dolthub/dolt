package swarm

import (
	"context"
	"fmt"
	"testing"
	"time"

	inet "gx/ipfs/QmNa31VPzC561NWwRsJLE7nGYZYuuD2QfpK2b1q9BK54J1/go-libp2p-net"
	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	tu "gx/ipfs/QmWRCn8vruNAzHx8i6SAXinuheRitKEGu8c7m26stKvsYx/go-testutil"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
)

func GenSwarmNetwork(t *testing.T, ctx context.Context) *Network {
	p := tu.RandPeerNetParamsOrFatal(t)
	ps := pstore.NewPeerstore()
	ps.AddPubKey(p.ID, p.PubKey)
	ps.AddPrivKey(p.ID, p.PrivKey)
	n, err := NewNetwork(ctx, []ma.Multiaddr{p.Addr}, p.ID, ps, nil)
	if err != nil {
		t.Fatal(err)
	}
	ps.AddAddrs(p.ID, n.ListenAddresses(), pstore.PermanentAddrTTL)
	return n
}

func DivulgeAddresses(a, b inet.Network) {
	id := a.LocalPeer()
	addrs := a.Peerstore().Addrs(id)
	b.Peerstore().AddAddrs(id, addrs, pstore.PermanentAddrTTL)
}

// TestConnectednessCorrect starts a few networks, connects a few
// and tests Connectedness value is correct.
func TestConnectednessCorrect(t *testing.T) {

	ctx := context.Background()

	nets := make([]inet.Network, 4)
	for i := 0; i < 4; i++ {
		nets[i] = GenSwarmNetwork(t, ctx)
	}

	// connect 0-1, 0-2, 0-3, 1-2, 2-3

	dial := func(a, b inet.Network) {
		DivulgeAddresses(b, a)
		if _, err := a.DialPeer(ctx, b.LocalPeer()); err != nil {
			t.Fatalf("Failed to dial: %s", err)
		}
	}

	dial(nets[0], nets[1])
	dial(nets[0], nets[3])
	dial(nets[1], nets[2])
	dial(nets[3], nets[2])

	// The notifications for new connections get sent out asynchronously.
	// There is the potential for a race condition here, so we sleep to ensure
	// that they have been received.
	time.Sleep(time.Millisecond * 100)

	// test those connected show up correctly

	// test connected
	expectConnectedness(t, nets[0], nets[1], inet.Connected)
	expectConnectedness(t, nets[0], nets[3], inet.Connected)
	expectConnectedness(t, nets[1], nets[2], inet.Connected)
	expectConnectedness(t, nets[3], nets[2], inet.Connected)

	// test not connected
	expectConnectedness(t, nets[0], nets[2], inet.NotConnected)
	expectConnectedness(t, nets[1], nets[3], inet.NotConnected)

	if len(nets[0].Peers()) != 2 {
		t.Fatal("expected net 0 to have two peers")
	}

	if len(nets[2].Conns()) != 2 {
		t.Fatal("expected net 2 to have two conns")
	}

	if len(nets[1].ConnsToPeer(nets[3].LocalPeer())) != 0 {
		t.Fatal("net 1 should have no connections to net 3")
	}

	if err := nets[2].ClosePeer(nets[1].LocalPeer()); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 50)

	expectConnectedness(t, nets[2], nets[1], inet.NotConnected)

	for _, n := range nets {
		n.Close()
	}

	for _, n := range nets {
		<-n.Process().Closed()
	}
}

func expectConnectedness(t *testing.T, a, b inet.Network, expected inet.Connectedness) {
	es := "%s is connected to %s, but Connectedness incorrect. %s %s %s"
	atob := a.Connectedness(b.LocalPeer())
	btoa := b.Connectedness(a.LocalPeer())
	if atob != expected {
		t.Errorf(es, a, b, printConns(a), printConns(b), atob)
	}

	// test symmetric case
	if btoa != expected {
		t.Errorf(es, b, a, printConns(b), printConns(a), btoa)
	}
}

func printConns(n inet.Network) string {
	s := fmt.Sprintf("Connections in %s:\n", n)
	for _, c := range n.Conns() {
		s = s + fmt.Sprintf("- %s\n", c)
	}
	return s
}

func TestNetworkOpenStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nets := make([]inet.Network, 4)
	for i := 0; i < 4; i++ {
		nets[i] = GenSwarmNetwork(t, ctx)
	}

	dial := func(a, b inet.Network) {
		DivulgeAddresses(b, a)
		if _, err := a.DialPeer(ctx, b.LocalPeer()); err != nil {
			t.Fatalf("Failed to dial: %s", err)
		}
	}

	dial(nets[0], nets[1])
	dial(nets[0], nets[3])
	dial(nets[1], nets[2])

	done := make(chan bool)
	nets[1].SetStreamHandler(func(s inet.Stream) {
		defer close(done)
		defer s.Close()

		buf := make([]byte, 10)
		_, err := s.Read(buf)
		if err != nil {
			t.Error(err)
			return
		}
		if string(buf) != "hello ipfs" {
			t.Error("got wrong message")
		}
	})

	s, err := nets[0].NewStream(ctx, nets[1].LocalPeer())
	if err != nil {
		t.Fatal(err)
	}

	streams, err := nets[0].ConnsToPeer(nets[1].LocalPeer())[0].GetStreams()
	if err != nil {
		t.Fatal(err)
	}

	if len(streams) != 1 {
		t.Fatal("should only have one stream there")
	}

	_, err = s.Write([]byte("hello ipfs"))
	if err != nil {
		t.Fatal(err)
	}

	err = s.Close()
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(time.Millisecond * 100):
		t.Fatal("timed out waiting on stream")
	}

	_, err = nets[1].NewStream(ctx, nets[3].LocalPeer())
	if err == nil {
		t.Fatal("expected stream open 1->3 to fail")
	}
}
