package corehttp

import (
	"context"
	"testing"
	"time"

	core "github.com/ipfs/go-ipfs/core"

	testutil "gx/ipfs/QmYdcTdkuCvFXLj2uejJF5aY3HWhtd8JLT4BjPxF9BNPYf/go-libp2p-netutil"
	bhost "gx/ipfs/QmZyngpQxUGyx1T2bzEcst6YzERkvVwDzBMbsSQF4f1smE/go-libp2p/p2p/host/basic"
	inet "gx/ipfs/QmahYsGWry85Y7WUe2SX5G4JkH2zifEQAUtJVLZ24aC9DF/go-libp2p-net"
)

// This test is based on go-libp2p/p2p/net/swarm.TestConnectednessCorrect
// It builds 4 nodes and connects them, one being the sole center.
// Then it checks that the center reports the correct number of peers.
func TestPeersTotal(t *testing.T) {
	ctx := context.Background()

	hosts := make([]*bhost.BasicHost, 4)
	for i := 0; i < 4; i++ {
		hosts[i] = bhost.New(testutil.GenSwarmNetwork(t, ctx))
	}

	dial := func(a, b inet.Network) {
		testutil.DivulgeAddresses(b, a)
		if _, err := a.DialPeer(ctx, b.LocalPeer()); err != nil {
			t.Fatalf("Failed to dial: %s", err)
		}
	}

	dial(hosts[0].Network(), hosts[1].Network())
	dial(hosts[0].Network(), hosts[2].Network())
	dial(hosts[0].Network(), hosts[3].Network())

	// there's something wrong with dial, i think. it's not finishing
	// completely. there must be some async stuff.
	<-time.After(100 * time.Millisecond)

	node := &core.IpfsNode{PeerHost: hosts[0]}
	collector := IpfsNodeCollector{Node: node}
	actual := collector.PeersTotalValues()
	if len(actual) != 1 {
		t.Fatalf("expected 1 peers transport, got %d", len(actual))
	}
	if actual["/ip4/tcp"] != float64(3) {
		t.Fatalf("expected 3 peers, got %s", actual["/ip4/tcp"])
	}
}
